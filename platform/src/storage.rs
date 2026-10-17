use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cluster::{Cluster, Execution, ExecutionStatus, ExecutionView};

const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_STORAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct Database(Arc<Mutex<Connection>>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    pub retention_hours: u64,
    pub max_storage_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct StorageView {
    #[serde(flatten)]
    pub settings: StorageSettings,
    pub used_storage_bytes: u64,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let new = !path.exists();
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        if new {
            connection.execute_batch("PRAGMA auto_vacuum=INCREMENTAL; VACUUM;")?;
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                pool TEXT NOT NULL,
                node TEXT,
                status TEXT NOT NULL,
                payload BLOB NOT NULL,
                result BLOB,
                error TEXT,
                created_at_ms INTEGER NOT NULL,
                started_at_ms INTEGER,
                finished_at_ms INTEGER,
                environment TEXT,
                metrics BLOB,
                size_bytes INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS executions_created ON executions(created_at_ms DESC);
            CREATE INDEX IF NOT EXISTS executions_finished ON executions(finished_at_ms);
            CREATE INDEX IF NOT EXISTS executions_status ON executions(status);",
        )?;
        let database = Self(Arc::new(Mutex::new(connection)));
        database.set_default("retention_ms", DEFAULT_RETENTION_MS)?;
        database.set_default("max_storage_bytes", DEFAULT_STORAGE_BYTES)?;
        Ok(database)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock poisoned"))
    }

    fn set_default(&self, key: &str, value: u64) -> Result<()> {
        self.connection()?.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES (?1, ?2)",
            params![key, value.to_string()],
        )?;
        Ok(())
    }

    pub fn controller_id(&self, legacy: Option<Uuid>) -> Result<Uuid> {
        let connection = self.connection()?;
        if let Some(value) = connection
            .query_row(
                "SELECT value FROM metadata WHERE key = 'controller_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Ok(Uuid::parse_str(&value)?);
        }
        let id = legacy.unwrap_or_else(Uuid::new_v4);
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES ('controller_id', ?1)",
            [id.to_string()],
        )?;
        Ok(id)
    }

    pub fn load_cluster(&self) -> Result<Option<Cluster>> {
        self.connection()?
            .query_row(
                "SELECT value FROM metadata WHERE key = 'cluster'",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(|value| serde_json::from_slice(&value).context("decode controller state"))
            .transpose()
    }

    pub fn save_cluster(&self, cluster: &Cluster) -> Result<()> {
        self.connection()?.execute(
            "INSERT INTO metadata(key, value) VALUES ('cluster', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [serde_json::to_vec(cluster)?],
        )?;
        Ok(())
    }

    pub fn load_active_executions(&self) -> Result<Vec<Execution>> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE executions SET status = 'queued', node = NULL, started_at_ms = NULL
             WHERE status IN ('queued', 'assigned', 'running')",
            [],
        )?;
        let mut statement = connection.prepare(
            "SELECT id, pool, node, status, payload, result, error, created_at_ms,
                    started_at_ms, finished_at_ms, environment, metrics
             FROM executions WHERE status = 'queued' ORDER BY created_at_ms",
        )?;
        let rows = statement.query_map([], execution_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn insert_execution(&self, execution: &Execution) -> Result<bool> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        upsert_execution(&transaction, execution)?;
        prune(&transaction, execution.created_at_ms, Some(execution.id))?;
        let limit = setting(&transaction, "max_storage_bytes")?;
        if storage_used(&transaction)? > limit {
            transaction.execute(
                "DELETE FROM executions WHERE id = ?1",
                [execution.id.to_string()],
            )?;
            transaction.commit()?;
            return Ok(false);
        }
        transaction.commit()?;
        Ok(true)
    }

    pub fn save_execution(&self, execution: &Execution) -> Result<()> {
        let connection = self.connection()?;
        upsert_execution(&connection, execution)
    }

    pub fn save_executions(&self, executions: &[Execution]) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for execution in executions {
            upsert_execution(&transaction, execution)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_execution(&self, execution: &Execution) -> Result<Execution> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        upsert_execution(&transaction, execution)?;
        prune(
            &transaction,
            execution.finished_at_ms.unwrap_or(execution.created_at_ms),
            Some(execution.id),
        )?;
        let mut saved = execution.clone();
        if storage_used(&transaction)? > setting(&transaction, "max_storage_bytes")? {
            saved.status = ExecutionStatus::Failed;
            saved.result = None;
            saved.error = Some("controller execution storage is full".into());
            upsert_execution(&transaction, &saved)?;
        }
        transaction.commit()?;
        Ok(saved)
    }

    pub fn delete_execution(&self, id: Uuid) -> Result<()> {
        self.connection()?
            .execute("DELETE FROM executions WHERE id = ?1", [id.to_string()])?;
        Ok(())
    }

    pub fn list_executions(&self, limit: Option<usize>) -> Result<Vec<ExecutionView>> {
        let connection = self.connection()?;
        if let Some(limit) = limit {
            let mut statement = connection.prepare(
                "SELECT id, pool, node, status, error, created_at_ms, started_at_ms,
                        finished_at_ms, environment, metrics
                 FROM executions ORDER BY created_at_ms DESC LIMIT ?1",
            )?;
            statement
                .query_map([limit as u64], execution_view_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        } else {
            let mut statement = connection.prepare(
                "SELECT id, pool, node, status, error, created_at_ms, started_at_ms,
                        finished_at_ms, environment, metrics
                 FROM executions ORDER BY created_at_ms DESC",
            )?;
            statement
                .query_map([], execution_view_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        }
    }

    pub fn execution(&self, id: Uuid) -> Result<Option<Execution>> {
        self.connection()?
            .query_row(
                "SELECT id, pool, node, status, payload, result, error, created_at_ms,
                        started_at_ms, finished_at_ms, environment, metrics
                 FROM executions WHERE id = ?1",
                [id.to_string()],
                execution_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn settings(&self) -> Result<StorageView> {
        let connection = self.connection()?;
        Ok(StorageView {
            settings: StorageSettings {
                retention_hours: setting(&connection, "retention_ms")? / 3_600_000,
                max_storage_bytes: setting(&connection, "max_storage_bytes")?,
            },
            used_storage_bytes: storage_used(&connection)?,
        })
    }

    pub fn update_settings(&self, settings: &StorageSettings) -> Result<()> {
        let retention_ms = settings
            .retention_hours
            .checked_mul(3_600_000)
            .context("retention is too large")?;
        let connection = self.connection()?;
        connection.execute(
            "UPDATE metadata SET value = ?1 WHERE key = 'retention_ms'",
            [retention_ms.to_string()],
        )?;
        connection.execute(
            "UPDATE metadata SET value = ?1 WHERE key = 'max_storage_bytes'",
            [settings.max_storage_bytes.to_string()],
        )?;
        Ok(())
    }

    pub fn prune(&self, now_ms: u64) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        prune(&transaction, now_ms, None)?;
        transaction.commit()?;
        connection
            .execute_batch("PRAGMA incremental_vacuum(256); PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }
}

fn upsert_execution(connection: &Connection, execution: &Execution) -> Result<()> {
    let payload = serde_json::to_vec(&execution.payload)?;
    let result = execution
        .result
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?;
    let metrics = execution
        .metrics
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?;
    let size = payload.len()
        + result.as_ref().map_or(0, Vec::len)
        + metrics.as_ref().map_or(0, Vec::len)
        + execution.error.as_ref().map_or(0, String::len);
    connection.execute(
        "INSERT INTO executions(
            id, pool, node, status, payload, result, error, created_at_ms,
            started_at_ms, finished_at_ms, environment, metrics, size_bytes
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
            pool = excluded.pool, node = excluded.node, status = excluded.status,
            payload = excluded.payload, result = excluded.result, error = excluded.error,
            started_at_ms = excluded.started_at_ms, finished_at_ms = excluded.finished_at_ms,
            environment = excluded.environment, metrics = excluded.metrics,
            size_bytes = excluded.size_bytes",
        params![
            execution.id.to_string(),
            execution.pool,
            execution.node.map(|id| id.to_string()),
            status_name(&execution.status),
            payload,
            result,
            execution.error,
            execution.created_at_ms,
            execution.started_at_ms,
            execution.finished_at_ms,
            execution.environment.map(|id| id.to_string()),
            metrics,
            size as u64,
        ],
    )?;
    Ok(())
}

fn execution_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Execution> {
    Ok(Execution {
        id: uuid(row.get::<_, String>(0)?)?,
        pool: row.get(1)?,
        node: optional_uuid(row.get(2)?)?,
        status: status(&row.get::<_, String>(3)?)?,
        payload: json(row.get::<_, Vec<u8>>(4)?)?,
        result: optional_json(row.get(5)?)?,
        error: row.get(6)?,
        created_at_ms: row.get(7)?,
        started_at_ms: row.get(8)?,
        finished_at_ms: row.get(9)?,
        environment: optional_uuid(row.get(10)?)?,
        metrics: optional_json(row.get(11)?)?,
    })
}

fn execution_view_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionView> {
    Ok(ExecutionView {
        id: uuid(row.get::<_, String>(0)?)?,
        pool: row.get(1)?,
        node: optional_uuid(row.get(2)?)?,
        status: status(&row.get::<_, String>(3)?)?,
        error: row.get(4)?,
        created_at_ms: row.get(5)?,
        started_at_ms: row.get(6)?,
        finished_at_ms: row.get(7)?,
        environment: optional_uuid(row.get(8)?)?,
        metrics: optional_json(row.get(9)?)?,
    })
}

fn prune(connection: &Connection, now_ms: u64, protected: Option<Uuid>) -> Result<()> {
    let retention = setting(connection, "retention_ms")?;
    connection.execute(
        "DELETE FROM executions
         WHERE status IN ('succeeded', 'failed', 'timed_out')
           AND finished_at_ms IS NOT NULL AND finished_at_ms <= ?1",
        [now_ms.saturating_sub(retention)],
    )?;
    let limit = setting(connection, "max_storage_bytes")?;
    let mut used = storage_used(connection)?;
    if used <= limit {
        return Ok(());
    }
    let protected = protected.map(|id| id.to_string());
    let mut statement = connection.prepare(
        "SELECT id, size_bytes FROM executions
         WHERE status IN ('succeeded', 'failed', 'timed_out') AND (?1 IS NULL OR id != ?1)
         ORDER BY finished_at_ms, created_at_ms",
    )?;
    let candidates = statement
        .query_map([protected], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (id, size) in candidates {
        connection.execute("DELETE FROM executions WHERE id = ?1", [id])?;
        used = used.saturating_sub(size);
        if used <= limit {
            break;
        }
    }
    Ok(())
}

fn setting(connection: &Connection, key: &str) -> Result<u64> {
    let value =
        connection.query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| {
            row.get::<_, String>(0)
        })?;
    Ok(value.parse()?)
}

fn storage_used(connection: &Connection) -> Result<u64> {
    Ok(connection.query_row(
        "SELECT COALESCE(SUM(size_bytes), 0) FROM executions",
        [],
        |row| row.get(0),
    )?)
}

fn status_name(status: &ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Queued => "queued",
        ExecutionStatus::Assigned => "assigned",
        ExecutionStatus::Running => "running",
        ExecutionStatus::Succeeded => "succeeded",
        ExecutionStatus::Failed => "failed",
        ExecutionStatus::TimedOut => "timed_out",
    }
}

fn status(value: &str) -> rusqlite::Result<ExecutionStatus> {
    match value {
        "queued" => Ok(ExecutionStatus::Queued),
        "assigned" => Ok(ExecutionStatus::Assigned),
        "running" => Ok(ExecutionStatus::Running),
        "succeeded" => Ok(ExecutionStatus::Succeeded),
        "failed" => Ok(ExecutionStatus::Failed),
        "timed_out" => Ok(ExecutionStatus::TimedOut),
        _ => Err(conversion("invalid execution status")),
    }
}

fn uuid(value: String) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|error| conversion(error.to_string()))
}

fn optional_uuid(value: Option<String>) -> rusqlite::Result<Option<Uuid>> {
    value.map(uuid).transpose()
}

fn json<T: serde::de::DeserializeOwned>(value: Vec<u8>) -> rusqlite::Result<T> {
    serde_json::from_slice(&value).map_err(|error| conversion(error.to_string()))
}

fn optional_json<T: serde::de::DeserializeOwned>(
    value: Option<Vec<u8>>,
) -> rusqlite::Result<Option<T>> {
    value.map(json).transpose()
}

fn conversion(message: impl ToString) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(message.to_string())),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn execution(created_at_ms: u64) -> Execution {
        Execution {
            id: Uuid::new_v4(),
            pool: "python".into(),
            node: None,
            status: ExecutionStatus::Queued,
            payload: json!({"encoding": "json", "data": "input"}),
            result: None,
            error: None,
            created_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
            environment: None,
            metrics: None,
        }
    }

    fn database() -> (Database, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("scraperack-{}.db", Uuid::new_v4()));
        (Database::open(&path).unwrap(), path)
    }

    #[test]
    fn stores_execution_data_outside_metadata_views() {
        let (database, path) = database();
        let mut execution = execution(1);
        database.insert_execution(&execution).unwrap();
        execution.status = ExecutionStatus::Succeeded;
        execution.finished_at_ms = Some(2);
        execution.result = Some(json!({"value": {"encoding": "json", "data": "output"}}));
        database.complete_execution(&execution).unwrap();

        let view = serde_json::to_value(&database.list_executions(None).unwrap()[0]).unwrap();
        assert!(view.get("payload").is_none());
        assert!(view.get("result").is_none());
        assert_eq!(
            database.execution(execution.id).unwrap().unwrap().result,
            execution.result
        );
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn retention_removes_completed_but_not_active_executions() {
        let (database, path) = database();
        database
            .update_settings(&StorageSettings {
                retention_hours: 1,
                max_storage_bytes: DEFAULT_STORAGE_BYTES,
            })
            .unwrap();
        let active = execution(1);
        database.insert_execution(&active).unwrap();
        let mut completed = execution(1);
        database.insert_execution(&completed).unwrap();
        completed.status = ExecutionStatus::Succeeded;
        completed.finished_at_ms = Some(2);
        database.complete_execution(&completed).unwrap();

        database.prune(3_600_003).unwrap();
        assert!(database.execution(active.id).unwrap().is_some());
        assert!(database.execution(completed.id).unwrap().is_none());
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
