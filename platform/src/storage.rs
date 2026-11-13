use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{ErrorKind, Write},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};
use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cluster::{Cluster, Task, TaskStatus, TaskView};

const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_STORAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct Database(Arc<Mutex<Connection>>);

#[derive(Clone)]
pub struct SecretCipher([u8; 32]);

impl SecretCipher {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(key) => return Self::from_bytes(&key),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut key = [0; 32];
        SystemRandom::new()
            .fill(&mut key)
            .map_err(|_| anyhow::anyhow!("could not generate task secret key"))?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(&key)?;
                file.sync_all()?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return Self::from_bytes(&std::fs::read(path)?);
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Self(key))
    }

    fn from_bytes(value: &[u8]) -> Result<Self> {
        Ok(Self(
            value
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid task secret key"))?,
        ))
    }

    pub fn encrypt(&self, task: Uuid, secrets: &HashMap<String, String>) -> Result<Vec<u8>> {
        if secrets.is_empty() {
            return Ok(Vec::new());
        }
        let mut nonce = [0; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("could not generate task secret nonce"))?;
        let mut encrypted = serde_json::to_vec(secrets)?;
        self.key()?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(task.as_bytes()),
                &mut encrypted,
            )
            .map_err(|_| anyhow::anyhow!("could not encrypt task secrets"))?;
        let mut result = nonce.to_vec();
        result.extend(encrypted);
        Ok(result)
    }

    pub fn decrypt(&self, task: Uuid, encrypted: &[u8]) -> Result<HashMap<String, String>> {
        if encrypted.is_empty() {
            return Ok(HashMap::new());
        }
        if encrypted.len() < 12 {
            anyhow::bail!("invalid encrypted task secrets");
        }
        let (nonce, encrypted) = encrypted.split_at(12);
        let mut plaintext = encrypted.to_vec();
        let plaintext = self
            .key()?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce.try_into().unwrap()),
                Aad::from(task.as_bytes()),
                &mut plaintext,
            )
            .map_err(|_| anyhow::anyhow!("could not decrypt task secrets"))?;
        Ok(serde_json::from_slice(plaintext)?)
    }

    fn key(&self) -> Result<LessSafeKey> {
        Ok(LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &self.0)
                .map_err(|_| anyhow::anyhow!("invalid task secret key"))?,
        ))
    }
}

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
        if table_exists(&connection, "executions")? && !table_exists(&connection, "tasks")? {
            connection.execute_batch(
                "ALTER TABLE executions RENAME TO tasks;
                 DROP INDEX IF EXISTS executions_created;
                 DROP INDEX IF EXISTS executions_finished;
                 DROP INDEX IF EXISTS executions_status;",
            )?;
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
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
                metrics BLOB,
                variables BLOB NOT NULL DEFAULT X'7B7D',
                secrets BLOB,
                size_bytes INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS tasks_created ON tasks(created_at_ms DESC);
            CREATE INDEX IF NOT EXISTS tasks_finished ON tasks(finished_at_ms);
            CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);",
        )?;
        if !column_exists(&connection, "tasks", "variables")? {
            connection.execute(
                "ALTER TABLE tasks ADD COLUMN variables BLOB NOT NULL DEFAULT X'7B7D'",
                [],
            )?;
        }
        if !column_exists(&connection, "tasks", "secrets")? {
            connection.execute("ALTER TABLE tasks ADD COLUMN secrets BLOB", [])?;
        }
        if column_exists(&connection, "tasks", "environment")? {
            connection.execute("ALTER TABLE tasks DROP COLUMN environment", [])?;
        }
        if !column_exists(&connection, "tasks", "owner")? {
            connection.execute("ALTER TABLE tasks ADD COLUMN owner TEXT", [])?;
        }
        connection.execute(
            "CREATE INDEX IF NOT EXISTS tasks_owner ON tasks(owner, created_at_ms DESC)",
            [],
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

    pub fn load_active_tasks(&self) -> Result<Vec<Task>> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE tasks SET status = 'queued', node = NULL, started_at_ms = NULL
             WHERE status IN ('queued', 'assigned', 'running')",
            [],
        )?;
        let mut statement = connection.prepare(
            "SELECT id, pool, node, status, payload, result, error, created_at_ms,
                    started_at_ms, finished_at_ms, metrics, variables, owner
             FROM tasks WHERE status = 'queued' ORDER BY created_at_ms",
        )?;
        let rows = statement.query_map([], task_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn insert_task(&self, task: &Task, secrets: &[u8]) -> Result<bool> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        upsert_task(&transaction, task)?;
        if !secrets.is_empty() {
            transaction.execute(
                "UPDATE tasks SET secrets = ?2, size_bytes = size_bytes + length(?2)
                 WHERE id = ?1",
                params![task.id.to_string(), secrets],
            )?;
        }
        prune(&transaction, task.created_at_ms, Some(task.id))?;
        let limit = setting(&transaction, "max_storage_bytes")?;
        if storage_used(&transaction)? > limit {
            transaction.execute("DELETE FROM tasks WHERE id = ?1", [task.id.to_string()])?;
            transaction.commit()?;
            return Ok(false);
        }
        transaction.commit()?;
        Ok(true)
    }

    pub fn save_task(&self, task: &Task) -> Result<()> {
        let connection = self.connection()?;
        upsert_task(&connection, task)
    }

    pub fn save_tasks(&self, tasks: &[Task]) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for task in tasks {
            upsert_task(&transaction, task)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_task(&self, task: &Task) -> Result<Task> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        upsert_task(&transaction, task)?;
        transaction.execute(
            "UPDATE tasks SET size_bytes = size_bytes - COALESCE(length(secrets), 0),
             secrets = NULL WHERE id = ?1",
            [task.id.to_string()],
        )?;
        prune(
            &transaction,
            task.finished_at_ms.unwrap_or(task.created_at_ms),
            Some(task.id),
        )?;
        let mut saved = task.clone();
        if storage_used(&transaction)? > setting(&transaction, "max_storage_bytes")? {
            saved.status = TaskStatus::Failed;
            saved.result = None;
            saved.error = Some("controller task storage is full".into());
            upsert_task(&transaction, &saved)?;
        }
        transaction.commit()?;
        Ok(saved)
    }

    pub fn delete_task(&self, id: Uuid) -> Result<()> {
        self.connection()?
            .execute("DELETE FROM tasks WHERE id = ?1", [id.to_string()])?;
        Ok(())
    }

    /// Newest tasks first, optionally only those on the given pools or owner.
    pub fn list_tasks(
        &self,
        limit: Option<usize>,
        pools: Option<&[String]>,
        owner: Option<&str>,
    ) -> Result<Vec<TaskView>> {
        let connection = self.connection()?;
        let mut sql = String::from(
            "SELECT id, pool, node, status, error, created_at_ms, started_at_ms,
                    finished_at_ms, metrics, owner
             FROM tasks WHERE 1 = 1",
        );
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(pools) = pools {
            if pools.is_empty() {
                return Ok(Vec::new());
            }
            sql.push_str(" AND pool IN (");
            sql.push_str(&vec!["?"; pools.len()].join(", "));
            sql.push(')');
            values.extend(pools.iter().cloned().map(Into::into));
        }
        if let Some(owner) = owner {
            sql.push_str(" AND owner = ?");
            values.push(owner.to_owned().into());
        }
        sql.push_str(" ORDER BY created_at_ms DESC");
        if let Some(limit) = limit {
            sql.push_str(" LIMIT ?");
            values.push((limit as i64).into());
        }
        let mut statement = connection.prepare(&sql)?;
        statement
            .query_map(rusqlite::params_from_iter(values), task_view_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Record the pool's owner on older tasks that were stored without one.
    pub fn backfill_task_owner(&self, pool: &str, owner: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE tasks SET owner = ?2 WHERE pool = ?1 AND owner IS NULL",
            params![pool, owner],
        )?;
        Ok(())
    }

    pub fn task(&self, id: Uuid) -> Result<Option<Task>> {
        self.connection()?
            .query_row(
                "SELECT id, pool, node, status, payload, result, error, created_at_ms,
                        started_at_ms, finished_at_ms, metrics, variables, owner
                 FROM tasks WHERE id = ?1",
                [id.to_string()],
                task_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn task_secrets(&self, id: Uuid) -> Result<Option<Vec<u8>>> {
        self.connection()?
            .query_row(
                "SELECT COALESCE(secrets, X'') FROM tasks WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
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

fn upsert_task(connection: &Connection, task: &Task) -> Result<()> {
    let payload = serde_json::to_vec(&task.payload)?;
    let variables = serde_json::to_vec(&task.variables)?;
    let result = task.result.as_ref().map(serde_json::to_vec).transpose()?;
    let metrics = task.metrics.as_ref().map(serde_json::to_vec).transpose()?;
    let size = payload.len()
        + variables.len()
        + result.as_ref().map_or(0, Vec::len)
        + metrics.as_ref().map_or(0, Vec::len)
        + task.error.as_ref().map_or(0, String::len);
    connection.execute(
        "INSERT INTO tasks(
            id, pool, node, status, payload, result, error, created_at_ms,
            started_at_ms, finished_at_ms, metrics, variables, size_bytes, owner
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(id) DO UPDATE SET
            pool = excluded.pool, owner = excluded.owner, node = excluded.node,
            status = excluded.status,
            payload = excluded.payload, result = excluded.result, error = excluded.error,
            started_at_ms = excluded.started_at_ms, finished_at_ms = excluded.finished_at_ms,
            metrics = excluded.metrics,
            variables = excluded.variables,
            size_bytes = excluded.size_bytes + COALESCE(length(tasks.secrets), 0)",
        params![
            task.id.to_string(),
            task.pool,
            task.node.map(|id| id.to_string()),
            status_name(&task.status),
            payload,
            result,
            task.error,
            task.created_at_ms,
            task.started_at_ms,
            task.finished_at_ms,
            metrics,
            variables,
            size as u64,
            task.owner,
        ],
    )?;
    Ok(())
}

fn task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
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
        metrics: optional_json(row.get(10)?)?,
        variables: json(row.get::<_, Vec<u8>>(11)?)?,
        owner: row.get(12)?,
    })
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn column_exists(connection: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns.iter().any(|name| name == column))
}

fn task_view_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskView> {
    Ok(TaskView {
        id: uuid(row.get::<_, String>(0)?)?,
        pool: row.get(1)?,
        node: optional_uuid(row.get(2)?)?,
        status: status(&row.get::<_, String>(3)?)?,
        error: row.get(4)?,
        created_at_ms: row.get(5)?,
        started_at_ms: row.get(6)?,
        finished_at_ms: row.get(7)?,
        metrics: optional_json(row.get(8)?)?,
        owner: row.get(9)?,
    })
}

fn prune(connection: &Connection, now_ms: u64, protected: Option<Uuid>) -> Result<()> {
    let retention = setting(connection, "retention_ms")?;
    connection.execute(
        "DELETE FROM tasks
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
        "SELECT id, size_bytes FROM tasks
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
        connection.execute("DELETE FROM tasks WHERE id = ?1", [id])?;
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
        "SELECT COALESCE(SUM(size_bytes), 0) FROM tasks",
        [],
        |row| row.get(0),
    )?)
}

fn status_name(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Assigned => "assigned",
        TaskStatus::Running => "running",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::TimedOut => "timed_out",
    }
}

fn status(value: &str) -> rusqlite::Result<TaskStatus> {
    match value {
        "queued" => Ok(TaskStatus::Queued),
        "assigned" => Ok(TaskStatus::Assigned),
        "running" => Ok(TaskStatus::Running),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "timed_out" => Ok(TaskStatus::TimedOut),
        _ => Err(conversion("invalid task status")),
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

    fn task(created_at_ms: u64) -> Task {
        Task {
            id: Uuid::new_v4(),
            pool: "python".into(),
            owner: None,
            node: None,
            status: TaskStatus::Queued,
            payload: json!({"encoding": "json", "data": "input"}),
            variables: HashMap::new(),
            result: None,
            error: None,
            created_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
            metrics: None,
        }
    }

    #[test]
    fn filters_tasks_by_pool_and_owner() {
        let (database, path) = database();
        let mut other = task(2);
        other.pool = "chrome".into();
        database.insert_task(&task(1), &[]).unwrap();
        database.insert_task(&other, &[]).unwrap();
        let pools = ["chrome".to_owned()];
        let listed = database.list_tasks(None, Some(&pools), None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].pool, "chrome");
        assert!(
            database
                .list_tasks(None, Some(&[]), None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(database.list_tasks(Some(1), None, None).unwrap().len(), 1);
        database
            .backfill_task_owner("python", "plugin:python")
            .unwrap();
        let owned = database
            .list_tasks(None, None, Some("plugin:python"))
            .unwrap();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].owner.as_deref(), Some("plugin:python"));
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn migrates_legacy_executions_table() {
        let path = std::env::temp_dir().join(format!("scraperack-{}.db", Uuid::new_v4()));
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE executions (
                        id TEXT PRIMARY KEY, pool TEXT NOT NULL, node TEXT, status TEXT NOT NULL,
                        payload BLOB NOT NULL, result BLOB, error TEXT,
                        created_at_ms INTEGER NOT NULL, started_at_ms INTEGER,
                        finished_at_ms INTEGER, environment TEXT, metrics BLOB,
                        size_bytes INTEGER NOT NULL
                    );
                    CREATE INDEX executions_created ON executions(created_at_ms DESC);
                    INSERT INTO executions(id, pool, status, payload, created_at_ms,
                        environment, size_bytes)
                    VALUES ('6f1c1b9e-58a4-4f2b-9d36-3f1f5f0b7f10', 'python', 'queued', X'7B7D', 1,
                        '6f1c1b9e-58a4-4f2b-9d36-3f1f5f0b7f11', 2);",
                )
                .unwrap();
        }
        let database = Database::open(&path).unwrap();
        let tasks = database.load_active_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].pool, "python");
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    fn database() -> (Database, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("scraperack-{}.db", Uuid::new_v4()));
        (Database::open(&path).unwrap(), path)
    }

    #[test]
    fn stores_task_data_outside_metadata_views() {
        let (database, path) = database();
        let mut task = task(1);
        database.insert_task(&task, &[]).unwrap();
        task.status = TaskStatus::Succeeded;
        task.finished_at_ms = Some(2);
        task.result = Some(json!({"value": {"encoding": "json", "data": "output"}}));
        database.complete_task(&task).unwrap();

        let view =
            serde_json::to_value(&database.list_tasks(None, None, None).unwrap()[0]).unwrap();
        assert!(view.get("payload").is_none());
        assert!(view.get("result").is_none());
        assert_eq!(database.task(task.id).unwrap().unwrap().result, task.result);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn retention_removes_completed_but_not_active_tasks() {
        let (database, path) = database();
        database
            .update_settings(&StorageSettings {
                retention_hours: 1,
                max_storage_bytes: DEFAULT_STORAGE_BYTES,
            })
            .unwrap();
        let active = task(1);
        database.insert_task(&active, &[]).unwrap();
        let mut completed = task(1);
        database.insert_task(&completed, &[]).unwrap();
        completed.status = TaskStatus::Succeeded;
        completed.finished_at_ms = Some(2);
        database.complete_task(&completed).unwrap();

        database.prune(3_600_003).unwrap();
        assert!(database.task(active.id).unwrap().is_some());
        assert!(database.task(completed.id).unwrap().is_none());
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn encrypts_queued_secrets_and_clears_them_on_completion() {
        let (database, path) = database();
        let cipher = SecretCipher::from_bytes(&[7; 32]).unwrap();
        let mut task = task(1);
        let secrets = HashMap::from([("TOKEN".into(), "do-not-store-plainly".into())]);
        let encrypted = cipher.encrypt(task.id, &secrets).unwrap();
        assert!(
            !encrypted
                .windows("do-not-store-plainly".len())
                .any(|value| value == b"do-not-store-plainly")
        );
        database.insert_task(&task, &encrypted).unwrap();
        let stored = database.task_secrets(task.id).unwrap().unwrap();
        assert_eq!(cipher.decrypt(task.id, &stored).unwrap(), secrets);

        task.status = TaskStatus::Succeeded;
        task.finished_at_ms = Some(2);
        database.complete_task(&task).unwrap();
        assert!(database.task_secrets(task.id).unwrap().unwrap().is_empty());
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
