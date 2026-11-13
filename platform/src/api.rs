use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    net::IpAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    Extension, Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    sync::{Notify, RwLock},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::cluster::{
    Cluster, Disk, Image, ImageKind, JoinToken, MicroVm, Node, NodeStatus, Pool, PoolDisk, Task,
    TaskMetrics, TaskStatus, TaskView, Work,
};
use crate::plugins::{self, Plugins};
use crate::storage::{Database, SecretCipher, StorageSettings, StorageView};

const JOIN_TOKEN_TTL_MS: u64 = 10 * 60 * 1000;
/// Mount point of the legacy Python environment disk inside the guest.
const LEGACY_ENVIRONMENT_MOUNT: &str = "/tmp/environment";

#[derive(Clone)]
pub struct App {
    pub cluster: Arc<RwLock<Cluster>>,
    database: Database,
    secrets: SecretCipher,
    controller_id: Uuid,
    pub data_dir: PathBuf,
    tasks: Arc<Notify>,
    work: Arc<Notify>,
    pub plugins: Arc<Plugins>,
    imports: Arc<Imports>,
}

/// Identifies requests that arrive on a plugin's private core socket.
#[derive(Clone)]
pub struct Caller(pub String);

impl Caller {
    fn owner(&self) -> String {
        format!("plugin:{}", self.0)
    }
}

fn owner_of(caller: &Option<Extension<Caller>>) -> Option<String> {
    caller.as_ref().map(|Extension(caller)| caller.owner())
}

pub type ApiResult<T> = Result<T, (StatusCode, Json<Value>)>;

pub fn error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message.into() })))
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn secret_hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn same_hash(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .fold(0, |difference, (left, right)| difference | (left ^ right))
            == 0
}

async fn authenticated_node(app: &App, headers: &HeaderMap) -> ApiResult<Uuid> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "node credential required"))?;
    let hash = secret_hash(token);
    app.cluster
        .read()
        .await
        .node_credentials
        .iter()
        .find_map(|(id, expected)| same_hash(expected, &hash).then_some(*id))
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid node credential"))
}

/// Hand built-in Python environments over to the Python plugin.
///
/// Their pools keep running unchanged: each environment artifact becomes a
/// generic disk, the pool names its executor explicitly, and the environment
/// ID and name stay valid as pool aliases. The environment definitions are
/// written into the Python plugin's data directory for it to adopt.
async fn migrate_environments(cluster: &mut Cluster, data_dir: &Path) -> Result<()> {
    let legacy_dir = data_dir.join("environments");
    if cluster.environments.is_empty() {
        if legacy_dir.exists() {
            fs::remove_dir_all(&legacy_dir).await.ok();
        }
        return Ok(());
    }
    let mut adopted = Vec::new();
    for environment in cluster.environments.values() {
        let mut disks = Vec::new();
        if let (Some(key), Some(sha256)) = (&environment.artifact, &environment.sha256) {
            let source = legacy_dir.join(key);
            let target = data_dir.join("disks").join(key);
            if source.exists() && !target.exists() {
                fs::rename(&source, &target).await?;
            }
            cluster.disks.insert(
                key.clone(),
                Disk {
                    key: key.clone(),
                    sha256: sha256.clone(),
                    size: environment.size,
                    owner: Some("python".into()),
                },
            );
            disks.push(PoolDisk {
                key: key.clone(),
                mount: LEGACY_ENVIRONMENT_MOUNT.into(),
            });
        }
        let executor = if disks.is_empty() {
            vec!["/opt/scraperack/executors/python".to_owned()]
        } else {
            vec![
                format!("{LEGACY_ENVIRONMENT_MOUNT}/bin/python"),
                "/opt/scraperack/executors/python".to_owned(),
            ]
        };
        if let Some(pool) = cluster.pools.get_mut(&environment.pool) {
            pool.managed_by = Some("plugin:python".into());
            pool.disks = disks;
            pool.executor = executor;
            pool.aliases = vec![environment.id.to_string(), environment.name.clone()];
            pool.display_name = Some(environment.name.clone());
            pool.environment_artifact = None;
        }
        adopted.push(environment.clone());
    }
    for node in cluster.nodes.values_mut() {
        let cached = std::mem::take(&mut node.environments);
        node.disks.extend(cached);
    }
    let plugin_data = data_dir.join("plugins").join("python").join("data");
    fs::create_dir_all(&plugin_data).await?;
    fs::write(
        plugin_data.join("migrated-environments.json"),
        serde_json::to_vec_pretty(&adopted)?,
    )
    .await?;
    cluster.environments.clear();
    fs::remove_dir_all(&legacy_dir).await.ok();
    tracing::info!(
        count = adopted.len(),
        "migrated Python environments to plugin pools"
    );
    Ok(())
}

pub async fn save(app: &App, cluster: &Cluster) -> ApiResult<()> {
    app.database.save_cluster(cluster).map_err(internal_error)
}

async fn save_task(app: &App, task: &Task) -> ApiResult<()> {
    app.database.save_task(task).map_err(internal_error)
}

pub fn internal_error(cause: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    error(StatusCode::INTERNAL_SERVER_ERROR, cause.to_string())
}

pub async fn serve(
    listen: std::net::SocketAddr,
    data_dir: PathBuf,
    allow_plugin_install: bool,
) -> Result<()> {
    fs::create_dir_all(data_dir.join("images")).await?;
    fs::create_dir_all(data_dir.join("disks")).await?;
    fs::create_dir_all(data_dir.join("plugins")).await?;
    fs::create_dir_all(data_dir.join("working-dirs")).await?;
    let database = Database::open(&data_dir.join("scraperack.db"))?;
    let secrets_key = data_dir.join("task-secrets.key");
    let legacy_secrets_key = data_dir.join("execution-secrets.key");
    if !secrets_key.exists() && legacy_secrets_key.exists() {
        fs::rename(&legacy_secrets_key, &secrets_key).await?;
    }
    let secrets = SecretCipher::load(&secrets_key)?;
    let controller_id_path = data_dir.join("controller-id");
    let legacy_controller_id = match fs::read_to_string(&controller_id_path).await {
        Ok(id) => Some(Uuid::parse_str(id.trim())?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let controller_id = database.controller_id(legacy_controller_id)?;
    let state_path = data_dir.join("state.json");
    let stored_cluster = database.load_cluster()?;
    let fresh = stored_cluster.is_none();
    let mut cluster = match stored_cluster {
        Some(cluster) => cluster,
        None => match fs::read(&state_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Cluster::default(),
            Err(error) => return Err(error.into()),
        },
    };
    migrate_environments(&mut cluster, &data_dir).await?;
    for pool in cluster.pools.values() {
        if let Some(owner) = &pool.managed_by {
            database.backfill_task_owner(&pool.name, owner)?;
        }
    }
    for task in database.load_active_tasks()? {
        cluster.tasks.insert(task.id, task);
    }
    database.save_cluster(&cluster)?;
    if fresh {
        fs::remove_file(state_path).await.ok();
        fs::remove_file(controller_id_path).await.ok();
        fs::remove_dir_all(data_dir.join("executions")).await.ok();
    }
    let plugin_records = cluster.plugins.values().cloned().collect::<Vec<_>>();
    let app = App {
        cluster: Arc::new(RwLock::new(cluster)),
        database,
        secrets,
        controller_id,
        plugins: Arc::new(Plugins::new(data_dir.join("plugins"), allow_plugin_install)),
        data_dir,
        tasks: Arc::new(Notify::new()),
        work: Arc::new(Notify::new()),
        imports: Arc::default(),
    };
    app.database.prune(now_ms())?;
    let garbage_collector = app.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(error) = garbage_collector.database.prune(now_ms()) {
                tracing::error!(%error, "task garbage collection failed");
            }
        }
    });
    let router = router(app.clone());
    app.plugins.set_router(router.clone());
    plugins::backfill_guest_digests(&app).await;
    let plugins = app.plugins.clone();
    tokio::spawn(async move { plugins.start_all(plugin_records).await });

    tracing::info!(%listen, "controller listening");
    axum::serve(
        TcpListener::bind(listen).await?,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

fn router(app: App) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({ "ok": true })) }))
        .route("/v1/images", get(list_images))
        .route("/v1/images/import", post(import_image))
        .route("/v1/images/imports", get(list_imports).post(create_import))
        .route(
            "/v1/images/imports/{id}",
            get(get_import).delete(delete_import),
        )
        .route(
            "/v1/images/{key}",
            axum::routing::patch(update_image).delete(delete_image),
        )
        .route("/v1/images/{id}/artifact", get(download_image))
        .route(
            "/v1/working-dirs/{digest}",
            axum::routing::head(working_dir_exists).put(upload_working_dir),
        )
        .route(
            "/internal/working-dirs/{digest}",
            get(download_working_dir_for_node),
        )
        .route("/v1/pools", get(list_pools).post(create_pool))
        .route(
            "/v1/pools/{name}",
            get(get_pool).put(update_pool).delete(delete_pool),
        )
        .route("/v1/pools/{name}/restart", post(restart_pool))
        .route("/v1/disks", get(list_disks).post(upload_disk))
        .route("/v1/disks/{key}", axum::routing::delete(delete_disk))
        .route("/internal/disks/{key}", get(download_disk))
        .route("/v1/plugins", get(plugins::list_plugins))
        .route("/v1/plugins/uploads", post(plugins::upload_plugin))
        .route(
            "/v1/plugins/uploads/{id}/install",
            post(plugins::install_upload),
        )
        .route(
            "/v1/plugins/{name}",
            axum::routing::delete(plugins::remove_plugin),
        )
        .route("/v1/plugins/{name}/log", get(plugins::get_plugin_log))
        .route("/v1/plugins/{name}/enable", post(plugins::enable_plugin))
        .route("/v1/plugins/{name}/disable", post(plugins::disable_plugin))
        .route(
            "/v1/plugins/{name}/rollback",
            post(plugins::rollback_plugin),
        )
        .route("/plugins/{name}/{*rest}", any(plugins::proxy))
        .route("/v1/nodes", get(list_nodes))
        .route(
            "/v1/nodes/{id}",
            axum::routing::put(update_node).delete(delete_node),
        )
        .route(
            "/v1/settings/storage",
            get(get_storage_settings).put(update_storage_settings),
        )
        .route("/v1/join-tokens", post(create_join_token))
        .route("/v1/join-tokens/{id}", get(get_join_token))
        .route("/v1/vms", get(list_vms))
        .route("/v1/tasks", get(list_tasks).post(create_task))
        .route("/v1/tasks/{id}", get(get_task))
        .route("/v1/tasks/{id}/wait", get(wait_task))
        .route("/v1/tasks/{id}/payload", get(get_task_payload))
        .route("/v1/tasks/{id}/result", get(get_task_result))
        .route("/v1/tasks/{id}/logs", get(get_task_logs))
        .route("/internal/nodes/register", post(register_node))
        .route("/internal/identity", get(controller_identity))
        .route("/internal/join", post(join_cluster))
        .route("/internal/local-join", post(local_join))
        .route(
            "/internal/nodes/{id}/images/{image}/ready",
            post(image_ready),
        )
        .route("/internal/nodes/{id}/disks/{key}/ready", post(disk_ready))
        .route(
            "/internal/nodes/{id}/pools/{pool}/idle",
            get(warm_instance_needed)
                .post(add_warm_instance)
                .delete(remove_idle_instance),
        )
        .route("/internal/nodes/{id}/work", get(next_work))
        .route("/internal/tasks/{id}/complete", post(complete_task))
        .layer(DefaultBodyLimit::disable())
        .with_state(app)
}

async fn controller_identity(State(app): State<App>) -> Json<Uuid> {
    Json(app.controller_id)
}

async fn list_images(State(app): State<App>) -> Json<Vec<ImageView>> {
    let cluster = app.cluster.read().await;
    Json(
        cluster
            .images
            .values()
            .map(|image| ImageView {
                image: image.clone(),
                ready: cluster.image_ready(image.id),
            })
            .collect(),
    )
}

#[derive(Serialize)]
struct ImageView {
    #[serde(flatten)]
    image: Image,
    ready: bool,
}

#[derive(Deserialize)]
struct ImportImage {
    name: String,
    reference: String,
    username: Option<String>,
    password: Option<String>,
}

/// Share of the overall import progress spent downloading layers.
const PULL_SHARE: f64 = 0.7;
/// Finished imports kept so their logs stay readable.
const KEPT_IMPORTS: usize = 50;
const IMPORT_LOG_LIMIT: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ImportStatus {
    Running,
    Succeeded,
    Failed,
}

/// A Docker image import running in the background, with its progress and log.
#[derive(Debug, Clone, Serialize)]
struct ImportJob {
    id: Uuid,
    name: String,
    reference: String,
    status: ImportStatus,
    stage: String,
    /// Overall progress from 0 to 1, absent while it cannot be measured.
    progress: Option<f64>,
    downloaded: u64,
    total: Option<u64>,
    /// The image this import creates, registered once it succeeds.
    image: Uuid,
    error: Option<String>,
    created_at: u64,
    updated_at: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    log: String,
}

struct ImportEntry {
    job: ImportJob,
    work: PathBuf,
    task: Option<tokio::task::AbortHandle>,
}

#[derive(Default)]
struct Imports {
    jobs: std::sync::Mutex<HashMap<Uuid, ImportEntry>>,
}

/// Reports progress and output of one import.
#[derive(Clone)]
struct ImportProgress {
    imports: Arc<Imports>,
    id: Uuid,
    started: std::time::Instant,
}

impl ImportProgress {
    fn update(&self, change: impl FnOnce(&mut ImportJob)) {
        if let Some(entry) = self.imports.jobs.lock().unwrap().get_mut(&self.id) {
            change(&mut entry.job);
            entry.job.updated_at = now_ms();
        }
    }

    fn log(&self, text: impl AsRef<str>) {
        let text = text.as_ref().trim_end();
        if text.is_empty() {
            return;
        }
        let line = format!("[{:>7.1}s] {text}\n", self.started.elapsed().as_secs_f64());
        self.update(|job| {
            job.log.push_str(&line);
            if job.log.len() > IMPORT_LOG_LIMIT {
                let mut cut = job.log.len() - IMPORT_LOG_LIMIT * 3 / 4;
                while !job.log.is_char_boundary(cut) {
                    cut += 1;
                }
                job.log.replace_range(..cut, "…\n");
            }
        });
    }

    fn stage(&self, stage: &str, progress: Option<f64>) {
        self.log(format!("== {stage}"));
        self.update(|job| {
            job.stage = stage.into();
            job.progress = progress;
        });
    }

    /// Run a command, recording it and its output in the log.
    async fn run<I, S>(&self, program: &str, args: I) -> Result<String, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|arg| arg.as_ref().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        self.log(format!("$ {program} {}", args.join(" ")));
        let result = command(program, &args).await;
        self.log_output(&result);
        result
    }

    fn log_output(&self, result: &Result<String, String>) {
        match result {
            Ok(output) => {
                let mut start = output.len().saturating_sub(4096);
                while !output.is_char_boundary(start) {
                    start += 1;
                }
                self.log(&output[start..]);
            }
            Err(message) => self.log(format!("failed: {message}")),
        }
    }
}

fn import_view(entry: &ImportEntry, with_log: bool) -> ImportJob {
    let mut job = entry.job.clone();
    if !with_log {
        job.log.clear();
    }
    job
}

async fn list_imports(State(app): State<App>) -> Json<Vec<ImportJob>> {
    let jobs = app.imports.jobs.lock().unwrap();
    let mut jobs = jobs
        .values()
        .map(|entry| import_view(entry, false))
        .collect::<Vec<_>>();
    jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at));
    Json(jobs)
}

async fn get_import(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<ImportJob>> {
    app.imports
        .jobs
        .lock()
        .unwrap()
        .get(&id)
        .map(|entry| Json(import_view(entry, true)))
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "import not found"))
}

/// Cancel a running import, or dismiss a finished one.
async fn delete_import(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let entry = app
        .imports
        .jobs
        .lock()
        .unwrap()
        .remove(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "import not found"))?;
    if entry.job.status == ImportStatus::Running {
        if let Some(task) = entry.task {
            task.abort();
        }
        fs::remove_dir_all(&entry.work).await.ok();
        if !app
            .cluster
            .read()
            .await
            .images
            .contains_key(&entry.job.image)
        {
            fs::remove_file(
                app.data_dir
                    .join("images")
                    .join(entry.job.image.to_string()),
            )
            .await
            .ok();
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Start an import in the background and return it at once.
async fn create_import(
    State(app): State<App>,
    Json(request): Json<ImportImage>,
) -> ApiResult<(StatusCode, Json<ImportJob>)> {
    let (job, _) = start_import(&app, request).await?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// Import an image and wait until it is converted.
async fn import_image(
    State(app): State<App>,
    Json(request): Json<ImportImage>,
) -> ApiResult<(StatusCode, Json<Image>)> {
    let (_, task) = start_import(&app, request).await?;
    match task.await {
        Ok(Ok(image)) => Ok((StatusCode::CREATED, Json(image))),
        Ok(Err(message)) => Err(error(StatusCode::BAD_REQUEST, message)),
        Err(_) => Err(error(StatusCode::CONFLICT, "import was cancelled")),
    }
}

async fn start_import(
    app: &App,
    request: ImportImage,
) -> ApiResult<(ImportJob, tokio::task::JoinHandle<Result<Image, String>>)> {
    let name = request.name.trim().to_owned();
    let reference = request.reference.trim();
    let reference = reference
        .strip_prefix("http://")
        .or_else(|| reference.strip_prefix("https://"))
        .unwrap_or(reference)
        .to_owned();
    if name.is_empty() || reference.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name and Docker image are required",
        ));
    }
    let username = request
        .username
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let password = request.password.filter(|value| !value.is_empty());
    if username.is_some() != password.is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "registry username and password must be provided together",
        ));
    }
    let id = Uuid::new_v4();
    let work = app.data_dir.join("imports").join(id.to_string());
    let job = {
        let cluster = app.cluster.read().await;
        let mut jobs = app.imports.jobs.lock().unwrap();
        if cluster.images.values().any(|image| image.name == name)
            || jobs
                .values()
                .any(|entry| entry.job.status == ImportStatus::Running && entry.job.name == name)
        {
            return Err(error(StatusCode::CONFLICT, "image name already exists"));
        }
        let now = now_ms();
        let job = ImportJob {
            id,
            name: name.clone(),
            reference: reference.clone(),
            status: ImportStatus::Running,
            stage: "Queued".into(),
            progress: None,
            downloaded: 0,
            total: None,
            image: Uuid::new_v4(),
            error: None,
            created_at: now,
            updated_at: now,
            log: String::new(),
        };
        let mut finished = jobs
            .values()
            .filter(|entry| entry.job.status != ImportStatus::Running)
            .map(|entry| (entry.job.created_at, entry.job.id))
            .collect::<Vec<_>>();
        finished.sort();
        for (_, old) in finished.iter().rev().skip(KEPT_IMPORTS - 1) {
            jobs.remove(old);
        }
        jobs.insert(
            id,
            ImportEntry {
                job: job.clone(),
                work: work.clone(),
                task: None,
            },
        );
        job
    };
    let progress = ImportProgress {
        imports: app.imports.clone(),
        id,
        started: std::time::Instant::now(),
    };
    progress.log(format!("Importing {reference} as {name}"));
    let image_id = job.image;
    let task_app = app.clone();
    let task = tokio::spawn(async move {
        let result = run_import(
            &task_app,
            &progress,
            &name,
            &reference,
            &work,
            image_id,
            username.as_deref(),
            password.as_deref(),
        )
        .await;
        fs::remove_dir_all(&work).await.ok();
        match &result {
            Ok(_) => {
                progress.log("Imported successfully");
                progress.update(|job| {
                    job.status = ImportStatus::Succeeded;
                    job.stage = "Imported".into();
                    job.progress = Some(1.0);
                });
            }
            Err(message) => {
                progress.log(format!("Import failed: {message}"));
                progress.update(|job| {
                    job.status = ImportStatus::Failed;
                    job.error = Some(message.clone());
                });
            }
        }
        result
    });
    if let Some(entry) = app.imports.jobs.lock().unwrap().get_mut(&id) {
        entry.task = Some(task.abort_handle());
    }
    Ok((job, task))
}

#[allow(clippy::too_many_arguments)]
async fn run_import(
    app: &App,
    progress: &ImportProgress,
    name: &str,
    reference: &str,
    work: &Path,
    id: Uuid,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<Image, String> {
    fs::create_dir_all(work).await.map_err(|e| e.to_string())?;
    let (artifact, docker_source) =
        build_docker_image(progress, reference, work, username, password).await?;
    let path = app.data_dir.join("images").join(id.to_string());
    fs::rename(&artifact, &path)
        .await
        .map_err(|e| e.to_string())?;
    progress.stage("Hashing image", Some(0.96));
    let size = fs::metadata(&path).await.map_err(|e| e.to_string())?.len();
    let sha256 = hash_file(&path).await.map_err(|e| e.to_string())?;
    progress.log(format!("{size} bytes, sha256 {sha256}"));
    let image = Image {
        id,
        name: name.into(),
        sha256,
        size,
        compatible: true,
        kind: ImageKind::MicroVm,
        executor: None,
        docker_source: Some(docker_source),
    };
    let mut cluster = app.cluster.write().await;
    if cluster.images.values().any(|image| image.name == name) {
        drop(cluster);
        fs::remove_file(&path).await.ok();
        return Err("image name already exists".into());
    }
    for node in cluster.nodes.values_mut() {
        node.status = NodeStatus::Syncing;
    }
    cluster.images.insert(id, image.clone());
    save(app, &cluster)
        .await
        .map_err(|(_, Json(body))| body["error"].as_str().unwrap_or("save failed").to_owned())?;
    Ok(image)
}

async fn build_docker_image(
    progress: &ImportProgress,
    reference: &str,
    work: &Path,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(PathBuf, String), String> {
    pull_docker_image(progress, reference, work, username, password).await?;
    progress.stage("Checking image", Some(0.78));
    progress.log(format!("$ docker image inspect {reference}"));
    let inspect = command("docker", ["image", "inspect", reference]).await;
    if let Err(message) = &inspect {
        progress.log(format!("failed: {message}"));
    }
    let inspect: Vec<Value> = serde_json::from_str(&inspect?).map_err(|e| e.to_string())?;
    let platform = inspect.first().ok_or("Docker image was not found")?;
    let docker_source = platform["Id"]
        .as_str()
        .ok_or("Docker image has no content ID")?
        .to_owned();
    progress.log(format!(
        "{}/{} {docker_source}",
        platform["Os"].as_str().unwrap_or("?"),
        platform["Architecture"].as_str().unwrap_or("?")
    ));
    if platform["Os"].as_str() != Some("linux")
        || platform["Architecture"].as_str() != Some("amd64")
    {
        return Err("Docker image must be linux/amd64".into());
    }
    progress
        .run(
            "docker",
            [
                "run",
                "--rm",
                "--entrypoint",
                "/usr/bin/python3",
                reference,
                "-c",
                "import sys",
            ],
        )
        .await
        .map_err(|_| {
            "Docker image must contain /usr/bin/python3 for the Scraperack guest agent".to_string()
        })?;
    progress.stage("Exporting filesystem", Some(0.8));
    // The container is only exported, never started. Docker refuses to create one for an
    // image without an entrypoint or command, so give it the interpreter checked above.
    let container = progress
        .run(
            "docker",
            ["create", "--entrypoint", "/usr/bin/python3", reference],
        )
        .await?;
    let container = container.trim();
    let archive = work.join("rootfs.tar");
    let exported = progress
        .run("docker", ["export", "--output", path(&archive)?, container])
        .await;
    let _ = command("docker", ["rm", "-f", container]).await;
    exported?;
    progress.stage("Unpacking filesystem", Some(0.85));
    let root = work.join("rootfs");
    let state = work.join("fakeroot.state");
    fs::create_dir(&root).await.map_err(|e| e.to_string())?;
    progress
        .run(
            "fakeroot",
            [
                "-s",
                path(&state)?,
                "--",
                "tar",
                "--numeric-owner",
                "-xf",
                path(&archive)?,
                "-C",
                path(&root)?,
            ],
        )
        .await?;
    fs::remove_file(&archive).await.ok();
    progress.stage("Installing guest agent", Some(0.88));
    let agent = work.join("scraperack-init");
    fs::write(&agent, include_bytes!("../guest/scraperack-init"))
        .await
        .map_err(|e| e.to_string())?;
    install(&state, &agent, &root.join("sbin/scraperack-init"), "0755").await?;
    let config = &platform["Config"];
    let service = json!({
        "entrypoint": config["Entrypoint"].as_array().cloned().unwrap_or_default(),
        "command": config["Cmd"].as_array().cloned().unwrap_or_default(),
        "env": config["Env"].as_array().cloned().unwrap_or_default(),
        "working_dir": config["WorkingDir"].as_str().unwrap_or("/"),
    });
    if service["entrypoint"]
        .as_array()
        .is_some_and(|value| !value.is_empty())
        || service["command"]
            .as_array()
            .is_some_and(|value| !value.is_empty())
    {
        progress.log(format!("Service: {service}"));
        let service_path = work.join("scraperack-service.json");
        fs::write(&service_path, serde_json::to_vec(&service).unwrap())
            .await
            .map_err(|e| e.to_string())?;
        install(
            &state,
            &service_path,
            &root.join("etc/scraperack-service.json"),
            "0644",
        )
        .await?;
    }
    progress.stage("Building root filesystem", Some(0.9));
    let used = progress.run("du", ["-sb", path(&root)?]).await?;
    let used = used
        .split_whitespace()
        .next()
        .ok_or("could not measure Docker filesystem")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let step = 16 * 1024 * 1024;
    let size = (used + used / 5 + 16 * 1024 * 1024)
        .max(64 * 1024 * 1024)
        .div_ceil(step)
        * step;
    let artifact = work.join("image.ext4");
    fs::File::create(&artifact)
        .await
        .map_err(|e| e.to_string())?
        .set_len(size)
        .await
        .map_err(|e| e.to_string())?;
    progress
        .run(
            "fakeroot",
            [
                "-i",
                path(&state)?,
                "--",
                "mkfs.ext4",
                "-q",
                "-F",
                "-d",
                path(&root)?,
                path(&artifact)?,
            ],
        )
        .await?;
    Ok((artifact, docker_source))
}

async fn pull_docker_image(
    progress: &ImportProgress,
    reference: &str,
    work: &Path,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), String> {
    let authfile = work.join("registry-auth.json");
    let result = pull_layers(progress, reference, work, &authfile, username, password).await;
    fs::remove_file(authfile).await.ok();
    result
}

async fn pull_layers(
    progress: &ImportProgress,
    reference: &str,
    work: &Path,
    authfile: &Path,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), String> {
    let mut auth = Vec::new();
    if let (Some(username), Some(password)) = (username, password) {
        progress.stage("Signing in to registry", Some(0.0));
        let registry = registry(reference);
        progress.log(format!(
            "$ skopeo login --username {username} --password-stdin {registry}"
        ));
        let login = command_input(
            "skopeo",
            [
                "login",
                "--tls-verify=false",
                "--authfile",
                path(authfile)?,
                "--username",
                username,
                "--password-stdin",
                registry,
            ],
            password,
        )
        .await;
        progress.log_output(&login);
        login?;
        auth = vec!["--authfile".to_owned(), path(authfile)?.to_owned()];
    }
    progress.stage("Pulling", None);
    // Layer sizes give the download a total to measure progress against.
    let mut inspect = vec![
        "inspect".to_owned(),
        "--override-os=linux".to_owned(),
        "--override-arch=amd64".to_owned(),
        "--tls-verify=false".to_owned(),
    ];
    inspect.extend(auth.iter().cloned());
    inspect.push(format!("docker://{reference}"));
    let total = match command("skopeo", &inspect).await {
        Ok(output) => serde_json::from_str::<Value>(&output)
            .ok()
            .and_then(|value| {
                value["LayersData"].as_array().map(|layers| {
                    (
                        layers.len(),
                        layers
                            .iter()
                            .filter_map(|layer| layer["Size"].as_u64())
                            .sum::<u64>(),
                    )
                })
            }),
        Err(message) => {
            progress.log(format!("Could not read the layer sizes: {message}"));
            None
        }
    };
    if let Some((layers, bytes)) = total {
        progress.log(format!("{layers} layers, {bytes} bytes to download"));
        progress.update(|job| {
            job.total = Some(bytes).filter(|bytes| *bytes > 0);
            job.progress = job.total.map(|_| 0.0);
        });
    }
    let layers = work.join("layers");
    fs::create_dir_all(&layers)
        .await
        .map_err(|e| e.to_string())?;
    let mut copy = vec!["copy".to_owned(), "--src-tls-verify=false".to_owned()];
    if !auth.is_empty() {
        copy.extend(["--src-authfile".to_owned(), path(authfile)?.to_owned()]);
    }
    copy.extend([
        format!("docker://{reference}"),
        format!("dir:{}", path(&layers)?),
    ]);
    let pulling = progress.run("skopeo", copy);
    tokio::pin!(pulling);
    let pulled = loop {
        tokio::select! {
            result = &mut pulling => break result,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                let downloaded = directory_size(&layers).await;
                progress.update(|job| {
                    job.downloaded = downloaded;
                    if let Some(total) = job.total {
                        job.progress = Some(PULL_SHARE * (downloaded as f64 / total as f64).min(1.0));
                    }
                });
            }
        }
    };
    pulled?;
    let downloaded = directory_size(&layers).await;
    progress.update(|job| {
        job.downloaded = downloaded;
        job.total = job.total.map(|total| total.max(downloaded));
    });
    progress.stage("Loading into Docker", Some(PULL_SHARE));
    let archive = work.join("docker-image.tar");
    let packed = progress
        .run(
            "skopeo",
            [
                "copy".to_owned(),
                format!("dir:{}", path(&layers)?),
                format!("docker-archive:{}:{reference}", path(&archive)?),
            ],
        )
        .await;
    fs::remove_dir_all(&layers).await.ok();
    packed?;
    let loaded = progress
        .run("docker", ["load", "--input", path(&archive)?])
        .await;
    fs::remove_file(archive).await.ok();
    loaded.map(|_| ())
}

async fn directory_size(directory: &Path) -> u64 {
    let mut total = 0;
    if let Ok(mut entries) = fs::read_dir(directory).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(metadata) = entry.metadata().await {
                total += metadata.len();
            }
        }
    }
    total
}

fn registry(reference: &str) -> &str {
    let first = reference.split('/').next().unwrap_or(reference);
    if reference.contains('/')
        && (first.contains('.') || first.contains(':') || first == "localhost")
    {
        first
    } else {
        "docker.io"
    }
}

async fn install(state: &Path, source: &Path, target: &Path, mode: &str) -> Result<(), String> {
    command(
        "fakeroot",
        [
            "-i",
            path(state)?,
            "-s",
            path(state)?,
            "--",
            "install",
            "-Dm",
            mode,
            path(source)?,
            path(target)?,
        ],
    )
    .await
    .map(|_| ())
}

fn path(value: &Path) -> Result<&str, String> {
    value
        .to_str()
        .ok_or_else(|| "invalid filesystem path".into())
}

async fn command<I, S>(program: &str, args: I) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| format!("{program}: {e}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn command_input<I, S>(program: &str, args: I, input: &str) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("{program}: {e}"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| format!("{program}: stdin unavailable"))?
        .write_all(input.as_bytes())
        .await
        .map_err(|e| format!("{program}: {e}"))?;
    let output = child
        .wait_with_output()
        .await
        .map_err(|e| format!("{program}: {e}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn hash_file(path: &Path) -> Result<String, std::io::Error> {
    let mut file = fs::File::open(path).await?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

#[derive(Deserialize)]
struct UpdateImage {
    name: String,
}

async fn update_image(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
    Json(update): Json<UpdateImage>,
) -> ApiResult<Json<Image>> {
    let mut cluster = app.cluster.write().await;
    if update.name.trim().is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "image name is required"));
    }
    if cluster
        .images
        .values()
        .any(|image| image.id != id && image.name == update.name)
    {
        return Err(error(StatusCode::CONFLICT, "image name already exists"));
    }
    let image = cluster
        .images
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "image not found"))?;
    image.name = update.name;
    let image = image.clone();
    save(&app, &cluster).await?;
    Ok(Json(image))
}

async fn download_image(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Response> {
    authenticated_node(&app, &headers).await?;
    if !app.cluster.read().await.images.contains_key(&id) {
        return Err(error(StatusCode::NOT_FOUND, "image not found"));
    }
    let file = fs::File::open(app.data_dir.join("images").join(id.to_string()))
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Body::from_stream(ReaderStream::new(file)).into_response())
}

fn working_dir_path(app: &App, digest: &str) -> ApiResult<PathBuf> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid working directory hash",
        ));
    }
    Ok(app
        .data_dir
        .join("working-dirs")
        .join(format!("{digest}.zip")))
}

async fn working_dir_exists(
    State(app): State<App>,
    AxumPath(digest): AxumPath<String>,
) -> ApiResult<StatusCode> {
    if working_dir_path(&app, &digest)?.exists() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(error(StatusCode::NOT_FOUND, "working directory not found"))
    }
}

async fn upload_working_dir(
    State(app): State<App>,
    AxumPath(digest): AxumPath<String>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let path = working_dir_path(&app, &digest)?;
    if path.exists() {
        return Ok(StatusCode::NO_CONTENT);
    }
    if hex::encode(Sha256::digest(&body)) != digest {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "working directory hash does not match content",
        ));
    }
    let temporary = path.with_extension(format!("{}.partial", Uuid::new_v4()));
    fs::write(&temporary, body)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    fs::rename(&temporary, &path)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::CREATED)
}

async fn download_working_dir_for_node(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(digest): AxumPath<String>,
) -> ApiResult<Response> {
    authenticated_node(&app, &headers).await?;
    let file = fs::File::open(working_dir_path(&app, &digest)?)
        .await
        .map_err(|cause| {
            if cause.kind() == std::io::ErrorKind::NotFound {
                error(StatusCode::NOT_FOUND, "working directory not found")
            } else {
                error(StatusCode::INTERNAL_SERVER_ERROR, cause.to_string())
            }
        })?;
    Ok(Body::from_stream(ReaderStream::new(file)).into_response())
}

async fn delete_image(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.pools.values().any(|pool| pool.image == id) {
        return Err(error(StatusCode::CONFLICT, "image is in use"));
    }
    let image = cluster
        .images
        .remove(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "image not found"))?;
    let docker_source = image.docker_source.filter(|source| {
        !cluster
            .images
            .values()
            .any(|image| image.docker_source.as_ref() == Some(source))
    });
    for node in cluster.nodes.values_mut() {
        node.images.remove(&id);
    }
    save(&app, &cluster).await?;
    drop(cluster);
    app.imports
        .jobs
        .lock()
        .unwrap()
        .retain(|_, entry| entry.job.image != id);
    fs::remove_file(app.data_dir.join("images").join(id.to_string()))
        .await
        .ok();
    if let Some(source) = docker_source {
        let _ = command("docker", ["image", "rm", "-f", source.as_str()]).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct PoolView {
    #[serde(flatten)]
    pool: Pool,
    ready: bool,
}

async fn list_pools(State(app): State<App>) -> Json<Vec<PoolView>> {
    let cluster = app.cluster.read().await;
    Json(
        cluster
            .pools
            .values()
            .map(|pool| PoolView {
                ready: cluster.pool_ready(pool),
                pool: pool.clone(),
            })
            .collect(),
    )
}

async fn get_pool(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<Pool>> {
    app.cluster
        .read()
        .await
        .pools
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "pool not found"))
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

/// Check the pool against the cluster; `previous` is set when updating.
fn validate_pool(cluster: &Cluster, pool: &Pool, previous: Option<&Pool>) -> ApiResult<()> {
    if !valid_name(&pool.name) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "pool names may only contain letters, digits, '-', '_' and '.'",
        ));
    }
    let Some(image) = cluster.images.get(&pool.image) else {
        return Err(error(StatusCode::BAD_REQUEST, "unknown image"));
    };
    if !image.compatible {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "image is not Scraperack compatible",
        ));
    }
    if previous.is_none_or(|old| old.image != pool.image) && !cluster.image_ready(pool.image) {
        return Err(error(StatusCode::CONFLICT, "image is still distributing"));
    }
    let mut mounts = HashSet::new();
    for disk in &pool.disks {
        if !cluster.disks.contains_key(&disk.key) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                format!("unknown disk {}", disk.key),
            ));
        }
        if !disk.mount.starts_with('/') || !mounts.insert(disk.mount.clone()) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "disk mounts must be unique absolute paths",
            ));
        }
    }
    if pool.disks.len() > 16 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "a pool may attach at most 16 disks",
        ));
    }
    for alias in &pool.aliases {
        if let Some(other) = cluster
            .pools
            .values()
            .find(|other| other.name != pool.name && other.matches(alias))
        {
            return Err(error(
                StatusCode::CONFLICT,
                format!("alias {alias} is already used by pool {}", other.name),
            ));
        }
    }
    if pool.max_instances.is_some_and(|limit| limit == 0) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "max instances must be positive",
        ));
    }
    if pool
        .max_instances
        .is_some_and(|limit| pool.warm_instances > limit)
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "warm instances cannot exceed max instances",
        ));
    }
    if !pool.network && !pool.ports.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "ports require networking"));
    }
    let mut names = HashSet::new();
    let mut guest_ports = HashSet::new();
    let mut host_ports = HashSet::new();
    for port in &pool.ports {
        if port.name.trim().is_empty() || !names.insert(port.name.clone()) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "port names must be unique and non-empty",
            ));
        }
        if port.guest_port == 0 || !guest_ports.insert((port.protocol, port.guest_port)) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "guest ports must be unique and non-zero",
            ));
        }
        if let Some(host_port) = port.host_port
            && (host_port == 0 || !host_ports.insert((port.protocol, host_port)))
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "host ports must be unique and non-zero",
            ));
        }
    }
    Ok(())
}

/// Plugins may only change their own pools; users may only change unmanaged ones.
fn check_owner(pool: &Pool, owner: &Option<String>) -> ApiResult<()> {
    if pool.managed_by == *owner {
        return Ok(());
    }
    Err(error(
        StatusCode::FORBIDDEN,
        match &pool.managed_by {
            Some(manager) => format!("pool is managed by {manager}"),
            None => "plugins may only change their own pools".into(),
        },
    ))
}

async fn create_pool(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    Json(mut pool): Json<Pool>,
) -> ApiResult<(StatusCode, Json<Pool>)> {
    let mut cluster = app.cluster.write().await;
    pool.managed_by = owner_of(&caller);
    pool.environment = None;
    pool.revision = 0;
    if cluster.pools.contains_key(&pool.name) {
        return Err(error(StatusCode::CONFLICT, "pool already exists"));
    }
    validate_pool(&cluster, &pool, None)?;
    cluster.pools.insert(pool.name.clone(), pool.clone());
    let scheduled = cluster.schedule_queued();
    save(&app, &cluster).await?;
    drop(cluster);
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok((StatusCode::CREATED, Json(pool)))
}

/// Whether a change requires new microVMs rather than reusing idle ones.
fn recycles_vms(old: &Pool, new: &Pool) -> bool {
    old.image != new.image
        || old.cpu_millis != new.cpu_millis
        || old.memory_mib != new.memory_mib
        || old.disk_mib != new.disk_mib
        || old.network != new.network
        || old.reuse != new.reuse
        || old.preload != new.preload
        || old.disks != new.disks
        || old.executor != new.executor
        || old.ports.len() != new.ports.len()
        || old.ports.iter().zip(&new.ports).any(|(left, right)| {
            left.name != right.name
                || left.guest_port != right.guest_port
                || left.host_port != right.host_port
                || left.protocol != right.protocol
        })
}

/// Bump the pool revision so nodes replace idle microVMs.
fn bump_revision(cluster: &mut Cluster, name: &str) {
    if let Some(pool) = cluster.pools.get_mut(name) {
        pool.revision += 1;
    }
}

async fn update_pool(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    AxumPath(name): AxumPath<String>,
    Json(mut pool): Json<Pool>,
) -> ApiResult<Json<Pool>> {
    let mut cluster = app.cluster.write().await;
    let old = cluster
        .pools
        .get(&name)
        .cloned()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "pool not found"))?;
    check_owner(&old, &owner_of(&caller))?;
    pool.name = name.clone();
    pool.managed_by = old.managed_by.clone();
    pool.environment = old.environment;
    pool.revision = old.revision + u64::from(recycles_vms(&old, &pool));
    validate_pool(&cluster, &pool, Some(&old))?;
    cluster.pools.insert(name, pool.clone());
    let scheduled = cluster.schedule_queued();
    save(&app, &cluster).await?;
    drop(cluster);
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok(Json(pool))
}

async fn restart_pool(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<Pool>> {
    let mut cluster = app.cluster.write().await;
    let pool = cluster
        .pools
        .get(&name)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "pool not found"))?;
    check_owner(pool, &owner_of(&caller))?;
    bump_revision(&mut cluster, &name);
    let pool = cluster.pools[&name].clone();
    save(&app, &cluster).await?;
    Ok(Json(pool))
}

/// Remove a pool and release the capacity of its idle instances.
pub fn remove_pool(cluster: &mut Cluster, name: &str) -> Option<Pool> {
    let pool = cluster.pools.remove(name)?;
    for node in cluster.nodes.values_mut() {
        let idle = node.idle_instances.remove(name).unwrap_or(0);
        node.used_cpu_millis = node
            .used_cpu_millis
            .saturating_sub(pool.cpu_millis.saturating_mul(idle));
        node.used_memory_mib = node
            .used_memory_mib
            .saturating_sub(pool.memory_mib.saturating_mul(idle));
    }
    Some(pool)
}

async fn delete_pool(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    let pool = cluster
        .pools
        .get(&name)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "pool not found"))?;
    check_owner(pool, &owner_of(&caller))?;
    if cluster.tasks.values().any(|task| {
        task.pool == name
            && matches!(
                task.status,
                TaskStatus::Queued | TaskStatus::Assigned | TaskStatus::Running
            )
    }) {
        return Err(error(StatusCode::CONFLICT, "pool has active tasks"));
    }
    remove_pool(&mut cluster, &name);
    save(&app, &cluster).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn valid_disk_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[derive(Serialize)]
struct DiskView {
    #[serde(flatten)]
    disk: Disk,
    ready: bool,
}

async fn list_disks(State(app): State<App>) -> Json<Vec<DiskView>> {
    let cluster = app.cluster.read().await;
    Json(
        cluster
            .disks
            .values()
            .map(|disk| DiskView {
                ready: cluster.disk_ready(&disk.key),
                disk: disk.clone(),
            })
            .collect(),
    )
}

/// Register an ext4 file as a content-addressed disk, moving it into the store.
pub async fn store_disk_file(app: &App, file: &Path, owner: Option<&str>) -> ApiResult<Disk> {
    let mut header = vec![0; 1082];
    let mut opened = fs::File::open(file).await.map_err(internal_error)?;
    if opened.read_exact(&mut header).await.is_err() || header[1080..1082] != [0x53, 0xef] {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "disk must be an ext4 filesystem",
        ));
    }
    drop(opened);
    let sha256 = hash_file(file).await.map_err(internal_error)?;
    let size = fs::metadata(file).await.map_err(internal_error)?.len();
    let target = app.data_dir.join("disks").join(&sha256);
    let mut cluster = app.cluster.write().await;
    if let Some(disk) = cluster.disks.get(&sha256) {
        fs::remove_file(file).await.ok();
        return Ok(disk.clone());
    }
    fs::rename(file, &target).await.map_err(internal_error)?;
    let disk = Disk {
        key: sha256.clone(),
        sha256,
        size,
        owner: owner.map(str::to_owned),
    };
    cluster.disks.insert(disk.key.clone(), disk.clone());
    save(app, &cluster).await?;
    Ok(disk)
}

/// Delete a disk unless a pool still attaches it. Returns whether it was removed.
pub async fn remove_unused_disk(app: &App, key: &str) -> ApiResult<bool> {
    let mut cluster = app.cluster.write().await;
    if !cluster.disks.contains_key(key)
        || cluster
            .pools
            .values()
            .any(|pool| pool.disks.iter().any(|disk| disk.key == key))
    {
        return Ok(false);
    }
    cluster.disks.remove(key);
    for node in cluster.nodes.values_mut() {
        node.disks.remove(key);
    }
    save(app, &cluster).await?;
    drop(cluster);
    fs::remove_file(app.data_dir.join("disks").join(key))
        .await
        .ok();
    Ok(true)
}

async fn upload_disk(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    body: Body,
) -> ApiResult<(StatusCode, Json<Disk>)> {
    let uploads = app.data_dir.join("imports");
    fs::create_dir_all(&uploads).await.map_err(internal_error)?;
    let temporary = uploads.join(format!("disk-{}", Uuid::new_v4()));
    let written = async {
        let mut file = fs::File::create(&temporary).await?;
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk.map_err(std::io::Error::other)?)
                .await?;
        }
        file.flush().await
    }
    .await;
    if let Err(cause) = written {
        fs::remove_file(&temporary).await.ok();
        return Err(internal_error(cause));
    }
    let owner = caller.map(|Extension(caller)| caller.0);
    let stored = store_disk_file(&app, &temporary, owner.as_deref()).await;
    if stored.is_err() {
        fs::remove_file(&temporary).await.ok();
    }
    Ok((StatusCode::CREATED, Json(stored?)))
}

async fn delete_disk(
    State(app): State<App>,
    caller: Option<Extension<Caller>>,
    AxumPath(key): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    let disk = cluster
        .disks
        .get(&key)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "disk not found"))?;
    if disk.owner != caller.map(|Extension(caller)| caller.0) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "disk belongs to another owner",
        ));
    }
    if let Some(pool) = cluster
        .pools
        .values()
        .find(|pool| pool.disks.iter().any(|disk| disk.key == key))
    {
        return Err(error(
            StatusCode::CONFLICT,
            format!("disk is attached to pool {}", pool.name),
        ));
    }
    cluster.disks.remove(&key);
    for node in cluster.nodes.values_mut() {
        node.disks.remove(&key);
    }
    save(&app, &cluster).await?;
    drop(cluster);
    fs::remove_file(app.data_dir.join("disks").join(&key))
        .await
        .ok();
    Ok(StatusCode::NO_CONTENT)
}

async fn download_disk(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(key): AxumPath<String>,
) -> ApiResult<Response> {
    authenticated_node(&app, &headers).await?;
    if !valid_disk_key(&key) || !app.cluster.read().await.disks.contains_key(&key) {
        return Err(error(StatusCode::NOT_FOUND, "disk not found"));
    }
    let file = fs::File::open(app.data_dir.join("disks").join(&key))
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Body::from_stream(ReaderStream::new(file)).into_response())
}

#[derive(Serialize)]
struct JoinTokenView {
    id: Uuid,
    token: Option<String>,
    expires_at_ms: u64,
    status: &'static str,
}

async fn create_join_token(State(app): State<App>) -> ApiResult<Json<JoinTokenView>> {
    let now = now_ms();
    let token = secret();
    let join = JoinToken {
        id: Uuid::new_v4(),
        token_hash: secret_hash(&token),
        expires_at_ms: now + JOIN_TOKEN_TTL_MS,
        used_at_ms: None,
    };
    let view = JoinTokenView {
        id: join.id,
        token: Some(token),
        expires_at_ms: join.expires_at_ms,
        status: "pending",
    };
    let mut cluster = app.cluster.write().await;
    cluster
        .join_tokens
        .retain(|_, item| item.expires_at_ms + 86_400_000 >= now);
    cluster.join_tokens.insert(join.id, join);
    save(&app, &cluster).await?;
    Ok(Json(view))
}

async fn get_join_token(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<JoinTokenView>> {
    let cluster = app.cluster.read().await;
    let token = cluster
        .join_tokens
        .get(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "join token not found"))?;
    let status = if token.used_at_ms.is_some() {
        "used"
    } else if token.expires_at_ms <= now_ms() {
        "expired"
    } else {
        "pending"
    };
    Ok(Json(JoinTokenView {
        id,
        token: None,
        expires_at_ms: token.expires_at_ms,
        status,
    }))
}

#[derive(Deserialize)]
struct JoinRequest {
    token: String,
}

#[derive(Deserialize)]
struct LocalJoinRequest {
    id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct NodeCredentials {
    node_id: Uuid,
    credential: String,
}

async fn join_cluster(
    State(app): State<App>,
    Json(request): Json<JoinRequest>,
) -> ApiResult<Json<NodeCredentials>> {
    let now = now_ms();
    let hash = secret_hash(&request.token);
    let mut cluster = app.cluster.write().await;
    let id = cluster
        .join_tokens
        .values()
        .find(|token| {
            token.used_at_ms.is_none()
                && token.expires_at_ms > now
                && same_hash(&token.token_hash, &hash)
        })
        .map(|token| token.id)
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid or expired join token"))?;
    cluster.join_tokens.get_mut(&id).unwrap().used_at_ms = Some(now);
    let node_id = Uuid::new_v4();
    let credential = secret();
    cluster
        .node_credentials
        .insert(node_id, secret_hash(&credential));
    save(&app, &cluster).await?;
    Ok(Json(NodeCredentials {
        node_id,
        credential,
    }))
}

async fn local_join(
    ConnectInfo(address): ConnectInfo<std::net::SocketAddr>,
    State(app): State<App>,
    Json(request): Json<LocalJoinRequest>,
) -> ApiResult<Json<NodeCredentials>> {
    if !address.ip().is_loopback() {
        return Err(error(StatusCode::FORBIDDEN, "local enrollment only"));
    }
    let node_id = request.id.unwrap_or_else(Uuid::new_v4);
    let credential = secret();
    let mut cluster = app.cluster.write().await;
    cluster
        .node_credentials
        .insert(node_id, secret_hash(&credential));
    save(&app, &cluster).await?;
    Ok(Json(NodeCredentials {
        node_id,
        credential,
    }))
}

async fn list_nodes(State(app): State<App>) -> Json<Vec<Node>> {
    Json(app.cluster.read().await.nodes.values().cloned().collect())
}

#[derive(Deserialize)]
struct UpdateNode {
    vm_start_concurrency: usize,
}

async fn update_node(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<UpdateNode>,
) -> ApiResult<Json<Node>> {
    if request.vm_start_concurrency == 0 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "VM start concurrency must be at least 1",
        ));
    }
    let mut cluster = app.cluster.write().await;
    let node = cluster
        .nodes
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "node not found"))?;
    node.vm_start_concurrency = Some(request.vm_start_concurrency);
    let node = node.clone();
    save(&app, &cluster).await?;
    Ok(Json(node))
}

async fn delete_node(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.tasks.values().any(|task| {
        task.node == Some(id) && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
    }) {
        return Err(error(StatusCode::CONFLICT, "node has active tasks"));
    }
    if cluster.nodes.remove(&id).is_none() && !cluster.node_credentials.contains_key(&id) {
        return Err(error(StatusCode::NOT_FOUND, "node not found"));
    }
    cluster.node_credentials.remove(&id);
    let scheduled = cluster.schedule_queued();
    save(&app, &cluster).await?;
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct VmView {
    #[serde(flatten)]
    vm: MicroVm,
    node: Uuid,
    node_name: String,
    node_address: Option<IpAddr>,
    last_seen_ms: u64,
}

async fn list_vms(State(app): State<App>) -> Json<Vec<VmView>> {
    let cluster = app.cluster.read().await;
    let cutoff = now_ms().saturating_sub(10_000);
    Json(
        cluster
            .nodes
            .values()
            .filter(|node| node.last_seen_ms >= cutoff)
            .flat_map(|node| {
                node.vms.iter().cloned().map(move |vm| VmView {
                    vm,
                    node: node.id,
                    node_name: node.name.clone(),
                    node_address: node.address,
                    last_seen_ms: node.last_seen_ms,
                })
            })
            .collect(),
    )
}

async fn get_storage_settings(State(app): State<App>) -> ApiResult<Json<StorageView>> {
    app.database.settings().map(Json).map_err(internal_error)
}

async fn update_storage_settings(
    State(app): State<App>,
    Json(settings): Json<StorageSettings>,
) -> ApiResult<Json<StorageView>> {
    if !(1..=24).contains(&settings.retention_hours) || settings.max_storage_bytes == 0 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "retention must be 1-24 hours and storage limit must be greater than zero",
        ));
    }
    app.database
        .update_settings(&settings)
        .map_err(internal_error)?;
    app.database.prune(now_ms()).map_err(internal_error)?;
    get_storage_settings(State(app)).await
}

#[derive(Deserialize)]
struct CreateTask {
    pool: String,
    payload: Value,
    #[serde(default)]
    variables: HashMap<String, String>,
    #[serde(default)]
    secrets: HashMap<String, String>,
    #[serde(default = "default_queue")]
    queue: bool,
}

fn default_queue() -> bool {
    true
}

async fn create_task(
    State(app): State<App>,
    Json(request): Json<CreateTask>,
) -> ApiResult<(StatusCode, Json<TaskView>)> {
    if let Some(working_dir) = request.payload.get("working_dir") {
        let digest = working_dir.as_str().ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "working directory hash must be a string",
            )
        })?;
        if !working_dir_path(&app, digest)?.exists() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "working directory was not uploaded",
            ));
        }
    }
    let mut cluster = app.cluster.write().await;
    let target = request.pool.as_str();
    let pool = cluster
        .find_pool(target)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, format!("unknown pool {target}")))?;
    if pool.executor.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "pool has no executor; it only provides microVMs",
        ));
    }
    let pool_name = pool.name.clone();
    let owner = pool.managed_by.clone();
    let id = Uuid::new_v4();
    let task = Task {
        id,
        pool: pool_name,
        owner,
        node: None,
        status: TaskStatus::Queued,
        payload: request.payload,
        variables: request.variables,
        result: None,
        error: None,
        created_at_ms: now_ms(),
        started_at_ms: None,
        finished_at_ms: None,
        metrics: None,
    };
    let secrets = app
        .secrets
        .encrypt(id, &request.secrets)
        .map_err(internal_error)?;
    if !app
        .database
        .insert_task(&task, &secrets)
        .map_err(internal_error)?
    {
        return Err(error(
            StatusCode::INSUFFICIENT_STORAGE,
            "controller task storage is full",
        ));
    }
    cluster.tasks.insert(id, task);
    if !cluster.schedule(id) && !request.queue {
        cluster.tasks.remove(&id);
        app.database.delete_task(id).map_err(internal_error)?;
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "cluster is full"));
    }
    let task = cluster.tasks[&id].clone();
    save_task(&app, &task).await?;
    drop(cluster);
    app.work.notify_waiters();
    Ok((StatusCode::ACCEPTED, Json(TaskView::from(&task))))
}

#[derive(Deserialize)]
struct ListTasks {
    limit: Option<usize>,
    /// Comma-separated pool names.
    pool: Option<String>,
    /// Pool owner, such as `plugin:python`.
    owner: Option<String>,
}

async fn list_tasks(
    State(app): State<App>,
    Query(query): Query<ListTasks>,
) -> ApiResult<Json<Vec<TaskView>>> {
    let pools = query.pool.as_deref().map(|pools| {
        pools
            .split(',')
            .map(str::trim)
            .filter(|pool| !pool.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
    });
    app.database
        .list_tasks(query.limit, pools.as_deref(), query.owner.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn get_task(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<TaskView>> {
    app.database
        .task(id)
        .map_err(internal_error)?
        .map(|task| Json(TaskView::from(&task)))
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))
}

async fn wait_task(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<TaskView>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
    loop {
        let notified = app.tasks.notified();
        let task = app
            .database
            .task(id)
            .map_err(internal_error)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
        if matches!(
            task.status,
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::TimedOut
        ) || tokio::time::Instant::now() >= deadline
        {
            return Ok(Json(TaskView::from(&task)));
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return get_task(State(app), AxumPath(id)).await;
        }
    }
}

#[derive(Serialize)]
struct TaskResult {
    status: TaskStatus,
    value: Option<Value>,
    error: Option<String>,
}

#[derive(Serialize)]
struct TaskLogs {
    stdout: String,
    stderr: String,
}

async fn get_task_payload(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<Value>> {
    app.database
        .task(id)
        .map_err(internal_error)?
        .map(|task| Json(task.payload))
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))
}

async fn get_task_result(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<TaskResult>> {
    let task = app
        .database
        .task(id)
        .map_err(internal_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    if !matches!(
        task.status,
        TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::TimedOut
    ) {
        return Err(error(StatusCode::CONFLICT, "task is not finished"));
    }
    let value = task
        .result
        .as_ref()
        .and_then(|result| result.get("value"))
        .cloned();
    Ok(Json(TaskResult {
        status: task.status,
        value,
        error: task.error,
    }))
}

async fn get_task_logs(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<TaskLogs>> {
    let task = app
        .database
        .task(id)
        .map_err(internal_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    let output = task.result.as_ref();
    Ok(Json(TaskLogs {
        stdout: output
            .and_then(|result| result.get("stdout"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        stderr: output
            .and_then(|result| result.get("stderr"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
    }))
}

#[derive(Deserialize)]
struct RegisterNode {
    id: Option<Uuid>,
    name: String,
    cpu_millis: u32,
    memory_mib: u32,
    #[serde(default)]
    vm_start_concurrency: Option<usize>,
    address: Option<IpAddr>,
    #[serde(default)]
    reset_idle: bool,
    #[serde(default)]
    vms: Vec<MicroVm>,
}

async fn register_node(
    State(app): State<App>,
    headers: HeaderMap,
    Json(request): Json<RegisterNode>,
) -> ApiResult<Json<Node>> {
    let authenticated = authenticated_node(&app, &headers).await?;
    if request.id != Some(authenticated) {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    let id = authenticated;
    let node = cluster.nodes.entry(id).or_insert_with(|| Node {
        id,
        name: request.name.clone(),
        cpu_millis: request.cpu_millis,
        memory_mib: request.memory_mib,
        vm_start_concurrency: None,
        address: request.address,
        used_cpu_millis: 0,
        used_memory_mib: 0,
        images: Default::default(),
        environments: Default::default(),
        disks: Default::default(),
        idle_instances: Default::default(),
        vms: Default::default(),
        status: NodeStatus::Syncing,
        last_seen_ms: now_ms(),
    });
    node.name = request.name;
    node.cpu_millis = request.cpu_millis;
    node.memory_mib = request.memory_mib;
    if node.vm_start_concurrency.is_none() {
        node.vm_start_concurrency = request.vm_start_concurrency.map(|value| value.max(1));
    }
    node.address = request.address;
    node.last_seen_ms = now_ms();
    node.vms = request.vms;
    if request.reset_idle {
        cluster.reset_idle_instances(id);
    }
    cluster.refresh_node(id);
    let scheduled = cluster.schedule_queued();
    let node = cluster.nodes[&id].clone();
    save(&app, &cluster).await?;
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok(Json(node))
}

async fn image_ready(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, image)): AxumPath<(Uuid, Uuid)>,
) -> ApiResult<StatusCode> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    if !cluster.images.contains_key(&image) {
        return Err(error(StatusCode::NOT_FOUND, "image not found"));
    }
    let node = cluster
        .nodes
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "node not found"))?;
    node.images.insert(image);
    cluster.refresh_node(id);
    let scheduled = cluster.schedule_queued();
    save(&app, &cluster).await?;
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

async fn disk_ready(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, key)): AxumPath<(Uuid, String)>,
) -> ApiResult<StatusCode> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    if !cluster.disks.contains_key(&key) {
        return Err(error(StatusCode::NOT_FOUND, "disk not found"));
    }
    let node = cluster
        .nodes
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "node not found"))?;
    if !node.disks.insert(key) {
        return Ok(StatusCode::NO_CONTENT);
    }
    let scheduled = cluster.schedule_queued();
    save(&app, &cluster).await?;
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

async fn warm_instance_needed(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, pool)): AxumPath<(Uuid, String)>,
) -> ApiResult<Json<bool>> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    Ok(Json(
        app.cluster.read().await.warm_instance_needed(id, &pool),
    ))
}

async fn add_warm_instance(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, pool)): AxumPath<(Uuid, String)>,
) -> ApiResult<Json<bool>> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    let added = cluster.add_warm_instance(id, &pool);
    if added {
        let scheduled = cluster.schedule_queued();
        save(&app, &cluster).await?;
        app.database
            .save_tasks(&scheduled)
            .map_err(internal_error)?;
        app.work.notify_waiters();
    }
    Ok(Json(added))
}

#[derive(Deserialize)]
struct RemoveIdle {
    #[serde(default)]
    force: bool,
}

async fn remove_idle_instance(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, pool)): AxumPath<(Uuid, String)>,
    Query(query): Query<RemoveIdle>,
) -> ApiResult<Json<bool>> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    let removed = if query.force {
        cluster.discard_idle_instance(id, &pool)
    } else {
        cluster.remove_idle_instance(id, &pool)
    };
    if removed {
        let scheduled = cluster.schedule_queued();
        save(&app, &cluster).await?;
        app.database
            .save_tasks(&scheduled)
            .map_err(internal_error)?;
        app.work.notify_waiters();
    }
    Ok(Json(removed))
}

async fn next_work(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<NextWork>,
) -> ApiResult<Json<Option<Work>>> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(query.wait_ms.min(2_000));
    loop {
        let notified = app.work.notified();
        let work = {
            let mut cluster = app.cluster.write().await;
            if !cluster.nodes.contains_key(&id) {
                return Err(error(StatusCode::NOT_FOUND, "node not found"));
            }
            let task = cluster
                .tasks
                .values_mut()
                .find(|task| task.node == Some(id) && task.status == TaskStatus::Assigned)
                .map(|task| {
                    task.status = TaskStatus::Running;
                    task.started_at_ms = Some(now_ms());
                    task.clone()
                });
            task.map(|task| Work {
                pool: cluster.pools[&task.pool].clone(),
                task,
                secrets: HashMap::new(),
            })
        };
        if let Some(mut work) = work {
            save_task(&app, &work.task).await?;
            let encrypted = app
                .database
                .task_secrets(work.task.id)
                .map_err(internal_error)?
                .unwrap_or_default();
            work.secrets = app
                .secrets
                .decrypt(work.task.id, &encrypted)
                .map_err(internal_error)?;
            return Ok(Json(Some(work)));
        }
        if query.wait_ms == 0 || tokio::time::timeout_at(deadline, notified).await.is_err() {
            return Ok(Json(None));
        }
    }
}

#[derive(Deserialize)]
struct NextWork {
    #[serde(default)]
    wait_ms: u64,
}

#[derive(Deserialize)]
struct CompleteTask {
    result: Option<Value>,
    error: Option<String>,
    timed_out: Option<bool>,
    #[serde(default)]
    metrics: Option<TaskMetrics>,
    #[serde(default)]
    keep_idle: bool,
}

async fn complete_task(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<CompleteTask>,
) -> ApiResult<StatusCode> {
    let node = authenticated_node(&app, &headers).await?;
    let mut cluster = app.cluster.write().await;
    let task = cluster
        .tasks
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    if task.node != Some(node) {
        return Err(error(StatusCode::FORBIDDEN, "task belongs to another node"));
    }
    task.status = if request.timed_out.unwrap_or(false) {
        TaskStatus::TimedOut
    } else if request.error.is_some() {
        TaskStatus::Failed
    } else {
        TaskStatus::Succeeded
    };
    task.result = request.result;
    task.error = request.error;
    task.metrics = request.metrics;
    task.finished_at_ms = Some(now_ms());
    cluster.release(id, request.keep_idle);
    let scheduled = cluster.schedule_queued();
    let task = cluster.tasks[&id].clone();
    cluster.tasks.remove(&id);
    save(&app, &cluster).await?;
    drop(cluster);
    app.database
        .save_tasks(&scheduled)
        .map_err(internal_error)?;
    app.database.complete_task(&task).map_err(internal_error)?;
    app.tasks.notify_waiters();
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    async fn app() -> App {
        let data_dir = std::env::temp_dir().join(format!("scraperack-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&data_dir).await.unwrap();
        let database = Database::open(&data_dir.join("scraperack.db")).unwrap();
        let secrets = SecretCipher::load(&data_dir.join("task-secrets.key")).unwrap();
        App {
            cluster: Arc::new(RwLock::new(Cluster::default())),
            database,
            secrets,
            controller_id: Uuid::new_v4(),
            plugins: Arc::new(Plugins::new(data_dir.join("plugins"), true)),
            data_dir,
            tasks: Arc::new(Notify::new()),
            work: Arc::new(Notify::new()),
            imports: Arc::default(),
        }
    }

    fn pool(name: &str, image: Uuid) -> Pool {
        Pool {
            name: name.into(),
            image,
            cpu_millis: 250,
            memory_mib: 256,
            disk_mib: 1024,
            timeout_secs: 60,
            idle_timeout_secs: 30,
            reuse: true,
            ..Default::default()
        }
    }

    async fn with_image(app: &App) -> Uuid {
        let image = Uuid::new_v4();
        app.cluster.write().await.images.insert(
            image,
            Image {
                id: image,
                name: "base".into(),
                sha256: String::new(),
                size: 0,
                compatible: true,
                executor: None,
                kind: ImageKind::MicroVm,
                docker_source: None,
            },
        );
        image
    }

    #[tokio::test]
    async fn plugins_own_their_pools() {
        let app = app().await;
        let image = with_image(&app).await;
        let plugin = Some(Extension(Caller("python".into())));
        let (_, Json(created)) =
            create_pool(State(app.clone()), plugin.clone(), Json(pool("p", image)))
                .await
                .unwrap();
        assert_eq!(created.managed_by.as_deref(), Some("plugin:python"));
        let denied = delete_pool(State(app.clone()), None, AxumPath("p".into()))
            .await
            .unwrap_err();
        assert_eq!(denied.0, StatusCode::FORBIDDEN);
        let other = Some(Extension(Caller("browser".into())));
        let denied = restart_pool(State(app.clone()), other, AxumPath("p".into()))
            .await
            .unwrap_err();
        assert_eq!(denied.0, StatusCode::FORBIDDEN);

        let mut changed = pool("p", image);
        changed.warm_instances = 2;
        let Json(updated) = update_pool(
            State(app.clone()),
            plugin.clone(),
            AxumPath("p".into()),
            Json(changed.clone()),
        )
        .await
        .unwrap();
        assert_eq!(updated.revision, 0, "warm count alone keeps VMs");
        changed.memory_mib = 512;
        let Json(updated) = update_pool(
            State(app.clone()),
            plugin.clone(),
            AxumPath("p".into()),
            Json(changed),
        )
        .await
        .unwrap();
        assert_eq!(updated.revision, 1, "memory change recycles VMs");
        assert_eq!(updated.managed_by.as_deref(), Some("plugin:python"));
        delete_pool(State(app.clone()), plugin, AxumPath("p".into()))
            .await
            .unwrap();
        fs::remove_dir_all(app.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn tasks_resolve_pool_aliases_and_require_an_executor() {
        let app = app().await;
        let image = with_image(&app).await;
        let mut vm_pool = pool("browsers", image);
        vm_pool.aliases = vec!["chrome".into()];
        let mut runner = pool("runner", image);
        runner.executor = vec!["/bin/executor".into()];
        runner.aliases = vec!["jobs".into()];
        runner.managed_by = Some("plugin:python".into());
        {
            let mut cluster = app.cluster.write().await;
            cluster.pools.insert(vm_pool.name.clone(), vm_pool);
            cluster.pools.insert(runner.name.clone(), runner);
        }
        let request = |target: &str| CreateTask {
            pool: target.into(),
            payload: json!({"encoding": "json", "data": 1}),
            variables: HashMap::new(),
            secrets: HashMap::new(),
            queue: true,
        };
        let (_, Json(task)) = create_task(State(app.clone()), Json(request("jobs")))
            .await
            .unwrap();
        assert_eq!(task.pool, "runner");
        assert_eq!(task.owner.as_deref(), Some("plugin:python"));
        let Json(owned) = list_tasks(
            State(app.clone()),
            Query(ListTasks {
                limit: None,
                pool: None,
                owner: Some("plugin:python".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(owned.len(), 1);
        let rejected = create_task(State(app.clone()), Json(request("chrome")))
            .await
            .unwrap_err();
        assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
        let unknown = create_task(State(app.clone()), Json(request("missing")))
            .await
            .unwrap_err();
        assert_eq!(unknown.0, StatusCode::BAD_REQUEST);
        fs::remove_dir_all(app.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn migrates_python_environments_to_plugin_pools() {
        let app = app().await;
        let data_dir = app.data_dir.clone();
        fs::create_dir_all(data_dir.join("environments"))
            .await
            .unwrap();
        fs::create_dir_all(data_dir.join("disks")).await.unwrap();
        fs::write(data_dir.join("environments").join("abc"), b"disk")
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let image = Uuid::new_v4();
        let mut cluster = Cluster::default();
        let mut legacy = pool("environment-x", image);
        legacy.managed_by = Some("python-executor".into());
        legacy.environment = Some(id);
        legacy.environment_artifact = Some("abc".into());
        cluster.pools.insert(legacy.name.clone(), legacy);
        cluster.environments.insert(
            id,
            crate::cluster::Environment {
                id,
                name: "jobsites".into(),
                image,
                packages: vec!["requests==2.0".into()],
                preload: Vec::new(),
                artifact: Some("abc".into()),
                sha256: Some("f00".into()),
                size: 4,
                pool: "environment-x".into(),
                warm_instances: 1,
                max_instances: None,
                cpu_millis: 250,
                memory_mib: 256,
                disk_mib: 1024,
                timeout_secs: 60,
                idle_timeout_secs: 30,
                reuse: true,
                network: true,
            },
        );
        migrate_environments(&mut cluster, &data_dir).await.unwrap();
        assert!(cluster.environments.is_empty());
        assert!(data_dir.join("disks").join("abc").exists());
        let pool = cluster.find_pool("jobsites").unwrap();
        assert_eq!(pool.name, "environment-x");
        assert!(cluster.find_pool(&id.to_string()).is_some());
        assert_eq!(pool.managed_by.as_deref(), Some("plugin:python"));
        assert_eq!(pool.disks[0].mount, LEGACY_ENVIRONMENT_MOUNT);
        assert_eq!(pool.executor[0], "/tmp/environment/bin/python");
        assert_eq!(cluster.disks["abc"].owner.as_deref(), Some("python"));
        let adopted = fs::read(
            data_dir
                .join("plugins")
                .join("python")
                .join("data")
                .join("migrated-environments.json"),
        )
        .await
        .unwrap();
        assert!(String::from_utf8_lossy(&adopted).contains("jobsites"));
        fs::remove_dir_all(data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn stores_working_directories_by_verified_hash() {
        let app = app().await;
        fs::create_dir(app.data_dir.join("working-dirs"))
            .await
            .unwrap();
        let body = Bytes::from_static(b"project archive");
        let digest = hex::encode(Sha256::digest(&body));
        assert_eq!(
            upload_working_dir(State(app.clone()), AxumPath(digest.clone()), body.clone(),)
                .await
                .unwrap(),
            StatusCode::CREATED
        );
        assert_eq!(
            working_dir_exists(State(app.clone()), AxumPath(digest))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            upload_working_dir(State(app), AxumPath("0".repeat(64)), body,)
                .await
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn join_tokens_are_single_use_and_create_authenticated_nodes() {
        let app = app().await;
        let token = create_join_token(State(app.clone())).await.unwrap().0;
        let plaintext = token.token.unwrap();
        assert!(
            !app.cluster
                .read()
                .await
                .join_tokens
                .values()
                .any(|token| token.token_hash == plaintext)
        );

        let credentials = join_cluster(
            State(app.clone()),
            Json(JoinRequest {
                token: plaintext.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", credentials.credential)).unwrap(),
        );
        assert_eq!(
            authenticated_node(&app, &headers).await.unwrap(),
            credentials.node_id
        );
        delete_node(State(app.clone()), AxumPath(credentials.node_id))
            .await
            .unwrap();
        assert_eq!(
            authenticated_node(&app, &headers).await.unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            join_cluster(State(app.clone()), Json(JoinRequest { token: plaintext }))
                .await
                .unwrap_err()
                .0,
            StatusCode::UNAUTHORIZED
        );
        fs::remove_dir_all(app.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn local_enrollment_rejects_remote_clients() {
        let app = app().await;
        let result = local_join(
            ConnectInfo("192.0.2.1:1234".parse().unwrap()),
            State(app.clone()),
            Json(LocalJoinRequest { id: None }),
        )
        .await;
        assert_eq!(result.unwrap_err().0, StatusCode::FORBIDDEN);
        fs::remove_dir_all(app.data_dir).await.unwrap();
    }

    fn import_request(name: &str) -> ImportImage {
        ImportImage {
            name: name.into(),
            reference: "https://registry.invalid/acme/browser:latest".into(),
            username: None,
            password: None,
        }
    }

    #[tokio::test]
    async fn imports_run_in_the_background_and_keep_their_log() {
        let app = app().await;
        let (StatusCode::ACCEPTED, Json(job)) =
            create_import(State(app.clone()), Json(import_request("browser")))
                .await
                .unwrap()
        else {
            panic!("import should be accepted");
        };
        assert_eq!(job.reference, "registry.invalid/acme/browser:latest");
        let taken = create_import(State(app.clone()), Json(import_request("browser")))
            .await
            .unwrap_err();
        assert!(
            taken.0 == StatusCode::CONFLICT
                || app.imports.jobs.lock().unwrap()[&job.id].job.status != ImportStatus::Running,
            "a running import reserves its name"
        );
        let finished = loop {
            let Json(current) = get_import(State(app.clone()), AxumPath(job.id))
                .await
                .unwrap();
            if current.status != ImportStatus::Running {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        assert_eq!(finished.status, ImportStatus::Failed);
        assert!(finished.error.is_some());
        assert!(finished.log.contains("Import failed"));
        assert!(
            !app.cluster
                .read()
                .await
                .images
                .contains_key(&finished.image)
        );
        let Json(listed) = list_imports(State(app.clone())).await;
        assert_eq!(listed.len(), 1);
        assert!(listed[0].log.is_empty(), "lists leave the log out");
        delete_import(State(app.clone()), AxumPath(job.id))
            .await
            .unwrap();
        assert!(list_imports(State(app.clone())).await.0.is_empty());
        fs::remove_dir_all(app.data_dir).await.unwrap();
    }
}
