//! Plugin manager: install, supervise, and proxy out-of-process plugins.
//!
//! A plugin is a `.tar.gz` of source code with a `scraperack-plugin.toml`
//! manifest. The controller unpacks it, runs its optional install command,
//! builds its optional `guest/` directory into a read-only disk, and runs it
//! as a supervised child process. The plugin serves HTTP on a Unix socket and
//! calls the controller's API on its own Unix socket, which identifies it.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Extension, Json, Router,
    body::{Body, Bytes},
    extract::{Path as AxumPath, Query, Request, State},
    http::{HeaderValue, StatusCode, header::HOST},
    response::{IntoResponse, Response},
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    process::Command,
    sync::{Mutex, watch},
};
use uuid::Uuid;

use crate::api::{
    ApiResult, App, Caller, error, internal_error, now_ms, remove_unused_disk, save,
    store_disk_file,
};
use crate::cluster::{PluginRecord, TaskStatus};

const MANIFEST: &str = "scraperack-plugin.toml";
const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub install: Vec<String>,
    pub run: Vec<String>,
    #[serde(default)]
    pub gui: bool,
    #[serde(default)]
    pub guest: Option<String>,
}

impl Manifest {
    fn validate(&self) -> Result<(), String> {
        let name_ok = !self.name.is_empty()
            && self.name.len() <= 32
            && self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        if !name_ok {
            return Err("plugin name must be 1-32 lowercase letters, digits, or dashes".into());
        }
        let version_ok = !self.version.is_empty()
            && self.version.len() <= 32
            && self
                .version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_+".contains(&byte));
        if !version_ok {
            return Err("plugin version must be 1-32 letters, digits, or . - _ +".into());
        }
        if self.run.is_empty() {
            return Err("manifest must declare a run command".into());
        }
        if let Some(guest) = &self.guest
            && !relative_path(guest)
        {
            return Err("guest must be a relative directory inside the plugin".into());
        }
        Ok(())
    }
}

fn relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && path.is_relative()
        && path
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginStatus {
    Stopped,
    Starting,
    Running,
    Failed,
}

struct Runtime {
    status: PluginStatus,
    message: Option<String>,
    stop: Option<watch::Sender<bool>>,
}

struct Staged {
    root: PathBuf,
    manifest: Manifest,
}

pub struct Plugins {
    root: PathBuf,
    allow_install: bool,
    router: OnceLock<Router>,
    runtimes: StdMutex<HashMap<String, Runtime>>,
    staged: StdMutex<HashMap<Uuid, Staged>>,
    operations: Mutex<()>,
}

impl Plugins {
    pub fn new(root: PathBuf, allow_install: bool) -> Self {
        Self {
            root,
            allow_install,
            router: OnceLock::new(),
            runtimes: StdMutex::new(HashMap::new()),
            staged: StdMutex::new(HashMap::new()),
            operations: Mutex::new(()),
        }
    }

    pub fn set_router(&self, router: Router) {
        let _ = self.router.set(router);
    }

    fn dir(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn version_dir(&self, name: &str, version: &str) -> PathBuf {
        self.dir(name).join("versions").join(version)
    }

    pub fn data_dir(&self, name: &str) -> PathBuf {
        self.dir(name).join("data")
    }

    fn plugin_socket(&self, name: &str) -> PathBuf {
        self.dir(name).join("plugin.sock")
    }

    fn core_socket(&self, name: &str) -> PathBuf {
        self.dir(name).join("core.sock")
    }

    fn log_path(&self, name: &str) -> PathBuf {
        self.dir(name).join("plugin.log")
    }

    fn set_status(&self, name: &str, status: PluginStatus, message: Option<String>) {
        let mut runtimes = self.runtimes.lock().expect("plugin state poisoned");
        let runtime = runtimes.entry(name.to_owned()).or_insert(Runtime {
            status: PluginStatus::Stopped,
            message: None,
            stop: None,
        });
        runtime.status = status;
        runtime.message = message;
    }

    fn status(&self, name: &str) -> (PluginStatus, Option<String>) {
        self.runtimes
            .lock()
            .expect("plugin state poisoned")
            .get(name)
            .map(|runtime| (runtime.status.clone(), runtime.message.clone()))
            .unwrap_or((PluginStatus::Stopped, None))
    }

    /// Start every enabled plugin; called once when the controller starts.
    pub async fn start_all(self: &Arc<Self>, records: Vec<PluginRecord>) {
        for record in records.into_iter().filter(|record| record.enabled) {
            if let Err(error) = self.start(&record).await {
                tracing::error!(plugin = %record.name, %error, "failed to start plugin");
                self.set_status(
                    &record.name,
                    PluginStatus::Failed,
                    Some(format!("{error:#}")),
                );
            }
        }
    }

    async fn start(self: &Arc<Self>, record: &PluginRecord) -> Result<()> {
        self.stop(&record.name).await;
        let directory = self.version_dir(&record.name, &record.version);
        let manifest = read_manifest(&directory).await?;
        let router = self
            .router
            .get()
            .cloned()
            .context("controller router is not ready")?;
        fs::create_dir_all(self.data_dir(&record.name)).await?;
        let core_socket = self.core_socket(&record.name);
        fs::remove_file(&core_socket).await.ok();
        let listener = UnixListener::bind(&core_socket)
            .with_context(|| format!("bind {}", core_socket.display()))?;
        let (stop, mut stopped) = watch::channel(false);
        self.runtimes.lock().expect("plugin state poisoned").insert(
            record.name.clone(),
            Runtime {
                status: PluginStatus::Starting,
                message: None,
                stop: Some(stop),
            },
        );
        let core = router.layer(Extension(Caller(record.name.clone())));
        let mut core_stopped = stopped.clone();
        tokio::spawn(async move {
            let server = axum::serve(listener, core);
            tokio::select! {
                result = server => if let Err(error) = result {
                    tracing::error!(%error, "plugin core socket failed");
                },
                _ = core_stopped.changed() => {}
            }
        });

        let plugins = self.clone();
        let name = record.name.clone();
        let env = [
            ("SCRAPERACK_PLUGIN_NAME", record.name.clone()),
            ("SCRAPERACK_PLUGIN_VERSION", record.version.clone()),
            (
                "SCRAPERACK_PLUGIN_SOCKET",
                self.plugin_socket(&record.name).display().to_string(),
            ),
            ("SCRAPERACK_CORE_SOCKET", core_socket.display().to_string()),
            (
                "SCRAPERACK_PLUGIN_DATA",
                self.data_dir(&record.name).display().to_string(),
            ),
            (
                "SCRAPERACK_PLUGIN_GUEST_DISK",
                record.guest_disk.clone().unwrap_or_default(),
            ),
        ];
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                let socket = plugins.plugin_socket(&name);
                fs::remove_file(&socket).await.ok();
                let log = match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(plugins.log_path(&name))
                {
                    Ok(log) => log,
                    Err(error) => {
                        plugins.set_status(&name, PluginStatus::Failed, Some(error.to_string()));
                        return;
                    }
                };
                let stderr = match log.try_clone() {
                    Ok(stderr) => stderr,
                    Err(error) => {
                        plugins.set_status(&name, PluginStatus::Failed, Some(error.to_string()));
                        return;
                    }
                };
                let spawned = Command::new(&manifest.run[0])
                    .args(&manifest.run[1..])
                    .current_dir(&directory)
                    .envs(env.iter().map(|(key, value)| (*key, value)))
                    .stdin(Stdio::null())
                    .stdout(Stdio::from(log))
                    .stderr(Stdio::from(stderr))
                    .kill_on_drop(true)
                    .spawn();
                let mut child = match spawned {
                    Ok(child) => child,
                    Err(error) => {
                        plugins.set_status(
                            &name,
                            PluginStatus::Failed,
                            Some(format!("could not start {}: {error}", manifest.run[0])),
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = stopped.changed() => return,
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(60));
                        continue;
                    }
                };
                plugins.set_status(&name, PluginStatus::Starting, None);
                let started = tokio::time::Instant::now();
                let watcher = {
                    let plugins = plugins.clone();
                    let name = name.clone();
                    let socket = socket.clone();
                    tokio::spawn(async move {
                        if wait_healthy(&socket, HEALTH_TIMEOUT).await {
                            plugins.set_status(&name, PluginStatus::Running, None);
                        }
                    })
                };
                tokio::select! {
                    status = child.wait() => {
                        watcher.abort();
                        let message = match status {
                            Ok(status) => format!("plugin exited with {status}"),
                            Err(error) => format!("plugin wait failed: {error}"),
                        };
                        tracing::warn!(plugin = %name, %message, "plugin stopped unexpectedly");
                        plugins.set_status(&name, PluginStatus::Failed, Some(message));
                        if started.elapsed() > Duration::from_secs(60) {
                            backoff = Duration::from_secs(1);
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = stopped.changed() => return,
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(60));
                    }
                    _ = stopped.changed() => {
                        watcher.abort();
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return;
                    }
                }
            }
        });
        Ok(())
    }

    async fn stop(&self, name: &str) {
        let stop = self
            .runtimes
            .lock()
            .expect("plugin state poisoned")
            .get_mut(name)
            .and_then(|runtime| runtime.stop.take());
        if let Some(stop) = stop {
            let _ = stop.send(true);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.set_status(name, PluginStatus::Stopped, None);
        fs::remove_file(self.plugin_socket(name)).await.ok();
        fs::remove_file(self.core_socket(name)).await.ok();
    }

    async fn wait_running(&self, name: &str) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + HEALTH_TIMEOUT + Duration::from_secs(2);
        loop {
            match self.status(name) {
                (PluginStatus::Running, _) => return Ok(()),
                (PluginStatus::Failed, message) => {
                    return Err(message.unwrap_or_else(|| "plugin failed".into()));
                }
                _ if tokio::time::Instant::now() >= deadline => {
                    return Err("plugin did not answer /health within 30 seconds".into());
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    }

    async fn log_tail(&self, name: &str) -> String {
        let log = fs::read(self.log_path(name)).await.unwrap_or_default();
        let start = log.len().saturating_sub(16 * 1024);
        String::from_utf8_lossy(&log[start..]).into_owned()
    }
}

async fn read_manifest(directory: &Path) -> Result<Manifest> {
    let text = fs::read_to_string(directory.join(MANIFEST))
        .await
        .with_context(|| format!("{MANIFEST} not found"))?;
    let manifest: Manifest = toml::from_str(&text).context("invalid plugin manifest")?;
    manifest.validate().map_err(anyhow::Error::msg)?;
    Ok(manifest)
}

async fn plugin_request(socket: &Path, request: Request) -> Result<Response> {
    let stream = UnixStream::connect(socket).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender.send_request(request).await?;
    Ok(response.map(Body::new))
}

async fn wait_healthy(socket: &Path, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let request = Request::builder()
            .uri("/health")
            .header(HOST, "plugin")
            .body(Body::empty())
            .expect("valid health request");
        if let Ok(Ok(response)) =
            tokio::time::timeout(Duration::from_secs(2), plugin_request(socket, request)).await
            && response.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn run_command(args: &[String], directory: &Path, env: &[(&str, String)]) -> Result<String> {
    let output = tokio::time::timeout(
        Duration::from_secs(20 * 60),
        Command::new(&args[0])
            .args(&args[1..])
            .current_dir(directory)
            .envs(env.iter().map(|(key, value)| (*key, value)))
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .context("install command timed out after 20 minutes")?
    .with_context(|| format!("could not run {}", args[0]))?;
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        bail!("install command failed ({})\n{log}", output.status);
    }
    Ok(log)
}

async fn shell(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("could not run {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Unpack an uploaded archive into a fresh directory and locate its manifest.
async fn unpack(archive: &Path, target: &Path) -> Result<(PathBuf, Manifest)> {
    let path = |value: &Path| value.to_str().context("invalid path").map(str::to_owned);
    let listing = shell("tar", &["-tvzf", &path(archive)?]).await?;
    for line in listing.lines() {
        if line.starts_with('l') || line.starts_with('h') || line.contains(" link to ") {
            bail!("plugin archives must not contain links");
        }
    }
    let names = shell("tar", &["-tzf", &path(archive)?]).await?;
    for name in names.lines() {
        let trimmed = name.trim_end_matches('/');
        if trimmed.is_empty() || trimmed == "." {
            continue;
        }
        let relative = trimmed.trim_start_matches("./");
        if !relative.is_empty() && !relative_path(relative) {
            bail!("unsafe path in plugin archive: {name}");
        }
    }
    fs::create_dir_all(target).await?;
    shell(
        "tar",
        &[
            "--no-same-owner",
            "-xzf",
            &path(archive)?,
            "-C",
            &path(target)?,
        ],
    )
    .await?;
    if target.join(MANIFEST).is_file() {
        let manifest = read_manifest(target).await?;
        return Ok((target.to_owned(), manifest));
    }
    let mut entries = fs::read_dir(target).await?;
    let mut directories = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            directories.push(entry.path());
        }
    }
    if let [only] = directories.as_slice()
        && only.join(MANIFEST).is_file()
    {
        let manifest = read_manifest(only).await?;
        return Ok((only.clone(), manifest));
    }
    bail!("{MANIFEST} must be at the top of the archive")
}

/// Build the plugin's guest directory into an ext4 disk and register it.
async fn build_guest_disk(app: &App, directory: &Path, owner: &str) -> Result<String> {
    let path = |value: &Path| value.to_str().context("invalid path").map(str::to_owned);
    if !directory.is_dir() {
        bail!("guest directory {} does not exist", directory.display());
    }
    let used = shell("du", &["-sb", &path(directory)?]).await?;
    let used = used
        .split_whitespace()
        .next()
        .context("could not measure guest directory")?
        .parse::<u64>()?;
    let step = 16 * 1024 * 1024;
    let size = (used + used / 5 + 8 * 1024 * 1024)
        .max(32 * 1024 * 1024)
        .div_ceil(step)
        * step;
    let work = app
        .data_dir
        .join("imports")
        .join(format!("guest-{}", Uuid::new_v4()));
    fs::create_dir_all(&work).await?;
    let disk = work.join("guest.ext4");
    let result = async {
        fs::File::create(&disk).await?.set_len(size).await?;
        shell(
            "mkfs.ext4",
            &["-q", "-F", "-d", &path(directory)?, &path(&disk)?],
        )
        .await?;
        store_disk_file(app, &disk, Some(owner))
            .await
            .map_err(|(_, Json(body))| anyhow::anyhow!("{}", body["error"]))
    }
    .await;
    fs::remove_dir_all(&work).await.ok();
    Ok(result?.key)
}

#[derive(Serialize)]
pub struct PluginView {
    #[serde(flatten)]
    record: PluginRecord,
    status: PluginStatus,
    message: Option<String>,
}

pub async fn list_plugins(State(app): State<App>) -> Json<Vec<PluginView>> {
    let records = app
        .cluster
        .read()
        .await
        .plugins
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut views = records
        .into_iter()
        .map(|record| {
            let (status, message) = app.plugins.status(&record.name);
            PluginView {
                record,
                status,
                message,
            }
        })
        .collect::<Vec<_>>();
    views.sort_by(|left, right| left.record.name.cmp(&right.record.name));
    Json(views)
}

pub async fn get_plugin_log(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<String> {
    if !app.cluster.read().await.plugins.contains_key(&name) {
        return Err(error(StatusCode::NOT_FOUND, "plugin not found"));
    }
    Ok(app.plugins.log_tail(&name).await)
}

#[derive(Serialize)]
pub struct UploadView {
    upload: Uuid,
    manifest: Manifest,
    installed_version: Option<String>,
    install_enabled: bool,
}

pub async fn upload_plugin(State(app): State<App>, body: Bytes) -> ApiResult<Json<UploadView>> {
    if body.len() > MAX_UPLOAD_BYTES {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "plugin archive is too large",
        ));
    }
    let id = Uuid::new_v4();
    let uploads = app.plugins.root.join(".uploads");
    fs::create_dir_all(&uploads).await.map_err(internal_error)?;
    let archive = uploads.join(format!("{id}.tar.gz"));
    let target = uploads.join(id.to_string());
    let mut file = fs::File::create(&archive).await.map_err(internal_error)?;
    file.write_all(&body).await.map_err(internal_error)?;
    file.flush().await.map_err(internal_error)?;
    drop(file);
    let unpacked = unpack(&archive, &target).await;
    fs::remove_file(&archive).await.ok();
    let (root, manifest) = match unpacked {
        Ok(unpacked) => unpacked,
        Err(cause) => {
            fs::remove_dir_all(&target).await.ok();
            return Err(error(StatusCode::BAD_REQUEST, format!("{cause:#}")));
        }
    };
    let installed_version = app
        .cluster
        .read()
        .await
        .plugins
        .get(&manifest.name)
        .map(|record| record.version.clone());
    app.plugins
        .staged
        .lock()
        .expect("plugin state poisoned")
        .insert(
            id,
            Staged {
                root,
                manifest: manifest.clone(),
            },
        );
    Ok(Json(UploadView {
        upload: id,
        manifest,
        installed_version,
        install_enabled: app.plugins.allow_install,
    }))
}

#[derive(Serialize)]
pub struct InstallResult {
    plugin: PluginRecord,
    log: String,
}

pub async fn install_upload(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<InstallResult>> {
    if !app.plugins.allow_install {
        return Err(error(
            StatusCode::FORBIDDEN,
            "plugin installation is disabled on this controller; set \
             SCRAPERACK_ALLOW_PLUGIN_INSTALL=true in /etc/scraperack/controller.env",
        ));
    }
    let staged = app
        .plugins
        .staged
        .lock()
        .expect("plugin state poisoned")
        .remove(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "upload not found; upload it again"))?;
    let _operation = app.plugins.operations.lock().await;
    let result = install(&app, &staged).await;
    fs::remove_dir_all(app.plugins.root.join(".uploads").join(id.to_string()))
        .await
        .ok();
    if result.is_err() {
        let manifest = &staged.manifest;
        let referenced = app
            .cluster
            .read()
            .await
            .plugins
            .get(&manifest.name)
            .is_some_and(|record| {
                record.version == manifest.version
                    || record.previous_version.as_ref() == Some(&manifest.version)
            });
        if !referenced {
            fs::remove_dir_all(app.plugins.version_dir(&manifest.name, &manifest.version))
                .await
                .ok();
        }
    }
    result
        .map(Json)
        .map_err(|cause| error(StatusCode::BAD_REQUEST, format!("{cause:#}")))
}

async fn install(app: &App, staged: &Staged) -> Result<InstallResult> {
    let manifest = &staged.manifest;
    let name = manifest.name.clone();
    let plugins = &app.plugins;
    let previous = app.cluster.read().await.plugins.get(&name).cloned();
    let directory = plugins.version_dir(&name, &manifest.version);
    let replacing_current = previous
        .as_ref()
        .is_some_and(|record| record.version == manifest.version);
    if replacing_current {
        plugins.stop(&name).await;
    }
    if directory.exists() {
        fs::remove_dir_all(&directory).await?;
    }
    fs::create_dir_all(directory.parent().context("invalid plugin directory")?).await?;
    fs::rename(&staged.root, &directory).await?;
    fs::create_dir_all(plugins.data_dir(&name)).await?;
    let mut log = String::new();
    let env = [
        ("SCRAPERACK_PLUGIN_NAME", name.clone()),
        ("SCRAPERACK_PLUGIN_VERSION", manifest.version.clone()),
        (
            "SCRAPERACK_PLUGIN_DATA",
            plugins.data_dir(&name).display().to_string(),
        ),
    ];
    if !manifest.install.is_empty() {
        log.push_str(&format!("$ {}\n", manifest.install.join(" ")));
        log.push_str(&run_command(&manifest.install, &directory, &env).await?);
    }
    let guest_disk = match &manifest.guest {
        Some(guest) => {
            let key = build_guest_disk(app, &directory.join(guest), &name).await?;
            log.push_str(&format!("Built guest disk {key}\n"));
            Some(key)
        }
        None => None,
    };
    let (previous_version, previous_guest_disk) = match &previous {
        Some(old) if old.version != manifest.version => {
            (Some(old.version.clone()), old.guest_disk.clone())
        }
        Some(old) => (
            old.previous_version.clone(),
            old.previous_guest_disk.clone(),
        ),
        None => (None, None),
    };
    let record = PluginRecord {
        name: name.clone(),
        version: manifest.version.clone(),
        description: manifest.description.clone(),
        previous_version,
        enabled: true,
        gui: manifest.gui,
        guest_disk,
        previous_guest_disk,
        installed_at_ms: now_ms(),
    };
    let save_record = |record: PluginRecord| async move {
        let mut cluster = app.cluster.write().await;
        cluster.plugins.insert(record.name.clone(), record);
        save(app, &cluster)
            .await
            .map_err(|(_, Json(body))| anyhow::anyhow!("{}", body["error"]))
    };
    save_record(record.clone()).await?;
    plugins.start(&record).await?;
    if let Err(message) = plugins.wait_running(&name).await {
        let tail = plugins.log_tail(&name).await;
        if let Some(old) = previous.filter(|old| old.version != record.version)
            && plugins.version_dir(&name, &old.version).is_dir()
        {
            save_record(old.clone()).await?;
            plugins.start(&old).await?;
            plugins.wait_running(&name).await.ok();
            prune_guest_disks(app, &record, &old).await;
            bail!(
                "{name} {} did not start ({message}); rolled back to {}\n{log}{tail}",
                record.version,
                old.version
            );
        }
        bail!(
            "{name} {} did not start ({message})\n{log}{tail}",
            record.version
        );
    }
    prune_versions(plugins, &record).await;
    if let Some(old) = &previous {
        prune_guest_disks(app, old, &record).await;
    }
    log.push_str(&format!("{name} {} is running\n", record.version));
    Ok(InstallResult {
        plugin: record,
        log,
    })
}

/// Remove guest disks `old` referenced that `current` no longer keeps.
async fn prune_guest_disks(app: &App, old: &PluginRecord, current: &PluginRecord) {
    let keep = [&current.guest_disk, &current.previous_guest_disk];
    for key in [&old.guest_disk, &old.previous_guest_disk]
        .into_iter()
        .flatten()
    {
        if !keep.iter().any(|kept| kept.as_ref() == Some(key)) {
            let _ = remove_unused_disk(app, key).await;
        }
    }
}

async fn prune_versions(plugins: &Plugins, record: &PluginRecord) {
    let Ok(mut entries) = fs::read_dir(plugins.dir(&record.name).join("versions")).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let version = entry.file_name().to_string_lossy().into_owned();
        if version != record.version && Some(&version) != record.previous_version.as_ref() {
            fs::remove_dir_all(entry.path()).await.ok();
        }
    }
}

async fn set_enabled(app: &App, name: &str, enabled: bool) -> ApiResult<Json<PluginRecord>> {
    let _operation = app.plugins.operations.lock().await;
    let record = {
        let mut cluster = app.cluster.write().await;
        let record = cluster
            .plugins
            .get_mut(name)
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "plugin not found"))?;
        record.enabled = enabled;
        let record = record.clone();
        save(app, &cluster).await?;
        record
    };
    if enabled {
        app.plugins
            .start(&record)
            .await
            .map_err(|cause| error(StatusCode::INTERNAL_SERVER_ERROR, format!("{cause:#}")))?;
    } else {
        app.plugins.stop(name).await;
    }
    Ok(Json(record))
}

pub async fn enable_plugin(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<PluginRecord>> {
    set_enabled(&app, &name, true).await
}

pub async fn disable_plugin(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<PluginRecord>> {
    set_enabled(&app, &name, false).await
}

pub async fn rollback_plugin(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<PluginRecord>> {
    let _operation = app.plugins.operations.lock().await;
    let record = {
        let mut cluster = app.cluster.write().await;
        let record = cluster
            .plugins
            .get_mut(&name)
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "plugin not found"))?;
        let previous = record
            .previous_version
            .clone()
            .filter(|version| app.plugins.version_dir(&name, version).is_dir())
            .ok_or_else(|| error(StatusCode::CONFLICT, "no previous version to roll back to"))?;
        record.previous_version = Some(std::mem::replace(&mut record.version, previous));
        std::mem::swap(&mut record.guest_disk, &mut record.previous_guest_disk);
        record.enabled = true;
        let record = record.clone();
        save(&app, &cluster).await?;
        record
    };
    app.plugins
        .start(&record)
        .await
        .map_err(|cause| error(StatusCode::INTERNAL_SERVER_ERROR, format!("{cause:#}")))?;
    app.plugins
        .wait_running(&name)
        .await
        .map_err(|message| error(StatusCode::BAD_GATEWAY, message))?;
    Ok(Json(record))
}

#[derive(Deserialize)]
pub struct RemoveOptions {
    #[serde(default)]
    keep_data: bool,
}

pub async fn remove_plugin(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
    Query(options): Query<RemoveOptions>,
) -> ApiResult<StatusCode> {
    let _operation = app.plugins.operations.lock().await;
    let owner = format!("plugin:{name}");
    {
        let cluster = app.cluster.read().await;
        if !cluster.plugins.contains_key(&name) {
            return Err(error(StatusCode::NOT_FOUND, "plugin not found"));
        }
        let owned = cluster
            .pools
            .values()
            .filter(|pool| pool.managed_by.as_deref() == Some(owner.as_str()))
            .map(|pool| pool.name.clone())
            .collect::<Vec<_>>();
        if cluster.tasks.values().any(|task| {
            owned.contains(&task.pool)
                && matches!(
                    task.status,
                    TaskStatus::Queued | TaskStatus::Assigned | TaskStatus::Running
                )
        }) {
            return Err(error(
                StatusCode::CONFLICT,
                "plugin pools have active tasks",
            ));
        }
    }
    app.plugins.stop(&name).await;
    let removed_disks = {
        let mut cluster = app.cluster.write().await;
        let pools = cluster
            .pools
            .values()
            .filter(|pool| pool.managed_by.as_deref() == Some(owner.as_str()))
            .map(|pool| pool.name.clone())
            .collect::<Vec<_>>();
        for pool in pools {
            crate::api::remove_pool(&mut cluster, &pool);
        }
        let disks = cluster
            .disks
            .values()
            .filter(|disk| disk.owner.as_deref() == Some(name.as_str()))
            .map(|disk| disk.key.clone())
            .filter(|key| {
                !cluster
                    .pools
                    .values()
                    .any(|pool| pool.disks.iter().any(|disk| &disk.key == key))
            })
            .collect::<Vec<_>>();
        for key in &disks {
            cluster.disks.remove(key);
            for node in cluster.nodes.values_mut() {
                node.disks.remove(key);
            }
        }
        cluster.plugins.remove(&name);
        app.plugins
            .runtimes
            .lock()
            .expect("plugin state poisoned")
            .remove(&name);
        save(&app, &cluster).await?;
        disks
    };
    for key in removed_disks {
        fs::remove_file(app.data_dir.join("disks").join(key))
            .await
            .ok();
    }
    let directory = app.plugins.dir(&name);
    fs::remove_dir_all(directory.join("versions")).await.ok();
    fs::remove_file(app.plugins.log_path(&name)).await.ok();
    if !options.keep_data {
        fs::remove_dir_all(&directory).await.ok();
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Forward `/plugins/{name}/{gui|api}/...` to the plugin's own HTTP server.
pub async fn proxy(
    State(app): State<App>,
    AxumPath((name, rest)): AxumPath<(String, String)>,
    mut request: Request,
) -> Response {
    let section = rest.split('/').next().unwrap_or_default();
    if section != "gui" && section != "api" {
        return error(
            StatusCode::NOT_FOUND,
            "plugin routes start with gui/ or api/",
        )
        .into_response();
    }
    match app.cluster.read().await.plugins.get(&name) {
        None => return error(StatusCode::NOT_FOUND, "plugin not found").into_response(),
        Some(record) if !record.enabled => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("plugin {name} is disabled"),
            )
            .into_response();
        }
        Some(_) => {}
    }
    let query = request
        .uri()
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    let uri = format!("/{rest}{query}");
    match uri.parse() {
        Ok(uri) => *request.uri_mut() = uri,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid plugin path").into_response(),
    }
    request
        .headers_mut()
        .insert(HOST, HeaderValue::from_static("plugin"));
    if let Ok(prefix) = HeaderValue::from_str(&format!("/plugins/{name}")) {
        request.headers_mut().insert("x-forwarded-prefix", prefix);
    }
    match plugin_request(&app.plugins.plugin_socket(&name), request).await {
        Ok(response) => response,
        Err(cause) => error(
            StatusCode::BAD_GATEWAY,
            format!("plugin {name} is not reachable: {cause}"),
        )
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_manifests() {
        let manifest: Manifest = toml::from_str(
            r#"
            name = "python"
            version = "0.1.0"
            run = ["python3", "service.py"]
            gui = true
            guest = "guest"
            "#,
        )
        .unwrap();
        assert!(manifest.validate().is_ok());
        for (name, version, guest) in [
            ("Python", "0.1.0", None),
            ("python", "0.1 0", None),
            ("python", "0.1.0", Some("../escape")),
            ("python", "0.1.0", Some("/abs")),
        ] {
            let mut bad = manifest.clone();
            bad.name = name.into();
            bad.version = version.into();
            bad.guest = guest.map(str::to_owned);
            assert!(bad.validate().is_err(), "{name} {version} {guest:?}");
        }
    }

    #[test]
    fn rejects_unsafe_relative_paths() {
        assert!(relative_path("guest"));
        assert!(relative_path("guest/bin"));
        assert!(!relative_path("../guest"));
        assert!(!relative_path("guest/../../x"));
        assert!(!relative_path("/etc"));
        assert!(!relative_path(""));
    }
}
