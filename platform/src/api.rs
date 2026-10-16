use std::{
    collections::HashSet,
    ffi::OsStr,
    net::IpAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
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
    Cluster, Environment, Execution, ExecutionMetrics, ExecutionStatus, Image, ImageKind,
    JoinToken, MicroVm, Node, NodeStatus, Pool, Work,
};

const JOIN_TOKEN_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Clone)]
struct App {
    cluster: Arc<RwLock<Cluster>>,
    data_dir: PathBuf,
    executions: Arc<Notify>,
    work: Arc<Notify>,
}

type ApiResult<T> = Result<T, (StatusCode, Json<Value>)>;

fn error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message.into() })))
}

fn now_ms() -> u64 {
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

fn migrate_legacy(cluster: &mut Cluster) {
    let legacy = cluster
        .pools
        .values()
        .filter(|pool| {
            pool.managed_by.is_none()
                && cluster
                    .images
                    .get(&pool.image)
                    .and_then(|image| image.executor.as_ref())
                    .is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    for old in legacy {
        let id = Uuid::new_v4();
        let environment = Environment {
            id,
            name: old.name.clone(),
            image: old.image,
            packages: Vec::new(),
            preload: Vec::new(),
            artifact: None,
            sha256: None,
            size: 0,
            pool: old.name.clone(),
            warm_instances: old.warm_instances,
            max_instances: old.max_instances,
            cpu_millis: old.cpu_millis,
            memory_mib: old.memory_mib,
            disk_mib: old.disk_mib,
            timeout_secs: old.timeout_secs,
            idle_timeout_secs: old.idle_timeout_secs,
            reuse: old.reuse,
            network: old.network,
        };
        if let Some(pool) = cluster.pools.get_mut(&old.name) {
            pool.managed_by = Some("python-executor".into());
            pool.environment = Some(id);
        }
        for execution in cluster
            .executions
            .values_mut()
            .filter(|execution| execution.pool == old.name)
        {
            execution.environment = Some(id);
        }
        cluster.environments.insert(id, environment);
    }
}

async fn save(app: &App, cluster: &Cluster) -> ApiResult<()> {
    let bytes = serde_json::to_vec(cluster)
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let temporary = app.data_dir.join("state.json.tmp");
    fs::write(&temporary, bytes)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    fs::rename(temporary, app.data_dir.join("state.json"))
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn save_execution(app: &App, execution: &Execution) -> ApiResult<()> {
    let bytes = serde_json::to_vec(execution)
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let path = app
        .data_dir
        .join("executions")
        .join(format!("{}.json", execution.id));
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    fs::rename(temporary, path)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn load_executions(data_dir: &Path, cluster: &mut Cluster) -> Result<()> {
    let directory = data_dir.join("executions");
    fs::create_dir_all(&directory).await?;
    for execution in cluster.executions.values() {
        fs::write(
            directory.join(format!("{}.json", execution.id)),
            serde_json::to_vec(execution)?,
        )
        .await?;
    }
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }
        let mut execution: Execution = serde_json::from_slice(&fs::read(entry.path()).await?)?;
        if !matches!(
            execution.status,
            ExecutionStatus::Succeeded | ExecutionStatus::Failed | ExecutionStatus::TimedOut
        ) {
            execution.status = ExecutionStatus::Queued;
            execution.node = None;
            execution.started_at_ms = None;
        }
        cluster.executions.insert(execution.id, execution);
    }
    Ok(())
}

pub async fn serve(listen: std::net::SocketAddr, data_dir: PathBuf) -> Result<()> {
    fs::create_dir_all(data_dir.join("images")).await?;
    fs::create_dir_all(data_dir.join("environments")).await?;
    let mut cluster: Cluster = match fs::read(data_dir.join("state.json")).await {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Cluster::default(),
        Err(e) => return Err(e.into()),
    };
    load_executions(&data_dir, &mut cluster).await?;
    migrate_legacy(&mut cluster);
    let app = App {
        cluster: Arc::new(RwLock::new(cluster)),
        data_dir,
        executions: Arc::new(Notify::new()),
        work: Arc::new(Notify::new()),
    };
    let router = Router::new()
        .route("/health", get(|| async { Json(json!({ "ok": true })) }))
        .route("/v1/images", get(list_images))
        .route("/v1/images/import", post(import_image))
        .route(
            "/v1/images/{key}",
            axum::routing::patch(update_image).delete(delete_image),
        )
        .route("/v1/images/{id}/artifact", get(download_image))
        .route("/v1/pools", get(list_pools).post(create_pool))
        .route("/v1/pools/{name}", delete(delete_pool))
        .route(
            "/v1/environments",
            get(list_environments).post(create_environment),
        )
        .route(
            "/v1/environments/{id}",
            axum::routing::put(update_environment).delete(delete_environment),
        )
        .route("/v1/environments/{id}/restart", post(restart_environment))
        .route("/v1/environments/{id}/artifact", get(download_environment))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/nodes/{id}", delete(delete_node))
        .route("/v1/join-tokens", post(create_join_token))
        .route("/v1/join-tokens/{id}", get(get_join_token))
        .route("/v1/vms", get(list_vms))
        .route(
            "/v1/executions",
            get(list_executions).post(create_execution),
        )
        .route("/v1/executions/{id}", get(get_execution))
        .route("/v1/executions/{id}/wait", get(wait_execution))
        .route("/internal/nodes/register", post(register_node))
        .route("/internal/join", post(join_cluster))
        .route("/internal/local-join", post(local_join))
        .route(
            "/internal/nodes/{id}/images/{image}/ready",
            post(image_ready),
        )
        .route(
            "/internal/nodes/{id}/environments/{artifact}/ready",
            post(environment_ready),
        )
        .route(
            "/internal/nodes/{id}/pools/{pool}/idle",
            get(warm_instance_needed)
                .post(add_warm_instance)
                .delete(remove_idle_instance),
        )
        .route("/internal/nodes/{id}/work", get(next_work))
        .route(
            "/internal/executions/{id}/complete",
            post(complete_execution),
        )
        .layer(DefaultBodyLimit::disable())
        .with_state(app);

    tracing::info!(%listen, "controller listening");
    axum::serve(
        TcpListener::bind(listen).await?,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
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
    kind: Option<ImageKind>,
    username: Option<String>,
    password: Option<String>,
}

async fn import_image(
    State(app): State<App>,
    Json(request): Json<ImportImage>,
) -> ApiResult<(StatusCode, Json<Image>)> {
    let name = request.name.trim();
    let reference = request
        .reference
        .trim()
        .strip_prefix("http://")
        .or_else(|| request.reference.trim().strip_prefix("https://"))
        .unwrap_or(request.reference.trim());
    if name.is_empty() || reference.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name and Docker image are required",
        ));
    }
    let username = request
        .username
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let password = request
        .password
        .as_deref()
        .filter(|value| !value.is_empty());
    if username.is_some() != password.is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "registry username and password must be provided together",
        ));
    }
    if app
        .cluster
        .read()
        .await
        .images
        .values()
        .any(|image| image.name == name)
    {
        return Err(error(StatusCode::CONFLICT, "image name already exists"));
    }
    let work = app
        .data_dir
        .join("imports")
        .join(Uuid::new_v4().to_string());
    fs::create_dir_all(&work)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (artifact, executor, docker_source) =
        match build_docker_image(reference, &work, username, password).await {
            Ok(built) => built,
            Err(message) => {
                fs::remove_dir_all(&work).await.ok();
                return Err(error(StatusCode::BAD_REQUEST, message));
            }
        };
    let id = Uuid::new_v4();
    let path = app.data_dir.join("images").join(id.to_string());
    fs::rename(&artifact, &path)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    fs::remove_dir_all(&work).await.ok();
    let size = fs::metadata(&path)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .len();
    let kind = request.kind.unwrap_or(if executor.is_some() {
        ImageKind::Runtime
    } else {
        ImageKind::MicroVm
    });
    if kind == ImageKind::Runtime && executor.is_none() {
        fs::remove_file(&path).await.ok();
        return Err(error(
            StatusCode::BAD_REQUEST,
            "runtime image must contain cloudpickle",
        ));
    }
    let image = Image {
        id,
        name: name.into(),
        sha256: hash_file(&path)
            .await
            .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
        size,
        compatible: true,
        kind,
        executor: (kind == ImageKind::Runtime).then_some(executor).flatten(),
        docker_source: Some(docker_source),
    };
    let mut cluster = app.cluster.write().await;
    for node in cluster.nodes.values_mut() {
        node.status = NodeStatus::Syncing;
    }
    cluster.images.insert(id, image.clone());
    save(&app, &cluster).await?;
    Ok((StatusCode::CREATED, Json(image)))
}

async fn build_docker_image(
    reference: &str,
    work: &Path,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(PathBuf, Option<String>, String), String> {
    pull_docker_image(reference, work, username, password).await?;
    let inspect = command("docker", ["image", "inspect", reference]).await?;
    let inspect: Vec<Value> = serde_json::from_str(&inspect).map_err(|e| e.to_string())?;
    let platform = inspect.first().ok_or("Docker image was not found")?;
    let docker_source = platform["Id"]
        .as_str()
        .ok_or("Docker image has no content ID")?
        .to_owned();
    if platform["Os"].as_str() != Some("linux")
        || platform["Architecture"].as_str() != Some("amd64")
    {
        return Err("Docker image must be linux/amd64".into());
    }
    let python = command(
        "docker",
        [
            "run",
            "--rm",
            "--entrypoint",
            "/usr/bin/python3",
            reference,
            "-c",
            "import sys; print(sys.version.split()[0])",
        ],
    )
    .await
    .map_err(|_| {
        "Docker image must contain /usr/bin/python3 for the Scraperack guest agent".to_string()
    })?;
    let executor = command(
        "docker",
        [
            "run",
            "--rm",
            "--entrypoint",
            "/usr/bin/python3",
            reference,
            "-c",
            "import cloudpickle",
        ],
    )
    .await
    .ok()
    .map(|_| format!("Python {}", python.trim()));
    let container = command("docker", ["create", reference]).await?;
    let container = container.trim();
    let archive = work.join("rootfs.tar");
    let exported = command("docker", ["export", "--output", path(&archive)?, container]).await;
    let _ = command("docker", ["rm", "-f", container]).await;
    exported?;
    let root = work.join("rootfs");
    let state = work.join("fakeroot.state");
    fs::create_dir(&root).await.map_err(|e| e.to_string())?;
    command(
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
    let agent = work.join("scraperack-init");
    let python_executor = work.join("scraperack-python-executor");
    fs::write(&agent, include_bytes!("../guest/scraperack-init"))
        .await
        .map_err(|e| e.to_string())?;
    fs::write(
        &python_executor,
        include_bytes!("../guest/scraperack-python-executor"),
    )
    .await
    .map_err(|e| e.to_string())?;
    install(&state, &agent, &root.join("sbin/scraperack-init"), "0755").await?;
    if executor.is_some() {
        install(
            &state,
            &python_executor,
            &root.join("opt/scraperack/executors/python"),
            "0755",
        )
        .await?;
    }
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
    let used = command("du", ["-sb", path(&root)?]).await?;
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
    command(
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
    Ok((artifact, executor, docker_source))
}

async fn pull_docker_image(
    reference: &str,
    work: &Path,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), String> {
    let authfile = work.join("registry-auth.json");
    if let (Some(username), Some(password)) = (username, password) {
        command_input(
            "skopeo",
            [
                "login",
                "--tls-verify=false",
                "--authfile",
                path(&authfile)?,
                "--username",
                username,
                "--password-stdin",
                registry(reference),
            ],
            password,
        )
        .await?;
    }
    let mut args = vec!["copy".to_owned(), "--src-tls-verify=false".to_owned()];
    if username.is_some() {
        args.extend(["--src-authfile".to_owned(), path(&authfile)?.to_owned()]);
    }
    let archive = work.join("docker-image.tar");
    args.extend([
        format!("docker://{reference}"),
        format!("docker-archive:{}:{reference}", path(&archive)?),
    ]);
    let result = match command("skopeo", args).await {
        Ok(_) => command("docker", ["load", "--input", path(&archive)?])
            .await
            .map(|_| ()),
        Err(error) => Err(error),
    };
    fs::remove_file(authfile).await.ok();
    fs::remove_file(archive).await.ok();
    result
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

fn environment_key(image_sha256: &str, packages: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"v2");
    digest.update(image_sha256.as_bytes());
    for package in packages {
        digest.update([0]);
        digest.update(package.as_bytes());
    }
    hex::encode(digest.finalize())
}

async fn build_environment(
    source: &str,
    packages: &[String],
    work: &Path,
    destination: &Path,
) -> Result<(), String> {
    let mut args = vec![
        "create".to_owned(),
        "--entrypoint".to_owned(),
        "/bin/sh".to_owned(),
        source.to_owned(),
        "-c".to_owned(),
        "/usr/bin/python3 -m venv /tmp/environment && /tmp/environment/bin/python -m pip install --no-cache-dir \"$@\"".to_owned(),
        "--".to_owned(),
        "cloudpickle".to_owned(),
    ];
    args.extend(packages.iter().cloned());
    let container = command("docker", args).await?;
    let container = container.trim();
    let result = async {
        command("docker", ["start", "-a", container]).await?;
        let root = work.join("root");
        fs::create_dir(&root).await.map_err(|e| e.to_string())?;
        command(
            "docker",
            [
                "cp",
                &format!("{container}:/tmp/environment/."),
                path(&root)?,
            ],
        )
        .await?;
        let used = command("du", ["-sb", path(&root)?]).await?;
        let used = used
            .split_whitespace()
            .next()
            .ok_or("could not measure environment")?
            .parse::<u64>()
            .map_err(|e| e.to_string())?;
        let step = 16 * 1024 * 1024;
        let size = (used + used / 5 + 8 * 1024 * 1024)
            .max(32 * 1024 * 1024)
            .div_ceil(step)
            * step;
        let artifact = work.join("environment.ext4");
        fs::File::create(&artifact)
            .await
            .map_err(|e| e.to_string())?
            .set_len(size)
            .await
            .map_err(|e| e.to_string())?;
        command(
            "mkfs.ext4",
            ["-q", "-F", "-d", path(&root)?, path(&artifact)?],
        )
        .await?;
        fs::rename(artifact, destination)
            .await
            .map_err(|e| e.to_string())
    }
    .await;
    let _ = command("docker", ["rm", "-f", container]).await;
    result
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
    executor: Option<String>,
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
    image.executor = update.executor.filter(|value| !value.trim().is_empty());
    image.kind = if image.executor.is_some() {
        ImageKind::Runtime
    } else {
        ImageKind::MicroVm
    };
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

async fn delete_image(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.pools.values().any(|pool| pool.image == id)
        || cluster
            .environments
            .values()
            .any(|environment| environment.image == id)
    {
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
    fs::remove_file(app.data_dir.join("images").join(id.to_string()))
        .await
        .ok();
    if let Some(source) = docker_source {
        let _ = command("docker", ["image", "rm", "-f", source.as_str()]).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn list_pools(State(app): State<App>) -> Json<Vec<Pool>> {
    Json(app.cluster.read().await.pools.values().cloned().collect())
}

async fn create_pool(
    State(app): State<App>,
    Json(pool): Json<Pool>,
) -> ApiResult<(StatusCode, Json<Pool>)> {
    let mut cluster = app.cluster.write().await;
    let Some(image) = cluster.images.get(&pool.image) else {
        return Err(error(StatusCode::BAD_REQUEST, "unknown image"));
    };
    if image.kind != ImageKind::MicroVm {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "runtime images belong to execution environments",
        ));
    }
    if pool.managed_by.is_some() || pool.environment.is_some() || !pool.preload.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "managed fields are reserved",
        ));
    }
    if !image.compatible {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "image is not Scraperack compatible",
        ));
    }
    if !cluster.image_ready(pool.image) {
        return Err(error(StatusCode::CONFLICT, "image is still distributing"));
    }
    if cluster.pools.contains_key(&pool.name) {
        return Err(error(StatusCode::CONFLICT, "pool already exists"));
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
    cluster.pools.insert(pool.name.clone(), pool.clone());
    save(&app, &cluster).await?;
    Ok((StatusCode::CREATED, Json(pool)))
}

async fn delete_pool(
    State(app): State<App>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.executions.values().any(|execution| {
        execution.pool == name
            && matches!(
                execution.status,
                ExecutionStatus::Queued | ExecutionStatus::Assigned | ExecutionStatus::Running
            )
    }) {
        return Err(error(StatusCode::CONFLICT, "pool has active executions"));
    }
    let Some(pool) = cluster.pools.remove(&name) else {
        return Err(error(StatusCode::NOT_FOUND, "pool not found"));
    };
    for node in cluster.nodes.values_mut() {
        let idle = node.idle_instances.remove(&name).unwrap_or(0);
        node.used_cpu_millis = node
            .used_cpu_millis
            .saturating_sub(pool.cpu_millis.saturating_mul(idle));
        node.used_memory_mib = node
            .used_memory_mib
            .saturating_sub(pool.memory_mib.saturating_mul(idle));
    }
    save(&app, &cluster).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct CreateEnvironment {
    name: String,
    image: Uuid,
    #[serde(default)]
    packages: Vec<String>,
    #[serde(default)]
    preload: Vec<String>,
    #[serde(default)]
    warm_instances: u32,
    max_instances: Option<u32>,
    #[serde(default = "default_cpu_millis")]
    cpu_millis: u32,
    #[serde(default = "default_memory_mib")]
    memory_mib: u32,
    #[serde(default = "default_disk_mib")]
    disk_mib: u32,
    #[serde(default = "default_timeout_secs")]
    timeout_secs: u64,
    #[serde(default = "default_idle_timeout_secs")]
    idle_timeout_secs: u64,
    #[serde(default = "default_true")]
    reuse: bool,
    #[serde(default = "default_true")]
    network: bool,
}

fn default_cpu_millis() -> u32 {
    1000
}

fn default_memory_mib() -> u32 {
    512
}

fn default_disk_mib() -> u32 {
    1024
}

fn default_timeout_secs() -> u64 {
    300
}

fn default_idle_timeout_secs() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

#[derive(Serialize)]
struct EnvironmentView {
    #[serde(flatten)]
    environment: Environment,
    ready: bool,
}

async fn list_environments(State(app): State<App>) -> Json<Vec<EnvironmentView>> {
    let cluster = app.cluster.read().await;
    Json(
        cluster
            .environments
            .values()
            .map(|environment| EnvironmentView {
                environment: environment.clone(),
                ready: cluster.environment_ready(environment),
            })
            .collect(),
    )
}

async fn prepare_environment_artifact(
    app: &App,
    image: &Image,
    packages: &[String],
) -> ApiResult<(Option<String>, Option<String>, u64)> {
    if packages.is_empty() {
        return Ok((None, None, 0));
    }
    let source = image.docker_source.as_deref().ok_or_else(|| {
        error(
            StatusCode::BAD_REQUEST,
            "package environments require a Docker-imported runtime image",
        )
    })?;
    let key = environment_key(&image.sha256, packages);
    let artifact = app.data_dir.join("environments").join(&key);
    if !artifact.exists() {
        let work = app
            .data_dir
            .join("imports")
            .join(format!("environment-{}", Uuid::new_v4()));
        fs::create_dir_all(&work)
            .await
            .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        if let Err(message) = build_environment(source, packages, &work, &artifact).await {
            fs::remove_dir_all(&work).await.ok();
            return Err(error(StatusCode::BAD_REQUEST, message));
        }
        fs::remove_dir_all(&work).await.ok();
    }
    let size = fs::metadata(&artifact)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .len();
    let sha256 = hash_file(&artifact)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((Some(key), Some(sha256), size))
}

async fn create_environment(
    State(app): State<App>,
    Json(mut request): Json<CreateEnvironment>,
) -> ApiResult<(StatusCode, Json<EnvironmentView>)> {
    request.name = request.name.trim().to_owned();
    request.packages = request
        .packages
        .into_iter()
        .map(|package| package.trim().to_owned())
        .filter(|package| !package.is_empty())
        .collect();
    request.packages.sort();
    request.packages.dedup();
    request.preload = request
        .preload
        .into_iter()
        .map(|module| module.trim().to_owned())
        .filter(|module| !module.is_empty())
        .collect();
    request.preload.sort();
    request.preload.dedup();
    if request.name.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "environment name is required",
        ));
    }
    if request.max_instances.is_some_and(|limit| limit == 0)
        || request
            .max_instances
            .is_some_and(|limit| request.warm_instances > limit)
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid instance limits"));
    }
    let image = {
        let cluster = app.cluster.read().await;
        if cluster
            .environments
            .values()
            .any(|environment| environment.name == request.name)
        {
            return Err(error(
                StatusCode::CONFLICT,
                "environment name already exists",
            ));
        }
        let image = cluster
            .images
            .get(&request.image)
            .cloned()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "unknown runtime image"))?;
        if image.kind != ImageKind::Runtime
            || !image
                .executor
                .as_deref()
                .is_some_and(|executor| executor.to_ascii_lowercase().starts_with("python "))
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "image does not provide the Python executor",
            ));
        }
        if !cluster.image_ready(image.id) {
            return Err(error(
                StatusCode::CONFLICT,
                "runtime image is still distributing",
            ));
        }
        image
    };
    let (artifact, sha256, size) =
        prepare_environment_artifact(&app, &image, &request.packages).await?;
    let id = Uuid::new_v4();
    let pool_name = format!("environment-{id}");
    let environment = Environment {
        id,
        name: request.name,
        image: image.id,
        packages: request.packages,
        preload: request.preload,
        artifact: artifact.clone(),
        sha256,
        size,
        pool: pool_name.clone(),
        warm_instances: request.warm_instances,
        max_instances: request.max_instances,
        cpu_millis: request.cpu_millis,
        memory_mib: request.memory_mib,
        disk_mib: request.disk_mib,
        timeout_secs: request.timeout_secs,
        idle_timeout_secs: request.idle_timeout_secs,
        reuse: request.reuse,
        network: request.network,
    };
    let pool = Pool {
        name: pool_name.clone(),
        image: image.id,
        warm_instances: environment.warm_instances,
        max_instances: environment.max_instances,
        cpu_millis: environment.cpu_millis,
        memory_mib: environment.memory_mib,
        disk_mib: environment.disk_mib,
        timeout_secs: environment.timeout_secs,
        idle_timeout_secs: environment.idle_timeout_secs,
        reuse: environment.reuse,
        network: environment.network,
        ports: Vec::new(),
        managed_by: Some("python-executor".into()),
        environment: Some(id),
        environment_artifact: artifact,
        preload: environment.preload.clone(),
    };
    let mut cluster = app.cluster.write().await;
    cluster.pools.insert(pool_name, pool);
    cluster.environments.insert(id, environment.clone());
    save(&app, &cluster).await?;
    Ok((
        StatusCode::CREATED,
        Json(EnvironmentView {
            ready: cluster.environment_ready(&environment),
            environment,
        }),
    ))
}

fn environment_pool(environment: &Environment) -> Pool {
    Pool {
        name: environment.pool.clone(),
        image: environment.image,
        warm_instances: environment.warm_instances,
        max_instances: environment.max_instances,
        cpu_millis: environment.cpu_millis,
        memory_mib: environment.memory_mib,
        disk_mib: environment.disk_mib,
        timeout_secs: environment.timeout_secs,
        idle_timeout_secs: environment.idle_timeout_secs,
        reuse: environment.reuse,
        network: environment.network,
        ports: Vec::new(),
        managed_by: Some("python-executor".into()),
        environment: Some(environment.id),
        environment_artifact: environment.artifact.clone(),
        preload: environment.preload.clone(),
    }
}

fn restart_environment_pool(cluster: &mut Cluster, environment: &mut Environment) -> Vec<Uuid> {
    let old_pool = environment.pool.clone();
    environment.pool = format!(
        "environment-{}-{}",
        environment.id,
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let reassigned = cluster
        .executions
        .values()
        .filter(|execution| {
            execution.pool == old_pool
                && matches!(
                    execution.status,
                    ExecutionStatus::Queued | ExecutionStatus::Assigned
                )
        })
        .map(|execution| execution.id)
        .collect::<Vec<_>>();
    for execution_id in &reassigned {
        if cluster.executions[execution_id].status == ExecutionStatus::Assigned {
            cluster.release(*execution_id, false);
        }
        let execution = cluster.executions.get_mut(execution_id).unwrap();
        execution.pool = environment.pool.clone();
        execution.node = None;
        execution.status = ExecutionStatus::Queued;
        execution.started_at_ms = None;
    }
    let running = cluster.executions.values().any(|execution| {
        execution.pool == old_pool && execution.status == ExecutionStatus::Running
    });
    if running {
        cluster.pools.get_mut(&old_pool).unwrap().warm_instances = 0;
    } else if let Some(pool) = cluster.pools.remove(&old_pool) {
        for node in cluster.nodes.values_mut() {
            let idle = node.idle_instances.remove(&pool.name).unwrap_or(0);
            node.used_cpu_millis = node
                .used_cpu_millis
                .saturating_sub(pool.cpu_millis.saturating_mul(idle));
            node.used_memory_mib = node
                .used_memory_mib
                .saturating_sub(pool.memory_mib.saturating_mul(idle));
        }
    }
    cluster
        .pools
        .insert(environment.pool.clone(), environment_pool(environment));
    reassigned
}

async fn update_environment(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
    Json(mut request): Json<CreateEnvironment>,
) -> ApiResult<Json<EnvironmentView>> {
    request.name = request.name.trim().to_owned();
    request.packages = request
        .packages
        .into_iter()
        .map(|package| package.trim().to_owned())
        .filter(|package| !package.is_empty())
        .collect();
    request.packages.sort();
    request.packages.dedup();
    request.preload = request
        .preload
        .into_iter()
        .map(|module| module.trim().to_owned())
        .filter(|module| !module.is_empty())
        .collect();
    request.preload.sort();
    request.preload.dedup();
    if request.name.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "environment name is required",
        ));
    }
    if request.max_instances.is_some_and(|limit| limit == 0)
        || request
            .max_instances
            .is_some_and(|limit| request.warm_instances > limit)
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid instance limits"));
    }
    let (old, image) = {
        let cluster = app.cluster.read().await;
        let old = cluster
            .environments
            .get(&id)
            .cloned()
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "environment not found"))?;
        if cluster
            .environments
            .values()
            .any(|environment| environment.id != id && environment.name == request.name)
        {
            return Err(error(
                StatusCode::CONFLICT,
                "environment name already exists",
            ));
        }
        let image = cluster
            .images
            .get(&request.image)
            .cloned()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "unknown runtime image"))?;
        if image.kind != ImageKind::Runtime
            || !image
                .executor
                .as_deref()
                .is_some_and(|executor| executor.to_ascii_lowercase().starts_with("python "))
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "image does not provide the Python executor",
            ));
        }
        if !cluster.image_ready(image.id) {
            return Err(error(
                StatusCode::CONFLICT,
                "runtime image is still distributing",
            ));
        }
        (old, image)
    };
    let rebuild = old.image != image.id
        || old.packages != request.packages
        || old.preload != request.preload
        || old.cpu_millis != request.cpu_millis
        || old.memory_mib != request.memory_mib
        || old.disk_mib != request.disk_mib
        || old.network != request.network;
    let (artifact, sha256, size) = if old.image != image.id || old.packages != request.packages {
        prepare_environment_artifact(&app, &image, &request.packages).await?
    } else {
        (old.artifact.clone(), old.sha256.clone(), old.size)
    };
    let mut environment = Environment {
        id,
        name: request.name,
        image: image.id,
        packages: request.packages,
        preload: request.preload,
        artifact: artifact.clone(),
        sha256,
        size,
        pool: old.pool.clone(),
        warm_instances: request.warm_instances,
        max_instances: request.max_instances,
        cpu_millis: request.cpu_millis,
        memory_mib: request.memory_mib,
        disk_mib: request.disk_mib,
        timeout_secs: request.timeout_secs,
        idle_timeout_secs: request.idle_timeout_secs,
        reuse: request.reuse,
        network: request.network,
    };
    let mut cluster = app.cluster.write().await;
    let reassigned = if rebuild {
        restart_environment_pool(&mut cluster, &mut environment)
    } else {
        cluster
            .pools
            .insert(environment.pool.clone(), environment_pool(&environment));
        Vec::new()
    };
    cluster.environments.insert(id, environment.clone());
    cluster.schedule_queued();
    let ready = cluster.environment_ready(&environment);
    save(&app, &cluster).await?;
    let reassigned = reassigned
        .into_iter()
        .map(|id| cluster.executions[&id].clone())
        .collect::<Vec<_>>();
    drop(cluster);
    for execution in &reassigned {
        save_execution(&app, execution).await?;
    }
    app.work.notify_waiters();
    Ok(Json(EnvironmentView { environment, ready }))
}

async fn restart_environment(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<EnvironmentView>> {
    let mut cluster = app.cluster.write().await;
    let mut environment = cluster
        .environments
        .get(&id)
        .cloned()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "environment not found"))?;
    let reassigned = restart_environment_pool(&mut cluster, &mut environment);
    cluster.environments.insert(id, environment.clone());
    cluster.schedule_queued();
    let ready = cluster.environment_ready(&environment);
    save(&app, &cluster).await?;
    let reassigned = reassigned
        .into_iter()
        .map(|id| cluster.executions[&id].clone())
        .collect::<Vec<_>>();
    drop(cluster);
    for execution in &reassigned {
        save_execution(&app, execution).await?;
    }
    app.work.notify_waiters();
    Ok(Json(EnvironmentView { environment, ready }))
}

async fn delete_environment(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.executions.values().any(|execution| {
        execution.environment == Some(id)
            && matches!(
                execution.status,
                ExecutionStatus::Queued | ExecutionStatus::Assigned | ExecutionStatus::Running
            )
    }) {
        return Err(error(
            StatusCode::CONFLICT,
            "environment has active executions",
        ));
    }
    let environment = cluster
        .environments
        .remove(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "environment not found"))?;
    if let Some(pool) = cluster.pools.remove(&environment.pool) {
        for node in cluster.nodes.values_mut() {
            let idle = node.idle_instances.remove(&pool.name).unwrap_or(0);
            node.used_cpu_millis = node
                .used_cpu_millis
                .saturating_sub(pool.cpu_millis.saturating_mul(idle));
            node.used_memory_mib = node
                .used_memory_mib
                .saturating_sub(pool.memory_mib.saturating_mul(idle));
        }
    }
    let remove_artifact = environment.artifact.as_ref().is_some_and(|artifact| {
        !cluster
            .environments
            .values()
            .any(|other| other.artifact.as_ref() == Some(artifact))
    });
    if remove_artifact && let Some(artifact) = &environment.artifact {
        for node in cluster.nodes.values_mut() {
            node.environments.remove(artifact);
        }
    }
    save(&app, &cluster).await?;
    if remove_artifact && let Some(artifact) = environment.artifact {
        fs::remove_file(app.data_dir.join("environments").join(artifact))
            .await
            .ok();
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn download_environment(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Response> {
    authenticated_node(&app, &headers).await?;
    let artifact = app
        .cluster
        .read()
        .await
        .environments
        .get(&id)
        .and_then(|environment| environment.artifact.clone())
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "environment artifact not found"))?;
    let file = fs::File::open(app.data_dir.join("environments").join(artifact))
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

async fn delete_node(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.executions.values().any(|execution| {
        execution.node == Some(id)
            && matches!(
                execution.status,
                ExecutionStatus::Assigned | ExecutionStatus::Running
            )
    }) {
        return Err(error(StatusCode::CONFLICT, "node has active executions"));
    }
    if cluster.nodes.remove(&id).is_none() && !cluster.node_credentials.contains_key(&id) {
        return Err(error(StatusCode::NOT_FOUND, "node not found"));
    }
    cluster.node_credentials.remove(&id);
    cluster.schedule_queued();
    save(&app, &cluster).await?;
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

#[derive(Deserialize)]
struct CreateExecution {
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    payload: Value,
    #[serde(default = "default_queue")]
    queue: bool,
}

fn default_queue() -> bool {
    true
}

async fn create_execution(
    State(app): State<App>,
    Json(request): Json<CreateExecution>,
) -> ApiResult<(StatusCode, Json<Execution>)> {
    let mut cluster = app.cluster.write().await;
    let environment = request.environment.as_deref().and_then(|key| {
        Uuid::parse_str(key)
            .ok()
            .and_then(|id| cluster.environments.get(&id))
            .or_else(|| {
                cluster
                    .environments
                    .values()
                    .find(|environment| environment.name == key)
            })
    });
    if request.environment.is_some() && environment.is_none() {
        return Err(error(StatusCode::BAD_REQUEST, "unknown environment"));
    }
    let pool_name = environment
        .map(|environment| environment.pool.clone())
        .or(request.pool)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "environment is required"))?;
    let pool = cluster
        .pools
        .get(&pool_name)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "unknown execution environment"))?;
    let environment_id = environment
        .map(|environment| environment.id)
        .or(pool.environment);
    if environment_id.is_none() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "VM pools cannot execute functions directly",
        ));
    }
    let image = &cluster.images[&pool.image];
    let executor = image
        .executor
        .as_deref()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "pool image has no executor"))?;
    let encoding = request.payload.get("encoding").and_then(Value::as_str);
    if matches!(
        encoding,
        Some("cloudpickle" | "python-function" | "python-source")
    ) && !executor.to_ascii_lowercase().starts_with("python ")
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "pool image does not have a Python executor",
        ));
    }
    let id = Uuid::new_v4();
    cluster.executions.insert(
        id,
        Execution {
            id,
            pool: pool_name,
            node: None,
            status: ExecutionStatus::Queued,
            payload: request.payload,
            result: None,
            error: None,
            created_at_ms: now_ms(),
            started_at_ms: None,
            finished_at_ms: None,
            environment: environment_id,
            metrics: None,
        },
    );
    if !cluster.schedule(id) && !request.queue {
        cluster.executions.remove(&id);
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "cluster is full"));
    }
    let execution = cluster.executions[&id].clone();
    save_execution(&app, &execution).await?;
    drop(cluster);
    app.work.notify_waiters();
    Ok((StatusCode::ACCEPTED, Json(execution)))
}

#[derive(Deserialize)]
struct ListExecutions {
    limit: Option<usize>,
}

async fn list_executions(
    State(app): State<App>,
    Query(query): Query<ListExecutions>,
) -> Json<Vec<Execution>> {
    let mut executions = app
        .cluster
        .read()
        .await
        .executions
        .values()
        .cloned()
        .collect::<Vec<_>>();
    executions.sort_unstable_by_key(|execution| std::cmp::Reverse(execution.created_at_ms));
    if let Some(limit) = query.limit {
        executions.truncate(limit);
    }
    Json(executions)
}

async fn get_execution(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<Execution>> {
    app.cluster
        .read()
        .await
        .executions
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "execution not found"))
}

async fn wait_execution(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<Execution>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
    loop {
        let notified = app.executions.notified();
        let execution = app
            .cluster
            .read()
            .await
            .executions
            .get(&id)
            .cloned()
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "execution not found"))?;
        if matches!(
            execution.status,
            ExecutionStatus::Succeeded | ExecutionStatus::Failed | ExecutionStatus::TimedOut
        ) || tokio::time::Instant::now() >= deadline
        {
            return Ok(Json(execution));
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return get_execution(State(app), AxumPath(id)).await;
        }
    }
}

#[derive(Deserialize)]
struct RegisterNode {
    id: Option<Uuid>,
    name: String,
    cpu_millis: u32,
    memory_mib: u32,
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
        address: request.address,
        used_cpu_millis: 0,
        used_memory_mib: 0,
        images: Default::default(),
        environments: Default::default(),
        idle_instances: Default::default(),
        vms: Default::default(),
        status: NodeStatus::Syncing,
        last_seen_ms: now_ms(),
    });
    node.name = request.name;
    node.cpu_millis = request.cpu_millis;
    node.memory_mib = request.memory_mib;
    node.address = request.address;
    node.last_seen_ms = now_ms();
    node.vms = request.vms;
    if request.reset_idle {
        cluster.reset_idle_instances(id);
    }
    cluster.refresh_node(id);
    cluster.schedule_queued();
    let node = cluster.nodes[&id].clone();
    save(&app, &cluster).await?;
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
    cluster.schedule_queued();
    save(&app, &cluster).await?;
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

async fn environment_ready(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, artifact)): AxumPath<(Uuid, String)>,
) -> ApiResult<StatusCode> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    if !cluster
        .environments
        .values()
        .any(|environment| environment.artifact.as_deref() == Some(&artifact))
    {
        return Err(error(StatusCode::NOT_FOUND, "environment not found"));
    }
    let node = cluster
        .nodes
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "node not found"))?;
    node.environments.insert(artifact);
    cluster.schedule_queued();
    save(&app, &cluster).await?;
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
        cluster.schedule_queued();
        save(&app, &cluster).await?;
        app.work.notify_waiters();
    }
    Ok(Json(added))
}

async fn remove_idle_instance(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath((id, pool)): AxumPath<(Uuid, String)>,
) -> ApiResult<Json<bool>> {
    if authenticated_node(&app, &headers).await? != id {
        return Err(error(StatusCode::FORBIDDEN, "node identity mismatch"));
    }
    let mut cluster = app.cluster.write().await;
    let removed = cluster.remove_idle_instance(id, &pool);
    if removed {
        cluster.schedule_queued();
        save(&app, &cluster).await?;
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
            let execution = cluster
                .executions
                .values_mut()
                .find(|execution| {
                    execution.node == Some(id) && execution.status == ExecutionStatus::Assigned
                })
                .map(|execution| {
                    execution.status = ExecutionStatus::Running;
                    execution.started_at_ms = Some(now_ms());
                    execution.clone()
                });
            execution.map(|execution| Work {
                pool: cluster.pools[&execution.pool].clone(),
                execution,
            })
        };
        if let Some(work) = work {
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
struct CompleteExecution {
    result: Option<Value>,
    error: Option<String>,
    timed_out: Option<bool>,
    #[serde(default)]
    metrics: Option<ExecutionMetrics>,
    #[serde(default)]
    keep_idle: bool,
}

async fn complete_execution(
    State(app): State<App>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<CompleteExecution>,
) -> ApiResult<StatusCode> {
    let node = authenticated_node(&app, &headers).await?;
    let mut cluster = app.cluster.write().await;
    let execution = cluster
        .executions
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "execution not found"))?;
    if execution.node != Some(node) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "execution belongs to another node",
        ));
    }
    execution.status = if request.timed_out.unwrap_or(false) {
        ExecutionStatus::TimedOut
    } else if request.error.is_some() {
        ExecutionStatus::Failed
    } else {
        ExecutionStatus::Succeeded
    };
    execution.result = request.result;
    execution.error = request.error;
    execution.metrics = request.metrics;
    execution.finished_at_ms = Some(now_ms());
    let pool_name = execution.pool.clone();
    cluster.release(id, request.keep_idle);
    let stale_pool = !cluster
        .environments
        .values()
        .any(|environment| environment.pool == pool_name)
        && !cluster.executions.values().any(|execution| {
            execution.pool == pool_name
                && matches!(
                    execution.status,
                    ExecutionStatus::Queued | ExecutionStatus::Assigned | ExecutionStatus::Running
                )
        });
    if stale_pool && let Some(pool) = cluster.pools.remove(&pool_name) {
        for node in cluster.nodes.values_mut() {
            let idle = node.idle_instances.remove(&pool.name).unwrap_or(0);
            node.used_cpu_millis = node
                .used_cpu_millis
                .saturating_sub(pool.cpu_millis.saturating_mul(idle));
            node.used_memory_mib = node
                .used_memory_mib
                .saturating_sub(pool.memory_mib.saturating_mul(idle));
        }
        save(&app, &cluster).await?;
    }
    cluster.schedule_queued();
    let execution = cluster.executions[&id].clone();
    drop(cluster);
    save_execution(&app, &execution).await?;
    app.executions.notify_waiters();
    app.work.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

#[allow(dead_code)]
fn artifact_path(data_dir: &Path, id: Uuid) -> PathBuf {
    data_dir.join("images").join(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    async fn app() -> App {
        let data_dir = std::env::temp_dir().join(format!("scraperack-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&data_dir).await.unwrap();
        App {
            cluster: Arc::new(RwLock::new(Cluster::default())),
            data_dir,
            executions: Arc::new(Notify::new()),
            work: Arc::new(Notify::new()),
        }
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
}
