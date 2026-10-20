use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    net::{IpAddr, Ipv4Addr, TcpListener, UdpSocket},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::StreamExt;
use reqwest::{
    Client,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
    sync::{Mutex, Notify},
};
use uuid::Uuid;

use crate::cluster::{
    Environment, ExecutionMetricPoint, ExecutionMetrics, Image, MicroVm, Pool, PortProtocol,
    PublishedPort, VmStatus, Work,
};

const GUEST_PORT: u32 = 5000;
const NETWORK: &str = "10.240.0.0/16";
const PORT_START: u16 = 30000;
const PORT_END: u16 = 49999;

#[derive(Deserialize)]
struct ImageView {
    #[serde(flatten)]
    image: Image,
}

#[derive(Deserialize)]
struct EnvironmentView {
    #[serde(flatten)]
    environment: Environment,
}

#[derive(Deserialize)]
struct RegisteredNode {
    id: Uuid,
    #[serde(default)]
    vm_start_concurrency: Option<usize>,
}

struct VmStartLimiter {
    state: std::sync::Mutex<VmStartLimiterState>,
    changed: Notify,
}

struct VmStartLimiterState {
    active: usize,
    limit: usize,
}

struct VmStartPermit<'a>(&'a VmStartLimiter);

impl VmStartLimiter {
    fn new(limit: usize) -> Self {
        Self {
            state: std::sync::Mutex::new(VmStartLimiterState {
                active: 0,
                limit: limit.max(1),
            }),
            changed: Notify::new(),
        }
    }

    async fn acquire(&self) -> VmStartPermit<'_> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().expect("VM start limiter poisoned");
                if state.active < state.limit {
                    state.active += 1;
                    return VmStartPermit(self);
                }
            }
            changed.await;
        }
    }

    fn set_limit(&self, limit: usize) -> bool {
        let limit = limit.max(1);
        let mut state = self.state.lock().expect("VM start limiter poisoned");
        if state.limit == limit {
            return false;
        }
        state.limit = limit;
        drop(state);
        self.changed.notify_waiters();
        true
    }
}

impl Drop for VmStartPermit<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("VM start limiter poisoned");
        state.active = state.active.saturating_sub(1);
        drop(state);
        self.0.changed.notify_one();
    }
}

#[derive(Deserialize)]
struct Credentials {
    node_id: Uuid,
    credential: String,
}

#[derive(Serialize)]
struct LocalJoin {
    id: Option<Uuid>,
}

#[derive(Serialize)]
struct Registration<'a> {
    id: Option<Uuid>,
    name: &'a str,
    cpu_millis: u32,
    memory_mib: u32,
    vm_start_concurrency: usize,
    address: IpAddr,
    reset_idle: bool,
    vms: Vec<MicroVm>,
}

#[derive(Deserialize, Serialize)]
struct Completion {
    result: Option<Value>,
    error: Option<String>,
    timed_out: bool,
    #[serde(default)]
    metrics: Option<ExecutionMetrics>,
    #[serde(default)]
    keep_idle: bool,
}

#[derive(Clone, Copy, Default)]
struct VmCounters {
    cpu_usage_usec: u64,
    memory_bytes: u64,
}

struct LiveVm {
    id: Uuid,
    pool: Pool,
    child: Child,
    directory: PathBuf,
    guest: UnixStream,
    network: Option<NetworkLease>,
    functions: HashSet<String>,
    working_dirs: HashSet<String>,
    last_used: Instant,
}

struct NetworkLease {
    subnet: u16,
    tap: String,
    guest_ip: Ipv4Addr,
    gateway: Ipv4Addr,
    ports: Vec<PublishedPort>,
}

#[derive(Clone)]
struct Network {
    state: Arc<Mutex<NetworkState>>,
}

struct NetworkState {
    next_subnet: u16,
    next_port: u16,
    subnets: HashSet<u16>,
    ports: HashSet<(PortProtocol, u16)>,
}

struct VmState {
    idle: Vec<LiveVm>,
    running: HashMap<Uuid, MicroVm>,
    active_cpu_millis: u32,
    active_memory_mib: u32,
    cpu_millis: u32,
    memory_mib: u32,
}

impl VmState {
    fn reports(&self) -> Vec<MicroVm> {
        let mut reports = self
            .idle
            .iter()
            .map(|vm| MicroVm {
                id: vm.id,
                pool: vm.pool.name.clone(),
                status: VmStatus::Idle,
                execution: None,
                pid: vm.child.id(),
                cpu_millis: vm.pool.cpu_millis,
                memory_mib: vm.pool.memory_mib,
                guest_ip: vm.network.as_ref().map(|network| network.guest_ip),
                ports: vm
                    .network
                    .as_ref()
                    .map(|network| network.ports.clone())
                    .unwrap_or_default(),
                environment: vm.pool.environment,
            })
            .chain(self.running.values().cloned())
            .collect::<Vec<_>>();
        reports.sort_by_key(|vm| vm.id);
        reports
    }

    fn acquire(&mut self, pool: &Pool) -> std::result::Result<Option<LiveVm>, &'static str> {
        if pool.cpu_millis > self.cpu_millis || pool.memory_mib > self.memory_mib {
            return Err("instance resources exceed node capacity");
        }
        self.active_cpu_millis = self.active_cpu_millis.saturating_add(pool.cpu_millis);
        self.active_memory_mib = self.active_memory_mib.saturating_add(pool.memory_mib);
        Ok(self
            .idle
            .iter()
            .enumerate()
            .filter(|(_, vm)| vm.pool.name == pool.name)
            .max_by_key(|(_, vm)| vm.last_used)
            .map(|(index, _)| index)
            .map(|index| self.idle.swap_remove(index)))
    }

    fn release(&mut self, pool: &Pool, vm: Option<LiveVm>) {
        self.active_cpu_millis = self.active_cpu_millis.saturating_sub(pool.cpu_millis);
        self.active_memory_mib = self.active_memory_mib.saturating_sub(pool.memory_mib);
        if let Some(vm) = vm {
            self.idle.push(vm);
        }
    }

    fn reserve_warm(&mut self, pool: &Pool) -> bool {
        if pool.cpu_millis > self.cpu_millis || pool.memory_mib > self.memory_mib {
            return false;
        }
        self.active_cpu_millis = self.active_cpu_millis.saturating_add(pool.cpu_millis);
        self.active_memory_mib = self.active_memory_mib.saturating_add(pool.memory_mib);
        true
    }
}

impl Network {
    async fn prepare() -> Result<Self> {
        let output = Command::new("ip")
            .args(["-o", "link", "show"])
            .output()
            .await?;
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let Some(name) = line
                .split(": ")
                .nth(1)
                .and_then(|name| name.split('@').next())
            else {
                continue;
            };
            if name.starts_with("srk") {
                let _ = host_command("ip", ["link", "del", name]).await;
            }
        }
        let _ = host_command("iptables", ["-t", "nat", "-N", "SCRAPERACK"]).await;
        host_command("iptables", ["-t", "nat", "-F", "SCRAPERACK"]).await?;
        ensure_rule("nat", "PREROUTING", ["-j", "SCRAPERACK"]).await?;
        ensure_rule(
            "nat",
            "OUTPUT",
            ["-m", "addrtype", "--dst-type", "LOCAL", "-j", "SCRAPERACK"],
        )
        .await?;
        ensure_rule("nat", "POSTROUTING", ["-s", NETWORK, "-j", "MASQUERADE"]).await?;
        ensure_rule("filter", "FORWARD", ["-s", NETWORK, "-j", "ACCEPT"]).await?;
        ensure_rule("filter", "FORWARD", ["-d", NETWORK, "-j", "ACCEPT"]).await?;
        Ok(Self {
            state: Arc::new(Mutex::new(NetworkState {
                next_subnet: 0,
                next_port: PORT_START,
                subnets: HashSet::new(),
                ports: HashSet::new(),
            })),
        })
    }

    async fn allocate(&self, id: Uuid, pool: &Pool) -> Result<NetworkLease> {
        let (subnet, ports) = {
            let mut state = self.state.lock().await;
            let mut available = None;
            for _ in 0..16384 {
                let subnet = state.next_subnet;
                state.next_subnet = state.next_subnet.wrapping_add(1) % 16384;
                if !state.subnets.contains(&subnet) {
                    available = Some(subnet);
                    break;
                }
            }
            let subnet = available.context("network address space exhausted")?;
            let mut ports = Vec::with_capacity(pool.ports.len());
            for spec in &pool.ports {
                let host_port = if let Some(port) = spec.host_port {
                    if state.ports.contains(&(spec.protocol, port))
                        || !port_available(spec.protocol, port)
                    {
                        bail!("host {}/{:?} is unavailable", port, spec.protocol);
                    }
                    port
                } else {
                    let mut found = None;
                    for _ in PORT_START..=PORT_END {
                        let port = state.next_port;
                        state.next_port = if port == PORT_END {
                            PORT_START
                        } else {
                            port + 1
                        };
                        if !state.ports.contains(&(spec.protocol, port))
                            && port_available(spec.protocol, port)
                        {
                            found = Some(port);
                            break;
                        }
                    }
                    found.context("dynamic host port range exhausted")?
                };
                state.ports.insert((spec.protocol, host_port));
                ports.push(PublishedPort {
                    name: spec.name.clone(),
                    guest_port: spec.guest_port,
                    host_port,
                    protocol: spec.protocol,
                });
            }
            state.subnets.insert(subnet);
            (subnet, ports)
        };
        let base = u32::from(Ipv4Addr::new(10, 240, 0, 0)) + u32::from(subnet) * 4;
        let tap = format!("srk{}", &id.simple().to_string()[..11]);
        let mut lease = NetworkLease {
            subnet,
            tap,
            gateway: Ipv4Addr::from(base + 1),
            guest_ip: Ipv4Addr::from(base + 2),
            ports,
        };
        if let Err(error) = self.setup(&lease).await {
            self.release(&mut lease).await;
            return Err(error);
        }
        Ok(lease)
    }

    async fn setup(&self, lease: &NetworkLease) -> Result<()> {
        host_command("ip", ["tuntap", "add", "dev", &lease.tap, "mode", "tap"]).await?;
        host_command(
            "ip",
            [
                "addr",
                "add",
                &format!("{}/30", lease.gateway),
                "dev",
                &lease.tap,
            ],
        )
        .await?;
        host_command("ip", ["link", "set", "dev", &lease.tap, "up"]).await?;
        for port in &lease.ports {
            host_command(
                "iptables",
                [
                    "-t",
                    "nat",
                    "-A",
                    "SCRAPERACK",
                    "-p",
                    protocol(port.protocol),
                    "--dport",
                    &port.host_port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", lease.guest_ip, port.guest_port),
                ],
            )
            .await?;
        }
        Ok(())
    }

    async fn release(&self, lease: &mut NetworkLease) {
        for port in &lease.ports {
            let _ = host_command(
                "iptables",
                [
                    "-t",
                    "nat",
                    "-D",
                    "SCRAPERACK",
                    "-p",
                    protocol(port.protocol),
                    "--dport",
                    &port.host_port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", lease.guest_ip, port.guest_port),
                ],
            )
            .await;
        }
        let _ = host_command("ip", ["link", "del", &lease.tap]).await;
        let mut state = self.state.lock().await;
        state.subnets.remove(&lease.subnet);
        for port in &lease.ports {
            state.ports.remove(&(port.protocol, port.host_port));
        }
    }
}

fn protocol(protocol: PortProtocol) -> &'static str {
    match protocol {
        PortProtocol::Tcp => "tcp",
        PortProtocol::Udp => "udp",
    }
}

fn port_available(protocol: PortProtocol, port: u16) -> bool {
    match protocol {
        PortProtocol::Tcp => TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).is_ok(),
        PortProtocol::Udp => UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).is_ok(),
    }
}

fn detect_address() -> Result<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    socket.connect((Ipv4Addr::new(1, 1, 1, 1), 80))?;
    Ok(socket.local_addr()?.ip())
}

async fn host_command<I, S>(program: &str, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(args).output().await?;
    if !output.status.success() {
        bail!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn ensure_rule<I, S>(table: &str, chain: &str, rule: I) -> Result<()>
where
    I: IntoIterator<Item = S> + Clone,
    S: AsRef<OsStr>,
{
    let mut check = Command::new("iptables");
    check.args(["-t", table, "-C", chain]).args(rule.clone());
    if !check.output().await?.status.success() {
        let mut add = Command::new("iptables");
        let output = add
            .args(["-t", table, "-A", chain])
            .args(rule)
            .output()
            .await?;
        if !output.status.success() {
            bail!(
                "iptables failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    controller: String,
    name: String,
    cpu_millis: u32,
    memory_mib: u32,
    vm_start_concurrency: Option<usize>,
    data_dir: PathBuf,
    firecracker: PathBuf,
    kernel: PathBuf,
    advertise_address: Option<IpAddr>,
    credential: Option<String>,
) -> Result<()> {
    let firecracker = fs::canonicalize(firecracker)
        .await
        .context("firecracker binary not found")?;
    let kernel = fs::canonicalize(kernel)
        .await
        .context("guest kernel not found")?;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .await
        .context("KVM is unavailable")?;
    prepare_cgroups().await?;
    let network = Network::prepare().await?;
    let address = advertise_address.unwrap_or(detect_address()?);
    fs::create_dir_all(data_dir.join("images")).await?;
    fs::create_dir_all(data_dir.join("environments")).await?;
    fs::create_dir_all(data_dir.join("working-dirs")).await?;
    fs::create_dir_all(data_dir.join("logs")).await?;
    fs::create_dir_all(data_dir.join("snapshots")).await?;
    let instances = data_dir.join("instances");
    if instances.exists() {
        fs::remove_dir_all(&instances).await?;
    }
    fs::create_dir(&instances).await?;

    let id_path = data_dir.join("node-id");
    let stored_id = match fs::read_to_string(&id_path).await {
        Ok(id) => Some(Uuid::parse_str(id.trim())?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let controller = controller.trim_end_matches('/');
    let (id, credential) = match credential.filter(|value| !value.is_empty()) {
        Some(credential) => (
            stored_id.context("authenticated node is missing its node ID")?,
            credential,
        ),
        None => {
            let credentials = Client::new()
                .post(format!("{controller}/internal/local-join"))
                .json(&LocalJoin { id: stored_id })
                .send()
                .await?
                .error_for_status()
                .context("local node enrollment failed")?
                .json::<Credentials>()
                .await?;
            (credentials.node_id, credentials.credential)
        }
    };
    if stored_id != Some(id) {
        fs::write(&id_path, id.to_string()).await?;
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {credential}"))?,
    );
    let client = Client::builder().default_headers(headers).build()?;

    let local_vm_start_concurrency = vm_start_concurrency
        .unwrap_or_else(|| default_vm_start_concurrency(cpu_millis))
        .max(1);
    let mut node: RegisteredNode = client
        .post(format!("{controller}/internal/nodes/register"))
        .json(&Registration {
            id: Some(id),
            name: &name,
            cpu_millis,
            memory_mib,
            vm_start_concurrency: local_vm_start_concurrency,
            address,
            reset_idle: true,
            vms: Vec::new(),
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let snapshot_lock = Arc::new(Mutex::new(()));
    let working_dir_lock = Arc::new(Mutex::new(()));
    let vm_start_concurrency = node
        .vm_start_concurrency
        .unwrap_or(local_vm_start_concurrency);
    tracing::info!(vm_start_concurrency, "configured VM start concurrency");
    let vm_starts = Arc::new(VmStartLimiter::new(vm_start_concurrency));
    let vms = Arc::new(Mutex::new(VmState {
        idle: Vec::new(),
        running: HashMap::new(),
        active_cpu_millis: 0,
        active_memory_mib: 0,
        cpu_millis,
        memory_mib,
    }));
    sync_images(&client, controller, node.id, &data_dir).await?;
    sync_environments(&client, controller, node.id, &data_dir).await?;
    let pools = sync_snapshots(
        &client,
        controller,
        &firecracker,
        &kernel,
        &data_dir,
        &snapshot_lock,
    )
    .await?;
    maintain_vms(
        &client,
        controller,
        node.id,
        &firecracker,
        &kernel,
        &data_dir,
        &snapshot_lock,
        &vm_starts,
        &network,
        &vms,
        &pools,
    )
    .await?;
    let mut heartbeat = Instant::now();

    loop {
        if heartbeat.elapsed() >= Duration::from_secs(2) {
            let reports = vms.lock().await.reports();
            node = client
                .post(format!("{controller}/internal/nodes/register"))
                .json(&Registration {
                    id: Some(node.id),
                    name: &name,
                    cpu_millis,
                    memory_mib,
                    vm_start_concurrency: local_vm_start_concurrency,
                    address,
                    reset_idle: false,
                    vms: reports,
                })
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let vm_start_concurrency = node
                .vm_start_concurrency
                .unwrap_or(local_vm_start_concurrency);
            if vm_starts.set_limit(vm_start_concurrency) {
                tracing::info!(vm_start_concurrency, "updated VM start concurrency");
            }
            sync_images(&client, controller, node.id, &data_dir).await?;
            sync_environments(&client, controller, node.id, &data_dir).await?;
            let pools = sync_snapshots(
                &client,
                controller,
                &firecracker,
                &kernel,
                &data_dir,
                &snapshot_lock,
            )
            .await?;
            maintain_vms(
                &client,
                controller,
                node.id,
                &firecracker,
                &kernel,
                &data_dir,
                &snapshot_lock,
                &vm_starts,
                &network,
                &vms,
                &pools,
            )
            .await?;
            heartbeat = Instant::now();
        }
        if let Some(work) = client
            .get(format!(
                "{controller}/internal/nodes/{}/work?wait_ms=1000",
                node.id
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<Work>>()
            .await?
        {
            let client = client.clone();
            let controller = controller.to_owned();
            let firecracker = firecracker.clone();
            let kernel = kernel.clone();
            let data_dir = data_dir.clone();
            let snapshot_lock = snapshot_lock.clone();
            let working_dir_lock = working_dir_lock.clone();
            let vm_starts = vm_starts.clone();
            let network = network.clone();
            let vms = vms.clone();
            tokio::spawn(async move {
                let execution = &work.execution;
                let completion = run_execution(
                    &client,
                    &controller,
                    &firecracker,
                    &kernel,
                    &data_dir,
                    &work,
                    &snapshot_lock,
                    &working_dir_lock,
                    &vm_starts,
                    &network,
                    &vms,
                )
                .await;
                if let Err(error) = client
                    .post(format!(
                        "{controller}/internal/executions/{}/complete",
                        execution.id
                    ))
                    .json(&completion)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                {
                    tracing::error!(id = %execution.id, %error, "failed to report completion");
                }
            });
        }
    }
}

async fn sync_images(client: &Client, controller: &str, node: Uuid, data_dir: &Path) -> Result<()> {
    let images = client
        .get(format!("{controller}/v1/images"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<ImageView>>()
        .await?;
    let wanted = images
        .iter()
        .map(|view| view.image.id)
        .collect::<HashSet<_>>();

    for view in images {
        let image = view.image;
        let path = data_dir.join("images").join(image.id.to_string());
        let marker = path.with_extension("sha256");
        let verified = fs::read_to_string(&marker).await.unwrap_or_default() == image.sha256;
        if !path.exists() || !verified {
            if !path.exists() || hash(&path).await? != image.sha256 {
                download(
                    client,
                    &format!("{controller}/v1/images/{}/artifact", image.id),
                    &path,
                )
                .await?;
                if hash(&path).await? != image.sha256 {
                    bail!("image {} failed checksum verification", image.name);
                }
            }
            fs::write(marker, &image.sha256).await?;
        }
        if !is_ext4(&path).await? {
            tracing::warn!(image = %image.name, "golden image is not ext4");
            continue;
        }
        client
            .post(format!(
                "{controller}/internal/nodes/{node}/images/{}/ready",
                image.id
            ))
            .send()
            .await?
            .error_for_status()?;
    }

    let mut entries = fs::read_dir(data_dir.join("images")).await?;
    while let Some(entry) = entries.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Ok(id) = Uuid::parse_str(&name)
            && !wanted.contains(&id)
        {
            fs::remove_file(entry.path()).await?;
            fs::remove_file(entry.path().with_extension("sha256"))
                .await
                .ok();
        }
    }
    Ok(())
}

async fn sync_environments(
    client: &Client,
    controller: &str,
    node: Uuid,
    data_dir: &Path,
) -> Result<()> {
    let environments = client
        .get(format!("{controller}/v1/environments"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<EnvironmentView>>()
        .await?;
    let wanted = environments
        .iter()
        .filter_map(|view| view.environment.artifact.clone())
        .collect::<HashSet<_>>();
    for environment in environments {
        let Some(artifact) = environment.environment.artifact else {
            continue;
        };
        let path = data_dir.join("environments").join(&artifact);
        let marker = path.with_extension("sha256");
        let sha256 = environment.environment.sha256.unwrap_or_default();
        let verified = fs::read_to_string(&marker).await.unwrap_or_default() == sha256;
        if !path.exists() || !verified {
            if !path.exists() || hash(&path).await? != sha256 {
                download(
                    client,
                    &format!(
                        "{controller}/v1/environments/{}/artifact",
                        environment.environment.id
                    ),
                    &path,
                )
                .await?;
                if hash(&path).await? != sha256 {
                    bail!(
                        "environment {} failed checksum verification",
                        environment.environment.name
                    );
                }
            }
            fs::write(marker, &sha256).await?;
        }
        client
            .post(format!(
                "{controller}/internal/nodes/{node}/environments/{artifact}/ready"
            ))
            .send()
            .await?
            .error_for_status()?;
    }
    let mut entries = fs::read_dir(data_dir.join("environments")).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".sha256") && !wanted.contains(&name) {
            fs::remove_file(entry.path()).await?;
            fs::remove_file(entry.path().with_extension("sha256"))
                .await
                .ok();
        }
    }
    Ok(())
}

async fn ensure_working_dir(
    client: &Client,
    controller: &str,
    data_dir: &Path,
    lock: &Mutex<()>,
    digest: &str,
) -> Result<PathBuf> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("invalid working directory hash");
    }
    let path = data_dir.join("working-dirs").join(format!("{digest}.zip"));
    let marker = path.with_extension("sha256");
    if path.exists() && fs::read_to_string(&marker).await.unwrap_or_default() == digest {
        return Ok(path);
    }
    let _guard = lock.lock().await;
    if path.exists() && fs::read_to_string(&marker).await.unwrap_or_default() == digest {
        return Ok(path);
    }
    if path.exists() && hash(&path).await? == digest {
        fs::write(&marker, digest).await?;
        return Ok(path);
    }
    download(
        client,
        &format!("{controller}/internal/working-dirs/{digest}"),
        &path,
    )
    .await?;
    if hash(&path).await? != digest {
        bail!("working directory failed checksum verification");
    }
    fs::write(marker, digest).await?;
    Ok(path)
}

#[allow(clippy::too_many_arguments)]
async fn run_execution(
    client: &Client,
    controller: &str,
    firecracker: &Path,
    kernel: &Path,
    data_dir: &Path,
    work: &Work,
    snapshot_lock: &Mutex<()>,
    working_dir_lock: &Mutex<()>,
    vm_starts: &VmStartLimiter,
    network: &Network,
    vms: &Mutex<VmState>,
) -> Completion {
    let working_dir = work
        .execution
        .payload
        .get("working_dir")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let working_dir_path = if let Some(digest) = &working_dir {
        match ensure_working_dir(client, controller, data_dir, working_dir_lock, digest).await {
            Ok(path) => Some(path),
            Err(error) => {
                return Completion {
                    result: None,
                    error: Some(format!("{error:#}")),
                    timed_out: false,
                    metrics: None,
                    keep_idle: false,
                };
            }
        }
    } else {
        None
    };
    let idle = match vms.lock().await.acquire(&work.pool) {
        Ok(idle) => idle,
        Err(error) => {
            return Completion {
                result: None,
                error: Some(error.into()),
                timed_out: false,
                metrics: None,
                keep_idle: false,
            };
        }
    };
    let reused = idle.is_some();
    let mut vm = match idle {
        Some(vm) => vm,
        None => {
            let permit = vm_starts.acquire().await;
            let started = start_vm(
                firecracker,
                kernel,
                data_dir,
                &work.pool,
                snapshot_lock,
                network,
            )
            .await;
            drop(permit);
            match started {
                Ok(vm) => vm,
                Err(error) => {
                    vms.lock().await.release(&work.pool, None);
                    return Completion {
                        result: None,
                        error: Some(format!("{error:#}")),
                        timed_out: false,
                        metrics: None,
                        keep_idle: false,
                    };
                }
            }
        }
    };
    let working_dir_archive = if working_dir
        .as_ref()
        .is_some_and(|digest| !vm.working_dirs.contains(digest))
    {
        match fs::read(working_dir_path.as_ref().unwrap()).await {
            Ok(archive) => Some(BASE64.encode(archive)),
            Err(error) => {
                destroy_vm(vm, network).await;
                vms.lock().await.release(&work.pool, None);
                return Completion {
                    result: None,
                    error: Some(format!("read working directory: {error}")),
                    timed_out: false,
                    metrics: None,
                    keep_idle: false,
                };
            }
        }
    } else {
        None
    };
    tracing::info!(
        id = %work.execution.id,
        pool = %work.execution.pool,
        vm = %vm.id,
        reused,
        "executing in microVM"
    );
    vms.lock().await.running.insert(
        vm.id,
        MicroVm {
            id: vm.id,
            pool: work.pool.name.clone(),
            status: VmStatus::Running,
            execution: Some(work.execution.id),
            pid: vm.child.id(),
            cpu_millis: work.pool.cpu_millis,
            memory_mib: work.pool.memory_mib,
            guest_ip: vm.network.as_ref().map(|network| network.guest_ip),
            ports: vm
                .network
                .as_ref()
                .map(|network| network.ports.clone())
                .unwrap_or_default(),
            environment: work.pool.environment,
        },
    );
    let mut payload = work.execution.payload.clone();
    if let Some(payload) = payload.as_object_mut() {
        payload.remove("working_dir");
    }
    let function = payload
        .get("cache_key")
        .or_else(|| payload.get("hash"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if function
        .as_ref()
        .is_some_and(|key| vm.functions.contains(key))
        && let Some(payload) = payload.as_object_mut()
    {
        payload.remove("function");
    }
    if !work.execution.variables.is_empty() || !work.secrets.is_empty() || working_dir.is_some() {
        payload = json!({
            "encoding": "scraperack-invocation",
            "payload": payload,
            "variables": &work.execution.variables,
            "secrets": &work.secrets,
            "working_dir": working_dir.as_ref().map(|digest| json!({
                "hash": digest,
                "archive": working_dir_archive,
            })),
        });
    }
    let cgroup = vm_cgroup(vm.id).await.ok();
    let before = match &cgroup {
        Some(path) => read_vm_counters(path).await.unwrap_or_default(),
        None => VmCounters::default(),
    };
    let metrics_started = Instant::now();
    let mut previous = before;
    let mut previous_elapsed_us = 0;
    let mut memory_total = u128::from(before.memory_bytes);
    let mut memory_count = 1u64;
    let mut samples = vec![metric_point(0, 0.0, before.memory_bytes, &work.pool)];
    let response = {
        let request = tokio::time::timeout(
            Duration::from_secs(work.pool.timeout_secs),
            guest_request_stream(&mut vm.guest, &payload),
        );
        tokio::pin!(request);
        let mut sampler = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                response = &mut request => break response,
                _ = sampler.tick() => {
                    if let Some(path) = &cgroup
                        && let Ok(current) = read_vm_counters(path).await
                    {
                        let elapsed_us = metrics_started.elapsed().as_micros() as u64;
                        push_metric_sample(
                            &mut samples,
                            &previous,
                            &current,
                            previous_elapsed_us,
                            elapsed_us,
                            &work.pool,
                        );
                        memory_total += u128::from(current.memory_bytes);
                        memory_count += 1;
                        previous = current;
                        previous_elapsed_us = elapsed_us;
                    }
                }
            }
        }
    };
    let after = match &cgroup {
        Some(path) => read_vm_counters(path).await.unwrap_or_default(),
        None => VmCounters::default(),
    };
    let wall_time_usec = metrics_started.elapsed().as_micros() as u64;
    push_metric_sample(
        &mut samples,
        &previous,
        &after,
        previous_elapsed_us,
        wall_time_usec,
        &work.pool,
    );
    memory_total += u128::from(after.memory_bytes);
    memory_count += 1;
    let (mut completion, healthy) = match response {
        Ok(Ok(completion)) => (completion, true),
        Ok(Err(error)) => (
            Completion {
                result: None,
                error: Some(format!("{error:#}")),
                timed_out: false,
                metrics: None,
                keep_idle: false,
            },
            false,
        ),
        Err(_) => (
            Completion {
                result: None,
                error: Some("execution timed out".into()),
                timed_out: true,
                metrics: None,
                keep_idle: false,
            },
            false,
        ),
    };
    completion.metrics = cgroup.map(|_| {
        execution_metrics(
            &before,
            &after,
            wall_time_usec,
            memory_total,
            memory_count,
            samples,
            &work.pool,
        )
    });
    redact_completion(&mut completion, &work.secrets);
    let keep_idle = healthy
        && work.pool.reuse
        && (work.pool.idle_timeout_secs > 0 || work.pool.warm_instances > 0);
    completion.keep_idle = keep_idle;
    if healthy && let Some(working_dir) = &working_dir {
        vm.working_dirs.insert(working_dir.clone());
    }
    if healthy
        && (working_dir.is_none() || completion.error.is_none())
        && let Some(function) = function
    {
        vm.functions.insert(function);
    }
    vms.lock().await.running.remove(&vm.id);
    if keep_idle {
        vm.last_used = Instant::now();
        vms.lock().await.release(&work.pool, Some(vm));
    } else {
        destroy_vm(vm, network).await;
        vms.lock().await.release(&work.pool, None);
    }
    completion
}

fn redact_completion(completion: &mut Completion, secrets: &HashMap<String, String>) {
    for secret in secrets.values().filter(|value| !value.is_empty()) {
        if let Some(error) = &mut completion.error {
            *error = error.replace(secret, "[REDACTED]");
        }
        if let Some(result) = completion.result.as_mut().and_then(Value::as_object_mut) {
            for field in ["stdout", "stderr"] {
                if let Some(value) = result.get(field).and_then(Value::as_str) {
                    result.insert(
                        field.into(),
                        Value::String(value.replace(secret, "[REDACTED]")),
                    );
                }
            }
        }
    }
}

async fn sync_snapshots(
    client: &Client,
    controller: &str,
    firecracker: &Path,
    kernel: &Path,
    data_dir: &Path,
    lock: &Mutex<()>,
) -> Result<Vec<Pool>> {
    let pools = client
        .get(format!("{controller}/v1/pools"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Pool>>()
        .await?;
    let wanted = pools.iter().map(snapshot_key).collect::<HashSet<_>>();
    let mut ready = Vec::new();
    for pool in pools {
        match ensure_snapshot(firecracker, kernel, data_dir, &pool, lock).await {
            Ok(_) => ready.push(pool),
            Err(error) => {
                tracing::error!(pool = %pool.name, %error, "failed to prepare pool snapshot")
            }
        }
    }
    let mut entries = fs::read_dir(data_dir.join("snapshots")).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with('.') && !wanted.contains(name.as_ref()) {
            fs::remove_dir_all(entry.path()).await?;
        }
    }
    Ok(ready)
}

#[allow(clippy::too_many_arguments)]
async fn maintain_vms(
    client: &Client,
    controller: &str,
    node: Uuid,
    firecracker: &Path,
    kernel: &Path,
    data_dir: &Path,
    snapshot_lock: &Mutex<()>,
    vm_starts: &VmStartLimiter,
    network: &Network,
    vms: &Mutex<VmState>,
    pools: &[Pool],
) -> Result<()> {
    let current = pools
        .iter()
        .map(|pool| (pool.name.as_str(), pool))
        .collect::<HashMap<_, _>>();
    let mut removed = Vec::new();
    {
        let mut state = vms.lock().await;
        for mut vm in std::mem::take(&mut state.idle) {
            if let Some(pool) = current.get(vm.pool.name.as_str()) {
                vm.pool = (*pool).clone();
                state.idle.push(vm);
            } else {
                removed.push(vm);
            }
        }
    }
    for vm in removed {
        destroy_vm(vm, network).await;
    }

    let mut idle = {
        let mut state = vms.lock().await;
        std::mem::take(&mut state.idle)
    };
    let mut retained = Vec::new();
    while let Some(mut vm) = idle.pop() {
        let expired = vm.last_used.elapsed() >= Duration::from_secs(vm.pool.idle_timeout_secs);
        let release = client
            .delete(format!(
                "{controller}/internal/nodes/{node}/pools/{}/idle",
                vm.pool.name
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<bool>()
            .await?;
        if release {
            destroy_vm(vm, network).await;
        } else {
            if expired {
                vm.last_used = Instant::now();
            }
            retained.push(vm);
        }
    }
    vms.lock().await.idle.append(&mut retained);

    for pool in pools {
        loop {
            let needed = client
                .get(format!(
                    "{controller}/internal/nodes/{node}/pools/{}/idle",
                    pool.name
                ))
                .send()
                .await?
                .error_for_status()?
                .json::<bool>()
                .await?;
            if !needed || !vms.lock().await.reserve_warm(pool) {
                break;
            }
            let permit = vm_starts.acquire().await;
            let started =
                start_vm(firecracker, kernel, data_dir, pool, snapshot_lock, network).await;
            drop(permit);
            let vm = match started {
                Ok(vm) => vm,
                Err(error) => {
                    vms.lock().await.release(pool, None);
                    tracing::error!(pool = %pool.name, %error, "failed to start warm microVM");
                    break;
                }
            };
            let accepted = client
                .post(format!(
                    "{controller}/internal/nodes/{node}/pools/{}/idle",
                    pool.name
                ))
                .send()
                .await?
                .error_for_status()?
                .json::<bool>()
                .await?;
            if accepted {
                tracing::info!(pool = %pool.name, vm = %vm.id, "warm microVM ready");
                vms.lock().await.release(pool, Some(vm));
            } else {
                destroy_vm(vm, network).await;
                vms.lock().await.release(pool, None);
                break;
            }
        }
    }
    Ok(())
}

fn default_vm_start_concurrency(cpu_millis: u32) -> usize {
    (cpu_millis / 2000).max(1) as usize
}

async fn start_vm(
    firecracker: &Path,
    kernel: &Path,
    data_dir: &Path,
    pool: &Pool,
    snapshot_lock: &Mutex<()>,
    network: &Network,
) -> Result<LiveVm> {
    let snapshot = ensure_snapshot(firecracker, kernel, data_dir, pool, snapshot_lock).await?;
    let id = Uuid::new_v4();
    let mut network_lease = if pool.network {
        Some(network.allocate(id, pool).await?)
    } else {
        None
    };
    let directory = data_dir.join("instances").join(id.to_string());
    fs::create_dir(&directory).await?;
    let api_socket = directory.join("api.sock");
    let vsock = directory.join("vsock.sock");
    let log = std::fs::File::create(data_dir.join("logs").join(format!("vm-{id}.log")))?;
    let mut child = start_firecracker(firecracker, &api_socket, id, log)?;
    let result = async {
        wait_for_path(&api_socket, &mut child).await?;
        attach_cgroup(child.id().context("firecracker exited")?, pool, id).await?;
        let mut load = json!({
            "snapshot_path": snapshot.state,
            "mem_backend": {
                "backend_path": snapshot.memory,
                "backend_type": "File"
            },
            "resume_vm": true,
            "vsock_override": {"uds_path": vsock}
        });
        if let Some(lease) = &network_lease {
            load["network_overrides"] = json!([{
                "iface_id": "net1",
                "host_dev_name": lease.tap,
            }]);
        }
        firecracker_request("PUT", &api_socket, "/snapshot/load", &load).await?;
        let ready = tokio::time::timeout(
            Duration::from_secs(10),
            guest_request(&vsock, &json!({"encoding": "scraperack-snapshot"})),
        )
        .await
        .context("restored guest did not become ready")??;
        if let Some(error) = ready.error {
            bail!("restored guest failed its readiness check: {error}");
        }
        if let Some(lease) = &network_lease {
            let configured = tokio::time::timeout(
                Duration::from_secs(10),
                guest_request(
                    &vsock,
                    &json!({
                        "encoding": "scraperack-network",
                        "address": lease.guest_ip,
                        "prefix": 30,
                        "gateway": lease.gateway,
                        "dns": ["1.1.1.1", "8.8.8.8"],
                    }),
                ),
            )
            .await
            .context("guest network configuration timed out")??;
            if let Some(error) = configured.error {
                bail!("guest network configuration failed: {error}");
            }
        }
        if !pool.preload.is_empty() {
            let mut preload = json!({"encoding": "scraperack-preload", "modules": &pool.preload});
            if let Some(lease) = &network_lease {
                preload["url"] = json!(format!("http://{}:7070/health", lease.gateway));
            }
            let preloaded =
                tokio::time::timeout(Duration::from_secs(30), guest_request(&vsock, &preload))
                    .await
                    .context("restored guest preload timed out")??;
            if let Some(error) = preloaded.error {
                bail!("restored guest preload failed: {error}");
            }
        }
        connect_vsock(&vsock).await
    }
    .await;
    let guest = match result {
        Ok(guest) => guest,
        Err(error) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = remove_cgroup(id).await;
            let _ = fs::remove_dir_all(&directory).await;
            if let Some(lease) = &mut network_lease {
                network.release(lease).await;
            }
            return Err(error);
        }
    };
    Ok(LiveVm {
        id,
        pool: pool.clone(),
        child,
        directory,
        guest,
        network: network_lease,
        functions: HashSet::new(),
        working_dirs: HashSet::new(),
        last_used: Instant::now(),
    })
}

async fn destroy_vm(mut vm: LiveVm, network: &Network) {
    let _ = vm.child.kill().await;
    let _ = vm.child.wait().await;
    let _ = remove_cgroup(vm.id).await;
    let _ = fs::remove_dir_all(vm.directory).await;
    if let Some(lease) = &mut vm.network {
        network.release(lease).await;
    }
}

struct SnapshotPaths {
    directory: PathBuf,
    state: PathBuf,
    memory: PathBuf,
}

impl SnapshotPaths {
    fn new(directory: PathBuf) -> Self {
        Self {
            state: directory.join("vmstate"),
            memory: directory.join("memory"),
            directory,
        }
    }

    fn ready(&self) -> bool {
        self.state.exists() && self.memory.exists() && self.directory.join("ready").exists()
    }
}

async fn ensure_snapshot(
    firecracker: &Path,
    kernel: &Path,
    data_dir: &Path,
    pool: &Pool,
    lock: &Mutex<()>,
) -> Result<SnapshotPaths> {
    let key = snapshot_key(pool);
    let snapshot = SnapshotPaths::new(data_dir.join("snapshots").join(&key));
    if snapshot.ready() {
        return Ok(snapshot);
    }
    let _guard = lock.lock().await;
    if snapshot.ready() {
        return Ok(snapshot);
    }
    if snapshot.directory.exists() {
        fs::remove_dir_all(&snapshot.directory).await?;
    }
    let temporary = SnapshotPaths::new(
        data_dir
            .join("snapshots")
            .join(format!(".{key}-{}", Uuid::new_v4())),
    );
    fs::create_dir(&temporary.directory).await?;
    let id = Uuid::new_v4();
    let dir = data_dir.join("instances").join(format!("snapshot-{id}"));
    fs::create_dir(&dir).await?;
    let api_socket = dir.join("api.sock");
    let vsock = dir.join("vsock.sock");
    let image = data_dir.join("images").join(pool.image.to_string());
    let environment = pool
        .environment_artifact
        .as_ref()
        .map(|artifact| data_dir.join("environments").join(artifact));
    let log = std::fs::File::create(data_dir.join("logs").join(format!("snapshot-{key}.log")))?;
    let mut child = start_firecracker(firecracker, &api_socket, id, log)?;
    let mut snapshot_tap = None;
    let result = async {
        wait_for_path(&api_socket, &mut child).await?;
        if pool.network {
            let tap = format!("srks{}", &id.simple().to_string()[..10]);
            host_command("ip", ["tuntap", "add", "dev", &tap, "mode", "tap"]).await?;
            host_command("ip", ["link", "set", "dev", &tap, "up"]).await?;
            snapshot_tap = Some(tap);
        }
        configure_boot(
            &api_socket,
            &vsock,
            kernel,
            &image,
            environment.as_deref(),
            pool,
            snapshot_tap.as_deref(),
        )
        .await?;
        firecracker_put(
            &api_socket,
            "/actions",
            &json!({"action_type": "InstanceStart"}),
        )
        .await?;
        let ready = tokio::time::timeout(
            Duration::from_secs(30),
            guest_request(
                &vsock,
                &json!({"encoding": "scraperack-snapshot", "preload": &pool.preload}),
            ),
        )
        .await
        .context("guest did not become snapshot-ready")??;
        if let Some(error) = ready.error {
            bail!("guest snapshot preparation failed: {error}");
        }
        firecracker_request("PATCH", &api_socket, "/vm", &json!({"state": "Paused"})).await?;
        firecracker_put(
            &api_socket,
            "/snapshot/create",
            &json!({
                "snapshot_type": "Full",
                "snapshot_path": temporary.state,
                "mem_file_path": temporary.memory,
                "sync_snapshot_files": false
            }),
        )
        .await?;
        fs::write(temporary.directory.join("ready"), []).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let _ = child.kill().await;
    let _ = child.wait().await;
    if let Some(tap) = snapshot_tap {
        let _ = host_command("ip", ["link", "del", &tap]).await;
    }
    let _ = fs::remove_dir_all(dir).await;
    if let Err(error) = result {
        let _ = fs::remove_dir_all(temporary.directory).await;
        return Err(error);
    }
    fs::rename(&temporary.directory, &snapshot.directory).await?;
    tracing::info!(image = %pool.image, memory_mib = pool.memory_mib, "created runtime snapshot");
    Ok(snapshot)
}

fn snapshot_key(pool: &Pool) -> String {
    let preload = if pool.preload.is_empty() {
        String::new()
    } else {
        let mut digest = Sha256::new();
        for module in &pool.preload {
            digest.update(module.as_bytes());
            digest.update([0]);
        }
        format!("-{}", &hex::encode(digest.finalize())[..16])
    };
    format!(
        "{}-{}-{}-{}-{}{preload}",
        pool.image,
        pool.cpu_millis.div_ceil(1000).max(1),
        pool.memory_mib,
        if pool.network { "net1" } else { "none" },
        pool.environment_artifact.as_deref().unwrap_or("base"),
    )
}

fn start_firecracker(
    firecracker: &Path,
    api_socket: &Path,
    id: Uuid,
    log: std::fs::File,
) -> Result<Child> {
    Command::new(firecracker)
        .arg("--api-sock")
        .arg(api_socket)
        .arg("--id")
        .arg(id.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()
        .context("start firecracker")
}

async fn configure_boot(
    api_socket: &Path,
    vsock: &Path,
    kernel: &Path,
    rootfs: &Path,
    environment: Option<&Path>,
    pool: &Pool,
    tap: Option<&str>,
) -> Result<()> {
    firecracker_put(
        api_socket,
        "/machine-config",
        &json!({
            "vcpu_count": pool.cpu_millis.div_ceil(1000).max(1),
            "mem_size_mib": pool.memory_mib,
            "smt": false,
            "track_dirty_pages": false
        }),
    )
    .await?;
    if let Some(environment) = environment {
        firecracker_put(
            api_socket,
            "/drives/environment",
            &json!({
                "drive_id": "environment",
                "path_on_host": environment,
                "is_root_device": false,
                "is_read_only": true
            }),
        )
        .await?;
    }
    firecracker_put(
        api_socket,
        "/boot-source",
        &json!({
            "kernel_image_path": kernel,
            "boot_args": "console=ttyS0 reboot=k panic=1 pci=off ro init=/sbin/scraperack-init quiet"
        }),
    )
    .await?;
    firecracker_put(
        api_socket,
        "/drives/rootfs",
        &json!({
            "drive_id": "rootfs",
            "path_on_host": rootfs,
            "is_root_device": true,
            "is_read_only": true
        }),
    )
    .await?;
    firecracker_put(
        api_socket,
        "/vsock",
        &json!({ "guest_cid": 3, "uds_path": vsock }),
    )
    .await?;
    if let Some(tap) = tap {
        firecracker_put(
            api_socket,
            "/network-interfaces/net1",
            &json!({
                "iface_id": "net1",
                "host_dev_name": tap,
                "guest_mac": "06:00:AC:10:00:02",
            }),
        )
        .await?;
    }
    Ok(())
}

async fn guest_request(vsock: &Path, payload: &Value) -> Result<Completion> {
    let mut stream = loop {
        match connect_vsock(vsock).await {
            Ok(stream) => break stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    guest_request_stream(&mut stream, payload).await
}

async fn guest_request_stream(stream: &mut UnixStream, payload: &Value) -> Result<Completion> {
    let mut request = serde_json::to_vec(payload)?;
    request.push(b'\n');
    stream.write_all(&request).await?;
    let mut response = String::new();
    BufReader::new(&mut *stream)
        .read_line(&mut response)
        .await?;
    if response.is_empty() {
        bail!("guest agent closed the connection without a response");
    }
    serde_json::from_str(&response).context("invalid guest response")
}

async fn connect_vsock(path: &Path) -> Result<UnixStream> {
    let mut stream = UnixStream::connect(path).await?;
    stream
        .write_all(format!("CONNECT {GUEST_PORT}\n").as_bytes())
        .await?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).await?;
    if !response.starts_with("OK ") {
        bail!("vsock rejected connection: {}", response.trim());
    }
    Ok(reader.into_inner())
}

async fn wait_for_path(path: &Path, child: &mut Child) -> Result<()> {
    for _ in 0..300 {
        if path.exists() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            bail!("firecracker exited before creating its API socket: {status}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("timed out waiting for firecracker API socket")
}

async fn firecracker_put(socket: &Path, path: &str, body: &Value) -> Result<()> {
    firecracker_request("PUT", socket, path, body).await
}

async fn firecracker_request(method: &str, socket: &Path, path: &str, body: &Value) -> Result<()> {
    let body = serde_json::to_vec(body)?;
    let mut stream = UnixStream::connect(socket).await?;
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    stream.write_all(&body).await?;
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status).await?;
    let mut content_length = 0;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).await?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse()?;
        }
    }
    let mut response = vec![0; content_length];
    reader.read_exact(&mut response).await?;
    if !status.contains(" 200 ") && !status.contains(" 204 ") {
        bail!(
            "firecracker {path} failed: {} {}",
            status.trim(),
            String::from_utf8_lossy(&response).trim()
        );
    }
    Ok(())
}

fn execution_metrics(
    before: &VmCounters,
    after: &VmCounters,
    wall_time_usec: u64,
    memory_total: u128,
    memory_count: u64,
    samples: Vec<ExecutionMetricPoint>,
    pool: &Pool,
) -> ExecutionMetrics {
    ExecutionMetrics {
        average_cpu_percent: cpu_percent(
            after.cpu_usage_usec.saturating_sub(before.cpu_usage_usec),
            wall_time_usec,
            pool.cpu_millis,
        ),
        average_memory_percent: memory_percent(
            (memory_total / u128::from(memory_count)) as u64,
            pool.memory_mib,
        ),
        peak_memory_percent: samples
            .iter()
            .map(|sample| sample.memory_percent)
            .fold(0.0, f32::max),
        samples,
    }
}

fn push_metric_sample(
    samples: &mut Vec<ExecutionMetricPoint>,
    before: &VmCounters,
    after: &VmCounters,
    before_elapsed_us: u64,
    elapsed_us: u64,
    pool: &Pool,
) {
    if samples.len() >= 1000 {
        *samples = samples.iter().step_by(2).cloned().collect();
    }
    samples.push(metric_point(
        elapsed_us / 1000,
        cpu_percent(
            after.cpu_usage_usec.saturating_sub(before.cpu_usage_usec),
            elapsed_us.saturating_sub(before_elapsed_us),
            pool.cpu_millis,
        ),
        after.memory_bytes,
        pool,
    ));
}

fn metric_point(
    elapsed_ms: u64,
    cpu_percent: f32,
    memory_bytes: u64,
    pool: &Pool,
) -> ExecutionMetricPoint {
    ExecutionMetricPoint {
        elapsed_ms,
        cpu_percent,
        memory_percent: memory_percent(memory_bytes, pool.memory_mib),
    }
}

fn cpu_percent(cpu_usec: u64, elapsed_usec: u64, cpu_millis: u32) -> f32 {
    if elapsed_usec == 0 || cpu_millis == 0 {
        return 0.0;
    }
    (cpu_usec as f64 * 100_000.0 / (elapsed_usec as f64 * f64::from(cpu_millis))).clamp(0.0, 100.0)
        as f32
}

fn memory_percent(memory_bytes: u64, memory_mib: u32) -> f32 {
    let limit = u64::from(memory_mib + 128) * 1024 * 1024;
    (memory_bytes as f64 * 100.0 / limit as f64).clamp(0.0, 100.0) as f32
}

async fn read_vm_counters(cgroup: &Path) -> Result<VmCounters> {
    let cpu = fs::read_to_string(cgroup.join("cpu.stat")).await?;
    let memory_bytes = read_number(&cgroup.join("memory.current")).await?;
    Ok(VmCounters {
        cpu_usage_usec: cpu
            .lines()
            .find_map(|line| line.strip_prefix("usage_usec "))
            .and_then(|value| value.parse().ok())
            .unwrap_or_default(),
        memory_bytes,
    })
}

async fn read_number(path: &Path) -> Result<u64> {
    Ok(fs::read_to_string(path).await?.trim().parse()?)
}

async fn attach_cgroup(pid: u32, pool: &Pool, id: Uuid) -> Result<()> {
    let path = vm_cgroup(id).await?;
    fs::create_dir(&path).await.context("create VM cgroup")?;
    fs::write(
        path.join("cpu.max"),
        format!("{} 100000", u64::from(pool.cpu_millis) * 100),
    )
    .await?;
    fs::write(
        path.join("memory.max"),
        ((u64::from(pool.memory_mib) + 128) * 1024 * 1024).to_string(),
    )
    .await?;
    fs::write(path.join("pids.max"), "64").await?;
    fs::write(path.join("cgroup.procs"), pid.to_string()).await?;
    Ok(())
}

async fn prepare_cgroups() -> Result<()> {
    let root = current_cgroup().await?;
    let supervisor = root.join("supervisor");
    fs::create_dir_all(&supervisor).await?;
    fs::write(
        supervisor.join("cgroup.procs"),
        std::process::id().to_string(),
    )
    .await?;
    fs::write(root.join("cgroup.subtree_control"), "+cpu +memory +pids").await?;
    Ok(())
}

async fn remove_cgroup(id: Uuid) -> Result<()> {
    fs::remove_dir(vm_cgroup(id).await?).await?;
    Ok(())
}

async fn vm_cgroup(id: Uuid) -> Result<PathBuf> {
    Ok(cgroup_root().await?.join(format!("vm-{id}")))
}

async fn cgroup_root() -> Result<PathBuf> {
    let mut path = current_cgroup().await?;
    if path.file_name().is_some_and(|name| name == "supervisor") {
        path.pop();
    }
    Ok(path)
}

async fn current_cgroup() -> Result<PathBuf> {
    let state = fs::read_to_string("/proc/self/cgroup").await?;
    let path = state
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("cgroup v2 is required")?;
    Ok(Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

async fn download(client: &Client, url: &str, path: &Path) -> Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;
    let temp = path.with_extension("partial");
    let mut file = fs::File::create(&temp).await?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    fs::rename(temp, path).await?;
    Ok(())
}

async fn is_ext4(path: &Path) -> Result<bool> {
    let mut file = fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(1080)).await?;
    let mut magic = [0; 2];
    Ok(file.read_exact(&mut magic).await.is_ok() && magic == [0x53, 0xef])
}

async fn hash(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut digest = Sha256::new();
    tokio::io::copy(&mut file, &mut DigestWriter(&mut digest)).await?;
    Ok(hex::encode(digest.finalize()))
}

struct DigestWriter<'a>(&'a mut Sha256);

impl tokio::io::AsyncWrite for DigestWriter<'_> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.update(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{Completion, default_vm_start_concurrency, redact_completion};

    #[test]
    fn vm_start_concurrency_is_half_the_node_vcpus_with_a_minimum_of_one() {
        assert_eq!(default_vm_start_concurrency(500), 1);
        assert_eq!(default_vm_start_concurrency(1000), 1);
        assert_eq!(default_vm_start_concurrency(4000), 2);
        assert_eq!(default_vm_start_concurrency(8000), 4);
        assert_eq!(default_vm_start_concurrency(16_000), 8);
    }

    #[test]
    fn redacts_secrets_from_output_and_errors_but_not_results() {
        let mut completion = Completion {
            result: Some(json!({
                "value": {"encoding": "json", "data": "keep secret-value"},
                "stdout": "printed secret-value",
                "stderr": "warned secret-value",
            })),
            error: Some("failed with secret-value".into()),
            timed_out: false,
            metrics: None,
            keep_idle: false,
        };
        redact_completion(
            &mut completion,
            &HashMap::from([("TOKEN".into(), "secret-value".into())]),
        );

        let result = completion.result.unwrap();
        assert_eq!(result["stdout"], "printed [REDACTED]");
        assert_eq!(result["stderr"], "warned [REDACTED]");
        assert_eq!(result["value"]["data"], "keep secret-value");
        assert_eq!(completion.error.unwrap(), "failed with [REDACTED]");
    }
}
