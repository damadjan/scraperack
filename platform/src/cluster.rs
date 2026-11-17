use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Image {
    pub id: Uuid,
    pub name: String,
    pub sha256: String,
    pub size: u64,
    #[serde(default = "default_compatible")]
    pub compatible: bool,
    #[serde(default = "default_executor")]
    pub executor: Option<String>,
    #[serde(default = "default_image_kind")]
    pub kind: ImageKind,
    #[serde(default)]
    pub docker_source: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageKind {
    MicroVm,
    Runtime,
}

fn default_image_kind() -> ImageKind {
    ImageKind::Runtime
}

fn default_compatible() -> bool {
    true
}

fn default_executor() -> Option<String> {
    Some("Python 3.10.11".into())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pool {
    pub name: String,
    pub image: Uuid,
    #[serde(default)]
    pub warm_instances: u32,
    pub max_instances: Option<u32>,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    pub disk_mib: u32,
    pub timeout_secs: u64,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_reuse")]
    pub reuse: bool,
    #[serde(default)]
    pub network: bool,
    #[serde(default)]
    pub ports: Vec<PortSpec>,
    #[serde(default)]
    pub managed_by: Option<String>,
    #[serde(default)]
    pub environment: Option<Uuid>,
    #[serde(default, skip_serializing)]
    pub environment_artifact: Option<String>,
    #[serde(default)]
    pub preload: Vec<String>,
    /// Bumped whenever VM-affecting settings change; stale VMs are recycled.
    #[serde(default)]
    pub revision: u64,
    /// Extra read-only disks attached after the root filesystem, in order.
    #[serde(default)]
    pub disks: Vec<PoolDisk>,
    /// Command the guest agent starts to handle task payloads.
    #[serde(default)]
    pub executor: Vec<String>,
    /// Extra names that resolve to this pool when submitting tasks.
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Where a task's volume is mounted in the guest. Pools with a mount get a
    /// volume drive in their snapshot; a task that names a volume attaches it there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mount: Option<String>,
    /// Back guest memory with 2 MiB huge pages. Faster for memory-hungry guests (browsers)
    /// under nested virtualization, where every new 4 KiB page costs a slow fault.
    #[serde(default)]
    pub huge_pages: bool,
}

impl Pool {
    pub fn disks_ready(&self, node: &Node) -> bool {
        self.disks.iter().all(|disk| node.disks.contains(&disk.key))
    }

    pub fn matches(&self, key: &str) -> bool {
        self.name == key || self.aliases.iter().any(|alias| alias == key)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolDisk {
    pub key: String,
    pub mount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Disk {
    pub key: String,
    pub sha256: String,
    pub size: u64,
    #[serde(default)]
    pub owner: Option<String>,
}

/// A persistent, writable disk that lives on one node and follows its tasks there.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    pub name: String,
    pub size_mib: u32,
    #[serde(default)]
    pub owner: Option<String>,
    /// Node that holds the volume's files; set when a task first uses it.
    #[serde(default)]
    pub node: Option<Uuid>,
    /// A suspended microVM's memory and device state is saved with the volume.
    #[serde(default)]
    pub suspended: bool,
    #[serde(default)]
    pub migration: Option<VolumeMigration>,
    #[serde(default)]
    pub migration_failed_at_ms: Option<u64>,
    #[serde(default)]
    pub migration_error: Option<String>,
    pub created_at_ms: u64,
}

/// The volume's files are being copied from one node to another.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeMigration {
    pub from: Uuid,
    pub to: Uuid,
    pub started_at_ms: u64,
}

/// A node that has not reported for this long is offline.
pub const NODE_ONLINE_MS: u64 = 15_000;
/// A migration that takes longer than this is abandoned.
pub const MIGRATION_TIMEOUT_MS: u64 = 60 * 60 * 1000;
/// After a failed migration, tasks start on the volume's node for this long.
pub const MIGRATION_RETRY_MS: u64 = 5 * 60 * 1000;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRecord {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub previous_version: Option<String>,
    pub enabled: bool,
    #[serde(default)]
    pub gui: bool,
    #[serde(default)]
    pub guest_disk: Option<String>,
    /// Guest disk of `previous_version`, kept so a rollback can use it.
    #[serde(default)]
    pub previous_guest_disk: Option<String>,
    /// Content digest of the guest folder `guest_disk` was built from; an
    /// update with the same files reuses the disk so microVMs keep running.
    #[serde(default)]
    pub guest_digest: Option<String>,
    #[serde(default)]
    pub previous_guest_digest: Option<String>,
    pub installed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: Uuid,
    pub name: String,
    pub image: Uuid,
    pub packages: Vec<String>,
    #[serde(default)]
    pub preload: Vec<String>,
    pub artifact: Option<String>,
    pub sha256: Option<String>,
    pub size: u64,
    pub pool: String,
    pub warm_instances: u32,
    pub max_instances: Option<u32>,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    pub disk_mib: u32,
    pub timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub reuse: bool,
    pub network: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum PortProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortSpec {
    pub name: String,
    pub guest_port: u16,
    pub host_port: Option<u16>,
    #[serde(default = "default_protocol")]
    pub protocol: PortProtocol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedPort {
    pub name: String,
    pub guest_port: u16,
    pub host_port: u16,
    pub protocol: PortProtocol,
}

fn default_protocol() -> PortProtocol {
    PortProtocol::Tcp
}

fn default_idle_timeout() -> u64 {
    30
}

fn default_reuse() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Syncing,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmStatus {
    Idle,
    Running,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroVm {
    pub id: Uuid,
    pub pool: String,
    pub status: VmStatus,
    pub task: Option<Uuid>,
    pub pid: Option<u32>,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub guest_ip: Option<Ipv4Addr>,
    #[serde(default)]
    pub ports: Vec<PublishedPort>,
    #[serde(default)]
    pub environment: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub name: String,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub vm_start_concurrency: Option<usize>,
    #[serde(default)]
    pub address: Option<IpAddr>,
    pub used_cpu_millis: u32,
    pub used_memory_mib: u32,
    pub images: HashSet<Uuid>,
    #[serde(default, skip_serializing)]
    pub environments: HashSet<String>,
    #[serde(default)]
    pub disks: HashSet<String>,
    #[serde(default)]
    pub idle_instances: HashMap<String, u32>,
    #[serde(default)]
    pub vms: Vec<MicroVm>,
    pub status: NodeStatus,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinToken {
    pub id: Uuid,
    pub token_hash: String,
    pub expires_at_ms: u64,
    pub used_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Assigned,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}

/// How a running task is ended early.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    /// Flush the guest's disks and destroy the microVM.
    Stop,
    /// Save the microVM's state with its volume, so a later task resumes it.
    Suspend,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskMetricPoint {
    pub elapsed_ms: u64,
    pub cpu_percent: f32,
    pub memory_percent: f32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskMetrics {
    pub average_cpu_percent: f32,
    pub average_memory_percent: f32,
    pub peak_memory_percent: f32,
    pub samples: Vec<TaskMetricPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub pool: String,
    /// Who managed the pool when the task was created, such as `plugin:python`.
    #[serde(default)]
    pub owner: Option<String>,
    pub node: Option<Uuid>,
    pub status: TaskStatus,
    pub payload: Value,
    #[serde(default)]
    pub variables: HashMap<String, String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    #[serde(default)]
    pub metrics: Option<TaskMetrics>,
    /// Volume attached to the task's microVM; the task runs where the volume is.
    #[serde(default)]
    pub volume: Option<String>,
    /// Requested early end, delivered to the node with its heartbeat.
    #[serde(skip)]
    pub stop: Option<StopMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskView {
    pub id: Uuid,
    pub pool: String,
    pub owner: Option<String>,
    pub node: Option<Uuid>,
    pub status: TaskStatus,
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub metrics: Option<TaskMetrics>,
    #[serde(default)]
    pub volume: Option<String>,
}

impl From<&Task> for TaskView {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id,
            pool: task.pool.clone(),
            owner: task.owner.clone(),
            node: task.node,
            status: task.status.clone(),
            error: task.error.clone(),
            created_at_ms: task.created_at_ms,
            started_at_ms: task.started_at_ms,
            finished_at_ms: task.finished_at_ms,
            metrics: task.metrics.clone(),
            volume: task.volume.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Work {
    pub task: Task,
    pub pool: Pool,
    #[serde(default)]
    pub secrets: HashMap<String, String>,
    #[serde(default)]
    pub volume: Option<Volume>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cluster {
    pub images: HashMap<Uuid, Image>,
    pub pools: HashMap<String, Pool>,
    pub nodes: HashMap<Uuid, Node>,
    #[serde(default, skip_serializing)]
    pub tasks: HashMap<Uuid, Task>,
    /// Legacy Python environments, migrated to the Python plugin on startup.
    #[serde(default, skip_serializing)]
    pub environments: HashMap<Uuid, Environment>,
    #[serde(default)]
    pub disks: HashMap<String, Disk>,
    #[serde(default)]
    pub volumes: HashMap<String, Volume>,
    #[serde(default)]
    pub plugins: HashMap<String, PluginRecord>,
    #[serde(default)]
    pub join_tokens: HashMap<Uuid, JoinToken>,
    #[serde(default)]
    pub node_credentials: HashMap<Uuid, String>,
    #[serde(default)]
    round_robin: usize,
}

impl Cluster {
    pub fn image_ready(&self, image: Uuid) -> bool {
        self.nodes.values().all(|node| node.images.contains(&image))
    }

    pub fn disk_ready(&self, key: &str) -> bool {
        self.nodes.values().all(|node| node.disks.contains(key))
    }

    pub fn pool_ready(&self, pool: &Pool) -> bool {
        self.image_ready(pool.image) && pool.disks.iter().all(|disk| self.disk_ready(&disk.key))
    }

    pub fn find_pool(&self, key: &str) -> Option<&Pool> {
        self.pools
            .get(key)
            .or_else(|| self.pools.values().find(|pool| pool.matches(key)))
    }

    pub fn refresh_node(&mut self, id: Uuid) {
        let images = self.images.keys().copied().collect::<HashSet<_>>();
        if let Some(node) = self.nodes.get_mut(&id) {
            node.status = if images.is_subset(&node.images) {
                NodeStatus::Ready
            } else {
                NodeStatus::Syncing
            };
        }
    }

    pub fn schedule(&mut self, task_id: Uuid) -> bool {
        let Some(task) = self.tasks.get(&task_id) else {
            return false;
        };
        let Some(pool) = self.pools.get(&task.pool) else {
            return false;
        };
        let active = self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count();
        if pool
            .max_instances
            .is_some_and(|limit| active >= limit as usize)
        {
            return false;
        }
        let idle = self
            .nodes
            .values()
            .map(|node| node.idle_instances.get(&pool.name).copied().unwrap_or(0) as usize)
            .sum::<usize>();
        let can_start = pool
            .max_instances
            .is_none_or(|limit| active + idle < limit as usize);
        if let Some(volume) = task.volume.clone() {
            let pool = pool.clone();
            return self.schedule_with_volume(task_id, &pool, &volume, can_start);
        }

        let mut candidates = self
            .nodes
            .values()
            .filter(|node| {
                let has_idle = node.idle_instances.get(&pool.name).copied().unwrap_or(0) > 0;
                node.status == NodeStatus::Ready
                    && node.images.contains(&pool.image)
                    && pool.disks_ready(node)
                    && (has_idle
                        || (can_start
                            && node.cpu_millis >= pool.cpu_millis
                            && node.memory_mib >= pool.memory_mib))
            })
            .map(|node| {
                (
                    node.idle_instances.get(&pool.name).copied().unwrap_or(0) == 0,
                    node.id,
                )
            })
            .collect::<Vec<_>>();
        candidates.sort();
        if candidates.is_empty() {
            return false;
        }

        let prefer_idle = candidates[0].0;
        let preferred = candidates
            .iter()
            .take_while(|candidate| candidate.0 == prefer_idle)
            .collect::<Vec<_>>();
        let node_id = preferred[self.round_robin % preferred.len()].1;
        self.round_robin = self.round_robin.wrapping_add(1);
        let node = self.nodes.get_mut(&node_id).unwrap();
        let idle = node.idle_instances.entry(pool.name.clone()).or_default();
        if *idle > 0 {
            *idle -= 1;
        } else {
            node.used_cpu_millis = node.used_cpu_millis.saturating_add(pool.cpu_millis);
            node.used_memory_mib = node.used_memory_mib.saturating_add(pool.memory_mib);
        }
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.node = Some(node_id);
        task.status = TaskStatus::Assigned;
        true
    }

    /// Place a task whose microVM attaches a volume.
    ///
    /// The task runs on the node that holds the volume. When that node has no free
    /// memory for the task but another online node does, the volume (with any
    /// suspended state) is moved there first and the task waits in the queue.
    /// When no node has room, the task overcommits the volume's node, like any task.
    fn schedule_with_volume(
        &mut self,
        task_id: Uuid,
        pool: &Pool,
        name: &str,
        can_start: bool,
    ) -> bool {
        let now = now_ms();
        let Some(volume) = self.volumes.get_mut(name) else {
            return false;
        };
        if let Some(migration) = &volume.migration {
            if now.saturating_sub(migration.started_at_ms) < MIGRATION_TIMEOUT_MS {
                return false;
            }
            volume.migration = None;
            volume.migration_failed_at_ms = Some(now);
            volume.migration_error = Some("the move timed out".into());
        }
        let volume = volume.clone();
        if self.tasks.values().any(|task| {
            task.id != task_id
                && task.volume.as_deref() == Some(name)
                && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
        }) {
            return false;
        }
        let idle_on = |node: &Node| node.idle_instances.get(&pool.name).copied().unwrap_or(0);
        let usable = |node: &Node| {
            node.status == NodeStatus::Ready
                && now.saturating_sub(node.last_seen_ms) < NODE_ONLINE_MS
                && node.images.contains(&pool.image)
                && pool.disks_ready(node)
                && node.cpu_millis >= pool.cpu_millis
                && node.memory_mib >= pool.memory_mib
                && (can_start || idle_on(node) > 0)
        };
        let fits = |node: &Node| {
            idle_on(node) > 0
                || node.used_memory_mib.saturating_add(pool.memory_mib) <= node.memory_mib
        };
        let free = |node: &Node| node.memory_mib.saturating_sub(node.used_memory_mib);
        // Roomiest usable node first; ties go to the lower ID so placement is stable.
        let best = |exclude: Option<Uuid>, need_fit: bool| {
            self.nodes
                .values()
                .filter(|node| Some(node.id) != exclude && usable(node))
                .filter(|node| !need_fit || fits(node))
                .max_by(|left, right| {
                    (fits(left), free(left))
                        .cmp(&(fits(right), free(right)))
                        .then(right.id.cmp(&left.id))
                })
                .map(|node| node.id)
        };
        let node_id = match volume.node {
            None => match best(None, false) {
                Some(node) => node,
                None => return false,
            },
            Some(home) => {
                let home_node = self.nodes.get(&home);
                if home_node.is_some_and(|node| usable(node) && fits(node)) {
                    home
                } else {
                    let home_online = home_node
                        .is_some_and(|node| now.saturating_sub(node.last_seen_ms) < NODE_ONLINE_MS);
                    let may_move = volume
                        .migration_failed_at_ms
                        .is_none_or(|failed| now.saturating_sub(failed) >= MIGRATION_RETRY_MS);
                    if home_online
                        && may_move
                        && let Some(target) = best(Some(home), true)
                    {
                        let volume = self.volumes.get_mut(name).unwrap();
                        volume.migration = Some(VolumeMigration {
                            from: home,
                            to: target,
                            started_at_ms: now,
                        });
                        volume.migration_error = None;
                        return false;
                    }
                    if home_node.is_some_and(usable) {
                        home
                    } else {
                        return false;
                    }
                }
            }
        };
        let node = self.nodes.get_mut(&node_id).unwrap();
        let idle = node.idle_instances.entry(pool.name.clone()).or_default();
        if *idle > 0 {
            *idle -= 1;
        } else {
            node.used_cpu_millis = node.used_cpu_millis.saturating_add(pool.cpu_millis);
            node.used_memory_mib = node.used_memory_mib.saturating_add(pool.memory_mib);
        }
        self.volumes.get_mut(name).unwrap().node = Some(node_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.node = Some(node_id);
        task.status = TaskStatus::Assigned;
        true
    }

    /// The target node holds the volume's files now.
    pub fn finish_migration(&mut self, name: &str, node: Uuid) -> bool {
        let Some(volume) = self.volumes.get_mut(name) else {
            return false;
        };
        if volume
            .migration
            .as_ref()
            .is_none_or(|move_| move_.to != node)
        {
            return false;
        }
        volume.node = Some(node);
        volume.migration = None;
        volume.migration_failed_at_ms = None;
        volume.migration_error = None;
        true
    }

    /// The volume stays where it was; its tasks start there for a while.
    pub fn fail_migration(&mut self, name: &str, node: Uuid, error: String) -> bool {
        let Some(volume) = self.volumes.get_mut(name) else {
            return false;
        };
        if volume
            .migration
            .as_ref()
            .is_none_or(|move_| move_.from != node && move_.to != node)
        {
            return false;
        }
        volume.migration = None;
        volume.migration_failed_at_ms = Some(now_ms());
        volume.migration_error = Some(error);
        true
    }

    pub fn schedule_queued(&mut self) -> Vec<Task> {
        let mut queued = self
            .tasks
            .values()
            .filter(|task| task.status == TaskStatus::Queued)
            .map(|task| (task.created_at_ms, task.id))
            .collect::<Vec<_>>();
        queued.sort();
        let mut scheduled = Vec::new();
        for (_, id) in queued {
            if self.schedule(id) {
                scheduled.push(self.tasks[&id].clone());
            }
        }
        scheduled
    }

    pub fn release(&mut self, task_id: Uuid, keep_idle: bool) {
        let Some(task) = self.tasks.get(&task_id) else {
            return;
        };
        let Some(node_id) = task.node else {
            return;
        };
        let Some(pool) = self.pools.get(&task.pool) else {
            return;
        };
        if let Some(node) = self.nodes.get_mut(&node_id) {
            if keep_idle {
                *node.idle_instances.entry(pool.name.clone()).or_default() += 1;
            } else {
                node.used_cpu_millis = node.used_cpu_millis.saturating_sub(pool.cpu_millis);
                node.used_memory_mib = node.used_memory_mib.saturating_sub(pool.memory_mib);
            }
        }
    }

    pub fn add_warm_instance(&mut self, node_id: Uuid, pool_name: &str) -> bool {
        let Some(pool) = self.pools.get(pool_name) else {
            return false;
        };
        let targets = self.warm_targets(pool);
        let Some(target) = targets.get(&node_id) else {
            return false;
        };
        let active = self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count() as u32;
        let idle = self
            .nodes
            .values()
            .map(|node| node.idle_instances.get(pool_name).copied().unwrap_or(0))
            .sum::<u32>();
        let warm = if pool.reuse { idle + active } else { idle };
        let local = self.warm_instances_on(node_id, pool);
        if local >= *target
            || warm >= targets.values().sum()
            || pool
                .max_instances
                .is_some_and(|limit| active + idle >= limit)
        {
            return false;
        }
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return false;
        };
        if node.cpu_millis < pool.cpu_millis || node.memory_mib < pool.memory_mib {
            return false;
        }
        node.used_cpu_millis = node.used_cpu_millis.saturating_add(pool.cpu_millis);
        node.used_memory_mib = node.used_memory_mib.saturating_add(pool.memory_mib);
        *node.idle_instances.entry(pool.name.clone()).or_default() += 1;
        true
    }

    pub fn warm_instance_needed(&self, node_id: Uuid, pool_name: &str) -> bool {
        let Some(pool) = self.pools.get(pool_name) else {
            return false;
        };
        let targets = self.warm_targets(pool);
        let Some(target) = targets.get(&node_id) else {
            return false;
        };
        let active = self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count() as u32;
        let idle = self
            .nodes
            .values()
            .map(|node| node.idle_instances.get(pool_name).copied().unwrap_or(0))
            .sum::<u32>();
        let warm = if pool.reuse { idle + active } else { idle };
        self.warm_instances_on(node_id, pool) < *target
            && warm < targets.values().sum()
            && pool.max_instances.is_none_or(|limit| active + idle < limit)
    }

    pub fn remove_idle_instance(&mut self, node_id: Uuid, pool_name: &str) -> bool {
        let Some(pool) = self.pools.get(pool_name) else {
            return false;
        };
        let targets = self.warm_targets(pool);
        let target = targets.get(&node_id).copied().unwrap_or(0);
        let active = self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count() as u32;
        let idle = self
            .nodes
            .values()
            .map(|node| node.idle_instances.get(pool_name).copied().unwrap_or(0))
            .sum::<u32>();
        let warm = if pool.reuse { idle + active } else { idle };
        if self.warm_instances_on(node_id, pool) <= target || warm < targets.values().sum() {
            return false;
        }
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return false;
        };
        let Some(idle) = node.idle_instances.get_mut(pool_name) else {
            return false;
        };
        if *idle == 0 {
            return false;
        }
        *idle -= 1;
        node.used_cpu_millis = node.used_cpu_millis.saturating_sub(pool.cpu_millis);
        node.used_memory_mib = node.used_memory_mib.saturating_sub(pool.memory_mib);
        true
    }

    /// Forget one idle instance regardless of warm targets, e.g. after a revision change.
    pub fn discard_idle_instance(&mut self, node_id: Uuid, pool_name: &str) -> bool {
        let Some(pool) = self.pools.get(pool_name) else {
            return false;
        };
        let (cpu, memory) = (pool.cpu_millis, pool.memory_mib);
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return false;
        };
        let Some(idle) = node
            .idle_instances
            .get_mut(pool_name)
            .filter(|idle| **idle > 0)
        else {
            return false;
        };
        *idle -= 1;
        node.used_cpu_millis = node.used_cpu_millis.saturating_sub(cpu);
        node.used_memory_mib = node.used_memory_mib.saturating_sub(memory);
        true
    }

    fn warm_targets(&self, pool: &Pool) -> HashMap<Uuid, u32> {
        let active = self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count() as u32;
        let target = if pool.reuse {
            pool.warm_instances
                .min(pool.max_instances.unwrap_or(u32::MAX))
        } else {
            pool.warm_instances.min(
                pool.max_instances
                    .map(|limit| limit.saturating_sub(active))
                    .unwrap_or(u32::MAX),
            )
        };
        let mut nodes = self
            .nodes
            .values()
            .filter(|node| {
                node.images.contains(&pool.image)
                    && pool.disks_ready(node)
                    && node.cpu_millis >= pool.cpu_millis
                    && node.memory_mib >= pool.memory_mib
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        nodes.sort_unstable();
        let Some(count) = u32::try_from(nodes.len()).ok().filter(|count| *count > 0) else {
            return HashMap::new();
        };
        nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                (
                    node,
                    target / count + u32::from(index < (target % count) as usize),
                )
            })
            .collect()
    }

    fn warm_instances_on(&self, node_id: Uuid, pool: &Pool) -> u32 {
        let idle = self
            .nodes
            .get(&node_id)
            .and_then(|node| node.idle_instances.get(&pool.name))
            .copied()
            .unwrap_or(0);
        if !pool.reuse {
            return idle;
        }
        idle + self
            .tasks
            .values()
            .filter(|task| {
                task.pool == pool.name
                    && task.node == Some(node_id)
                    && matches!(task.status, TaskStatus::Assigned | TaskStatus::Running)
            })
            .count() as u32
    }

    pub fn reset_idle_instances(&mut self, node_id: Uuid) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return;
        };
        for (name, count) in std::mem::take(&mut node.idle_instances) {
            if let Some(pool) = self.pools.get(&name) {
                node.used_cpu_millis = node
                    .used_cpu_millis
                    .saturating_sub(pool.cpu_millis.saturating_mul(count));
                node.used_memory_mib = node
                    .used_memory_mib
                    .saturating_sub(pool.memory_mib.saturating_mul(count));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: Uuid, image: Uuid) -> Node {
        Node {
            id,
            name: id.to_string(),
            cpu_millis: 1000,
            memory_mib: 512,
            vm_start_concurrency: None,
            address: None,
            used_cpu_millis: 0,
            used_memory_mib: 0,
            images: HashSet::from([image]),
            environments: HashSet::new(),
            disks: HashSet::new(),
            idle_instances: HashMap::new(),
            vms: Vec::new(),
            status: NodeStatus::Ready,
            last_seen_ms: 0,
        }
    }

    #[test]
    fn schedules_round_robin_and_overcommits_capacity() {
        let image = Uuid::new_v4();
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let mut cluster = Cluster::default();
        cluster.nodes.insert(first, node(first, image));
        cluster.nodes.insert(second, node(second, image));
        cluster.pools.insert(
            "p".into(),
            Pool {
                name: "p".into(),
                image,
                warm_instances: 0,
                max_instances: None,
                cpu_millis: 250,
                memory_mib: 512,
                disk_mib: 1024,
                timeout_secs: 60,
                idle_timeout_secs: 30,
                reuse: true,
                network: false,
                ports: Vec::new(),
                managed_by: None,
                environment: None,
                environment_artifact: None,
                preload: Vec::new(),
                ..Default::default()
            },
        );

        for expected in [first, second] {
            let id = Uuid::new_v4();
            cluster.tasks.insert(
                id,
                Task {
                    id,
                    pool: "p".into(),
                    owner: None,
                    node: None,
                    status: TaskStatus::Queued,
                    payload: Value::Null,
                    variables: HashMap::new(),
                    result: None,
                    error: None,
                    created_at_ms: 0,
                    started_at_ms: None,
                    finished_at_ms: None,
                    metrics: None,
                    volume: None,
                    stop: None,
                },
            );
            assert!(cluster.schedule(id));
            assert_eq!(cluster.tasks[&id].node, Some(expected));
        }

        let id = Uuid::new_v4();
        cluster.tasks.insert(
            id,
            Task {
                id,
                pool: "p".into(),
                owner: None,
                node: None,
                status: TaskStatus::Queued,
                payload: Value::Null,
                variables: HashMap::new(),
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                metrics: None,
                volume: None,
                stop: None,
            },
        );
        assert!(cluster.schedule(id));
        assert_eq!(cluster.tasks[&id].node, Some(first));
        assert_eq!(cluster.nodes[&first].used_memory_mib, 1024);
        cluster.pools.get_mut("p").unwrap().warm_instances = 4;
        assert!(cluster.add_warm_instance(second, "p"));
        assert_eq!(cluster.nodes[&second].used_memory_mib, 1024);
    }

    #[test]
    fn balances_warm_instances_between_nodes() {
        let image = Uuid::new_v4();
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let mut cluster = Cluster::default();
        cluster.nodes.insert(first, node(first, image));
        cluster.nodes.insert(second, node(second, image));
        cluster.pools.insert(
            "p".into(),
            Pool {
                name: "p".into(),
                image,
                warm_instances: 4,
                max_instances: None,
                cpu_millis: 250,
                memory_mib: 256,
                disk_mib: 1024,
                timeout_secs: 60,
                idle_timeout_secs: 30,
                reuse: true,
                network: false,
                ports: Vec::new(),
                managed_by: None,
                environment: None,
                environment_artifact: None,
                preload: Vec::new(),
                ..Default::default()
            },
        );

        for node in [first, second] {
            assert!(cluster.warm_instance_needed(node, "p"));
            assert!(cluster.add_warm_instance(node, "p"));
            assert!(cluster.warm_instance_needed(node, "p"));
            assert!(cluster.add_warm_instance(node, "p"));
            assert!(!cluster.warm_instance_needed(node, "p"));
            assert!(!cluster.add_warm_instance(node, "p"));
        }
        assert_eq!(cluster.nodes[&first].idle_instances["p"], 2);
        assert_eq!(cluster.nodes[&second].idle_instances["p"], 2);

        cluster
            .nodes
            .get_mut(&first)
            .unwrap()
            .idle_instances
            .insert("p".into(), 4);
        cluster
            .nodes
            .get_mut(&second)
            .unwrap()
            .idle_instances
            .insert("p".into(), 0);
        cluster.nodes.get_mut(&first).unwrap().used_cpu_millis = 1000;
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 1024;
        cluster.nodes.get_mut(&second).unwrap().used_cpu_millis = 0;
        cluster.nodes.get_mut(&second).unwrap().used_memory_mib = 0;
        assert!(cluster.remove_idle_instance(first, "p"));
        assert!(!cluster.remove_idle_instance(first, "p"));
        assert!(cluster.add_warm_instance(second, "p"));
        assert!(cluster.remove_idle_instance(first, "p"));
        assert!(cluster.add_warm_instance(second, "p"));
        assert_eq!(cluster.nodes[&first].idle_instances["p"], 2);
        assert_eq!(cluster.nodes[&second].idle_instances["p"], 2);

        cluster.nodes.get_mut(&first).unwrap().images.clear();
        for _ in 0..2 {
            assert!(cluster.remove_idle_instance(first, "p"));
            assert!(cluster.add_warm_instance(second, "p"));
        }
        assert_eq!(cluster.nodes[&first].idle_instances["p"], 0);
        assert_eq!(cluster.nodes[&second].idle_instances["p"], 4);
    }

    #[test]
    fn rejects_an_instance_larger_than_a_node() {
        let image = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let mut cluster = Cluster::default();
        cluster.nodes.insert(node_id, node(node_id, image));
        cluster.pools.insert(
            "p".into(),
            Pool {
                name: "p".into(),
                image,
                warm_instances: 1,
                max_instances: None,
                cpu_millis: 1001,
                memory_mib: 513,
                disk_mib: 1024,
                timeout_secs: 60,
                idle_timeout_secs: 30,
                reuse: true,
                network: false,
                ports: Vec::new(),
                managed_by: None,
                environment: None,
                environment_artifact: None,
                preload: Vec::new(),
                ..Default::default()
            },
        );
        cluster.tasks.insert(
            task_id,
            Task {
                id: task_id,
                pool: "p".into(),
                owner: None,
                node: None,
                status: TaskStatus::Queued,
                payload: Value::Null,
                variables: HashMap::new(),
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                metrics: None,
                volume: None,
                stop: None,
            },
        );

        assert!(!cluster.schedule(task_id));
        assert!(!cluster.add_warm_instance(node_id, "p"));
    }

    #[test]
    fn reuses_idle_capacity_without_reserving_it_twice() {
        let image = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let mut cluster = Cluster::default();
        let mut worker = node(node_id, image);
        worker.used_cpu_millis = 250;
        worker.used_memory_mib = 256;
        worker.idle_instances.insert("p".into(), 1);
        cluster.nodes.insert(node_id, worker);
        cluster.pools.insert(
            "p".into(),
            Pool {
                name: "p".into(),
                image,
                warm_instances: 0,
                max_instances: Some(1),
                cpu_millis: 250,
                memory_mib: 256,
                disk_mib: 1024,
                timeout_secs: 60,
                idle_timeout_secs: 30,
                reuse: true,
                network: false,
                ports: Vec::new(),
                managed_by: None,
                environment: None,
                environment_artifact: None,
                preload: Vec::new(),
                ..Default::default()
            },
        );
        cluster.tasks.insert(
            task_id,
            Task {
                id: task_id,
                pool: "p".into(),
                owner: None,
                node: None,
                status: TaskStatus::Queued,
                payload: Value::Null,
                variables: HashMap::new(),
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                metrics: None,
                volume: None,
                stop: None,
            },
        );

        assert!(cluster.schedule(task_id));
        assert_eq!(cluster.nodes[&node_id].idle_instances["p"], 0);
        assert_eq!(cluster.nodes[&node_id].used_memory_mib, 256);
        cluster.release(task_id, true);
        assert_eq!(cluster.nodes[&node_id].idle_instances["p"], 1);
        assert_eq!(cluster.nodes[&node_id].used_memory_mib, 256);
    }

    fn volume_task(cluster: &mut Cluster, volume: &str) -> Uuid {
        let id = Uuid::new_v4();
        cluster.tasks.insert(
            id,
            Task {
                id,
                pool: "desk".into(),
                owner: None,
                node: None,
                status: TaskStatus::Queued,
                payload: Value::Null,
                variables: HashMap::new(),
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                metrics: None,
                volume: Some(volume.into()),
                stop: None,
            },
        );
        id
    }

    fn volume_cluster() -> (Cluster, Uuid, Uuid) {
        let image = Uuid::new_v4();
        let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut cluster = Cluster::default();
        for id in [first, second] {
            let mut node = node(id, image);
            node.memory_mib = 8192;
            node.last_seen_ms = now_ms();
            cluster.nodes.insert(id, node);
        }
        cluster.pools.insert(
            "desk".into(),
            Pool {
                name: "desk".into(),
                image,
                cpu_millis: 1000,
                memory_mib: 2048,
                disk_mib: 1024,
                timeout_secs: 60,
                volume_mount: Some("/home".into()),
                ..Default::default()
            },
        );
        cluster.volumes.insert(
            "home".into(),
            Volume {
                name: "home".into(),
                size_mib: 1024,
                owner: None,
                node: None,
                suspended: false,
                migration: None,
                migration_failed_at_ms: None,
                migration_error: None,
                created_at_ms: 0,
            },
        );
        (cluster, first, second)
    }

    #[test]
    fn places_a_new_volume_on_the_roomiest_node_and_follows_it() {
        let (mut cluster, first, second) = volume_cluster();
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 4096;
        let task = volume_task(&mut cluster, "home");
        assert!(cluster.schedule(task));
        assert_eq!(cluster.tasks[&task].node, Some(second));
        assert_eq!(cluster.volumes["home"].node, Some(second));

        let again = volume_task(&mut cluster, "home");
        assert!(!cluster.schedule(again), "one task per volume at a time");
        cluster.release(task, false);
        cluster.tasks.remove(&task);
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 0;
        assert!(cluster.schedule(again));
        assert_eq!(
            cluster.tasks[&again].node,
            Some(second),
            "tasks follow the volume"
        );
    }

    #[test]
    fn moves_a_volume_to_a_node_with_room() {
        let (mut cluster, first, second) = volume_cluster();
        cluster.volumes.get_mut("home").unwrap().node = Some(first);
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 7000;
        let task = volume_task(&mut cluster, "home");
        assert!(!cluster.schedule(task));
        let migration = cluster.volumes["home"].migration.clone().unwrap();
        assert_eq!((migration.from, migration.to), (first, second));
        assert!(!cluster.schedule(task), "the task waits for the move");
        assert!(
            !cluster.finish_migration("home", first),
            "only the target finishes"
        );
        assert!(cluster.finish_migration("home", second));
        assert!(cluster.schedule(task));
        assert_eq!(cluster.tasks[&task].node, Some(second));
    }

    #[test]
    fn overcommits_the_volume_node_when_no_node_has_room() {
        let (mut cluster, first, second) = volume_cluster();
        cluster.volumes.get_mut("home").unwrap().node = Some(first);
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 7000;
        cluster.nodes.get_mut(&second).unwrap().used_memory_mib = 7000;
        let task = volume_task(&mut cluster, "home");
        assert!(cluster.schedule(task));
        assert_eq!(cluster.tasks[&task].node, Some(first));
        assert!(cluster.volumes["home"].migration.is_none());
    }

    #[test]
    fn waits_for_an_offline_volume_node_and_retries_failed_moves_later() {
        let (mut cluster, first, second) = volume_cluster();
        cluster.volumes.get_mut("home").unwrap().node = Some(first);
        cluster.nodes.get_mut(&first).unwrap().last_seen_ms = 0;
        let task = volume_task(&mut cluster, "home");
        assert!(!cluster.schedule(task), "the files are on the offline node");
        assert!(cluster.volumes["home"].migration.is_none());

        cluster.nodes.get_mut(&first).unwrap().last_seen_ms = now_ms();
        cluster.nodes.get_mut(&first).unwrap().used_memory_mib = 7000;
        assert!(!cluster.schedule(task));
        assert!(cluster.fail_migration("home", second, "copy failed".into()));
        assert_eq!(
            cluster.volumes["home"].migration_error.as_deref(),
            Some("copy failed")
        );
        assert!(
            cluster.schedule(task),
            "after a failed move it starts at home"
        );
        assert_eq!(cluster.tasks[&task].node, Some(first));
    }
}
