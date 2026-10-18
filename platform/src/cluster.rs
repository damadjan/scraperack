use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(default)]
    pub environment_artifact: Option<String>,
    #[serde(default)]
    pub preload: Vec<String>,
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
    pub execution: Option<Uuid>,
    pub pid: Option<u32>,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub guest_ip: Option<Ipv4Addr>,
    #[serde(default)]
    pub ports: Vec<PublishedPort>,
    #[serde(default)]
    pub environment: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub name: String,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub address: Option<IpAddr>,
    pub used_cpu_millis: u32,
    pub used_memory_mib: u32,
    pub images: HashSet<Uuid>,
    #[serde(default)]
    pub environments: HashSet<String>,
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
pub enum ExecutionStatus {
    Queued,
    Assigned,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionMetricPoint {
    pub elapsed_ms: u64,
    pub cpu_percent: f32,
    pub memory_percent: f32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionMetrics {
    pub average_cpu_percent: f32,
    pub average_memory_percent: f32,
    pub peak_memory_percent: f32,
    pub samples: Vec<ExecutionMetricPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: Uuid,
    pub pool: String,
    pub node: Option<Uuid>,
    pub status: ExecutionStatus,
    pub payload: Value,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    #[serde(default)]
    pub environment: Option<Uuid>,
    #[serde(default)]
    pub metrics: Option<ExecutionMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionView {
    pub id: Uuid,
    pub pool: String,
    pub node: Option<Uuid>,
    pub status: ExecutionStatus,
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub environment: Option<Uuid>,
    pub metrics: Option<ExecutionMetrics>,
}

impl From<&Execution> for ExecutionView {
    fn from(execution: &Execution) -> Self {
        Self {
            id: execution.id,
            pool: execution.pool.clone(),
            node: execution.node,
            status: execution.status.clone(),
            error: execution.error.clone(),
            created_at_ms: execution.created_at_ms,
            started_at_ms: execution.started_at_ms,
            finished_at_ms: execution.finished_at_ms,
            environment: execution.environment,
            metrics: execution.metrics.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Work {
    pub execution: Execution,
    pub pool: Pool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cluster {
    pub images: HashMap<Uuid, Image>,
    pub pools: HashMap<String, Pool>,
    pub nodes: HashMap<Uuid, Node>,
    #[serde(default, skip_serializing)]
    pub executions: HashMap<Uuid, Execution>,
    #[serde(default)]
    pub environments: HashMap<Uuid, Environment>,
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

    pub fn environment_ready(&self, environment: &Environment) -> bool {
        environment.artifact.as_ref().is_none_or(|artifact| {
            self.nodes
                .values()
                .all(|node| node.environments.contains(artifact))
        })
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

    pub fn schedule(&mut self, execution_id: Uuid) -> bool {
        let Some(execution) = self.executions.get(&execution_id) else {
            return false;
        };
        let Some(pool) = self.pools.get(&execution.pool) else {
            return false;
        };
        let active = self
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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

        let mut candidates = self
            .nodes
            .values()
            .filter(|node| {
                let has_idle = node.idle_instances.get(&pool.name).copied().unwrap_or(0) > 0;
                node.status == NodeStatus::Ready
                    && node.images.contains(&pool.image)
                    && pool
                        .environment_artifact
                        .as_ref()
                        .is_none_or(|artifact| node.environments.contains(artifact))
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
        let execution = self.executions.get_mut(&execution_id).unwrap();
        execution.node = Some(node_id);
        execution.status = ExecutionStatus::Assigned;
        true
    }

    pub fn schedule_queued(&mut self) -> Vec<Execution> {
        let mut queued = self
            .executions
            .values()
            .filter(|execution| execution.status == ExecutionStatus::Queued)
            .map(|execution| (execution.created_at_ms, execution.id))
            .collect::<Vec<_>>();
        queued.sort();
        let mut scheduled = Vec::new();
        for (_, id) in queued {
            if self.schedule(id) {
                scheduled.push(self.executions[&id].clone());
            }
        }
        scheduled
    }

    pub fn release(&mut self, execution_id: Uuid, keep_idle: bool) {
        let Some(execution) = self.executions.get(&execution_id) else {
            return;
        };
        let Some(node_id) = execution.node else {
            return;
        };
        let Some(pool) = self.pools.get(&execution.pool) else {
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
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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

    fn warm_targets(&self, pool: &Pool) -> HashMap<Uuid, u32> {
        let active = self
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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
                    && pool
                        .environment_artifact
                        .as_ref()
                        .is_none_or(|artifact| node.environments.contains(artifact))
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
            .executions
            .values()
            .filter(|execution| {
                execution.pool == pool.name
                    && execution.node == Some(node_id)
                    && matches!(
                        execution.status,
                        ExecutionStatus::Assigned | ExecutionStatus::Running
                    )
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
            address: None,
            used_cpu_millis: 0,
            used_memory_mib: 0,
            images: HashSet::from([image]),
            environments: HashSet::new(),
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
            },
        );

        for expected in [first, second] {
            let id = Uuid::new_v4();
            cluster.executions.insert(
                id,
                Execution {
                    id,
                    pool: "p".into(),
                    node: None,
                    status: ExecutionStatus::Queued,
                    payload: Value::Null,
                    result: None,
                    error: None,
                    created_at_ms: 0,
                    started_at_ms: None,
                    finished_at_ms: None,
                    environment: None,
                    metrics: None,
                },
            );
            assert!(cluster.schedule(id));
            assert_eq!(cluster.executions[&id].node, Some(expected));
        }

        let id = Uuid::new_v4();
        cluster.executions.insert(
            id,
            Execution {
                id,
                pool: "p".into(),
                node: None,
                status: ExecutionStatus::Queued,
                payload: Value::Null,
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                environment: None,
                metrics: None,
            },
        );
        assert!(cluster.schedule(id));
        assert_eq!(cluster.executions[&id].node, Some(first));
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
        let execution_id = Uuid::new_v4();
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
            },
        );
        cluster.executions.insert(
            execution_id,
            Execution {
                id: execution_id,
                pool: "p".into(),
                node: None,
                status: ExecutionStatus::Queued,
                payload: Value::Null,
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                environment: None,
                metrics: None,
            },
        );

        assert!(!cluster.schedule(execution_id));
        assert!(!cluster.add_warm_instance(node_id, "p"));
    }

    #[test]
    fn reuses_idle_capacity_without_reserving_it_twice() {
        let image = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
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
            },
        );
        cluster.executions.insert(
            execution_id,
            Execution {
                id: execution_id,
                pool: "p".into(),
                node: None,
                status: ExecutionStatus::Queued,
                payload: Value::Null,
                result: None,
                error: None,
                created_at_ms: 0,
                started_at_ms: None,
                finished_at_ms: None,
                environment: None,
                metrics: None,
            },
        );

        assert!(cluster.schedule(execution_id));
        assert_eq!(cluster.nodes[&node_id].idle_instances["p"], 0);
        assert_eq!(cluster.nodes[&node_id].used_memory_mib, 256);
        cluster.release(execution_id, true);
        assert_eq!(cluster.nodes[&node_id].idle_instances["p"], 1);
        assert_eq!(cluster.nodes[&node_id].used_memory_mib, 256);
    }
}
