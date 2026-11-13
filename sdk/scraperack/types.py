from typing import Any, Literal, TypedDict

NodeStatus = Literal["syncing", "ready"]
PortProtocol = Literal["tcp", "udp"]
TaskStatus = Literal["queued", "assigned", "running", "succeeded", "failed", "timed_out"]
FINISHED: frozenset[str] = frozenset({"succeeded", "failed", "timed_out"})


class Image(TypedDict):
    id: str
    name: str
    sha256: str
    size: int
    compatible: bool
    executor: str | None
    kind: Literal["micro_vm", "runtime"]
    docker_source: str | None


class ImageView(Image):
    ready: bool


class PortSpec(TypedDict):
    name: str
    guest_port: int
    host_port: int | None
    protocol: PortProtocol


class PublishedPort(TypedDict):
    name: str
    guest_port: int
    host_port: int
    protocol: PortProtocol


class PoolDisk(TypedDict):
    key: str
    mount: str


class Pool(TypedDict, total=False):
    name: str
    image: str
    warm_instances: int
    max_instances: int | None
    cpu_millis: int
    memory_mib: int
    disk_mib: int
    timeout_secs: int
    idle_timeout_secs: int
    reuse: bool
    network: bool
    ports: list[PortSpec]
    preload: list[str]
    disks: list[PoolDisk]
    executor: list[str]
    aliases: list[str]
    display_name: str | None
    managed_by: str | None
    revision: int


class PoolView(Pool, total=False):
    ready: bool


class Disk(TypedDict):
    key: str
    sha256: str
    size: int
    owner: str | None


class DiskView(Disk):
    ready: bool


class MicroVM(TypedDict):
    id: str
    pool: str
    status: Literal["idle", "running"]
    task: str | None
    pid: int | None
    cpu_millis: int
    memory_mib: int
    guest_ip: str | None
    ports: list[PublishedPort]
    node: str
    node_name: str
    node_address: str | None
    last_seen_ms: int


class Node(TypedDict):
    id: str
    name: str
    cpu_millis: int
    memory_mib: int
    vm_start_concurrency: int | None
    address: str | None
    used_cpu_millis: int
    used_memory_mib: int
    images: list[str]
    disks: list[str]
    idle_instances: dict[str, int]
    vms: list[MicroVM]
    status: NodeStatus
    last_seen_ms: int


class Plugin(TypedDict):
    name: str
    version: str
    description: str
    previous_version: str | None
    enabled: bool
    gui: bool
    installed_at_ms: int


class TaskMetricPoint(TypedDict):
    elapsed_ms: int
    cpu_percent: float
    memory_percent: float


class TaskMetrics(TypedDict):
    average_cpu_percent: float
    average_memory_percent: float
    peak_memory_percent: float
    samples: list[TaskMetricPoint]


class Task(TypedDict):
    """A payload queued for a pool's executor; its meaning belongs to the pool's owner."""

    id: str
    pool: str
    owner: str | None
    node: str | None
    status: TaskStatus
    error: str | None
    created_at_ms: int
    started_at_ms: int | None
    finished_at_ms: int | None
    metrics: TaskMetrics | None


class TaskResult(TypedDict):
    status: TaskStatus
    value: Any
    error: str | None


class TaskLogs(TypedDict):
    stdout: str
    stderr: str


class StorageSettings(TypedDict):
    retention_hours: int
    max_storage_bytes: int
    used_storage_bytes: int
