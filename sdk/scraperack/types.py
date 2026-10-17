from typing import Any, Literal, TypedDict


NodeStatus = Literal["syncing", "ready"]
PortProtocol = Literal["tcp", "udp"]
ExecutionStatus = Literal[
    "queued", "assigned", "running", "succeeded", "failed", "timed_out"
]


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


class Pool(TypedDict):
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
    ports: list["PortSpec"]
    managed_by: str | None
    environment: str | None
    environment_artifact: str | None


class Environment(TypedDict):
    id: str
    name: str
    image: str
    packages: list[str]
    preload: list[str]
    artifact: str | None
    sha256: str | None
    size: int
    pool: str
    warm_instances: int
    max_instances: int | None
    cpu_millis: int
    memory_mib: int
    disk_mib: int
    timeout_secs: int
    idle_timeout_secs: int
    reuse: bool
    network: bool
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


class MicroVM(TypedDict):
    id: str
    pool: str
    status: Literal["idle", "running"]
    execution: str | None
    pid: int | None
    cpu_millis: int
    memory_mib: int
    guest_ip: str | None
    ports: list[PublishedPort]
    node: str
    node_name: str
    node_address: str | None
    last_seen_ms: int
    environment: str | None


class Node(TypedDict):
    id: str
    name: str
    cpu_millis: int
    memory_mib: int
    address: str | None
    used_cpu_millis: int
    used_memory_mib: int
    images: list[str]
    environments: list[str]
    idle_instances: dict[str, int]
    vms: list[MicroVM]
    status: NodeStatus
    last_seen_ms: int


class EncodedValue(TypedDict):
    encoding: Literal["json", "cloudpickle"]
    data: Any


class ExecutionOutput(TypedDict, total=False):
    value: EncodedValue
    stdout: str
    stderr: str


class ExecutionMetricPoint(TypedDict):
    elapsed_ms: int
    cpu_percent: float
    memory_percent: float


class ExecutionMetrics(TypedDict):
    average_cpu_percent: float
    average_memory_percent: float
    peak_memory_percent: float
    samples: list[ExecutionMetricPoint]


class Execution(TypedDict):
    id: str
    pool: str
    node: str | None
    status: ExecutionStatus
    error: str | None
    created_at_ms: int
    started_at_ms: int | None
    finished_at_ms: int | None
    environment: str | None
    metrics: ExecutionMetrics | None
