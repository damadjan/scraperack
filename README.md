# Scraperack

PoC control plane for dense, short-lived Firecracker workloads.

## Install

ScrapeRack currently supports fresh Ubuntu 24.04 x86_64 hosts with KVM:

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

GitHub Actions verifies every pull request and publishes a checksum-verified rolling Linux build from `main`. The installer downloads that build, installs the controller, node, and GUI as systemd services, and preserves `/var/lib/scraperack-controller` and `/var/lib/scraperack-node` when rerun. It skips unchanged host dependencies, Firecracker, the kernel, and the GUI environment on node-only machines, so the same command also performs fast updates.

Additional installed machines can join the cluster from **Settings → Join nodes** in the GUI. Set the controller URL, optionally set a node name, generate the ten-minute single-use command, and run it on the other machine. The command refuses to run on that controller itself. Joining disables the other machine's local controller and GUI, keeps their data, and authenticates the node with a persistent per-node credential. Re-running the installer preserves its node-only role.

## Repository layout

```text
platform/      Rust controller and node, guest agents, GUI, and deployment files
sdk/           Installable Python SDK
images/        Docker build contexts imported as VM and runtime images
verification/  Local execution checks and benchmarks
```

## Current vertical slice

- One Rust binary with controller and node modes
- Docker image import, conversion, cluster-wide distribution tracking, and deletion
- Separate general-purpose VM pools and Python execution environments
- Immutable, content-addressed Python package environments distributed and cached on nodes
- Automatic runtime snapshots, live warm instances, idle expiry, reuse, and scale-out limits
- Fractional CPU, fixed memory/disk, and execution timeouts in pool definitions
- Round-robin node scheduling with queue-or-fail submission
- Persistent execution history and cluster metadata
- Python SDK with `cloudpickle` and JSON payloads
- Firecracker process lifecycle through its Unix-socket API
- Shared read-only golden rootfs with isolated in-memory workspaces
- cgroup v2 CPU, memory, and PID limits
- Host-to-guest execution over virtio-vsock
- Optional per-VM TAP networking with outbound NAT and TCP/UDP host-port publishing
- Node-local Firecracker memory snapshots and concurrent restores
- Python source, `cloudpickle`, file bundle, and JSON payloads
- Guest stdout, stderr, values, tracebacks, and hard timeouts
- NiceGUI management console with separate execution and infrastructure resources

VM images contain only the core `/sbin/scraperack-init` guest agent. Runtime images additionally contain an executor such as Python. Python environments combine a runtime image, an immutable package artifact, resources, and scaling policy. Package artifacts are built once from Docker-imported runtime images, distributed to nodes, mounted read-only, and shared by compatible MicroVMs. Every environment owns an internal VM pool; its MicroVMs remain visible in the GUI and never switch environments.

The Images pages import `linux/amd64` Docker images by registry reference. The controller accepts an optional registry username and password, permits plain HTTP and untrusted TLS registries by default, injects the core agent, detects `/usr/bin/python3` and `cloudpickle`, and converts the filesystem to ext4. Docker and Skopeo must be available to the controller service.

Network-enabled pools give every MicroVM an isolated `/30` address behind node NAT. Port entries use dynamic host ports when `host_port` is `None`; the assigned node endpoints are returned by `/v1/vms` and shown in the GUI.

## Intended API

```python
from scraperack import Cluster

cluster = Cluster()
image = cluster.import_image("python-runtime", "registry.example.com/python-runtime:latest")
environment = cluster.create_environment(
    "numpy", image["id"], packages=["numpy==2.3.3"],
    cpu=0.25, memory_mib=256, warm=2, max_instances=8,
)

def add(a, b):
    return a + b

execution = cluster.run(environment["id"], add, (2, 3))
result = cluster.wait(execution["id"])
```

## Development

```text
cd platform
cargo run -- controller
cargo run -- node --controller http://127.0.0.1:7070 --name node-1 --kernel /path/to/vmlinux
cd ..
pip install -e sdk
pip install -e platform/gui
python platform/gui/app.py
```

The GUI listens on port `8080` and reads the controller address from `SCRAPERACK_URL`.

Firecracker nodes require Linux, KVM access, and a compatible CPU. The controller and SDK do not.

## Prepare an Ubuntu node

```text
sudo ./platform/deploy/install-firecracker.sh
sudo ./platform/deploy/build-rootfs.sh python.ext4
```

`install-firecracker.sh` installs the latest official Firecracker release and current Firecracker CI kernel. `build-rootfs.sh` creates a minimal Ubuntu 24.04 golden image with CPython 3.10.11 and cloudpickle 3.1.2, without Docker.

## License

ScrapeRack is licensed under the [Apache License 2.0](LICENSE). Rust dependencies compiled into the platform binary are documented in [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt). Components installed from their publishers are documented in [docs/third-party-components.md](docs/third-party-components.md).
