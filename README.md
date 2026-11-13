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
platform/      Rust controller and node, guest agent, GUI, and deployment files
images/        Docker build contexts imported as VM images
```

The core only runs microVMs. Workload types live in plugins, outside this repository: what a
task's payload means, and the history of what users ran, belong to the plugin. The Python
plugin (environments and executions) and the `scraperack` Python SDK are in
[scraperack-plugins](https://github.com/damadjan/scraperack-plugins).

## Core

- One Rust binary with controller and node modes
- Docker image import, conversion, cluster-wide distribution tracking, and deletion
- Pools of microVMs with automatic snapshots, warm instances, idle expiry, reuse, and scale-out limits
- Pool revisions: changing a pool's VM settings replaces its idle microVMs without dropping work
- Read-only ext4 disks, content-addressed, distributed to and cached on every node
- Tasks (`/v1/tasks`): opaque payloads queued, scheduled onto a warm microVM, and sent to its pool's executor over virtio-vsock, with stdout, stderr, results, metrics, and hard timeouts. Each task records its pool's owner so plugins can list their own (`?owner=plugin:<name>` or `?pool=a,b`)
- Task-scoped variables and encrypted secrets, content-addressed working directories
- Per-VM CPU, memory, and disk limits with owner-managed aggregate overcommit
- Round-robin node scheduling with queue-or-fail submission
- Short-lived task storage (retention and size limits) and persistent cluster metadata
- Optional per-VM TAP networking with outbound NAT and TCP/UDP host-port publishing
- Shared read-only golden rootfs with isolated in-memory workspaces
- cgroup v2 CPU, memory, and PID limits
- NiceGUI management console
- Plugins, installed from the GUI

VM images contain only the core `/sbin/scraperack-init` guest agent. A pool may attach
extra read-only disks, mounted in order at the paths it names, and an `executor` command
that the agent starts to handle task payloads. Pools without an executor only
provide microVMs, for example browsers reached through published ports.

The Images page imports `linux/amd64` Docker images by registry reference. The controller accepts an optional registry username and password, permits plain HTTP and untrusted TLS registries by default, injects the core agent, requires `/usr/bin/python3` for it, and converts the filesystem to ext4. Docker and Skopeo must be available to the controller service.

Network-enabled pools give every MicroVM an isolated `/30` address behind node NAT. Port entries use dynamic host ports when `host_port` is `None`; the assigned node endpoints are returned by `/v1/vms` and shown in the GUI.

## Plugins

A plugin is a separate program, in any language, that adds a workload type. Pack the
plugin folder as a `.tar.gz` and install it from **Plugins → Install plugin** in the GUI.
The controller unpacks it, runs its install step, builds its `guest` folder into a disk
that nodes download, and runs it as a supervised process with its own Unix sockets.
Updates keep the previous version and roll back automatically if the new one does not
start. Plugin pages appear in the GUI sidebar, and plugin APIs are served at
`/plugins/<name>/api/`.

Plugins run with the controller's permissions. Installation can be turned off with
`SCRAPERACK_ALLOW_PLUGIN_INSTALL=false` in `/etc/scraperack/controller.env`. The manifest
format and the API available to plugins are documented in
[scraperack-plugins](https://github.com/damadjan/scraperack-plugins#write-a-plugin).

## Development

```text
cd platform
cargo run -- controller
cargo run -- node --controller http://127.0.0.1:7070 --name node-1 --kernel /path/to/vmlinux
cd ..
pip install -e platform/gui
python platform/gui/app.py
```

The GUI listens on port `8080` and reads the controller address from `SCRAPERACK_URL`.

Firecracker nodes require Linux, KVM access, and a compatible CPU. The controller does not.

## Prepare an Ubuntu node

```text
sudo ./platform/deploy/install-firecracker.sh
sudo ./platform/deploy/build-rootfs.sh python.ext4
```

`install-firecracker.sh` installs the latest official Firecracker release and current Firecracker CI kernel. `build-rootfs.sh` creates a minimal Ubuntu 24.04 golden image with CPython 3.10.11 for the guest agent, without Docker.

## License

ScrapeRack is licensed under the [Apache License 2.0](LICENSE). Rust dependencies compiled into the platform binary are documented in [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt). Components installed from their publishers are documented in [docs/third-party-components.md](docs/third-party-components.md).
