# Installation

## Requirements

- Ubuntu 24.04, x86_64
- KVM (`/dev/kvm`), so bare metal or a VM with nested virtualization
- Root access for the installer

## Install a controller

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

The installer:

- installs host packages (Docker, Skopeo, e2fsprogs, iptables, Python 3)
- downloads the rolling build of `main` and verifies its checksum
- installs Firecracker and its CI kernel, and dnsproxy for guest DNS
- creates the `scraperack` user and the systemd services below
- prints the controller and GUI URLs

The machine runs the controller, the GUI, and a node, so it is a one-machine cluster.

## Update

Run the same command again. It keeps `/var/lib/scraperack-controller`,
`/var/lib/scraperack-node`, and `/etc/scraperack/node.env`, skips anything unchanged, and
restarts the services. Update the controller machine first, then the other nodes. See
[Operations](operations.md#upgrades).

Pin a release instead of the rolling build with `SCRAPERACK_RELEASE=<tag>`.

## Add nodes

1. Install ScrapeRack on the new machine with the same command.
2. In the GUI open **Settings → Join nodes**, check the controller URL, optionally name the
   node, and generate the command. It is single-use and valid for ten minutes.
3. Run the command on the new machine.

Joining switches the machine to node-only: its controller and GUI are disabled, their data
kept, and the node authenticates with a persistent per-node credential. Re-running the
installer keeps the node-only role (`/etc/scraperack/role`).

## Configuration

The installer writes these files (mode 0600). Edit them and restart the matching service.

`/etc/scraperack/controller.env`

| Variable | Default | Meaning |
|---|---|---|
| `SCRAPERACK_LISTEN` | `0.0.0.0:7070` | Controller API address |
| `SCRAPERACK_ALLOW_PLUGIN_INSTALL` | `true` | Allow installing plugins from the API and GUI |

`/etc/scraperack/node.env`

| Variable | Default | Meaning |
|---|---|---|
| `SCRAPERACK_CONTROLLER` | `http://127.0.0.1:7070` | Controller URL |
| `SCRAPERACK_NODE_NAME` | hostname | Name shown in the GUI |
| `SCRAPERACK_NODE_CPU_MILLIS` | all cores × 1000 | CPU offered to microVMs |
| `SCRAPERACK_NODE_MEMORY_MIB` | total − 2048 | Memory offered to microVMs |
| `SCRAPERACK_NODE_VM_START_CONCURRENCY` | cores ÷ 2, at least 1 | MicroVMs started at once |
| `SCRAPERACK_NODE_CREDENTIAL` | set by joining | Node credential |

Installer-time overrides: `SCRAPERACK_NODE_CPU_MILLIS`, `SCRAPERACK_NODE_MEMORY_MIB`,
`SCRAPERACK_NODE_VM_START_CONCURRENCY`, `SCRAPERACK_NODE_NAME`, `SCRAPERACK_LISTEN`,
`SCRAPERACK_GUI_PORT`, `SCRAPERACK_PUBLIC_URL`, `SCRAPERACK_USER`, `SCRAPERACK_REPOSITORY`.

`/etc/scraperack/gui.env`: `SCRAPERACK_URL` (controller), `SCRAPERACK_PUBLIC_URL` (URL shown
in join commands), `SCRAPERACK_GUI_PORT` (`8080`).

## Services

| Service | Runs on | Purpose |
|---|---|---|
| `scraperack-controller` | controller | API, scheduler, plugin supervisor |
| `scraperack-gui` | controller | Web console |
| `scraperack-node` | every machine | Runs microVMs |
| `scraperack-dns` | every machine | DNS for microVMs at `10.239.255.254`, DNS-over-HTTPS upstream |

## Ports and addresses

| Port / range | Where | Use |
|---|---|---|
| 7070/tcp | controller | API, plugin APIs (`/plugins/<name>/api/`), node traffic |
| 8080/tcp | controller | GUI |
| 30000–49999 | nodes | Host ports published for microVMs |
| `10.240.0.0/16` | nodes | MicroVM addresses, one `/30` per VM, NAT to the outside |

## Files

| Path | Contents |
|---|---|
| `/usr/local/bin/scraperack` | The binary (controller, node, join) |
| `/opt/scraperack/BUILD_COMMIT` | Installed commit |
| `/var/lib/scraperack-controller` | Database, images, disks, plugins, working directories |
| `/var/lib/scraperack-node` | Kernel, cached images and disks, snapshots |
