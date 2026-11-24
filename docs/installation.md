# Installation

## Requirements

- Ubuntu 24.04, x86_64
- Root access for the installer
- KVM (`/dev/kvm`) for single-machine and worker installs; a control-plane-only install
  can run on a regular VPS

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

The installer asks which type to install:

- **Single machine** (`all`): controller, GUI, and worker. This is the default and forms a
  one-machine cluster.
- **Control plane only** (`control-plane`): controller and GUI. It runs no microVMs and
  does not require KVM, Firecracker, dnsproxy, or worker networking packages.
- **Worker only** (`worker`): Firecracker worker and guest DNS. Join it to an existing
  control plane after installation.

The installer downloads the rolling build of `main`, verifies its checksum, installs only
the packages required by the selected role, creates the `scraperack` user and systemd
services, and starts the selected components.

For an unattended installation, pass the role explicitly:

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh |
    sudo sh -s -- --role control-plane --yes
```

Valid roles are `all`, `control-plane`, and `worker`. Pin a release instead of the rolling
build with `SCRAPERACK_RELEASE=<tag>`.

## Update or change type

Run the same install command again. The installer offers to update the current type or
change it. For unattended changes, pass `--role` and `--yes`.

Changing type keeps `/var/lib/scraperack-controller`, `/var/lib/scraperack-node`, and the
configuration files. Removing the local worker stops its workloads and disables its
services; changing to `worker` stops the local controller and GUI. Changing to `all` or
`worker` checks for KVM before making changes.

Updates preserve the role in `/etc/scraperack/role`. Existing installations whose role is
`node` are treated as `worker`.

## Add workers

1. Run the installer on the new machine and choose **Worker only**, or install it
   unattended:

   ```sh
   curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh |
       sudo sh -s -- --role worker --yes
   ```

2. In the GUI open **Settings → Join nodes**, check the controller URL, optionally name the
   worker, and generate the command. It is single-use and valid for ten minutes.
3. Run the command on the worker.

Until it is joined, a fresh worker installation leaves the worker and DNS services
disabled. Joining stores a persistent per-worker credential and starts them. Re-running
the installer updates the worker without requiring another join.

## Configuration

The installer writes these files (mode 0600). Edit them and restart the matching service.

`/etc/scraperack/controller.env`

| Variable | Default | Meaning |
|---|---|---|
| `SCRAPERACK_LISTEN` | `0.0.0.0:7070` | Controller API address; every request needs an API key |
| `SCRAPERACK_ADMIN_LISTEN` | `127.0.0.1:7071` | Admin address for the GUI and key management, no keys; keep it private |
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

Installer options: `--role all|control-plane|worker`, `--yes`, and `--help`. The
`SCRAPERACK_ROLE` environment variable is equivalent to `--role`.

Installer-time overrides: `SCRAPERACK_NODE_CPU_MILLIS`, `SCRAPERACK_NODE_MEMORY_MIB`,
`SCRAPERACK_NODE_VM_START_CONCURRENCY`, `SCRAPERACK_NODE_NAME`, `SCRAPERACK_LISTEN`,
`SCRAPERACK_ADMIN_LISTEN`,
`SCRAPERACK_GUI_PORT`, `SCRAPERACK_PUBLIC_URL`, `SCRAPERACK_USER`, `SCRAPERACK_REPOSITORY`.

`/etc/scraperack/gui.env`: `SCRAPERACK_URL` (the controller's admin address),
`SCRAPERACK_PUBLIC_URL` (URL shown in join commands and key examples), `SCRAPERACK_GUI_PORT`
(`8080`).

## API keys

The API on port 7070 refuses every request until you create a key. Open **API keys** in the
GUI, choose what the key opens (the core API, single plugins, or everything), and copy the
key; it is shown once. Programs send it as `Authorization: Bearer <key>`, and the SDKs read
it from `SCRAPERACK_API_KEY`. See [API](api.md#authentication).

## Services

| Service | Runs on | Purpose |
|---|---|---|
| `scraperack-controller` | controller | API, scheduler, plugin supervisor |
| `scraperack-gui` | controller | Web console |
| `scraperack-node` | workers | Runs microVMs |
| `scraperack-dns` | workers | DNS for microVMs at `10.239.255.254`, DNS-over-HTTPS upstream |

## Ports and addresses

| Port / range | Where | Use |
|---|---|---|
| 7070/tcp | controller | API and plugin APIs (`/plugins/<name>/api/`) with API keys, node traffic |
| 7071/tcp | controller, `127.0.0.1` | Admin API for the GUI, no keys |
| 8080/tcp | controller | GUI, no login yet |
| 30000–49999 | nodes | Host ports published for microVMs |
| `10.240.0.0/16` | nodes | MicroVM addresses, one `/30` per VM, NAT to the outside |

## Files

| Path | Contents |
|---|---|
| `/usr/local/bin/scraperack` | The binary (controller, node, join) |
| `/opt/scraperack/BUILD_COMMIT` | Installed commit |
| `/var/lib/scraperack-controller` | Database, images, disks, plugins, working directories |
| `/var/lib/scraperack-node` | Kernel, cached images and disks, snapshots |
