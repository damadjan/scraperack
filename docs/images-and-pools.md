# Images and pools

## Images

An image is the root filesystem of a microVM, imported from a Docker image.

**Images → Import image** in the GUI, or `POST /v1/images/imports` (background) or
`POST /v1/images/import` (waits until done):

```json
{"name": "python-runtime", "reference": "registry.example.com/python-runtime:3.10",
 "username": null, "password": null}
```

The controller pulls `linux/amd64` with Skopeo (plain HTTP and self-signed registries are
allowed), injects the guest agent, and converts the filesystem to ext4. Images need
`/usr/bin/python3`, which the guest agent uses. The image's Docker entrypoint and command,
if any, are started inside each microVM as its service, for example a browser listening on
a port.

An import job reports its `status` (`running`, `succeeded`, `failed`), `stage`, overall
`progress` from 0 to 1 (layer download measured in bytes), and a log of every step. The GUI
shows running imports as rows with a progress bar; clicking a row opens its log. Jobs are
kept in controller memory (the last 50 finished ones) and are lost on restart.

Nodes download images in the background. A pool can use an image once `ready` is true,
meaning every ready node has it. Images can be renamed (`PATCH`) or deleted when no pool
uses them.

Build contexts for the images used so far are in [`images/`](../images): `python-runtime`,
`python-requests-runtime`, `chromium`, `headless-chromium`, and `chrome`.

## Pools

A pool is a set of identical microVMs from one image.

| Field | Default | Meaning |
|---|---|---|
| `name` | | Letters, digits, `-`, `_`, `.` |
| `image` | | Image ID |
| `cpu_millis` | | vCPU share per VM (1000 = one core) |
| `memory_mib` | | Memory per VM |
| `disk_mib` | | Writable space per VM |
| `timeout_secs` | | Hard limit for one task |
| `warm_instances` | 0 | Idle VMs kept ready across the cluster |
| `max_instances` | none | Cap on VMs (busy + idle); extra tasks queue |
| `idle_timeout_secs` | 30 | How long an extra idle VM is kept |
| `reuse` | true | Run further tasks in the same VM; false destroys it after each task |
| `network` | false | Give each VM a network interface |
| `ports` | `[]` | Ports to publish on the node (needs `network`) |
| `disks` | `[]` | Read-only disks to mount, in order (up to 16) |
| `executor` | `[]` | Command that handles task payloads; empty means VMs only |
| `preload` | `[]` | Passed to the executor before the snapshot is taken |
| `aliases` | `[]` | Other names that resolve to this pool when submitting tasks |
| `display_name` | none | Name shown in the GUI |

Read-only fields: `managed_by` (set when a plugin owns the pool), `revision` (bumped when a
VM-affecting setting changes; idle VMs are replaced), and `ready` in listings.

Pools created by a plugin are owned by it: only that plugin can change or delete them,
and they are removed with the plugin.

### Warm, idle, and reused VMs

- `warm_instances` VMs are kept idle across the cluster at all times.
- After a task, a healthy VM stays idle for `idle_timeout_secs` if `reuse` is on, then
  stops unless it is one of the warm ones.
- A VM that failed, timed out, or broke its executor is destroyed.

Reuse makes repeated tasks fast (the executor, loaded modules, and caches stay in memory),
at the cost of isolation between tasks of the same pool.

### Disks

`POST /v1/disks` uploads a read-only ext4 file. Its key is its SHA-256. Nodes download disks
on demand; a pool is schedulable on a node once its image and all its disks are there.
Disks attach as `/dev/vdb`, `/dev/vdc`, … and mount at the pool's `mount` paths:

```json
"disks": [{"key": "<sha256>", "mount": "/tmp/environment"}]
```

A disk can be deleted when no pool uses it.

### Networking and ports

With `network: true` each VM gets its own `/30` from `10.240.0.0/16`, outbound NAT through
the node, and DNS at `10.239.255.254`. Without it the VM has no network at all.

```json
"ports": [{"name": "cdp", "guest_port": 9222, "host_port": null, "protocol": "tcp"}]
```

`host_port: null` picks a free port in 30000–49999 on the node. `GET /v1/vms` returns each
VM's `node_address` and published ports, and the MicroVMs page shows them.

### Restarting

`POST /v1/pools/{name}/restart` replaces the pool's idle VMs, for example after the image's
service needs a fresh start.
