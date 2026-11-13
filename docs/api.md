# API reference

The controller serves JSON over HTTP on port 7070. Errors are `{"error": "message"}` with a
4xx or 5xx status. There is no authentication on the public API: keep port 7070 on a trusted
network. Plugins reach the same API through their private Unix socket, where requests act
as the plugin (see `managed_by`).

The Python client [`scraperack`](sdk.md) wraps all of these.

## Images

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/images` | | Images with `ready` |
| `POST /v1/images/import` | `{name, reference, username?, password?}` | Image (blocks until converted) |
| `PATCH /v1/images/{id}` | `{name}` | Renamed image |
| `DELETE /v1/images/{id}` | | 204 |

## Pools

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/pools` | | Pools with `ready` |
| `POST /v1/pools` | Pool ([fields](images-and-pools.md#pools)) | 201, pool |
| `GET /v1/pools/{name}` | | Pool |
| `PUT /v1/pools/{name}` | Full pool | Pool; VM-affecting changes bump `revision` |
| `DELETE /v1/pools/{name}` | | 204; 409 while tasks are active |
| `POST /v1/pools/{name}/restart` | | Replaces idle VMs |

## Disks

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/disks` | | Disks with `ready` |
| `POST /v1/disks` | Raw ext4 bytes | 201, `{key, sha256, size, owner}` |
| `DELETE /v1/disks/{key}` | | 204; refused while a pool uses it |

## Tasks

| Method and path | Body / query | Result |
|---|---|---|
| `POST /v1/tasks` | `{pool, payload, queue?, variables?, secrets?}` | 202, task |
| `GET /v1/tasks` | `limit`, `pool` (comma-separated), `owner` | Tasks, newest first |
| `GET /v1/tasks/{id}` | | Task |
| `GET /v1/tasks/{id}/wait` | | Task when finished, or after 25 s |
| `GET /v1/tasks/{id}/result` | | `{status, value, error}` |
| `GET /v1/tasks/{id}/logs` | | `{stdout, stderr}` |
| `GET /v1/tasks/{id}/payload` | | Payload |
| `HEAD /v1/working-dirs/{sha256}` | | 204 if uploaded, else 404 |
| `PUT /v1/working-dirs/{sha256}` | Zip bytes | 201 |

See [Tasks](tasks.md).

## Inventory and settings

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/vms` | | Live microVMs with node, IP, published ports, current task |
| `GET /v1/nodes` | | Nodes with capacity, usage, images, disks, VMs |
| `PUT /v1/nodes/{id}` | `{vm_start_concurrency}` | Node |
| `DELETE /v1/nodes/{id}` | | 204; 409 while it runs tasks |
| `POST /v1/join-tokens` | | `{id, token, expires_at_ms, status}`; single-use, 10 minutes |
| `GET /v1/join-tokens/{id}` | | Whether the token was used |
| `GET /v1/settings/storage` | | `{retention_hours, max_storage_bytes, used_storage_bytes}` |
| `PUT /v1/settings/storage` | `{retention_hours, max_storage_bytes}` | Same |
| `GET /health` | | `{"ok": true}` |

## Plugins

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/plugins` | | Plugins with status |
| `POST /v1/plugins/uploads` | `.tar.gz` bytes | `{upload, manifest, installed_version, install_enabled}` |
| `POST /v1/plugins/uploads/{upload}/install` | | `{plugin, log}` |
| `POST /v1/plugins/{name}/enable` | | Plugin |
| `POST /v1/plugins/{name}/disable` | | Plugin |
| `POST /v1/plugins/{name}/rollback` | | Plugin, now on the previous version |
| `GET /v1/plugins/{name}/log` | | Recent plugin output |
| `DELETE /v1/plugins/{name}` | | 204; removes its pools and disks; 409 while its tasks are active |
| `ANY /plugins/{name}/api/...` | | Proxied to the plugin |
| `ANY /plugins/{name}/gui/...` | | Proxied to the plugin |

`/internal/...` routes are used by nodes and are not part of the public API.
