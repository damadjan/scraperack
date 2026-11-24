# API reference

The controller serves JSON over HTTP on port 7070. Errors are `{"error": "message"}` with a
4xx or 5xx status. Plugins reach the same API through their private Unix socket, where
requests act as the plugin (see `managed_by`).

## Authentication

Every request on port 7070 needs an API key:

```sh
curl -H "Authorization: Bearer sr_..." http://controller:7070/v1/pools
```

A key has scopes. `core` opens `/v1/*`, a plugin name opens `/plugins/<name>/*`, and `*`
opens everything, plugins installed later included. A missing, unknown, disabled, or expired
key gets 401; a key without the path's scope gets 403. `/health` and the node routes under
`/internal/` need no key; nodes use their own credential. The controller forwards plugin
requests without the `Authorization` header and with `X-Scraperack-Key: <key name>`.

There is no default key: until you create one, the API on port 7070 refuses every request.
Create keys in the GUI under **API keys**, or with the routes below.

The controller also listens on an admin address, `127.0.0.1:7071` by default, that serves
the same API without keys plus key management. The GUI uses it. Keep it private.

| Method and path (admin address only) | Body / query | Result |
|---|---|---|
| `GET /v1/keys` | | Keys, without their secret |
| `POST /v1/keys` | `{name, scopes, expires_at_ms?}` | Key with `key`, shown only here |
| `PATCH /v1/keys/{id}` | `{name?, scopes?, enabled?, expires_at_ms?}` | Key; `expires_at_ms: null` removes the expiry |
| `POST /v1/keys/{id}/rotate` | `{grace_minutes?}` | Key with the new `key`; the old one works for `grace_minutes` |
| `DELETE /v1/keys/{id}` | | 204 |

A key is `{id, name, prefix, scopes, enabled, created_at_ms, expires_at_ms, last_used_at_ms,
previous_expires_at_ms}`. Only a hash of each key is stored.

The Python client [`scraperack`](sdk.md) wraps all of these.

## Images

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/images` | | Images with `ready` |
| `POST /v1/images/import` | `{name, reference, username?, password?}` | Image (blocks until converted) |
| `POST /v1/images/imports` | same as above | 202, import job (runs in the background) |
| `GET /v1/images/imports` | | Import jobs, newest first, without logs |
| `GET /v1/images/imports/{id}` | | Import job with `log` |
| `DELETE /v1/images/imports/{id}` | | 204; cancels a running import or dismisses a finished one |
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

## Volumes

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/volumes` | | Volumes, with their node and any move in progress |
| `POST /v1/volumes` | `{name, size_mib}` | 201, volume |
| `GET /v1/volumes/{name}` | | Volume |
| `DELETE /v1/volumes/{name}` | | 204; 409 while a task uses it |

See [Volumes](volumes.md).

## Tasks

| Method and path | Body / query | Result |
|---|---|---|
| `POST /v1/tasks` | `{pool, payload, queue?, variables?, secrets?, volume?}` | 202, task |
| `GET /v1/tasks` | `limit`, `pool` (comma-separated), `owner`, `ids` (comma-separated, at most 500) | Tasks, newest first |
| `GET /v1/tasks/{id}` | | Task |
| `GET /v1/tasks/{id}/wait` | | Task when finished, or after 25 s |
| `GET /v1/tasks/{id}/result` | | `{status, value, error}` |
| `GET /v1/tasks/{id}/logs` | | `{stdout, stderr}` |
| `GET /v1/tasks/{id}/payload` | | Payload |
| `POST /v1/tasks/{id}/stop` | `{suspend?}` | Ends the task early; 202 while the node stops it |
| `HEAD /v1/working-dirs/{sha256}` | | 204 if uploaded, else 404 |
| `PUT /v1/working-dirs/{sha256}` | Zip bytes | 201 |

See [Tasks](tasks.md).

## Inventory and settings

| Method and path | Body / query | Result |
|---|---|---|
| `GET /v1/vms` | | Live microVMs with node, IP, published ports, current task |
| `GET /v1/nodes` | | Nodes with capacity, usage, images, disks, VMs |
| `GET /v1/capacity` | | Worker, VM pool, and plugin capacity for the homepage |
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

### Capacity reports

Plugins can send a complete snapshot with `PUT /v1/capacity` over
`SCRAPERACK_CORE_SOCKET`. The core identifies the sender from its socket and
returns 204. Send on startup and refresh periodically (for example, every 10
seconds). Reports expire after 30 seconds and are cleared when a plugin stops.

```json
{
  "dependencies": ["storage"],
  "resources": [{"label": "Worker slots", "used": 12, "total": 16, "unit": "slots"}],
  "queued": 3
}
```

All top-level fields are optional. Dependencies name other plugins and link to
them on the Capacity page when installed. Each resource requires a label and a
nonnegative integer `used`. Omit `total` for a count without a capacity limit.
Reports can contain up to 12 dependencies and 12 resources and are limited to
16 KiB. The core shows plugin status and managed VM pools even when a plugin
doesn't send reports.

Worker RAM comes from the worker's `MemTotal` and `MemAvailable` readings.
VM reservations are shown separately.

`/internal/...` routes are used by nodes and are not part of the public API.
