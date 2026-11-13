# Tasks

A task sends one JSON payload to the executor of a pool, on a warm microVM, and keeps the
reply for a while. The core does not interpret the payload; the pool's executor and owner
do. Users of the Python plugin see tasks as **executions** and never call these endpoints
directly.

## Submit

```http
POST /v1/tasks
{"pool": "my-pool", "payload": {...}, "queue": true,
 "variables": {"MODE": "fast"}, "secrets": {"TOKEN": "..."}}
```

- `pool`: pool name or alias. The pool must have an `executor`.
- `queue`: when the pool is at `max_instances` or no node fits, wait (`true`) or fail with
  503 (`false`).
- `variables` and `secrets`: handed to the executor with the payload (see below).

The reply is the task with status `queued` or `assigned`.

## Lifecycle

`queued` → `assigned` → `running` → `succeeded` | `failed` | `timed_out`

- `failed`: the executor returned an error, or the VM broke.
- `timed_out`: the pool's `timeout_secs` passed; the VM is destroyed.
- Tasks that were queued, assigned, or running when the controller restarts are queued again.

## Read

| Endpoint | Returns |
|---|---|
| `GET /v1/tasks/{id}` | Status, node, timings, error, metrics |
| `GET /v1/tasks/{id}/wait` | Same, once finished or after 25 seconds; call again until finished |
| `GET /v1/tasks/{id}/result` | `{status, value, error}` (409 until finished) |
| `GET /v1/tasks/{id}/logs` | `{stdout, stderr}` |
| `GET /v1/tasks/{id}/payload` | The submitted payload |
| `GET /v1/tasks?owner=&pool=a,b&limit=` | Newest first, filtered |

Each task records `owner`, the `managed_by` of its pool when it was created (for example
`plugin:python`). Plugins list their own tasks with `?owner=plugin:<name>`.

Metrics: average CPU, average and peak memory as a share of the VM's size, and samples
every 250 ms.

## Variables, secrets, and working directories

When a task has variables, secrets, or a working directory, the executor receives:

```json
{"encoding": "scraperack-invocation", "payload": {...},
 "variables": {...}, "secrets": {...},
 "working_dir": {"hash": "<sha256>", "archive": "<base64 zip, only the first time on this VM>"}}
```

Secrets are encrypted at rest (AES-256-GCM, key in `task-secrets.key`), deleted when the
task finishes, and redacted from stdout, stderr, and error text.

A working directory is a zip uploaded once with `PUT /v1/working-dirs/{sha256}` (check with
`HEAD`) and referenced from the payload as `"working_dir": "<sha256>"`. Nodes download it
and send it to each VM only once.

A payload with `hash` or `cache_key` is treated as cacheable: when a reused VM has already
seen that key, the node drops the payload's `function` field, and the executor is expected
to use its cached copy. This is what makes repeated Python calls cheap.

## Storage

Finished tasks are a short-lived buffer, not an archive. **Settings → Task storage** or
`PUT /v1/settings/storage`:

- `retention_hours`: default 24, at most 24 in the GUI
- `max_storage_bytes`: default 2 GiB; the oldest finished tasks are evicted first

If storage is full of active tasks, new tasks are rejected with 507.
