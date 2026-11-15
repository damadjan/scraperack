# Volumes

A volume is a persistent, writable ext4 disk for long-lived work, such as a desktop's home
folder. It lives on one node, follows its tasks there, can hold a suspended microVM, and
moves to another node when its own is full.

## Create and use

```http
POST /v1/volumes
{"name": "alice-home", "size_mib": 10240}
```

Names use letters, digits, `-` and `_`. The volume starts empty and has no node yet. The
first task that uses it formats it on the node it runs on.

A pool that takes volumes sets `volume_mount`:

```json
{"name": "desktops", "volume_mount": "/mnt/workspace", "reuse": false, "...": "..."}
```

Its snapshot then has an extra, empty drive after the pool's disks. A task names the volume:

```http
POST /v1/tasks
{"pool": "desktops", "volume": "alice-home", "payload": {...}}
```

After the microVM is restored, the node swaps the volume in for the empty drive and the
guest agent mounts it at `volume_mount` before the executor gets the payload. The volume
is a sparse file: it uses as much node disk as has been written to it.

Rules:

- One active task per volume. A second one is refused with 409 while the first is queued
  or running.
- A volume created by a plugin belongs to it: only that plugin can attach or delete it.
- MicroVMs with a volume are never kept idle for reuse.
- `DELETE /v1/volumes/{name}` forgets the volume (409 while a task uses it). Its node moves
  the files to `volumes/.trash` and deletes them an hour later.

## Suspend and resume

```http
POST /v1/tasks/{id}/stop
{"suspend": true}
```

Within a heartbeat (about 2 seconds) the node pauses the microVM, saves its memory and
device state next to the volume, and destroys it. The task succeeds with
`{"suspended": true}` and the volume reports `suspended: true`. The saved state is as large
as the pool's `memory_mib`.

The next task with the volume resumes that microVM instead of a fresh one from the pool
snapshot: open programs, windows, and memory are back, the clock is set, the network is
configured again, and the payload goes to the executor as usual. The saved state is used
once. If it no longer fits the pool (its image, size, disks, or executor changed) or
cannot be restored, the node discards it and starts a fresh microVM with the volume; files
are kept either way.

Without `suspend`, `POST /v1/tasks/{id}/stop` flushes the guest's disks and destroys the
microVM; the task fails with `task was stopped`. Queued tasks are stopped at once.

## Placement and moves

A task with a volume runs on the node that holds the volume. A volume without a node goes
to the online node with the most free memory.

When the volume's node has no room for the task (its reserved memory plus the pool's
`memory_mib` would exceed the node's memory) and another online node does, the volume moves
there first. The task waits in the queue while the volume's files (disk and any suspended
state) stream from the old node through the controller to the new one. Zero blocks are
skipped, so a mostly empty 10 GiB volume moves in seconds, and the stream is checked with
SHA-256. Then the task starts on the new node; the old node keeps its copy in the trash for
an hour.

`GET /v1/volumes/{name}` shows a move in progress as `migration: {from, to, started_at_ms}`.
If a move fails, `migration_error` says why, and for the next five minutes tasks start on
the volume's own node instead. When no node has room, the task starts on the volume's node
anyway, like any task when the cluster is full. When the volume's node is offline, the
task waits for it.

A suspended microVM that moved resumes on the new node when the nodes have the same CPU
model; otherwise it starts fresh with the volume.

## Restarts

Tasks with a volume that were running when the controller restarts keep running: the
controller waits for the node's next report. When a node reports that it no longer has such
a task (it restarted, for example), the task is queued again and starts with the volume on
a fresh microVM.

Deleting a node forgets which of its volumes it held; their next task starts them empty.

## API

| Method and path | Body | Result |
|---|---|---|
| `GET /v1/volumes` | | Volumes |
| `POST /v1/volumes` | `{name, size_mib}` | 201, volume |
| `GET /v1/volumes/{name}` | | Volume |
| `DELETE /v1/volumes/{name}` | | 204; 409 while a task uses it |
| `POST /v1/tasks/{id}/stop` | `{suspend?}` | 202 for a running task, else the finished task |

A volume: `{name, size_mib, owner, node, suspended, migration, migration_error,
migration_failed_at_ms, created_at_ms}`. Raising `size_mib` is not supported yet.
