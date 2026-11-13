# Architecture

Scraperack runs short-lived workloads in Firecracker microVMs across a cluster of Linux
machines. The core knows nothing about what runs inside the VMs: it schedules microVMs and
moves opaque payloads to them. Plugins give the payloads a meaning.

```text
               ┌───────────────────────────── controller host ─────────────────────────────┐
  clients ───▶ │  controller :7070  ──  SQLite (cluster state, tasks)                       │
  GUI :8080 ─▶ │      │   └── plugins (supervised processes, one Unix socket each)          │
               └──────┼────────────────────────────────────────────────────────────────────┘
                      │ HTTP (node credential)
          ┌───────────┴────────────┐
     ┌────▼─────┐             ┌────▼─────┐
     │  node    │   ...       │  node    │   pulls work, images, disks; reports VMs
     │ ┌──────┐ │             │ ┌──────┐ │
     │ │ VM   │ │ vsock :5000 │ │ VM   │ │   /sbin/scraperack-init (guest agent, PID 1)
     │ │ exec │ │             │ │ exec │ │   └── pool executor (optional)
     │ └──────┘ │             │ └──────┘ │
     └──────────┘             └──────────┘
```

## Pieces

**Controller** (`scraperack controller`) holds cluster state: images, pools, disks, nodes,
plugins, and tasks. It schedules tasks onto nodes, serves the API on port 7070, distributes
images and disks, and supervises plugins. State lives in
`/var/lib/scraperack-controller/scraperack.db`.

**Node** (`scraperack node`) runs on every machine that hosts microVMs, including the
controller machine. It downloads images and disks, keeps one snapshot per pool, keeps warm
microVMs ready, long-polls the controller for work, runs it, and reports the result with
CPU and memory metrics. Each VM runs in its own cgroup v2 with CPU, memory, and PID limits.

**Guest agent** (`/sbin/scraperack-init`) is injected into every imported image and runs as
PID 1. It mounts the pool's extra disks, starts the pool's executor or the image's own
entrypoint, configures networking, and relays each task payload to the executor over a
Unix socket. See [Executors](executors.md).

**GUI** (`platform/gui`, port 8080) is a NiceGUI console over the same API. Plugins add
their own pages to its sidebar.

**Plugins** are separate programs, in any language, that add a workload type: they create
pools, upload disks, ship an executor, and give tasks a meaning. The Python plugin turns
pools into Python environments and tasks into executions. See
[scraperack-plugins](https://github.com/damadjan/scraperack-plugins/tree/main/docs).

## A task, end to end

1. A client (usually a plugin SDK) sends `POST /v1/tasks` with a pool name and payload.
2. The controller stores it, then assigns it to a ready node that has the pool's image and
   disks, preferring nodes with an idle VM of that pool, round-robin among equals. If the
   pool is at `max_instances` the task waits in the queue, or is rejected with `queue: false`.
3. The node takes an idle VM, or restores one from the pool snapshot in a few hundred
   milliseconds, and sends the payload over vsock to the guest agent.
4. The guest agent passes it to the executor and returns its reply.
5. The node reports result, stdout, stderr, error, and metrics. The VM goes back to the idle
   set (`reuse`) or is destroyed.
6. The client reads the result with `/v1/tasks/{id}/wait` and `/result`.

## Scheduling and capacity

Each node advertises CPU (millicores) and memory (MiB). A pool's VM must fit on a node, but
the sum of VMs is not capped: overcommit is managed by the pool owner through
`max_instances`. Idle VMs count against their node until they expire.

## Snapshots and pool revisions

A node boots each pool's VM once, lets the guest agent mount disks, start the executor and
preload modules, then snapshots it. Every later VM of that pool is restored from the
snapshot. Changing a VM-affecting pool setting (image, CPU, memory, disk size, disks,
executor, network, ports, preload, reuse) bumps the pool's `revision`: idle VMs are replaced and running ones finish
first.

Restores are fast because the guest does not fault its memory in page by page. Firecracker
hands the guest memory to the node over userfaultfd; the first restore of each snapshot
records which pages it touches in its first 15 seconds (`working-set.json` next to the
snapshot), and later restores copy exactly those pages in before the VM resumes. The
restored guest then gets its clock, network and preload in one request. Set
`SCRAPERACK_NODE_PREFAULT=0` in `/etc/scraperack/node.env` to map the memory file instead.
