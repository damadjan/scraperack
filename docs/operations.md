# Operations

## Upgrades

Re-run the installer on each machine, controller first, then the other nodes right after:

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

Services stop for a few seconds. Tasks that were queued or running are queued again when
the controller comes back, and nodes reconnect on their own. Pools whose VM settings did
not change keep their snapshots.

Check what is installed:

```sh
cat /opt/scraperack/BUILD_COMMIT
```

Plugins are updated separately, from **Plugins → Install plugin**. A plugin update keeps
the previous version; if the new one does not start it rolls back automatically, and
**Roll back** stays available.

## Backups

Everything the controller knows is under `/var/lib/scraperack-controller`. Before an upgrade,
copy the database consistently while the controller runs:

```sh
sudo install -d /var/backups/scraperack
sudo python3 -c "import sqlite3; s=sqlite3.connect('file:/var/lib/scraperack-controller/scraperack.db?mode=ro', uri=True); d=sqlite3.connect('/var/backups/scraperack/scraperack.db'); s.backup(d)"
sudo cp -a /var/lib/scraperack-controller/task-secrets.key /usr/local/bin/scraperack \
    /var/lib/scraperack-controller/plugins /var/backups/scraperack/
```

Images and disks can be re-imported or rebuilt, but copying `images/` and `disks/` avoids
that.

## Rollback

```sh
sudo systemctl stop scraperack-gui scraperack-node scraperack-controller
sudo cp /var/backups/scraperack/scraperack /usr/local/bin/scraperack
sudo cp /var/backups/scraperack/scraperack.db /var/lib/scraperack-controller/scraperack.db
sudo rm -f /var/lib/scraperack-controller/scraperack.db-wal /var/lib/scraperack-controller/scraperack.db-shm
sudo systemctl start scraperack-controller scraperack-node scraperack-gui
```

Roll back nodes by restoring the same binary on them.

## Logs

```sh
journalctl -u scraperack-controller -f
journalctl -u scraperack-node -f
```

A plugin's own output is on its page under **Plugins**, or `GET /v1/plugins/{name}/log`.

## Disk space

Space goes to images and disks (controller and nodes), snapshots (nodes, one per pool
revision), and finished tasks (controller, bounded by **Settings → Task storage**). Delete
unused images and pools to free it; nodes remove snapshots of pools that no longer exist.

## Troubleshooting

| Symptom | Look at |
|---|---|
| Tasks stay `queued` | Is the pool `ready` (image and disks on a node)? Is it at `max_instances`? Does a node have room for one VM of that size? |
| A pool never becomes ready | `journalctl -u scraperack-node`: snapshot errors, executor that never opened its socket, failing preload |
| `image is still distributing` | Wait for **Images** to show it on every ready node |
| Node stuck `syncing` | It is still downloading images or disks; check its free space |
| No network inside VMs | `network` must be on for the pool; `systemctl status scraperack-dns`; `sysctl net.ipv4.ip_forward` |
| Slow VM starts, node log says `/dev/userfaultfd is not usable` | The node service gives group `kvm` access to `/dev/userfaultfd` (Linux 6.1+); check `ls -l /dev/userfaultfd` |
| 507 on submit | Task storage is full of active tasks; raise the limit or lower concurrency |
