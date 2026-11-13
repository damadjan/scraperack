# Executors

An executor is the program inside a microVM that handles task payloads. Pools without one
only provide VMs (for example browsers reached through published ports).

Plugins usually ship the executor in their `guest` folder, which becomes a read-only disk,
and set the pool's `executor` to run it. The Python plugin's
[`executor.py`](https://github.com/damadjan/scraperack-plugins/blob/main/python/plugin/guest/executor.py)
is a complete example.

## Startup

Before a pool's snapshot is taken, the guest agent:

1. mounts the pool's `disks` in order,
2. starts the `executor` command (stdin, stdout, and stderr go to `/dev/null`),
3. starts the image's own entrypoint, if it has one,
4. sends `{"encoding": "scraperack-preload", "modules": [...]}` if the pool has `preload`.

The executor must listen on the Unix socket `/tmp/scraperack-executor.sock` within
10 seconds. It is restarted if it exits.

Working directory is `/tmp` (a tmpfs), `HOME` and `TMPDIR` are `/tmp`. The root filesystem
is shared and read-only underneath.

## Protocol

Each message, in both directions, is a 4-byte big-endian length followed by that many bytes
of UTF-8 JSON. Requests arrive one at a time on one connection.

Request: the task payload as submitted, or the `scraperack-invocation` wrapper described in
[Tasks](tasks.md#variables-secrets-and-working-directories).

Reply:

```json
{"result": {"value": <any JSON>, "stdout": "...", "stderr": "..."},
 "error": null,
 "timed_out": false}
```

- `result.value` is returned by `GET /v1/tasks/{id}/result` as `value`.
- `result.stdout` and `result.stderr` are returned by `/logs`.
- A non-null `error` marks the task failed. Use it for tracebacks.

The core enforces the timeout itself; an executor never needs to set `timed_out`.

## Minimal executor

```python
#!/usr/bin/python3
import json, socket
from pathlib import Path

path = Path("/tmp/scraperack-executor.sock")
path.unlink(missing_ok=True)
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(str(path))
server.listen(1)

def read(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data

while True:
    conn, _ = server.accept()
    with conn:
        while header := read(conn, 4):
            request = json.loads(read(conn, int.from_bytes(header, "big")))
            if request.get("encoding") == "scraperack-invocation":
                request = request["payload"]
            reply = {"result": {"value": {"echo": request}, "stdout": "", "stderr": ""},
                     "error": None, "timed_out": False}
            data = json.dumps(reply).encode()
            conn.sendall(len(data).to_bytes(4, "big") + data)
```
