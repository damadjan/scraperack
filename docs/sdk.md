# Python client

`scraperack` is the Python client for the core API, in [`sdk/`](../sdk). It depends only on
httpx. To run Python functions, use
[`scraperack-python`](https://github.com/damadjan/scraperack-plugins/blob/main/docs/python-sdk.md),
which builds on it.

```sh
pip install scraperack
```

```python
from scraperack import APIError, Cluster

with Cluster("http://controller:7070") as cluster:
    image = next(i for i in cluster.images() if i["name"] == "headless-chromium")
    cluster.create_pool("browsers", image["id"], cpu=1, memory_mib=1024, warm=4,
                        network=True,
                        ports=[{"name": "cdp", "guest_port": 9222, "host_port": None,
                                "protocol": "tcp"}])
    cluster.update_pool("browsers", warm_instances=8)
    for vm in cluster.vms():
        print(vm["pool"], vm["node_address"], vm["ports"])
```

## Methods

| Area | Methods |
|---|---|
| Images | `images()`, `import_image(name, reference, username=None, password=None)`, `rename_image(id, name)`, `delete_image(id)` |
| Pools | `pools()`, `pool(name)`, `create_pool(name, image, cpu=1, memory_mib=512, disk_mib=1024, timeout=300, warm=0, max_instances=None, idle_timeout=30, reuse=True, network=False, ports=(), **fields)`, `update_pool(name, **changes)`, `restart_pool(name)`, `delete_pool(name)` |
| Disks | `disks()`, `upload_disk(path_or_file)`, `delete_disk(key)` |
| Tasks | `submit(pool, payload, queue=True, variables=None, secrets=None)`, `tasks(limit=None, pool=None, owner=None)`, `task(id)`, `wait(id)`, `result(id)`, `logs(id)`, `payload(id)`, `upload_working_dir(path)` |
| Inventory | `vms()`, `nodes()`, `plugins()`, `storage()` |
| Anything else | `request(method, path, **httpx_kwargs)` |

`create_pool(**fields)` passes other pool fields through, such as `disks`, `executor`,
`aliases`, and `preload`. `update_pool` reads the pool, applies the changes, and writes it
back.

Errors raise `APIError` with `status` and the controller's `message`.

`AsyncCluster` has the same methods as coroutines:

```python
from scraperack import AsyncCluster

async with AsyncCluster("http://controller:7070") as cluster:
    task = await cluster.submit("my-pool", {"encoding": "json", "data": 1})
    print(await cluster.result(task["id"]))
```
