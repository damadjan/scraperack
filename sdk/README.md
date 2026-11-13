# ScrapeRack core SDK

Python client for the ScrapeRack core API: images, microVM pools, disks, tasks, and live
inventory. The core only runs microVMs; what runs inside them is up to plugins.

To run Python functions, use [`scraperack-python`](https://github.com/damadjan/scraperack-plugins/tree/main/python/sdk),
the Python plugin's SDK, which builds on this package.

> ScrapeRack is alpha software. Use it on trusted infrastructure and expect breaking changes.

```sh
pip install scraperack
```

```python
from scraperack import Cluster

with Cluster("http://controller:7070") as cluster:
    image = next(i for i in cluster.images() if i["name"] == "headless-chromium")
    cluster.create_pool("browsers", image["id"], cpu=1, memory_mib=1024, warm=4,
                        network=True, ports=[{"name": "cdp", "guest_port": 9222,
                                              "host_port": None, "protocol": "tcp"}])
    for vm in cluster.vms():
        print(vm["pool"], vm["node_address"], vm["ports"])
```

A **task** sends an opaque JSON payload to the executor of a pool and stores its result,
stdout, stderr, and metrics for a while. Plugins give payloads their meaning:

```python
task = cluster.submit("my-pool", {"encoding": "json", "data": {"hello": "world"}})
print(cluster.result(task["id"]))
print(cluster.tasks(owner="plugin:python", limit=20))
```

`AsyncCluster` has the same methods for `asyncio`. `cluster.request(method, path)` calls any
other endpoint, such as a plugin API under `/plugins/<name>/api/`.
