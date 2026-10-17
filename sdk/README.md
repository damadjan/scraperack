# ScrapeRack SDK

Python client for submitting trusted Python functions to a self-hosted ScrapeRack cluster.

> ScrapeRack is alpha software. Use it on trusted infrastructure and expect breaking changes.

## Install

```sh
pip install scraperack
```

## Run a function

The environment must already exist on the cluster.

```python
from scraperack import Cluster


def add(a, b):
    return a + b


cluster = Cluster("http://controller:7070")
try:
    execution = cluster.run("python", add, (2, 3))
    result = cluster.wait(execution["id"])
    print(result["result"]["value"]["data"])
finally:
    cluster.close()
```

Installation, cluster setup, and source code are available at [github.com/damadjan/scraperack](https://github.com/damadjan/scraperack).
