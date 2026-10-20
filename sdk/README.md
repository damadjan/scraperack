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
    from scraperack_runtime import secret, variable

    print(variable("LOG_LEVEL", "INFO"))
    database_url = secret("DATABASE_URL")
    return a + b


cluster = Cluster("http://controller:7070")
try:
    execution = cluster.run(
        "python",
        add,
        (2, 3),
        working_dir="./project",
        variables={"LOG_LEVEL": "DEBUG"},
        secrets={"DATABASE_URL": "postgresql://..."},
    )
    print(cluster.result(execution["id"]))
finally:
    cluster.close()
```

`working_dir` uploads project modules and files using a content-addressed cache. It is
made importable and becomes the current directory for the invocation. Third-party
packages still belong in the selected ScrapeRack environment.

Installation, cluster setup, and source code are available at [github.com/damadjan/scraperack](https://github.com/damadjan/scraperack).
