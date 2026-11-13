# ScrapeRack

Self-hosted Firecracker microVMs for dense, short-lived workloads. ScrapeRack keeps pools of
warm microVMs across a cluster of Linux machines, restores new ones from snapshots in a few
hundred milliseconds, and sends work to them. The core only runs microVMs; plugins decide
what runs inside them, such as Python functions or browser fleets.

> Alpha software. Run it on trusted infrastructure and expect breaking changes.

## Install

On a fresh Ubuntu 24.04 x86_64 machine with KVM:

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

That installs the controller (API on port 7070), the GUI (port 8080), and a node. Run the
same command to update. Add machines from **Settings → Join nodes**.

## Run Python code

Install the Python plugin from **Plugins → Install plugin**, create an environment on its
page, then:

```python
from scraperack_python import Python


def status(url):
    import httpx
    return httpx.get(url).status_code


with Python("http://controller:7070") as py:
    print(py.call("scrapers", status, "https://example.com"))
```

Every call shows up on the plugin's **Executions** tab with its output and errors. See
[scraperack-plugins](https://github.com/damadjan/scraperack-plugins).

## Documentation

- [Architecture](docs/architecture.md)
- [Installation](docs/installation.md)
- [Images and pools](docs/images-and-pools.md)
- [Tasks](docs/tasks.md) and [executors](docs/executors.md)
- [API reference](docs/api.md) and the [Python client](docs/sdk.md)
- [Operations](docs/operations.md)
- [Development](docs/development.md)

## Repository

```text
platform/   Rust controller and node, guest agent, GUI, deployment files
images/     Docker build contexts for VM images
sdk/        scraperack, the Python client for the core API
docs/       Documentation
```

## License

[Apache License 2.0](LICENSE). Rust dependencies compiled into the binary are listed in
[THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt); components the installer fetches from
their publishers in [docs/third-party-components.md](docs/third-party-components.md).
