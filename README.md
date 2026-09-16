# ScrapeRack

Run trusted Python functions across self-hosted machines with a normal synchronous call.

ScrapeRack is a Docker-based convenience layer over Ray for one trusted owner. It uses Ray for serialization, scheduling, execution, and results without requiring application code to connect directly to the cluster.

## Architecture

```text
Python SDK -> HTTP gateway -> control plane (Ray head) -> nodes
```

The SDK sends one function invocation per HTTP request. The gateway submits one real Ray task and holds the response open until the task returns or raises. The control plane advertises zero CPUs, so functions execute only on nodes.

The PoC intentionally has no background jobs, batching, database, multi-tenancy, or Kubernetes integration.

```text
sdk/        Installable Python SDK
platform/   Shared Docker image, gateway, and platform tests
examples/   Example function invocations
```

## Requirements

- Docker with Compose
- Python 3.10 or newer

## Start the cluster

Start the gateway, control plane, and one node:

```powershell
docker compose up -d
docker compose ps
```

Scale the execution capacity when the host has enough resources:

```powershell
docker compose up -d --scale node=2
```

The gateway listens on <http://127.0.0.1:42800>. The Ray dashboard is not published to the host.

Do not expose the gateway to the public internet. Function payloads use Python serialization and therefore grant arbitrary code execution by design.

## Invoke a function

Install the SDK:

```powershell
python -m pip install -e sdk
```

Decorate a function and call it normally:

```python
import scraperack

@scraperack.function(
    requirements=["requests==2.32.5", "beautifulsoup4==4.13.4"]
)
def scrape(url):
    return {"url": url, "status": 200}

result = scrape("https://example.com")
```

The call is synchronous. ScrapeRack opens one HTTP request, runs one Ray task on a node, returns the value or remote traceback, and closes the request. Run the included example with:

```powershell
python examples/single_call.py
```

Set `SCRAPERACK_ADDRESS` before importing the SDK when the gateway is not local, or configure it in code:

```python
scraperack.configure("http://scraperack.example:42800")
```

Requirements are explicit and belong to the function. Ray installs them in an isolated runtime environment on the node before deserializing the function. Identical requirement lists reuse Ray's per-node runtime-environment cache, so installation is normally a first-call cost on each node.

Operating-system packages, browser binaries, and drivers still belong in the platform image.

Stop the cluster when finished:

```powershell
docker compose down
```

## Container image

GitHub Actions builds the shared gateway, control-plane, and node image for every change. The image starts from Python 3.10 and installs the pinned Ray version. Changes merged into `main` are published as `ghcr.io/damadjan/scraperack:main` and with an immutable commit tag.

## Tests

```powershell
python -m pip install -r platform/requirements-dev.txt -e sdk
python -m unittest discover -s sdk/tests -v
python -m unittest discover -s platform/tests -t platform -v
ruff check sdk platform examples
ruff format --check sdk platform examples
docker compose config --quiet
```
