# ScrapeRack

Run trusted Python functions across self-hosted machines with a normal synchronous call.

ScrapeRack is a Docker-based convenience layer over Ray for one trusted owner. It uses Ray for serialization, scheduling, execution, and results without requiring application code to connect directly to the cluster.

## Architecture

```text
Python SDK -> HTTP gateway -> control plane (Ray head) -> nodes
NiceGUI dashboard -> persistent cache volumes (read-only)
```

The SDK sends one function invocation per HTTP request. The gateway submits one real Ray task and holds the response open until the task returns or raises. The control plane advertises zero CPUs, so functions execute only on nodes.

The PoC intentionally has no background jobs, batching, database, multi-tenancy, or Kubernetes integration.

```text
sdk/        Installable Python SDK
platform/   Shared Docker image, gateway, and platform tests
frontend/   NiceGUI dashboard
examples/   Example function invocations
```

## Requirements

- Docker with Compose
- Python 3.10 or newer

## Start the cluster

Start the gateway, control plane, one node, and the dashboard:

```powershell
Copy-Item .env.example .env
docker compose up -d
docker compose ps
```

Set `RAY_PIP_CACHING` in `.env` to either `true` or `false`. It defaults to `true`.

Scale the execution capacity when the host has enough resources:

```powershell
docker compose up -d --scale node=2
```

The gateway listens on <http://127.0.0.1:42800>. The Ray dashboard is not published to the host.

The ScrapeRack dashboard listens on <http://127.0.0.1:9987>. Its first view shows live storage usage for the persistent pip and working-directory caches. It performs one initial scan, then uses filesystem events instead of repeatedly scanning the volumes.

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

The SDK hashes each serialized function and uploads it only when that version is missing from the gateway. Cached functions are stored in the persistent `function-cache` volume. Set `SCRAPERACK_FUNCTION_CACHING=false` on the gateway to send functions inline instead; caching is enabled by default.

Set `SCRAPERACK_ADDRESS` before importing the SDK when the gateway is not local, or configure it in code:

```python
scraperack.configure("http://scraperack.example:42800")
```

Optionally organize invocations by project. Project names may contain lowercase
English letters, numbers, and single dashes. ScrapeRack uses the decorated Python
function's name automatically.

```python
scraperack.configure(
    "http://scraperack.example:42800",
    project="linkedin-jobs",
)
```

The SDK warns by default when the included `working_dir` files exceed 5 MiB. Configure or disable that warning centrally:

```python
scraperack.configure(
    "http://scraperack.example:42800",
    working_dir_warning=True,
    working_dir_warning_bytes=10 * 1024 * 1024,
)
```

Set `working_dir_warning=False` to disable it. The warning does not prevent execution.

Requirements are explicit and belong to the function. Ray installs them in an isolated runtime environment on the node before deserializing the function. Identical requirement lists reuse Ray's per-node runtime-environment cache, so installation is normally a first-call cost on each node.

When `RAY_PIP_CACHING=true`, pip downloads are reused across different runtime environments and stored in the persistent `pip-cache` volume. `false` preserves Ray's default behavior.

Use `working_dir` when a function needs local project modules or files:

```python
@scraperack.function(
    requirements=["requests==2.32.5"],
    working_dir=".",
)
def scrape(url):
    from project.parser import parse

    return parse(url)
```

The SDK hashes the directory, uploads a ZIP only when that version is missing from the gateway, and references the resulting immutable artifact in Ray's `working_dir`. The gateway keeps uploaded artifacts in the persistent `working-dir-cache` volume, while each Ray node maintains its own extracted cache.

The SDK skips symlinks and directories named `.git`, `.venv`, `venv`, `__pycache__`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, or `.tox`. It also skips `.env`, `credentials.txt`, and `AGENTS.md`. Point `working_dir` at a dedicated project directory rather than a directory containing unrelated data.

Operating-system packages, browser binaries, and drivers still belong in the platform image.

Stop the cluster when finished:

```powershell
docker compose down
```

## Container image

GitHub Actions builds the shared gateway, control-plane, and node image and the separate dashboard image for every change. Changes merged into `main` are published as `ghcr.io/damadjan/scraperack:main` and `ghcr.io/damadjan/scraperack-dashboard:main`, along with immutable commit tags.

## Tests

```powershell
.venv\Scripts\python -m pip install -r platform/requirements-dev.txt -e sdk
.venv\Scripts\python -m unittest discover -s sdk/tests -v
.venv\Scripts\python -m unittest discover -s platform/tests -t platform -v
.venv\Scripts\ruff check sdk platform examples
.venv\Scripts\ruff format --check sdk platform examples
$env:RAY_PIP_CACHING="true"
docker compose config --quiet
```

The dashboard uses a separate environment because it is deployed in a separate image:

```powershell
python -m venv .venv-dashboard
.venv-dashboard\Scripts\python -m pip install -r frontend/requirements-dev.txt
.venv-dashboard\Scripts\python -m unittest discover -s frontend/tests -t frontend -v
.venv-dashboard\Scripts\ruff check frontend
.venv-dashboard\Scripts\ruff format --check frontend
```
