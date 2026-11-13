# Scraperack documentation

| Page | What it covers |
|---|---|
| [Architecture](architecture.md) | Controller, nodes, guest agent, plugins, and how a task flows through them |
| [Installation](installation.md) | Installing, updating, joining nodes, configuration files, services, ports |
| [Images and pools](images-and-pools.md) | Importing images, pool settings, warm and idle microVMs, disks, networking |
| [Tasks](tasks.md) | Sending payloads to executors, results, logs, secrets, working directories, storage |
| [Executors](executors.md) | Writing the program inside a microVM that handles task payloads |
| [API reference](api.md) | Every core HTTP endpoint |
| [Python client](sdk.md) | `scraperack`, the Python client for the core API |
| [Operations](operations.md) | Upgrades, backups, rollback, logs, disk space, troubleshooting |
| [Development](development.md) | Building and testing from source |
| [Third-party components](third-party-components.md) | Software the installer fetches from its publishers |

Plugins, including the Python plugin and its `scraperack-python` SDK, are documented in
[scraperack-plugins](https://github.com/damadjan/scraperack-plugins/tree/main/docs).
