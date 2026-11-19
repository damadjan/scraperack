# Scraperack

Self-hosted Firecracker microVMs for dense, short-lived workloads. Scraperack keeps pools of
warm microVMs across a cluster of Linux machines, restores new ones from snapshots in a few
hundred milliseconds, and sends work to them. The core only runs microVMs; plugins decide
what runs inside them.

> Alpha software. Run it on trusted infrastructure and expect breaking changes.

## Install

On a fresh Ubuntu 24.04 x86_64 machine:

```sh
curl -fsSL https://raw.githubusercontent.com/damadjan/scraperack/main/install.sh | sudo sh
```

Choose a single-machine install (controller, GUI, and worker), a control plane without a
worker, or a worker to join to an existing control plane. KVM is required only for roles
that run a worker. Run the same command to update or change the installation type.

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
sdk/        scraperack, the Python client for the core API
docs/       Documentation
```

## License

[Apache License 2.0](LICENSE). Rust dependencies compiled into the binary are listed in
[THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt); components the installer fetches from
their publishers in [docs/third-party-components.md](docs/third-party-components.md).
