# Development

```text
platform/src/       Rust: controller (api.rs, cluster.rs, storage.rs, plugins.rs), node.rs, join.rs
platform/guest/     scraperack-init, the guest agent injected into images
platform/gui/       NiceGUI console
platform/deploy/    systemd units, Firecracker installer, rootfs builder
images/             Docker build contexts for VM images
sdk/                scraperack, the Python client
```

## Run from source

```sh
cd platform
cargo run -- controller --data-dir data/controller
cargo run -- node --controller http://127.0.0.1:7070 --name node-1 --kernel /path/to/vmlinux
cd ..
pip install -e platform/gui
SCRAPERACK_URL=http://127.0.0.1:7070 python platform/gui/app.py
```

Nodes need Linux, KVM, and Firecracker (`sudo ./platform/deploy/install-firecracker.sh`).
The controller and GUI run anywhere.

`platform/deploy/build-rootfs.sh` builds a minimal Ubuntu 24.04 image with CPython 3.10
for the guest agent, without Docker.

## Checks

CI runs these on every pull request; run them before pushing:

```sh
cargo fmt --manifest-path platform/Cargo.toml --all -- --check
cargo clippy --manifest-path platform/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path platform/Cargo.toml --locked
ruff check --select E4,E7,E9,F platform/gui platform/guest/scraperack-init sdk
pip install ./sdk && (cd sdk && python -m unittest discover -s tests)
```

On Windows, run the Rust checks in a container:

```sh
docker run --rm -v "$PWD:/src" -w /src/platform rust:1 sh -c \
  "rustup component add clippy rustfmt && cargo clippy --all-targets -- -D warnings && cargo test"
```

## Releases

Every push to `main` builds `scraperack-linux-x86_64.tar.gz` and publishes it as the
`rolling` release, which the installer downloads.

## Conventions

One small branch and pull request per change, Conventional Commit titles (`feat:`, `fix:`,
`refactor:`, `chore:`), squash-merged.
