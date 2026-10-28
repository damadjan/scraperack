# Third-party components

ScrapeRack releases contain the project's source code, Python SDK, and compiled Rust platform binary. The Rust binary includes third-party crates whose licenses are reproduced in [`THIRD_PARTY_LICENSES.txt`](../THIRD_PARTY_LICENSES.txt).

The SDK wheel contains only ScrapeRack code. Its declared dependencies, currently `cloudpickle` and `httpx`, are resolved and installed separately by pip.

The GUI is distributed as source. Its Python dependencies are resolved and installed separately by pip.

The files under `images/` are build recipes. ScrapeRack does not distribute their resulting filesystems, browsers, operating-system packages, or container images.

The deployment scripts install or download the following components directly from their publishers:

| Component | Publisher |
| --- | --- |
| Firecracker | [Firecracker releases](https://github.com/firecracker-microvm/firecracker/releases) |
| Firecracker CI kernel | [Firecracker CI artifacts](https://s3.amazonaws.com/spec.ccfc.min/) |
| dnsproxy | [AdGuardTeam dnsproxy releases](https://github.com/AdguardTeam/dnsproxy/releases) |
| Ubuntu packages | [Ubuntu archive](https://archive.ubuntu.com/ubuntu/) |
| CPython | [Python.org](https://www.python.org/downloads/source/) |
| Python packages | [PyPI](https://pypi.org/) |
| Google Chrome | [Google's Linux repository](https://www.google.com/chrome/) |
| Chromium and Debian packages | [Debian](https://www.debian.org/) |

Those components remain governed by their respective licenses and terms. They are not covered by ScrapeRack's Apache-2.0 license.
