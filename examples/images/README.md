# Example images

These are normal `linux/amd64` Docker build contexts. Scraperack injects its guest agent when the image is imported, so the agent must not be copied into either image.

```text
docker build --platform linux/amd64 -t registry.local/scraperack/python-runtime:3.12 python-runtime
docker build --platform linux/amd64 -t registry.local/scraperack/chromium:latest chromium
docker build --platform linux/amd64 -t registry.local/scraperack/chrome:latest chrome
docker push registry.local/scraperack/python-runtime:3.12
docker push registry.local/scraperack/chromium:latest
docker push registry.local/scraperack/chrome:latest
```

Import `python-runtime` under Runtime Images and create an execution environment from it. Import `chromium` under VM Images and create a network-enabled VM pool with:

```text
vnc=5900, novnc=6080, cdp=9222
```

The `chrome` image is adapted from `caas-u24chrome` and uses Google Chrome Stable, TigerVNC, noVNC, Fluxbox, and a CDP proxy. Its pool ports are:

```text
vnc=5900, novnc=6080, cdp=9223
```

The Chromium and Chrome VNC endpoints have no password; publish them only on a trusted network or put authentication in front of them.
