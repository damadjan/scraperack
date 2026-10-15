FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        fonts-liberation \
        novnc \
        python3 \
        util-linux \
        websockify \
        x11vnc \
        xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY start-browser /usr/local/bin/start-browser
RUN chmod 0755 /usr/local/bin/start-browser

ENV DISPLAY=:0 HOME=/tmp/chromium
EXPOSE 5900 6080 9222
ENTRYPOINT ["/usr/local/bin/start-browser"]
