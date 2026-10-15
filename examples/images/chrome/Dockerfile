FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fluxbox \
        fonts-liberation \
        gnupg \
        novnc \
        python3 \
        socat \
        tigervnc-standalone-server \
        tigervnc-tools \
        util-linux \
        websockify \
        x11-utils \
    && install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
        | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

COPY start-chrome /usr/local/bin/start-chrome
RUN chmod 0755 /usr/local/bin/start-chrome

ENV DISPLAY=:0 HOME=/tmp/chrome RESOLUTION=1280x720
EXPOSE 5900 6080 9223
ENTRYPOINT ["/usr/local/bin/start-chrome"]
CMD ["about:blank"]
