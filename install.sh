#!/bin/sh
set -eu

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
[ "$(uname -m)" = x86_64 ] || { echo "only x86_64 is supported" >&2; exit 1; }
. /etc/os-release
[ "${ID:-}" = ubuntu ] && [ "${VERSION_ID:-}" = 24.04 ] || {
    echo "only Ubuntu 24.04 is supported" >&2
    exit 1
}
[ -e /dev/kvm ] || { echo "/dev/kvm is required" >&2; exit 1; }

repo=${SCRAPERACK_REPOSITORY:-damadjan/scraperack}
ref=${SCRAPERACK_REF:-main}
user=${SCRAPERACK_USER:-scraperack}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

echo "Installing host dependencies"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential ca-certificates curl docker.io e2fsprogs fakeroot \
    iproute2 iptables pkg-config python3 python3-venv skopeo
systemctl enable --now docker

if ! id "$user" >/dev/null 2>&1; then
    getent group "$user" >/dev/null 2>&1 || groupadd --system "$user"
    useradd --system --home-dir /var/lib/scraperack --create-home \
        --gid "$user" --shell /usr/sbin/nologin "$user"
fi
usermod -aG docker,kvm "$user"
group=$(id -gn "$user")

echo "Downloading ScrapeRack $ref"
curl -fsSL "https://codeload.github.com/$repo/tar.gz/$ref" -o "$work/source.tar.gz"
mkdir "$work/source"
tar -xzf "$work/source.tar.gz" -C "$work/source" --strip-components=1

echo "Building ScrapeRack"
export CARGO_HOME="$work/cargo"
export RUSTUP_HOME="$work/rustup"
curl -fsSL https://sh.rustup.rs -o "$work/rustup.sh"
sh "$work/rustup.sh" -y --profile minimal --default-toolchain stable --no-modify-path
"$CARGO_HOME/bin/cargo" build --release --locked \
    --manifest-path "$work/source/platform/Cargo.toml"

systemctl stop scraperack-gui scraperack-node scraperack-controller 2>/dev/null || true
install -m 0755 "$work/source/platform/target/release/scraperack" /usr/local/bin/scraperack

rm -rf /opt/scraperack
install -d /opt/scraperack/platform
cp -a "$work/source/platform/gui" /opt/scraperack/platform/gui
install -m 0644 "$work/source/LICENSE" /opt/scraperack/LICENSE
install -m 0644 "$work/source/THIRD_PARTY_LICENSES.txt" \
    /opt/scraperack/THIRD_PARTY_LICENSES.txt
python3 -m venv /opt/scraperack/gui-venv
/opt/scraperack/gui-venv/bin/pip install --disable-pip-version-check \
    'ago>=0.1,<1' 'httpx>=0.27,<1' 'nicegui>=3,<4' 'tzdata>=2025.2'

SCRAPERACK_USER="$user" SCRAPERACK_GROUP="$group" \
    sh "$work/source/platform/deploy/install-firecracker.sh"

install -d /etc/scraperack /var/lib/scraperack-controller /var/lib/scraperack-node
chown -R "$user:$group" /var/lib/scraperack-controller /var/lib/scraperack-node

cpu_millis=${SCRAPERACK_NODE_CPU_MILLIS:-$(( $(nproc) * 1000 ))}
total_memory=$(awk '/MemTotal/ { print int($2 / 1024) }' /proc/meminfo)
default_memory=$((total_memory - 2048))
[ "$default_memory" -ge 512 ] || default_memory=512
memory_mib=${SCRAPERACK_NODE_MEMORY_MIB:-$default_memory}

cat > /etc/scraperack/controller.env <<EOF
SCRAPERACK_LISTEN=${SCRAPERACK_LISTEN:-0.0.0.0:7070}
EOF
cat > /etc/scraperack/node.env <<EOF
SCRAPERACK_CONTROLLER=http://127.0.0.1:7070
SCRAPERACK_NODE_NAME=${SCRAPERACK_NODE_NAME:-$(hostname)}
SCRAPERACK_NODE_CPU_MILLIS=$cpu_millis
SCRAPERACK_NODE_MEMORY_MIB=$memory_mib
EOF
cat > /etc/scraperack/gui.env <<EOF
SCRAPERACK_URL=http://127.0.0.1:7070
SCRAPERACK_GUI_PORT=${SCRAPERACK_GUI_PORT:-8080}
EOF
chmod 0644 /etc/scraperack/*.env

for service in controller node gui; do
    sed "s/^User=.*/User=$user/" \
        "$work/source/platform/deploy/scraperack-$service.service" \
        > "/etc/systemd/system/scraperack-$service.service"
done

systemctl daemon-reload
systemctl enable --now scraperack-controller scraperack-node scraperack-gui

attempt=0
until [ "$(curl -fsS http://127.0.0.1:7070/v1/nodes 2>/dev/null)" != "[]" ] && \
      curl -fsS http://127.0.0.1:8080/ >/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 30 ] || {
        systemctl --no-pager --full status scraperack-controller scraperack-node scraperack-gui || true
        exit 1
    }
    sleep 1
done

address=$(hostname -I | awk '{ print $1 }')
echo "ScrapeRack is running"
echo "Controller: http://$address:7070"
echo "GUI:        http://$address:8080"
