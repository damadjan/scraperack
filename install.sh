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
release=${SCRAPERACK_RELEASE:-rolling}
user=${SCRAPERACK_USER:-scraperack}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
role=$(cat /etc/scraperack/role 2>/dev/null || true)

packages="ca-certificates curl docker.io e2fsprogs fakeroot iproute2 iptables python3 python3-venv skopeo"
missing=
for package in $packages; do
    dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed' || missing=1
done
if [ -n "$missing" ]; then
    echo "Installing host dependencies"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y ca-certificates curl docker.io e2fsprogs fakeroot \
        iproute2 iptables python3 python3-venv skopeo
fi
systemctl enable --now docker

if ! id "$user" >/dev/null 2>&1; then
    getent group "$user" >/dev/null 2>&1 || groupadd --system "$user"
    useradd --system --home-dir /var/lib/scraperack --create-home \
        --gid "$user" --shell /usr/sbin/nologin "$user"
fi
usermod -aG docker,kvm "$user"
group=$(id -gn "$user")

echo "Downloading ScrapeRack $release"
base="https://github.com/$repo/releases/download/$release"
archive=scraperack-linux-x86_64.tar.gz
curl -fsSL "$base/$archive" -o "$work/$archive"
curl -fsSL "$base/$archive.sha256" -o "$work/$archive.sha256"
(cd "$work" && sha256sum -c "$archive.sha256")
tar -xzf "$work/$archive" -C "$work"
bundle="$work/scraperack"

systemctl stop scraperack-gui scraperack-node scraperack-controller 2>/dev/null || true
install -m 0755 "$bundle/bin/scraperack" /usr/local/bin/scraperack

install -d /opt/scraperack/platform
if [ "$role" != node ]; then
    rm -rf /opt/scraperack/platform/gui
    cp -a "$bundle/platform/gui" /opt/scraperack/platform/gui
    [ -x /opt/scraperack/gui-venv/bin/python ] || python3 -m venv /opt/scraperack/gui-venv
    /opt/scraperack/gui-venv/bin/pip install --disable-pip-version-check \
        'ago>=0.1,<1' 'httpx>=0.27,<1' 'nicegui>=3,<4' 'tzdata>=2025.2'
fi
install -m 0644 "$bundle/LICENSE" /opt/scraperack/LICENSE
install -m 0644 "$bundle/THIRD_PARTY_LICENSES.txt" \
    /opt/scraperack/THIRD_PARTY_LICENSES.txt
install -m 0644 "$bundle/BUILD_COMMIT" /opt/scraperack/BUILD_COMMIT

if [ ! -x /usr/local/bin/firecracker ] || [ ! -f /var/lib/scraperack-node/vmlinux ]; then
    SCRAPERACK_USER="$user" SCRAPERACK_GROUP="$group" \
        sh "$bundle/platform/deploy/install-firecracker.sh"
fi

install -d /etc/scraperack /var/lib/scraperack-controller /var/lib/scraperack-node
chown -R "$user:$group" /var/lib/scraperack-controller /var/lib/scraperack-node

cpu_millis=${SCRAPERACK_NODE_CPU_MILLIS:-$(( $(nproc) * 1000 ))}
total_memory=$(awk '/MemTotal/ { print int($2 / 1024) }' /proc/meminfo)
default_memory=$((total_memory - 2048))
[ "$default_memory" -ge 512 ] || default_memory=512
memory_mib=${SCRAPERACK_NODE_MEMORY_MIB:-$default_memory}
vm_start_concurrency=$((cpu_millis / 2000))
[ "$vm_start_concurrency" -ge 1 ] || vm_start_concurrency=1
vm_start_concurrency=${SCRAPERACK_NODE_VM_START_CONCURRENCY:-$vm_start_concurrency}

cat > /etc/scraperack/controller.env <<EOF
SCRAPERACK_LISTEN=${SCRAPERACK_LISTEN:-0.0.0.0:7070}
EOF
if [ ! -f /etc/scraperack/node.env ]; then
cat > /etc/scraperack/node.env <<EOF
SCRAPERACK_CONTROLLER=http://127.0.0.1:7070
SCRAPERACK_NODE_CREDENTIAL=
SCRAPERACK_NODE_NAME=${SCRAPERACK_NODE_NAME:-$(hostname)}
SCRAPERACK_NODE_CPU_MILLIS=$cpu_millis
SCRAPERACK_NODE_MEMORY_MIB=$memory_mib
SCRAPERACK_NODE_VM_START_CONCURRENCY=$vm_start_concurrency
EOF
fi
grep -q '^SCRAPERACK_NODE_CREDENTIAL=' /etc/scraperack/node.env || \
    printf 'SCRAPERACK_NODE_CREDENTIAL=\n' >> /etc/scraperack/node.env
grep -q '^SCRAPERACK_NODE_VM_START_CONCURRENCY=' /etc/scraperack/node.env || \
    printf 'SCRAPERACK_NODE_VM_START_CONCURRENCY=%s\n' "$vm_start_concurrency" >> /etc/scraperack/node.env
cat > /etc/scraperack/gui.env <<EOF
SCRAPERACK_URL=http://127.0.0.1:7070
SCRAPERACK_PUBLIC_URL=${SCRAPERACK_PUBLIC_URL:-}
SCRAPERACK_GUI_PORT=${SCRAPERACK_GUI_PORT:-8080}
EOF
chmod 0600 /etc/scraperack/*.env

for service in controller node gui; do
    sed "s/^User=.*/User=$user/" \
        "$bundle/platform/deploy/scraperack-$service.service" \
        > "/etc/systemd/system/scraperack-$service.service"
done

systemctl daemon-reload
if [ "$role" = node ]; then
    systemctl disable --now scraperack-controller scraperack-gui
    systemctl enable --now scraperack-node
    echo "ScrapeRack node is running"
    exit
fi
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
