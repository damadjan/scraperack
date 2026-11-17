#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage: install.sh [--role all|control-plane|worker] [--yes]

Roles:
  all            Controller, GUI, and worker (default)
  control-plane  Controller and GUI only
  worker         Worker only; join it from the controller after installation
EOF
}

die() {
    echo "$*" >&2
    exit 1
}

role_arg=${SCRAPERACK_ROLE:-}
yes=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --role)
            [ "$#" -gt 1 ] || die "--role requires a value"
            role_arg=$2
            shift 2
            ;;
        --role=*)
            role_arg=${1#--role=}
            shift
            ;;
        -y|--yes)
            yes=1
            shift
            ;;
        -h|--help)
            usage
            exit
            ;;
        *) die "unknown option: $1" ;;
    esac
done

normalize_role() {
    case "$1" in
        node) echo worker ;;
        all|control-plane|worker) echo "$1" ;;
        *) return 1 ;;
    esac
}

role_name() {
    case "$1" in
        all) echo "Single machine (controller, GUI, and worker)" ;;
        control-plane) echo "Control plane only (controller and GUI)" ;;
        worker) echo "Worker only" ;;
    esac
}

choose_role() {
    while :; do
        cat >/dev/tty <<'EOF'
Choose installation type:

  1) Single machine — controller, GUI, and worker [default]
  2) Control plane only — controller and GUI; runs no workloads
  3) Worker only — connects to an existing control plane

EOF
        printf 'Selection [1]: ' >/dev/tty
        IFS= read -r choice </dev/tty || choice=
        case "$choice" in
            ''|1) echo all; return ;;
            2) echo control-plane; return ;;
            3) echo worker; return ;;
            *) echo "Enter 1, 2, or 3." >/dev/tty ;;
        esac
    done
}

confirm_change() {
    [ -n "$yes" ] && return
    printf 'Continue? [y/N] ' >/dev/tty
    IFS= read -r answer </dev/tty || answer=
    case "$answer" in
        y|Y|yes|YES|Yes) ;;
        *) echo "Cancelled"; exit ;;
    esac
}

[ "$(id -u)" = 0 ] || die "run as root"
[ "$(uname -m)" = x86_64 ] || die "only x86_64 is supported"
. /etc/os-release
[ "${ID:-}" = ubuntu ] && [ "${VERSION_ID:-}" = 24.04 ] || \
    die "only Ubuntu 24.04 is supported"

interactive=
if [ -r /dev/tty ] && [ -w /dev/tty ] && (: </dev/tty) 2>/dev/null; then
    interactive=1
fi
installed=
[ -x /usr/local/bin/scraperack ] && installed=1
stored_role=$(cat /etc/scraperack/role 2>/dev/null || true)
if [ -n "$stored_role" ]; then
    current_role=$(normalize_role "$stored_role") || die "unknown installed role: $stored_role"
elif [ -n "$installed" ]; then
    current_role=all
else
    current_role=
fi

if [ -n "$role_arg" ]; then
    role=$(normalize_role "$role_arg") || die "invalid role: $role_arg"
elif [ -z "$current_role" ]; then
    role=all
    [ -z "$interactive" ] || role=$(choose_role)
elif [ -z "$interactive" ]; then
    role=$current_role
else
    printf 'Scraperack is already installed.\n\nCurrent type: %s\n\n' "$(role_name "$current_role")" >/dev/tty
    cat >/dev/tty <<'EOF'
  1) Update the current installation [default]
  2) Change installation type
  3) Cancel

EOF
    while :; do
        printf 'Selection [1]: ' >/dev/tty
        IFS= read -r choice </dev/tty || choice=
        case "$choice" in
            ''|1) role=$current_role; break ;;
            2) role=$(choose_role); break ;;
            3) echo "Cancelled"; exit ;;
            *) echo "Enter 1, 2, or 3." >/dev/tty ;;
        esac
    done
fi

if [ -n "$current_role" ] && [ "$role" != "$current_role" ]; then
    if [ -n "$interactive" ]; then
        output=/dev/tty
    else
        output=/dev/stdout
    fi
    printf 'Change from: %s\nChange to:   %s\n\n' \
        "$(role_name "$current_role")" "$(role_name "$role")" >"$output"
    case "$role" in
        control-plane) echo "The local worker will stop. Worker data will be kept." >"$output" ;;
        worker) echo "The controller and GUI will stop. Controller data will be kept." >"$output" ;;
        all) echo "The controller, GUI, and local worker will be enabled." >"$output" ;;
    esac
    [ -z "$interactive" ] || confirm_change
fi

has_control=
has_worker=
[ "$role" = worker ] || has_control=1
[ "$role" = control-plane ] || has_worker=1
[ -z "$has_worker" ] || [ -e /dev/kvm ] || die "/dev/kvm is required for role '$role'"

repo=${SCRAPERACK_REPOSITORY:-damadjan/scraperack}
release=${SCRAPERACK_RELEASE:-rolling}
user=${SCRAPERACK_USER:-scraperack}
dnsproxy_version=0.85.0
dnsproxy_sha256=740af768b17fe8ecc2dbc8c82c7b5224e43278a181b4fe31b0cce5d8da656332
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

packages="ca-certificates curl e2fsprogs"
[ -z "$has_control" ] || packages="$packages docker.io fakeroot python3 python3-venv skopeo"
[ -z "$has_worker" ] || packages="$packages iproute2 iptables"
missing=
for package in $packages; do
    dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed' || missing=1
done
if [ -n "$missing" ]; then
    echo "Installing host dependencies"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y $packages
fi
[ -z "$has_control" ] || systemctl enable --now docker

if ! id "$user" >/dev/null 2>&1; then
    getent group "$user" >/dev/null 2>&1 || groupadd --system "$user"
    useradd --system --home-dir /var/lib/scraperack --create-home \
        --gid "$user" --shell /usr/sbin/nologin "$user"
fi
case "$role" in
    all) usermod -aG docker,kvm "$user" ;;
    control-plane) usermod -aG docker "$user" ;;
    worker) usermod -aG kvm "$user" ;;
esac
group=$(id -gn "$user")

echo "Downloading Scraperack $release"
base="https://github.com/$repo/releases/download/$release"
archive=scraperack-linux-x86_64.tar.gz
curl -fsSL "$base/$archive" -o "$work/$archive"
curl -fsSL "$base/$archive.sha256" -o "$work/$archive.sha256"
(cd "$work" && sha256sum -c "$archive.sha256")
tar -xzf "$work/$archive" -C "$work"
bundle="$work/scraperack"

if [ -n "$has_worker" ] && { ! command -v dnsproxy >/dev/null 2>&1 || \
   [ "$(cat /opt/scraperack/DNSPROXY_VERSION 2>/dev/null || true)" != "$dnsproxy_version" ]; }; then
    dnsproxy_archive="dnsproxy-linux-amd64-v$dnsproxy_version.tar.gz"
    curl -fsSL "https://github.com/AdguardTeam/dnsproxy/releases/download/v$dnsproxy_version/$dnsproxy_archive" \
        -o "$work/$dnsproxy_archive"
    printf '%s  %s\n' "$dnsproxy_sha256" "$dnsproxy_archive" > "$work/dnsproxy.sha256"
    (cd "$work" && sha256sum -c dnsproxy.sha256)
    tar -xzf "$work/$dnsproxy_archive" -C "$work"
fi

systemctl stop scraperack-gui scraperack-node scraperack-controller scraperack-dns 2>/dev/null || true
install -m 0755 "$bundle/bin/scraperack" /usr/local/bin/scraperack

install -d /opt/scraperack/platform
if [ -d "$work/linux-amd64" ]; then
    install -m 0755 "$work/linux-amd64/dnsproxy" /usr/local/bin/dnsproxy
    install -d /opt/scraperack/licenses
    install -m 0644 "$work/linux-amd64/LICENSE" /opt/scraperack/licenses/dnsproxy-LICENSE
    printf '%s\n' "$dnsproxy_version" > /opt/scraperack/DNSPROXY_VERSION
fi
if [ -n "$has_control" ]; then
    rm -rf /opt/scraperack/platform/gui
    cp -a "$bundle/platform/gui" /opt/scraperack/platform/gui
    [ -x /opt/scraperack/gui-venv/bin/python ] || python3 -m venv /opt/scraperack/gui-venv
    /opt/scraperack/gui-venv/bin/pip install --disable-pip-version-check \
        'ago>=0.1,<1' 'httpx>=0.27,<1' 'nicegui>=3,<4' 'tzdata>=2025.2'
fi
install -m 0644 "$bundle/LICENSE" /opt/scraperack/LICENSE
install -m 0644 "$bundle/THIRD_PARTY_LICENSES.txt" /opt/scraperack/THIRD_PARTY_LICENSES.txt
install -m 0644 "$bundle/BUILD_COMMIT" /opt/scraperack/BUILD_COMMIT

if [ -n "$has_worker" ] && \
   { [ ! -x /usr/local/bin/firecracker ] || [ ! -f /var/lib/scraperack-node/vmlinux ]; }; then
    SCRAPERACK_USER="$user" SCRAPERACK_GROUP="$group" \
        sh "$bundle/platform/deploy/install-firecracker.sh"
fi

install -d /etc/scraperack
if [ -n "$has_control" ]; then
    install -d /var/lib/scraperack-controller
    chown -R "$user:$group" /var/lib/scraperack-controller
fi
if [ -n "$has_worker" ]; then
    install -d /var/lib/scraperack-node
    chown -R "$user:$group" /var/lib/scraperack-node
    cpu_millis=${SCRAPERACK_NODE_CPU_MILLIS:-$(( $(nproc) * 1000 ))}
    total_memory=$(awk '/MemTotal/ { print int($2 / 1024) }' /proc/meminfo)
    default_memory=$((total_memory - 2048))
    [ "$default_memory" -ge 512 ] || default_memory=512
    memory_mib=${SCRAPERACK_NODE_MEMORY_MIB:-$default_memory}
    vm_start_concurrency=$((cpu_millis / 2000))
    [ "$vm_start_concurrency" -ge 1 ] || vm_start_concurrency=1
    vm_start_concurrency=${SCRAPERACK_NODE_VM_START_CONCURRENCY:-$vm_start_concurrency}
fi

if [ -n "$has_control" ]; then
    allow_plugins=$(sed -n 's/^SCRAPERACK_ALLOW_PLUGIN_INSTALL=//p' /etc/scraperack/controller.env 2>/dev/null || true)
    cat > /etc/scraperack/controller.env <<EOF
SCRAPERACK_LISTEN=${SCRAPERACK_LISTEN:-0.0.0.0:7070}
SCRAPERACK_ALLOW_PLUGIN_INSTALL=${SCRAPERACK_ALLOW_PLUGIN_INSTALL:-${allow_plugins:-true}}
EOF
    cat > /etc/scraperack/gui.env <<EOF
SCRAPERACK_URL=http://127.0.0.1:7070
SCRAPERACK_PUBLIC_URL=${SCRAPERACK_PUBLIC_URL:-}
SCRAPERACK_GUI_PORT=${SCRAPERACK_GUI_PORT:-8080}
EOF
fi
if [ -n "$has_worker" ]; then
    if [ ! -f /etc/scraperack/node.env ] || \
       { [ "$role" = all ] && [ "$current_role" = worker ]; }; then
        cat > /etc/scraperack/node.env <<EOF
SCRAPERACK_CONTROLLER=http://127.0.0.1:7070
SCRAPERACK_NODE_CREDENTIAL=
SCRAPERACK_NODE_NAME=${SCRAPERACK_NODE_NAME:-$(hostname)}
SCRAPERACK_NODE_CPU_MILLIS=$cpu_millis
SCRAPERACK_NODE_MEMORY_MIB=$memory_mib
SCRAPERACK_NODE_VM_START_CONCURRENCY=$vm_start_concurrency
EOF
    fi
    [ ! -s /etc/scraperack/node.env ] || [ -z "$(tail -c 1 /etc/scraperack/node.env)" ] || \
        printf '\n' >> /etc/scraperack/node.env
    grep -q '^SCRAPERACK_NODE_CREDENTIAL=' /etc/scraperack/node.env || \
        printf 'SCRAPERACK_NODE_CREDENTIAL=\n' >> /etc/scraperack/node.env
    grep -q '^SCRAPERACK_NODE_VM_START_CONCURRENCY=' /etc/scraperack/node.env || \
        printf 'SCRAPERACK_NODE_VM_START_CONCURRENCY=%s\n' "$vm_start_concurrency" >> /etc/scraperack/node.env
fi
chmod 0600 /etc/scraperack/*.env
printf '%s' "$role" > /etc/scraperack/role

for service in controller node gui dns; do
    sed "s/^User=.*/User=$user/" \
        "$bundle/platform/deploy/scraperack-$service.service" \
        > "/etc/systemd/system/scraperack-$service.service"
done

systemctl daemon-reload
case "$role" in
    all)
        systemctl enable --now scraperack-controller scraperack-dns scraperack-node scraperack-gui
        attempt=0
        until [ "$(curl -fsS http://127.0.0.1:7070/v1/nodes 2>/dev/null)" != "[]" ] && \
              curl -fsS http://127.0.0.1:8080/ >/dev/null; do
            attempt=$((attempt + 1))
            [ "$attempt" -lt 30 ] || {
                systemctl --no-pager --full status scraperack-controller scraperack-dns \
                    scraperack-node scraperack-gui || true
                exit 1
            }
            sleep 1
        done
        ;;
    control-plane)
        systemctl disable --now scraperack-node scraperack-dns
        systemctl enable --now scraperack-controller scraperack-gui
        attempt=0
        until curl -fsS http://127.0.0.1:7070/health >/dev/null && \
              curl -fsS http://127.0.0.1:8080/ >/dev/null; do
            attempt=$((attempt + 1))
            [ "$attempt" -lt 30 ] || {
                systemctl --no-pager --full status scraperack-controller scraperack-gui || true
                exit 1
            }
            sleep 1
        done
        ;;
    worker)
        systemctl disable --now scraperack-controller scraperack-gui
        credential=$(sed -n 's/^SCRAPERACK_NODE_CREDENTIAL=//p' /etc/scraperack/node.env)
        if [ -n "$credential" ]; then
            systemctl enable --now scraperack-dns scraperack-node
        else
            systemctl disable --now scraperack-dns scraperack-node
        fi
        ;;
esac

address=$(hostname -I | awk '{ print $1 }')
case "$role" in
    all)
        echo "Scraperack is running"
        echo "Controller: http://$address:7070"
        echo "GUI:        http://$address:8080"
        ;;
    control-plane)
        echo "Scraperack control plane is running"
        echo "Controller: http://$address:7070"
        echo "GUI:        http://$address:8080"
        ;;
    worker)
        if [ -n "$credential" ]; then
            echo "Scraperack worker is running"
        else
            echo "Scraperack worker is installed but not joined"
            echo "Generate and run a join command from Settings -> Join nodes on the control plane"
        fi
        ;;
esac
