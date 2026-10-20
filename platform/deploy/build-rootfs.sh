#!/bin/sh
set -eu

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
[ "$#" = 1 ] || { echo "usage: $0 OUTPUT.ext4" >&2; exit 1; }
output=$(realpath -m "$1")
[ ! -e "$output" ] || { echo "$output already exists" >&2; exit 1; }
script=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
python_version=3.10.11

apt-get update
apt-get install -y debootstrap e2fsprogs build-essential curl xz-utils \
    libbz2-dev libffi-dev libgdbm-dev liblzma-dev libncurses-dev \
    libreadline-dev libsqlite3-dev libssl-dev uuid-dev zlib1g-dev
debootstrap --variant=minbase --components=main,universe noble "$work/rootfs" http://archive.ubuntu.com/ubuntu
chroot "$work/rootfs" apt-get update
chroot "$work/rootfs" apt-get install -y --no-install-recommends ca-certificates \
    iproute2 \
    libbz2-1.0 libffi8 libgdbm6t64 liblzma5 libncursesw6 libreadline8t64 \
    libsqlite3-0 libssl3 libuuid1 util-linux zlib1g
curl -fsSLo "$work/python.tar.xz" "https://www.python.org/ftp/python/$python_version/Python-$python_version.tar.xz"
tar -C "$work" -xf "$work/python.tar.xz"
(
    cd "$work/Python-$python_version"
    ./configure --prefix=/usr/local --with-ensurepip=install
    make -j"$(nproc)"
    make DESTDIR="$work/rootfs" altinstall
)
ln -s /usr/local/bin/python3.10 "$work/rootfs/usr/bin/python3"
chroot "$work/rootfs" /usr/local/bin/python3.10 -m pip install --no-cache-dir cloudpickle==3.1.2
install -m 0755 "$script/../guest/scraperack-init" "$work/rootfs/sbin/scraperack-init"
install -Dm 0755 "$script/../guest/scraperack-python-executor" \
    "$work/rootfs/opt/scraperack/executors/python"
install -Dm 0644 "$script/../guest/scraperack_runtime.py" \
    "$work/rootfs/opt/scraperack/runtime/scraperack_runtime.py"
rm -rf "$work/rootfs/usr/local/lib/python3.10/test" "$work/rootfs/usr/local/lib/python3.10/__pycache__"
rm -rf "$work/rootfs/var/lib/apt/lists/"* "$work/rootfs/var/cache/apt/archives/"*.deb
mkdir -p "$(dirname "$output")"
truncate -s 1G "$output"
mkfs.ext4 -q -F -d "$work/rootfs" "$output"
e2fsck -fn "$output"
