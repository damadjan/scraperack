#!/bin/sh
set -eu

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
arch=$(uname -m)
release_url=https://github.com/firecracker-microvm/firecracker/releases
version=$(basename "$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$release_url/latest")")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

apt-get update
apt-get install -y ca-certificates curl e2fsprogs
curl -fsSL "$release_url/download/$version/firecracker-$version-$arch.tgz" -o "$work/firecracker.tgz"
tar -xzf "$work/firecracker.tgz" -C "$work"
install -m 0755 "$work/release-$version-$arch/firecracker-$version-$arch" /usr/local/bin/firecracker

store=/var/lib/scraperack-node
owner=${SCRAPERACK_USER:-${SUDO_USER:-root}}
group=${SCRAPERACK_GROUP:-$(id -gn "$owner")}
install -d -o "$owner" -g "$group" "$store"
s3=https://s3.amazonaws.com/spec.ccfc.min
prefix=$(curl -fsSL "$s3?list-type=2&prefix=firecracker-ci/&delimiter=/" | grep -oP '(?<=<Prefix>)firecracker-ci/[0-9]{8}-[^/]+/(?=</Prefix>)' | sort | tail -1)
kernel=$(curl -fsSL "$s3?list-type=2&prefix=$prefix$arch/vmlinux-" | grep -oP "(?<=<Key>)($prefix$arch/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3})(?=</Key>)" | sort -V | tail -1)
curl -fsSL "$s3/$kernel" -o "$store/vmlinux"
chown "$owner:$group" "$store/vmlinux"
chmod 0644 "$store/vmlinux"
/usr/local/bin/firecracker --version
