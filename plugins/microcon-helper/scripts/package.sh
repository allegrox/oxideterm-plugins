#!/usr/bin/env sh

# SPDX-License-Identifier: MIT

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
plugin_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
manifest_path="$plugin_root/plugin.json"
dist_dir="$plugin_root/dist"
binary_name=oxideterm-microcon-helper
binary_path="$plugin_root/bin/$binary_name"
# Publish one archive per platform target so the marketplace record can map
# each ZIP to its tested Rust target triple.
package_target=${PACKAGE_TARGET:-x86_64-unknown-linux-gnu}

if [ ! -x "$binary_path" ]; then
  echo "Missing compiled binary $binary_path; run: cargo build --release --locked" >&2
  exit 1
fi

# Validate identity components before using them in the artifact path.
manifest_id=$(sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest_path" | head -n 1)
manifest_version=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest_path" | head -n 1)
case "$manifest_id" in
  '' | *[!a-z0-9.-]*)
    echo "Invalid plugin id: $manifest_id" >&2
    exit 1
    ;;
esac
case "$manifest_version" in
  '' | *[!0-9A-Za-z.+]*)
    echo "Invalid plugin version: $manifest_version" >&2
    exit 1
    ;;
esac

artifact_path="$dist_dir/$manifest_id-$manifest_version-$package_target.zip"

mkdir -p "$dist_dir"
rm -f "$artifact_path"

(
  cd "$plugin_root"
  zip -X -q -r "$artifact_path" \
    plugin.json \
    bin/plugin \
    bin/plugin.cmd \
    "bin/$binary_name" \
    locales
)

if command -v shasum >/dev/null 2>&1; then
  package_digest=$(shasum -a 256 "$artifact_path" | awk '{print $1}')
else
  package_digest=$(sha256sum "$artifact_path" | awk '{print $1}')
fi
package_size=$(wc -c < "$artifact_path" | tr -d ' ')

printf 'Artifact: %s\nSHA-256: %s\nSize: %s bytes\n' \
  "$artifact_path" "$package_digest" "$package_size"
