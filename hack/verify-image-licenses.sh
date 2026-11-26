#!/usr/bin/env bash
set -euo pipefail

image=${1:?image required}
kind=${2:?manager or toolchain required}
work=$(mktemp -d)
container=$(docker create "$image")
trap 'docker rm "$container" >/dev/null; rm -rf "$work"' EXIT
test "$(docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.licenses" }}' "$image")" = AGPL-3.0-only
docker cp "$container:/usr/share/doc/courier/." "$work"
cmp LICENSE "$work/LICENSE"
cmp NOTICE "$work/NOTICE"
cmp LICENSES/Apache-2.0.txt "$work/LICENSES/Apache-2.0.txt"
if [[ "$kind" == toolchain ]]; then
  git_dir="$work/third-party/git"
  echo "f61ffa245b9a353a8d32b1c6c22d92ccf9a888419edb850260ceb4ce1b2b3cae  $git_dir/git-v2.47.3.tar.gz" | sha256sum -c -
  tar -xOzf "$git_dir/git-v2.47.3.tar.gz" git-2.47.3/COPYING | cmp - "$git_dir/COPYING"
fi
