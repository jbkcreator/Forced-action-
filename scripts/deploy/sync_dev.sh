#!/bin/bash
# Move the server checkout's dev branch to the commit being deployed. Called by deploy.sh (step 1).
#
# Usage: sync_dev.sh <repo_dir> [<commit_sha>]
#   With a SHA (CI deploys): dev moves to exactly that commit, the one the Tests job passed, never to a newer
#   commit merged while those tests ran. Fast-forward only. Refuses a SHA that is not a full SHA or not on
#   origin/dev. When dev already contains the SHA (a newer, separately tested commit is live), dev stays put.
#   Without a SHA (manual run): git pull origin dev, as before.
set -euo pipefail

repo="$1"
target="${2:-}"
cd "$repo"
git checkout -q dev

if [ -z "$target" ]; then
    git pull -q origin dev
    exit 0
fi

if ! [[ "$target" =~ ^[0-9a-f]{40}$ ]]; then
    echo "sync_dev: not a full commit SHA: $target" >&2
    exit 2
fi
git fetch -q origin dev
if ! git merge-base --is-ancestor "$target" origin/dev; then
    echo "sync_dev: $target is not on origin/dev" >&2
    exit 3
fi
if git merge-base --is-ancestor "$target" HEAD; then
    echo "sync_dev: dev already contains $target; not moved"
    exit 0
fi
git merge -q --ff-only "$target"
echo "sync_dev: dev at $target"
