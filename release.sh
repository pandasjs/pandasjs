#!/bin/bash
set -e

SRC="/Users/rock/git/rock2/pandas"
REPO="/Users/rock/git/pandasjs"
PAGES="/Users/rock/git/pandasjs.github.io"

# build dist + docs
echo "=== building ==="
cd "$SRC"
node build.js

# sync code repo
echo "=== syncing code to pandasjs ==="
rsync -av --delete \
    --exclude node_modules \
    --exclude .git \
	--exclude .venv \
    --exclude uv.lock \
    --exclude pyproject.toml \
    --exclude architecture.md \
    --exclude test \
    --exclude docs/vite.config.js \
    --exclude docs/manual-ssr.js \
    --exclude server.js \
    "$SRC/" "$REPO/"

cd "$REPO"
git add -A
if git diff --cached --quiet; then
    echo "no changes in pandasjs, skipping commit"
else
    git commit -m "release $(node -p "require('./package.json').version")"
    git push origin main
fi

# sync landing page
echo "=== syncing landing page ==="
rsync -av --delete \
    --exclude .git \
    "$SRC/docs/" "$PAGES/"

echo "=== syncing landing page ==="
rsync -av --delete \
    --exclude .git \
    "$SRC/dist/" "$PAGES/dist"

cd "$PAGES"
git add -A
if git diff --cached --quiet; then
    echo "no changes in pages, skipping commit"
else
    git commit -m "update docs"
    git push origin main
fi

# publish to npm
echo "=== publishing to npm ==="
cd "$REPO"
# npm publish --access public

echo "=== done ==="
