#!/usr/bin/env bash
# Builds a variant directory (a copy of submissions/ruby-async-cicloid with edits) into an image.
#   bash local-bench/build-variant.sh <variant-dir> <tag>     -> image tdsc-cicloid:<tag>
# Layers: toolchain (install.sh) and gems (Gemfile*) are cached unless those files change.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$1"; TAG="$2"
docker build -q -f "$ROOT/local-bench/Dockerfile" -t "tdsc-cicloid:$TAG" "$DIR"
echo "built tdsc-cicloid:$TAG"
