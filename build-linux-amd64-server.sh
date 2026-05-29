#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker volume create heartwith-cargo-registry >/dev/null
docker volume create heartwith-cargo-git >/dev/null

docker run --rm \
  --platform linux/amd64 \
  -v "$PWD":/work \
  -v heartwith-cargo-registry:/usr/local/cargo/registry \
  -v heartwith-cargo-git:/usr/local/cargo/git \
  -w /work \
  rust:bookworm \
  bash -lc 'export PATH=/usr/local/cargo/bin:$PATH; cargo build -p heartwith-server --release --target-dir target-linux-amd64 && strip target-linux-amd64/release/heartwith-server'

file target-linux-amd64/release/heartwith-server
