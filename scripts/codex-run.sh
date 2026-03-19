#!/usr/bin/env bash
set -e

echo "Run Codex in your existing tool, then come back here."
read -r -p "Press ENTER after Codex finishes applying changes..."

echo
echo "===== DIFF ====="
git diff --stat || true
git diff || true

echo
echo "===== STATUS ====="
git status
