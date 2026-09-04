#!/usr/bin/env bash
# Kill leftover crowdb server/cli processes. Called by `pixi run clean`.
# The parent process is bash -c <clean task body>, whose cmdline contains
# these binary names (in comments, rm paths, etc.), so a bare pkill -f
# would SIGTERM the parent and abort the clean task. We exclude $PPID
# (the parent) to avoid that. This script itself runs as
# "bash tools/kill-servers.sh" (no binary names), so it never self-matches.
set -e
for name in crowdb-kv-server crowdb-web crowdb-cli; do
  pgrep -f "$name" | grep -vx "$PPID" | xargs -r kill 2>/dev/null || true
done
