#!/usr/bin/env bash
# Start (or stop) the 3-node Redis deployment used by the chaos harness,
# for manual inspection. The harness manages its own instances; this
# script is only a convenience for poking at a live cluster.
#
#   scripts/run-cluster.sh start [redis-bin] [data-dir]
#   scripts/run-cluster.sh stop  [data-dir]
set -euo pipefail

REDIS_BIN="${2:-$HOME/workspace/tools/redis/bin/redis-server}"
CLI="${REDIS_BIN%redis-server}redis-cli"
DATA="${3:-./chaos-run}"

start_node() { # port replicaof
  local port="$1" rep="${2:-}"
  local dir="$DATA/n$port"
  mkdir -p "$dir"
  local args=(--port "$port" --bind 127.0.0.1 --dir "$PWD/$dir"
              --daemonize yes --save '' --appendonly yes
              --protected-mode no --logfile "$PWD/$dir/redis.log"
              --pidfile "$PWD/$dir/redis.pid")
  if [ -n "$rep" ]; then args+=(--replicaof 127.0.0.1 "$rep"); fi
  "$REDIS_BIN" "${args[@]}"
}

case "${1:-start}" in
  start)
    start_node 7301
    start_node 7302 7301
    start_node 7303 7301
    echo "cluster up: primary 7301, replicas 7302 7303 (data in $DATA)"
    ;;
  stop)
    for p in 7301 7302 7303; do
      "$CLI" -p "$p" shutdown nosave 2>/dev/null || true
    done
    echo "cluster stopped"
    ;;
  *) echo "usage: $0 {start|stop} [redis-bin] [data-dir]" >&2; exit 2 ;;
esac
