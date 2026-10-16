#!/usr/bin/env bash
#
# Runs one compliance suite against a server started for it.
#
# Starts <server-module>:start() in a rebar3 test shell, waits for the
# PORT:<n> line it prints, runs <command> with PORT in its environment,
# stops the server and its process group, and exits with the status of
# <command>. That status is the receipt: a suite that fails here fails
# the make target and the CI job.
#
# Usage: run.sh <server-module> <command>
#
# <command> is one string, run with `bash -c`, so it reads $PORT at run
# time. Example:
#   run.sh nhttp_compliance 'docker run --rm --network=host h2spec -p $PORT'

set -uo pipefail

module=${1:?usage: run.sh <server-module> <command>}
command=${2:?usage: run.sh <server-module> <command>}
rebar3=${REBAR3:-rebar3}
start_timeout_ticks=300

log=$(mktemp)
server_pid=""

cleanup() {
    if [ -n "$server_pid" ]; then
        pkill -TERM -P "$server_pid" 2>/dev/null
        kill -TERM "$server_pid" 2>/dev/null
    fi
    pkill -TERM -f "${module}:start\(\)" 2>/dev/null
    rm -f "$log"
}
trap cleanup EXIT

( tail -f /dev/null | "$rebar3" as test shell --eval "${module}:start()." ) > "$log" 2>&1 &
server_pid=$!

port=""
for _ in $(seq 1 "$start_timeout_ticks"); do
    port=$(grep -oE 'PORT:[0-9]+' "$log" | head -1 | cut -d: -f2)
    if [ -n "$port" ]; then
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

if [ -z "$port" ]; then
    echo "compliance: ${module} did not report a port. Server output:" >&2
    cat "$log" >&2
    exit 1
fi

echo "compliance: ${module} listening on port ${port}"
PORT=$port bash -c "$command"
