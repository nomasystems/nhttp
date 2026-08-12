#!/usr/bin/env bash
#
# Reads the Autobahn master report and decides whether the run passed.
#
# This is what `make ws-compliance` gains over `make h1-compliance`, which
# prints its result and leaves reading it to whoever is watching. The suite
# writes reports/index.json with one entry per case, and a run nobody reads is
# not a receipt, so the exit status here is the receipt instead.
#
# Two fields decide a case. `behavior` is the case itself and `behaviorClose`
# is the closing handshake that ended it, and a wrong close code is a protocol
# error whatever the case was testing, so both gate.
#
#   OK, INFORMATIONAL      pass.
#   UNIMPLEMENTED          pass. Sections 12 and 13 test permessage-deflate,
#                          which this server does not negotiate: a reserved
#                          bit in a frame is a protocol error here rather
#                          than a hint.
#   NON-STRICT             pass, and named. Non-strict is the lenient reading
#                          of a case that has a strict one, so each one has to
#                          be justified or fixed.
#   everything else        fail, named, with the case description.
#
# Usage: gate.sh <reports-dir>

set -euo pipefail

reports=${1:?usage: gate.sh <reports-dir>}
index="$reports/index.json"

command -v jq >/dev/null 2>&1 || {
    echo "Error: jq not found in PATH. The Autobahn gate reads index.json with it." >&2
    exit 1
}

[ -f "$index" ] || {
    echo "Error: $index is missing. The suite did not finish." >&2
    exit 1
}

# One line per case: id, behavior, behaviorClose. The agent is the outer key
# and there is exactly one, but iterating over them costs nothing and keeps
# this correct if a second target is ever added to the spec.
cases=$(jq -r '.[] | to_entries[]
               | [.key, .value.behavior, .value.behaviorClose] | @tsv' "$index")

[ -n "$cases" ] || {
    echo "Error: $index records no case. The suite reached no server." >&2
    exit 1
}

total=$(echo "$cases" | grep -c '')
count() { echo "$cases" | awk -F'\t' -v b="$1" '$2 == b' | grep -c '' || true; }

echo ""
echo "Autobahn: $total cases"
for behavior in OK NON-STRICT INFORMATIONAL UNIMPLEMENTED FAILED; do
    printf '  %-16s %s\n' "$behavior" "$(count "$behavior")"
done

# The description lives in the per-case report rather than the index, and is
# what makes a failure readable without opening a browser. It is written for
# the HTML report, so the markup in it comes out here. A case whose payload is
# a lone surrogate leaves a report jq refuses to parse, and a missing line of
# prose is not a reason to fail the run, so that read is allowed to come back
# empty.
describe() {
    local file
    file=$(jq -r --arg c "$1" '.[][$c].reportfile' "$index")
    if [ -f "$reports/$file" ]; then
        jq -r '.description' "$reports/$file" 2>/dev/null \
            | tr '\n' ' ' | sed 's/<[^>]*>/ /g; s/  */ /g' | cut -c1-90
    fi
}

nonstrict=$(echo "$cases" | awk -F'\t' '$2 == "NON-STRICT" { print $1 }')
if [ -n "$nonstrict" ]; then
    echo ""
    echo "NON-STRICT, each of which must be justified or fixed:"
    for c in $nonstrict; do printf '  %-10s %s\n' "$c" "$(describe "$c")"; done
fi

bad=$(echo "$cases" | awk -F'\t' '
    $2 != "OK" && $2 != "NON-STRICT" && $2 != "INFORMATIONAL" \
        && $2 != "UNIMPLEMENTED" { print $1 "\t" $2; next }
    $3 != "OK" && $3 != "INFORMATIONAL" { print $1 "\tclose " $3 }')

if [ -n "$bad" ]; then
    echo ""
    echo "Failed:"
    while IFS=$'\t' read -r c verdict; do
        printf '  %-10s %-14s %s\n' "$c" "$verdict" "$(describe "$c")"
    done <<< "$bad"
    echo ""
    echo "Full report: $reports/index.html"
    exit 1
fi

echo ""
echo "Autobahn: no failed case. Full report: $reports/index.html"
