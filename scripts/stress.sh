#!/usr/bin/env bash
# Run a set of raft tests many times in parallel and keep the logs of
# failed runs. Concurrency bugs often show up once in hundreds of runs.
#
#   scripts/stress.sh [-n runs] [-p parallel] [-r] '<test pattern>'
#
#   -n  number of runs (default 100)
#   -p  runs at a time (default: number of CPUs / 2)
#   -r  disable the race detector (faster, less thorough)
set -euo pipefail

runs=100
par=$(( $(nproc) / 2 > 0 ? $(nproc) / 2 : 1 ))
race=-race
while getopts "n:p:r" opt; do
  case $opt in
    n) runs=$OPTARG ;;
    p) par=$OPTARG ;;
    r) race= ;;
    *) sed -n '2,10p' "$0"; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
pattern=${1:-.}

root=$(cd "$(dirname "$0")/.." && pwd)
logs=$root/stress-logs
mkdir -p "$logs"
bin=$(mktemp -d)/raft.test
trap 'rm -rf "$(dirname "$bin")"' EXIT

# Build once so parallel runs don't each compile.
(cd "$root" && go test $race -c -o "$bin" ./raft/)

run() {
  local i=$1 log="$logs/run-$1.log"
  if "$bin" -test.run "$pattern" -test.count=1 -test.timeout=15m >"$log" 2>&1; then
    rm -f "$log"
    echo pass
  else
    echo "fail  run $i  -> $log" >&2
    echo fail
  fi
}
export -f run
export bin pattern logs

echo "running '$pattern' $runs times, $par at a time${race:+, with -race}"
results=$(seq 1 "$runs" | xargs -P "$par" -I{} bash -c 'run {}')
failed=$(grep -c fail <<<"$results" || true)
echo "$((runs - failed))/$runs passed"
[ "$failed" -eq 0 ]
