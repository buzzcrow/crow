#!/usr/bin/env bash
# Measure each pixi test suite's wall-clock time and test count.
# Output: TSV lines "suite<TAB>tests<TAB>seconds" to stdout.
set -u

TIMEFORMAT='%R'

run_one() {
  local suite="$1"
  local t
  t=$( { time pixi run "$suite" > "/tmp/measure-${suite}.out" 2>&1; } 2>&1 )
  local rc=$?
  local count=""
  if [[ "$suite" == *-ct ]]; then
    count=$(grep -oE '[0-9]+ tests' "/tmp/measure-${suite}.out" | head -1 | grep -oE '^[0-9]+')
  else
    count=$(grep -oE 'test result: ok\. [0-9]+ passed' "/tmp/measure-${suite}.out" \
            | grep -oE '[0-9]+ passed' | grep -oE '^[0-9]+' | awk '{s+=$1} END{print s+0}')
  fi
  printf '%s\t%s\t%s\n' "$suite" "$count" "$t"
  if [[ $rc -ne 0 ]]; then
    printf '# WARN: %s exited rc=%s (see /tmp/measure-%s.out)\n' "$suite" "$rc" "$suite" >&2
  fi
}

echo -e "suite\ttests\tseconds"

# CppTests
pixi run clean-env > /dev/null 2>&1
run_one test-tree-ct
run_one test-tree-ffi
run_one test-rpc-ct
run_one test-rpc-ffi
run_one test-diskio-ct

# UnitTests
run_one test-common
run_one test-protocol
run_one test-kv-core
run_one test-kv-client
run_one test-chunkdb-client

# ServerTests
pixi run clean-env > /dev/null 2>&1
run_one test-kv-server
run_one test-diskdb
run_one test-diskdb-client
run_one test-chunkdb
run_one test-chunk-client
run_one test-diskio-client

# ConsoleTests
pixi run clean-env > /dev/null 2>&1
run_one test-console-shared
run_one test-console-cli
run_one test-console-server

# UITests
pixi run clean-env > /dev/null 2>&1
run_one test-console-ui

echo "# done"
