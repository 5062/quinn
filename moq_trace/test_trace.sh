#!/usr/bin/env bash
# Capture this fork's QUIC fixtures under LTTng and check them with the
# toolkit's `moq-trace verify`, as the mvfst and Google QUICHE forks do.
#
#   moq_trace/test_trace.sh [cargo flags...]
#
# Needs LTTng and the moq-trace command on PATH, which the moq-trace checkout's
# development shell provides. The fixture, quinn/tests/quic_trace.rs, runs
# transfers whose writes block on stream flow control, connection flow control,
# and the send buffer.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
if ! command -v moq-trace >/dev/null 2>&1; then
  echo "moq-trace must be on PATH; run from the moq-trace development shell" >&2
  exit 1
fi
cd "$here/.."
# The hooks always compile; the lttng feature binds them to the provider.
binary=$(cargo test -p quinn --features quic-trace/lttng --test quic_trace --no-run "$@" 2>&1 |
  sed -n 's/.*Executable tests\/quic_trace\.rs (\(.*\))$/\1/p')
if [ -z "$binary" ]; then
  echo "cargo built no quic_trace fixture" >&2
  exit 1
fi

mkdir -p "$here/artifacts"
output=$(mktemp -d "$here/artifacts/test-trace.XXXXXX")
echo "Trace evidence: $output"
moq-trace verify "$output" \
  --run "$binary" \
  --send-blocked stream_flow_control/stream \
  --send-blocked connection_flow_control/stream \
  --send-blocked send_buffer/stream
