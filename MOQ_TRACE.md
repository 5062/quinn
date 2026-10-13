# quic_trace instrumentation

This branch adds the `quic_trace:*` transport hooks of the
[moq-trace](https://github.com/5062/moq-trace) toolkit to Quinn, so that the
moq-dev-moq and moq-rs relays' MoQ objects can be joined to the QUIC packets,
STREAM frames, and UDP system calls that carried them. It is a measurement fork,
not intended for upstream submission.

## Revision

The branch `moq-trace/quinn-0.11` starts at `a96949f6`, the `quinn-proto-0.11.16`
release the relays build against.

## What the instrumentation emits

| Call site | Emission |
| --- | --- |
| `quinn-udp` `send` and `recv` on Unix | `udp_socket` around each `sendmsg`, `sendmsg_x`, `recvmmsg`, `recvmsg_x`, or `recvmsg`, with buffers, GSO and GRO datagrams, and bytes; TX names the connection whose transmit it sends, and a `MSG_TRUNC` read is the call's success |
| `Endpoint::handle_at` | the read's completion and the parse of the first packet's header, carried with the datagram |
| `quinn` endpoint driver | the instant a datagram is handed to its connection's task |
| `Connection::handle_event`, `handle_coalesced`, `handle_decode`, `handle_packet` | RX packet from the read's completion: `read_queue`, `header_parse`, `routing`, `scheduling`, `header_unprotect`, `payload_decrypt`, one `frame_process` per frame other than padding, and its outcome |
| `Connection::handle_first_packet` | the stages of the packet that created a server connection, measured in the endpoint and the application's accept before the connection existed |
| `process_payload` STREAM handling | RX `quic_stream_frame`, `success` when the stream buffered the bytes, `dropped` when the stream was closed or stopped, and `malformed` when processing it failed |
| `PacketBuilder` | TX packet from the start of its encoding: `frame_encode`, its STREAM frames with their retransmission flag, `packet_encrypt`, then `send_queue` |
| `quinn` connection driver, `AsyncUdpSocket::try_send_timed` | TX packet end at the completion of the send that carried its transmit, or `dropped` when that send failed |
| `ConnectionRef::new`, migration and its revert | `quic_connection_path` |
| `Connection::poll_transmit` | `quic_send_blocked` for the first of amplification, the congestion window, and pacing that keeps data from being sent |
| stream writes, `MAX_DATA`, `MAX_STREAM_DATA`, acknowledgments | per stream `quic_send_blocked` while the stream's own flow control, the connection's, or the send window blocks a write |

Each connection allocates a process-wide connection ID from the toolkit when it
is created and exposes it as `Connection::quic_trace_connection_id`, which the
web-transport fork reports to the relays for their MoQ objects. RX socket events
carry no connection ID, because one endpoint socket serves every connection.

## Boundaries

- **Endpoint and connection tasks.** The endpoint parses a datagram's first
  header and hands it to the connection's task, so an RX packet records that
  hand-off as `routing` and the wait for the task as `scheduling`. mvfst and
  Google QUICHE process a packet on the thread that read it and record neither.
- **Application.** A packet ends before the application reads its data, so Quinn
  never records the `application` phase that Google QUICHE does.
- **Sends.** A transmit's packets wait in `send_queue` until the system call that
  sends it returns. A send that would block is retried with the same packets,
  which keep waiting; any other failure discards the transmit as Quinn did
  before, and its packets end `dropped`.
- **Platforms.** The hooks time system calls in the Unix `quinn-udp` backend.
  The fallback and Windows backends make no socket events, and their packets
  start and end at instants read just after the call.
- **Packet outcomes.** These match mvfst's and Google QUICHE's. A packet that
  parses but leaves processing through an error ends `malformed`, a duplicate,
  one that arrives before its keys, or one from an unexpected peer ends
  `dropped`, and one whose processing completes, including one that closes the
  connection, ends `success`. A frame that ends processing with an error leaves
  its `frame_process` phase abandoned.
- **Send-blocked intervals.** The transmit reasons are evaluated at each
  `poll_transmit`, and only the first limit a transmit attempt hits is recorded.
  A blocked write records its interval on its stream, with the stream ID, as
  mvfst and Google QUICHE do: from the refused write until the limit relaxes or
  the stream goes away.

## Build

The hooks always compile and record nothing until the `quic-trace` crate's
`lttng` feature binds them to the provider, which the relays enable through their
own trace features. The toolkit is not on crates.io, so the workspace manifest
patches `quic-trace` to the moq-trace repository for a standalone build, and a
consumer applies its own patch.

## Verification

```sh
cargo test -p quinn-proto -p quinn -p quinn-udp
moq_trace/test_trace.sh
```

`moq_trace/test_trace.sh` runs `quinn/tests/quic_trace.rs` under LTTng and checks
the events with the toolkit's `moq-trace verify`, as the mvfst and Google QUICHE
forks check theirs. The fixture's transfers block writes on stream flow control,
connection flow control, and the send window, and the check requires a
per-stream interval for each, besides balanced lifecycles and phases, queue
phases on their socket boundaries, and no abandoned packet. It needs LTTng and
the `moq-trace` command, which the moq-trace development shell provides; from
the moq-workspace, `just test-trace` runs it with the other stacks' checks.
