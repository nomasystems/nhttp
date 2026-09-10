# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-09-10

### Changed

- Requires `nhttp_lib` 1.1.1

## [1.1.0] - 2026-09-10

### Changed

- Requires `nhttp_lib` 1.1.0
- Socket re-arming for the next read is centralized. A `setopts` error on a peer that closed ends the HTTP/1.1, HTTP/2, and WebSocket loops as a normal close
- The `nhttp.http.request.duration` metric records native time units. The consumer converts them

### Fixed

- `t:nhttp:start_error/0` includes `{already_started, pid()}`

## [1.0.4] - 2026-08-24

### Fixed

- HTTP/1.1 connections no longer crash when the peer closes the socket. A `setopts` call that gives an error now closes the connection normally. A body read that gives an error tells the handler that the peer closed the connection

## [1.0.3] - 2026-08-20

### Changed

- Requires `nhttp_lib` 1.0.4 for correct HTTP/1.1 `Content-Length` handling. An empty-body response now emits `Content-Length: 0`, a 1xx, 204, or 304 response emits no `Content-Length` field, and a 2xx response to a `CONNECT` request can suppress it (RFC 9110 Section 8.6)

## [1.0.2] - 2026-08-13

### Added

- Support for Erlang/OTP 29.

## [1.0.1] - 2026-08-12

### Fixed

- WebSocket fragmented messages on HTTP/1.1. The connection keeps decoder state across reads, so a message split over several frames is reassembled and delivered once (RFC 6455 section 5.4)
- WebSocket CLOSE codes for decode errors. A text payload or CLOSE reason that is not valid UTF-8 gives 1007, a message over `max_message_size` gives 1009, and other framing errors keep 1002. Before, every decode error gave 1002

### Changed

- Requires `nhttp_lib` 1.0.3 for the stateful frame decoder and incremental UTF-8 validation

### Added

- WebSocket RFC 6455 compliance tests with Autobahn. Run them with `make ws-compliance`

## [1.0.0] - 2026-06-01

Initial public release.

### Added

- HTTP/1.1 and HTTP/2 server with automatic protocol selection via TLS ALPN (RFC 9112, RFC 9113)
- HTTP/3 server over QUIC via in-process `nquic` library mode (RFC 9114)
- Handler behaviour (`nhttp_handler`) with a small callback set for request handling, streaming, and WebSocket sessions
- WebSocket support across HTTP/1.1, HTTP/2, and HTTP/3 with one callback set (RFC 6455, RFC 8441, RFC 9220)
- Streaming responses via producer funs with end-to-end backpressure
- Streaming request bodies with handler-driven backpressure
- Server-Sent Events helpers (`nhttp_sse`)
- CORS preflight and response header helpers (`nhttp_cors`)
- Response compression (gzip / deflate) with MIME-aware defaults
- PROXY protocol v1/v2 for HAProxy / AWS NLB deployments
- SNI with static lookup table and dynamic callback
- OpenTelemetry spans and metrics (opt-in)
- Graceful shutdown with connection draining
- One process per connection, with HTTP/2 and HTTP/3 streams multiplexed inside it
- Per-stream handler workers on HTTP/2 to isolate slow handlers
