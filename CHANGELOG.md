# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
