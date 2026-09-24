# nhttp

[![Hex.pm](https://img.shields.io/hexpm/v/nhttp.svg)](https://hex.pm/packages/nhttp)
[![CI](https://github.com/nomasystems/nhttp/actions/workflows/ci.yml/badge.svg)](https://github.com/nomasystems/nhttp/actions/workflows/ci.yml)

HTTP/1.1, HTTP/2, and HTTP/3 server for Erlang/OTP 27+.

## Getting started

```erlang
%% rebar.config
{deps, [nhttp]}.
```

### Minimal server

```erlang
-module(my_handler).
-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

init(_Args) ->
    {ok, #{}}.

handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello, World!">>), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.
```

```erlang
{ok, Pid} = nhttp:start_link(my_server, #{
    port => 8080,
    handler => my_handler
}).
```

See the [`examples/`](https://github.com/nomasystems/nhttp/tree/main/examples)
directory for runnable handler skeletons covering plain HTTP, streaming
responses, WebSocket, SSE, CORS, and chunked uploads.

### TLS with HTTP/1.1 + HTTP/2 (ALPN)

```erlang
{ok, Pid} = nhttp:start_link(my_server, #{
    port => 8443,
    versions => [http1_1, http2],
    handler => my_handler,
    tls => #{certfile => "server.pem", keyfile => "server.key"}
}).
```

### HTTP/3 over QUIC

HTTP/3 runs over QUIC on a UDP socket. HTTP/1.1 and HTTP/2 run over TCP on their own socket. List all three versions and a single listener opens both sockets for you, and sets Alt-Svc on the TCP side so clients know where to find HTTP/3.

```erlang
{ok, Pid} = nhttp:start_link(my_server, #{
    port => 8443,
    versions => [http1_1, http2, http3],
    handler => my_handler,
    tls => #{certfile => "server.pem", keyfile => "server.key"}
}).

%% nhttp:get_ports(Pid) => #{tcp => 8443, quic => 8443}
```

## Features

- **HTTP/1.1** ([RFC 9112](https://www.rfc-editor.org/rfc/rfc9112)) and **HTTP/2** ([RFC 9113](https://www.rfc-editor.org/rfc/rfc9113)) with automatic protocol selection via TLS ALPN
- **HTTP/3** over QUIC ([RFC 9114](https://www.rfc-editor.org/rfc/rfc9114)), in-process `nquic` library mode
- **WebSocket** over HTTP/1.1 ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)), HTTP/2 ([RFC 8441](https://www.rfc-editor.org/rfc/rfc8441)), and HTTP/3 ([RFC 9220](https://www.rfc-editor.org/rfc/rfc9220)) with one callback set
- **Server-Sent Events** helpers ([W3C SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html))
- **CORS** preflight and response header helpers ([Fetch](https://fetch.spec.whatwg.org/))
- **PROXY protocol v1/v2** for HAProxy and AWS NLB deployments
- **SNI** with static lookup table and/or dynamic callback
- **Streaming responses** via producer funs with end-to-end backpressure
- **Streaming request bodies** with handler-driven backpressure
- **Response compression** (gzip / deflate) with MIME-aware defaults
- **OpenTelemetry** spans and metrics (opt-in)
- **Graceful shutdown** with connection draining
- **One process per connection**, with HTTP/2 and HTTP/3 streams multiplexed inside it

### HTTP/2 test affordances

These listener options exist so that a client can be tested against a
slow or stingy HTTP/2 peer. Each one defaults to the current behavior and
changes nothing on HTTP/1.1 or HTTP/3.

| Option | Default | Effect |
|--------|---------|--------|
| `h2_initial_window_size` | 65535 | Alias of `initial_window_size` in `h2_settings`: the receive window that each new stream grants the peer. |
| `h2_max_frame_size` | 16384 | Alias of `max_frame_size` in `h2_settings`: the largest frame payload the server accepts. |
| `h2_response_delay` | 0 | Milliseconds to hold the response headers of a `{reply, _, _}` result. `{uniform, MinMs, MaxMs}` draws a value per response. |
| `h2_connection_window_policy` | `eager` | Credit policy for the connection receive window. |
| `h2_stream_window_policy` | `eager` | Credit policy for each stream receive window. |
| `h2_credit_batch` | 0 | Octets per WINDOW_UPDATE under the `on_response` connection policy. 0 sends the whole accumulator with each response. Valid only when a policy is `on_response`. |

`h2_settings` also accepts `max_send_buffer` and `max_queued_streams`, the
bounds of the codec send queue that holds response bodies while the peer
withholds credit. The server defaults both to `infinity`.

An alias must equal the `h2_settings` key when both are present. The
delay applies to `{reply, _, _}` results only. Error responses, producer
streams and WebSocket upgrades go out at once.

A credit policy has five shapes. `eager` sends a WINDOW_UPDATE for each
body chunk as soon as the handler consumed it. `{threshold, N}` holds the
credit until the uncredited octets reach `N`, then sends the accumulated
total. `{delay, Ms}` sends the credit for each chunk `Ms` milliseconds
after the handler consumed it. `on_response` holds the credit of a request
until the HEADERS of its `{reply, _, _}` result go out. The credit goes in
the same socket write, ahead of the HEADERS. `never` sends no credit.

The two policies are independent. No policy credits more than the handler
consumed. Error responses, `{abort, _, _}` results and stream resets
release no `on_response` credit. A stream that closes before its delayed
credit is due gets no stream WINDOW_UPDATE. The connection credit for
those octets still goes out.

Under the `on_response` connection policy the responded octets move into
one accumulator. Each time the accumulator reaches `h2_credit_batch`, one
WINDOW_UPDATE of exactly that size goes out with the response, and the
remainder carries. This example imitates APNs, which credits half of its
window at a time, sends no stream credit, and answers 129 to 388 ms after
the request:

```erlang
#{
    h2_connection_window_policy => on_response,
    h2_credit_batch => 32830,
    h2_stream_window_policy => never,
    h2_response_delay => {uniform, 129, 388}
}
```

## Documentation

[nhttp on HexDocs](https://hexdocs.pm/nhttp)

## License

Apache License 2.0
