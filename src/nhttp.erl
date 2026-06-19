-module(nhttp).

-moduledoc """
Server public API for nhttp.

This module provides convenience functions for starting HTTP servers.
For production use, embed `nhttp_listener` in your supervision tree.

## Quick Start

```erlang
{ok, Pid} = nhttp:start_link(#{
    port => 8080,
    handler => my_handler
}),

nhttp:stop(Pid).
```

## Production Usage

For production, add the listener to your supervision tree:

```erlang
ChildSpec = nhttp_listener:child_spec(my_http_listener, #{
    port => 8080,
    handler => my_handler
}),
{ok, {SupFlags, [ChildSpec | OtherChildren]}}.
```

## TLS and SNI

A single TCP/TLS listener can serve several virtual hosts by attaching
SNI options to the `tls` map (see `t:tls/0`). Both shapes are passed
through to the underlying `m:ssl` server options unchanged.

Static lookup with `sni_hosts`:

```erlang
{ok, _} = nhttp:start_link(my_server, #{
    port => 8443,
    handler => my_handler,
    versions => [http1_1, http2],
    tls => #{
        certfile  => "default.pem",
        keyfile   => "default.key",
        sni_hosts => [
            {"a.example.com",
                [{certfile, "vhost_a.pem"}, {keyfile, "vhost_a.key"}]},
            {"b.example.com",
                [{certfile, "vhost_b.pem"}, {keyfile, "vhost_b.key"}]}
        ]
    }
}).
```

Dynamic resolution with `sni_fun` (return `undefined` to fall back to
the listener's default `certfile`/`keyfile`):

```erlang
SniFun = fun(ServerName) ->
    case my_cert_store:lookup(ServerName) of
        {ok, CertOpts} -> CertOpts;
        not_found      -> undefined
    end
end,
{ok, _} = nhttp:start_link(my_server, #{
    port => 8443,
    handler => my_handler,
    versions => [http1_1, http2],
    tls => #{
        certfile => "default.pem",
        keyfile  => "default.key",
        sni_fun  => SniFun
    }
}).
```

`sni_hosts` and `sni_fun` may be combined and are TCP/TLS only. HTTP/3
listeners ignore them.

## Mixed h1/h2/h3 listeners

`versions => [http1_1, http2, http3]` serves all three from one call. h1
and h2 share a single TCP/TLS listener (ALPN-negotiated). h3 runs over a
separate UDP/QUIC listener bound, by convention, on the same port number.

```erlang
{ok, Pid} = nhttp:start_link(my_srv, #{
    port     => 443,
    handler  => my_handler,
    versions => [http1_1, http2, http3],
    tls      => #{certfile => "cert.pem", keyfile => "key.pem"}
}),
{ok, TcpPort}  = nhttp:get_port(Pid),         % h1/h2 (TCP)
{ok, QuicPort} = nhttp:get_port(Pid, quic).   % h3 (UDP)
```

`max_connections` applies **per transport**: a mixed listener admits up
to `max_connections` TCP connections *and* up to `max_connections` QUIC
connections, each with its own counter. The two never share a budget.

With `port => 0` the TCP and UDP transports bind *different* ephemeral
ports. `get_port/1` returns the TCP port. Read the UDP port with
`get_port(Pid, quic)` or `get_ports/1`. Never assume the two are equal.

## Alt-Svc advertisement

A mixed listener auto-emits `Alt-Svc: h3=":<quic_port>"; ma=86400` on
every HTTP/1.1 and HTTP/2 response so clients discover and upgrade to h3
(RFC 7838 §3, RFC 9114 §3.1). With `port => 0` the advertised port is the
QUIC ephemeral port. Tune the freshness lifetime with
`alt_svc => #{ma => Seconds}`, or disable advertisement with
`alt_svc => false`. A handler overrides the header by setting its own
`alt-svc` on the response, and suppresses it by setting `alt-svc` to an
empty value.

Advertisement tracks live QUIC readiness, not static config (RFC 7838
§2.1). h3 is advertised only while the QUIC transport is accepting. Once
it drains or goes down the response carries `Alt-Svc: clear` instead, so
clients drop the cached alternative rather than waste a UDP attempt and
fall back (RFC 7838 §4). Draining a mixed listener stops advertising
before it stops accepting.

## 0-RTT

QUIC 0-RTT (early data) is not enabled: nquic refuses early data unless a
replay-protection module is configured, which this server never sets. No
application bytes are processed before the handshake completes, so a
replayed non-idempotent method cannot reach the handler (RFC 8470,
RFC 9001 §9.2).
""".

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_defaults.hrl").

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    active_connections/0,
    active_requests/0,
    active_ws_sessions/0,
    child_spec/1,
    child_spec/2,
    drain/1,
    drain/2,
    get_port/1,
    get_port/2,
    get_ports/1,
    header/2,
    header/3,
    start_link/1,
    start_link/2,
    stop/1,
    stop/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    authority/0,
    header_name/0,
    header_value/0,
    headers/0,

    method/0,
    name/0,
    opts/0,

    peer/0,
    proxy_protocol_opts/0,
    quic_error/0,

    request/0,
    response/0,

    scheme/0,
    start_error/0,

    status/0,
    stop_opts/0,

    stream_id/0,
    timeouts/0,
    tls/0,

    version/0
]).

-type opts() :: #{
    acceptor_count => pos_integer(),
    alt_svc => #{ma => non_neg_integer()} | false,
    backlog => pos_integer(),
    buffer => pos_integer(),
    h2_settings => nhttp_h2:settings(),
    handler := module(),
    handler_args => term(),
    compression => boolean(),
    compression_level => 1..9,
    compression_threshold => non_neg_integer(),
    max_connections => pos_integer() | infinity,
    nodelay => boolean(),
    port := inet:port_number(),
    versions => [http1_1 | http2 | http3],
    timeouts => timeouts(),
    tls => tls(),
    proxy_protocol => boolean() | proxy_protocol_opts()
}.

-doc """
Connection timeouts (milliseconds, or `infinity`).

- `idle`: keep-alive gap between requests; re-arms per packet.
  Default 60000.
- `request`: absolute deadline from the first byte of a request head to
  a complete head (HTTP/1.1). Caps slowloris drip regardless of `idle`.
  Default 30000.
- `body`: per-chunk idle while receiving a request body; re-arms per
  chunk. Default 60000.
- `body_deadline`: absolute deadline from head-complete to a fully
  received body (HTTP/1.1). Default `infinity` (opt-in) so legitimate
  large or slow uploads are not aborted.
- `send`: socket send timeout. Default 30000.
""".
-type timeouts() :: #{
    idle => timeout(),
    request => timeout(),
    body => timeout(),
    body_deadline => timeout(),
    send => timeout()
}.

-type tls() :: #{
    certfile := file:filename(),
    keyfile := file:filename(),
    cacertfile => file:filename(),
    verify => verify_none | verify_peer,
    sni_fun => fun((string()) -> [ssl:tls_server_option()] | undefined),
    sni_hosts => [{string(), [ssl:tls_server_option()]}],
    extra => [ssl:tls_server_option()]
}.

-type name() :: nhttp_listener:name().

-type proxy_protocol_opts() :: #{
    version => v1 | v2 | both,
    timeout => timeout()
}.

-type stop_opts() :: #{
    drain => boolean(),
    drain_timeout => timeout(),
    force_kill_delay => non_neg_integer()
}.

-type start_error() ::
    {invalid_opts, {nhttp_error:category(), nhttp_error:reason()}}
    | {listen_failed, inet:posix() | quic_error()}.

-type quic_error() :: term().

-type request() :: nhttp_lib:request().
-type response() :: nhttp_lib:response().
-type headers() :: nhttp_lib:headers().
-type header_name() :: nhttp_lib:header_name().
-type header_value() :: nhttp_lib:header_value().
-type status() :: nhttp_lib:status().
-type method() :: nhttp_lib:method().
-type scheme() :: nhttp_lib:scheme().
-type authority() :: nhttp_lib:authority().
-type peer() :: nhttp_lib:peer().
-type version() :: nhttp_lib:version().
-type stream_id() :: nhttp_lib:stream_id().

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(SHUTDOWN_FORCE_KILL_DELAY, 10000).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Best-effort count of in-flight connections across every nhttp listener
in this VM.

Backed by an atomics counter incremented on connection accept and
decremented on connection terminate. A brutal kill that bypasses
`terminate/2` leaks the slot. The counter never goes below zero
because the reader floors at 0.
""".
-spec active_connections() -> non_neg_integer().
active_connections() ->
    nhttp_stats:active_connections().

-doc """
Count of in-flight requests across every nhttp listener in this VM.
Backed by `nhttp_otel`'s active-requests metric (always on for queries
even when otel export is disabled).
""".
-spec active_requests() -> non_neg_integer().
active_requests() ->
    nhttp_otel:active_requests().

-doc """
Best-effort count of live WebSocket sessions across every nhttp listener
in this VM.
Incremented after the upgrade response is on the wire, decremented when
the session terminates. Same brutal-kill caveat as `active_connections/0`.
""".
-spec active_ws_sessions() -> non_neg_integer().
active_ws_sessions() ->
    nhttp_stats:active_ws_sessions().

-doc """
Generate a child spec for embedding the listener in your supervisor with
an auto-generated id. Use this form when you do not need to look the
listener up by name later.
""".
-spec child_spec(opts()) -> supervisor:child_spec().
child_spec(Opts) ->
    nhttp_listener:child_spec(Opts).

-doc """
Generate a child spec for embedding the listener in your supervisor
under a caller-supplied name.
The supervisor child id is `{nhttp_listener, Name}` and the listener
registers itself under `Name` when it starts.
""".
-spec child_spec(name(), opts()) -> supervisor:child_spec().
child_spec(Name, Opts) ->
    nhttp_listener:child_spec(Name, Opts).

-doc """
Drain connections with the default 10-second timeout.
Stops the acceptor sub-supervisor, signals every live connection to
wind down, and blocks until they exit or the timeout elapses. The
listener itself stays up. Call `stop/1` afterwards to tear it down.
""".
-spec drain(pid()) -> ok.
drain(Pid) when is_pid(Pid) ->
    drain(Pid, ?DEFAULT_DRAIN_TIMEOUT).

-doc """
Drain connections with a custom timeout (milliseconds or `infinity`).
See `drain/1` for the behaviour. The listener itself stays up.
""".
-spec drain(pid(), timeout()) -> ok.
drain(Pid, Timeout) when is_pid(Pid) ->
    nhttp_listener:drain(Pid, Timeout).

-doc """
Get the TCP port a server is listening on.
Returns the TCP/TLS transport's port. For a QUIC-only server it returns
the sole transport's UDP port. Useful when port 0 was specified to get an
ephemeral port.
A mixed `[http1_1, http2, http3]` server binds two transports: TCP/TLS
for h1/h2 and UDP/QUIC for h3. With `port => 0` they bind *different*
ephemeral ports. Use `get_port/2` or `get_ports/1` to read the UDP port.
""".
-spec get_port(pid()) -> {ok, inet:port_number()} | {error, term()}.
get_port(Pid) ->
    nhttp_listener:get_port(Pid).

-doc """
Get the bound port for a specific transport (`tcp` or `quic`).
`tcp` covers the h1/h2 listener, `quic` the h3 listener. Returns
`{error, {server, {no_transport, Kind}}}` when the server does not serve
that transport.
""".
-spec get_port(pid(), tcp | quic) -> {ok, inet:port_number()} | {error, term()}.
get_port(Pid, Kind) ->
    nhttp_listener:get_port(Pid, Kind).

-doc """
Get every bound transport port as a map keyed by transport.
A single-transport server yields a one-entry map. A mixed server yields
`#{tcp => P1, quic => P2}`. With `port => 0` the two ports differ.
""".
-spec get_ports(pid()) -> #{tcp | quic => inet:port_number()}.
get_ports(Pid) ->
    nhttp_listener:get_ports(Pid).

-doc """
Look up a request header by name (case-insensitive).
Top-level convenience wrapper over `nhttp_req:header/2`. Returns
`undefined` when the header is missing.
""".
-spec header(header_name(), request()) -> header_value() | undefined.
header(Name, Request) ->
    nhttp_req:header(Name, Request).

-doc """
Look up a request header by name (case-insensitive), with a default.
Top-level convenience wrapper over `nhttp_req:header/3`.
""".
-spec header(header_name(), request(), Default) -> header_value() | Default.
header(Name, Request, Default) ->
    nhttp_req:header(Name, Request, Default).

-doc """
Start an unnamed HTTP server linked to the calling process.
Returns the supervisor pid. For production, use
`nhttp_listener:child_spec/1,2` to embed in your supervision tree
instead.
OpenTelemetry can be enabled via `#{otel => true}` or
`#{otel => #{traces => true, metrics => true}}`.
""".
-spec start_link(opts()) -> {ok, pid()} | ignore | {error, start_error()}.
start_link(Opts) ->
    nhttp_listener:start_link(Opts).

-doc """
Start a named HTTP server linked to the calling process.
`Name` follows the standard `gen_server` registration shapes
(`atom()`, `{local, atom()}`, `{global, term()}`, `{via, module(),
term()}`) and is also threaded into log / otel attributes as
`listener_name`.
""".
-spec start_link(name(), opts()) -> {ok, pid()} | ignore | {error, start_error()}.
start_link(Name, Opts) ->
    nhttp_listener:start_link(Name, Opts).

-doc "Stop a running server immediately.".
-spec stop(pid()) -> ok.
stop(Pid) when is_pid(Pid) ->
    immediate_stop(Pid, ?SHUTDOWN_FORCE_KILL_DELAY).

-doc """
Stop a running server with options.
Options:
  - `drain => false` (default): Stop immediately. `drain_timeout` is
    rejected in this mode to surface configuration mistakes.
  - `drain => true`: Drain connections, then stop. Defaults to a 10
    second `drain_timeout` if none is supplied.
  - `drain_timeout => timeout()`: Override the drain deadline (ms or
    `infinity`). Only meaningful when `drain => true`.
During drain:
  1. Stop accepting new connections
  2. Signal existing connections to complete and close
  3. Wait for connections to drain (up to `drain_timeout`)
  4. Force-close remaining connections after the timeout elapses
""".
-spec stop(pid(), stop_opts()) -> ok | {error, {invalid_stop_opts, atom()}}.
stop(Pid, Opts) when is_pid(Pid), is_map(Opts) ->
    Drain = maps:get(drain, Opts, false),
    HasTimeout = maps:is_key(drain_timeout, Opts),
    KillDelay = maps:get(force_kill_delay, Opts, ?SHUTDOWN_FORCE_KILL_DELAY),
    case {Drain, HasTimeout} of
        {false, true} ->
            {error, {invalid_stop_opts, drain_timeout_without_drain}};
        {false, false} ->
            immediate_stop(Pid, KillDelay);
        {true, _} ->
            Timeout = maps:get(drain_timeout, Opts, ?DEFAULT_DRAIN_TIMEOUT),
            graceful_stop(Pid, Timeout, KillDelay)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec graceful_stop(pid(), timeout(), non_neg_integer()) -> ok.
graceful_stop(Pid, Timeout, KillDelay) ->
    ok = drain(Pid, Timeout),
    immediate_stop(Pid, KillDelay).

-spec immediate_stop(pid(), non_neg_integer()) -> ok.
immediate_stop(Pid, KillDelay) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after KillDelay ->
        exit(Pid, kill),
        receive
            {'DOWN', Ref, process, Pid, _} -> ok
        after 1000 ->
            ok
        end
    end.
