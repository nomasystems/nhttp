%%%-----------------------------------------------------------------------------
%%% Private records shared across nhttp_conn_h3* modules.
%%%
%%% Internal to the nhttp app. Included by `nhttp_conn_h3` (the connection
%%% process), `nhttp_conn_h3_push` (server-initiated stream push), and
%%% `nhttp_conn_ws_h3` (the RFC 9220 WebSocket session layer) so that they
%%% can share `#state{}`, `#h3_stream{}`, and `#h3_push_ctx{}` without
%%% leaking these records to public consumers. Push-stream state lives in
%%% `#h3_push_ctx{}` (mirroring `#h1_push_ctx{}`), populated only for
%%% `type = push` streams. The WS session record `#ws_state{}` comes from
%%% `nhttp_conn_ws.hrl`, shared with the H1/H2 header.
%%%
%%% H1 and H2 records live in `nhttp_conn.hrl`; H3 keeps its own header
%%% because the `#state{}` record shape diverges (QUIC ctx + nhttp_h3 conn
%%% instead of socket + nhttp_h2 conn).
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_CONN_H3_HRL).
-define(NHTTP_CONN_H3_HRL, true).

-include("nhttp_conn_ws.hrl").
-include("nhttp_defaults.hrl").

-record(h3_push_ctx, {
    worker :: pid(),
    ref :: reference(),
    mref :: reference(),
    status :: nhttp_lib:status(),
    req_span :: {nhttp_otel:span_ctx(), integer()},
    bytes = 0 :: non_neg_integer(),
    started = false :: boolean(),
    pending_ack :: reference() | undefined,
    send_buffer = <<>> :: binary(),
    send_end_stream = false :: boolean()
}).

-record(h3_stream, {
    request = undefined :: nhttp_lib:request() | undefined,
    body_size = 0 :: non_neg_integer(),
    type = http :: http | websocket | push,
    ws_buffer = <<>> :: binary(),
    ws_decoder :: nhttp_ws:ws_decoder() | undefined,
    ws :: undefined | #ws_state{},
    accept_body = false :: boolean(),
    body_state :: term(),
    req_span :: {nhttp_otel:span_ctx(), integer()} | undefined,
    push :: undefined | #h3_push_ctx{}
}).

-record(state, {
    name :: term(),
    peer = {{0, 0, 0, 0}, 0} :: {inet:ip_address(), inet:port_number()},
    ctx :: nquic:ctx(),
    h3_conn :: nhttp_h3:conn(),
    h3_streams = #{} :: #{nhttp_lib:stream_id() => #h3_stream{}},
    h3_push_workers = #{} :: #{pid() => nhttp_lib:stream_id()},
    handler :: module(),
    handler_state :: term(),
    opts :: nhttp:opts(),
    limits = #{} :: nhttp_limits:t(),
    idle_timeout = ?DEFAULT_IDLE_TIMEOUT :: timeout(),
    requests_count = 0 :: non_neg_integer(),
    otel_config = false :: nhttp_otel:config(),
    conn_span :: nhttp_otel:span_ctx(),
    compression_enabled = true :: boolean(),
    compression_level = ?DEFAULT_COMPRESSION_LEVEL :: 1..9,
    compression_threshold = ?DEFAULT_COMPRESSION_THRESHOLD :: non_neg_integer(),
    compress_mime_types :: [binary()] | undefined,
    closing = false :: boolean(),
    parent :: pid(),
    debug :: [sys:debug_option()]
}).

-endif.
