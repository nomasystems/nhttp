%%%-----------------------------------------------------------------------------
%%% Private records shared across nhttp_conn* modules.
%%%
%%% This header is internal to the nhttp app. It is included by
%%% `nhttp_conn` and the protocol-aligned modules (`nhttp_conn_h1`,
%%% `nhttp_conn_h2`, `nhttp_conn_ws`, `nhttp_conn_ws_h1`,
%%% `nhttp_conn_ws_h2`) so that they can share `#state{}`, `#h2_stream{}`,
%%% and `#h1_push_ctx{}` without leaking these records to public
%%% consumers. The WS session record `#ws_state{}` comes from
%%% `nhttp_conn_ws.hrl`, shared with the H3 header.
%%%
%%% `#state{}` holds only protocol-agnostic fields. Protocol-specific
%%% state lives under `protocol_state`: `#h1_state{}` for `family`
%%% `http1` and `websocket` (the H1 upgrade keeps its record, ws fields
%%% included), `#h2_state{}` for `http2`, and `undefined` before
%%% classification in `nhttp_conn`. H3 has its own `#state{}` in
%%% `nhttp_conn_h3.hrl`.
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_CONN_HRL).
-define(NHTTP_CONN_HRL, true).

-include("nhttp_conn_ws.hrl").
-include("nhttp_defaults.hrl").

-record(h2_stream, {
    body_acc = [] :: [binary()],
    body_size = 0 :: non_neg_integer(),
    end_stream = false :: boolean(),
    send_buffer = <<>> :: binary(),
    send_end_stream = false :: boolean(),
    type = http :: http | request | stream | websocket,
    ws_buffer = <<>> :: binary(),
    ws_decoder :: nhttp_ws:ws_decoder() | undefined,
    ws :: undefined | #ws_state{},
    worker :: pid() | undefined,
    worker_ref :: reference() | undefined,
    worker_mref :: reference() | undefined,
    pending_ack :: reference() | undefined,
    streaming_body = false :: boolean(),
    pending_trailers :: nhttp_lib:headers() | undefined,
    body_window_pending = queue:new() :: queue:queue(non_neg_integer()),
    status :: nhttp_lib:status() | undefined,
    req_span :: {nhttp_otel:span_ctx(), integer()} | undefined,
    bytes_sent = 0 :: non_neg_integer(),
    response_started = false :: boolean(),
    request :: nhttp_lib:request() | undefined
}).

-record(h1_push_ctx, {
    worker :: pid(),
    ref :: reference(),
    mref :: reference(),
    status :: nhttp_lib:status(),
    req_span :: {nhttp_otel:span_ctx(), integer()},
    bytes = 0 :: non_neg_integer(),
    started = false :: boolean()
}).

-record(h1_state, {
    buffer = <<>> :: binary(),
    body_stream :: nhttp_h1:body_stream() | undefined,
    ws_buffer = <<>> :: binary(),
    h1_ws :: undefined | #ws_state{},
    keep_alive = true :: boolean(),
    body_timeout = ?DEFAULT_BODY_TIMEOUT :: timeout(),
    request_timeout = ?DEFAULT_REQUEST_TIMEOUT :: timeout(),
    body_deadline = infinity :: timeout(),
    head_deadline_at :: integer() | undefined,
    body_deadline_at :: integer() | undefined
}).

-record(h2_state, {
    h2_conn :: nhttp_h2:conn(),
    h2_streams = #{} :: #{nhttp_lib:stream_id() => #h2_stream{}},
    h2_workers = #{} :: #{pid() => nhttp_lib:stream_id()},
    drain_deadline :: integer() | undefined
}).

-record(state, {
    name :: term(),
    socket :: nhttp_sock:t() | undefined,
    transport :: tcp | ssl,
    peer = {{0, 0, 0, 0}, 0} :: {inet:ip_address(), inet:port_number()},
    proxy_peer :: {inet:ip_address(), inet:port_number()} | undefined,
    handler :: module(),
    handler_state :: term(),
    family :: http1 | http2 | websocket | undefined,
    protocol_state :: undefined | #h1_state{} | #h2_state{},
    opts :: nhttp:opts(),
    limits = #{} :: nhttp_limits:t(),
    idle_timeout = ?DEFAULT_IDLE_TIMEOUT :: timeout(),
    requests_count = 0 :: non_neg_integer(),
    otel_config = false :: nhttp_otel:config(),
    conn_span :: nhttp_otel:span_ctx(),
    compression_enabled = true :: boolean(),
    compression_level = ?DEFAULT_COMPRESSION_LEVEL :: 1..9,
    compression_threshold = ?DEFAULT_COMPRESSION_THRESHOLD :: non_neg_integer(),
    compress_mime_types :: [binary()] | undefined
}).

-endif.
