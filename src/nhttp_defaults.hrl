%%%-----------------------------------------------------------------------------
%%% Constants shared across nhttp modules.
%%%
%%% Internal to the nhttp app. Included by modules that emit HTTP status
%%% codes at error paths, WebSocket close codes, HTTP/2 wire-format limits,
%%% or the project's default timeouts.
%%%
%%% Canonical mapping tables (e.g. nhttp_resp:reason_phrase/1 and
%%% nhttp_conn_h3:h3_error_to_code/1) deliberately use literals because the
%%% function name already gives each value its semantic meaning; macros
%%% would just shadow them.
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_DEFAULTS_HRL).
-define(NHTTP_DEFAULTS_HRL, true).

%%%-----------------------------------------------------------------------------
%%% Default timeouts (milliseconds).
%%%-----------------------------------------------------------------------------
-define(DEFAULT_BODY_TIMEOUT, 60000).
-define(DEFAULT_IDLE_TIMEOUT, 60000).
-define(DEFAULT_REQUEST_TIMEOUT, 30000).
-define(DEFAULT_DRAIN_TIMEOUT, 10000).
-define(DEFAULT_DRAIN_GOAWAY_DELAY, 5000).
-define(DEFAULT_PROXY_TIMEOUT, 5000).
-define(DEFAULT_LISTEN_SEND_TIMEOUT, 30000).
-define(DEFAULT_QUIC_IDLE_TIMEOUT, 30000).
-define(DEFAULT_TLS_HANDSHAKE_TIMEOUT, 5000).
-define(WORKER_SHUTDOWN_TIMEOUT, 5000).

%%%-----------------------------------------------------------------------------
%%% Alt-Svc advertisement defaults (RFC 7838 §3).
%%%-----------------------------------------------------------------------------
-define(DEFAULT_ALT_SVC_MA, 86400).

%%%-----------------------------------------------------------------------------
%%% Compression defaults.
%%%-----------------------------------------------------------------------------
-define(DEFAULT_COMPRESSION_LEVEL, 6).
-define(DEFAULT_COMPRESSION_THRESHOLD, 1024).

-endif.
