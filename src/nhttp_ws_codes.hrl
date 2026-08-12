%%%-----------------------------------------------------------------------------
%%% WebSocket close codes (RFC 6455 §7.4).
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_WS_CODES_HRL).
-define(NHTTP_WS_CODES_HRL, true).

-define(WS_CLOSE_NORMAL, 1000).
-define(WS_CLOSE_GOING_AWAY, 1001).
-define(WS_CLOSE_PROTOCOL_ERROR, 1002).
-define(WS_CLOSE_NO_STATUS, 1005).
-define(WS_CLOSE_INVALID_PAYLOAD, 1007).
-define(WS_CLOSE_MESSAGE_TOO_BIG, 1009).
-define(WS_CLOSE_INTERNAL_ERROR, 1011).

-endif.
