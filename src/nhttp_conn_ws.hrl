%%%-----------------------------------------------------------------------------
%%% WebSocket session record shared by every transport.
%%%
%%% Internal to the nhttp app. Included (guarded) by `nhttp_conn.hrl`
%%% (H1/H2) and `nhttp_conn_h3.hrl` so the per-transport WS layers share
%%% one `#ws_state{}` shape without either conn header pulling in the
%%% other's records.
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_CONN_WS_HRL).
-define(NHTTP_CONN_WS_HRL, true).

-record(ws_state, {
    session :: nhttp_ws:session(),
    ref :: reference(),
    handler_state :: term(),
    deliver_ping = false :: boolean(),
    deliver_pong = false :: boolean(),
    max_message_size = infinity :: pos_integer() | infinity
}).

-endif.
