%%%-----------------------------------------------------------------------------
%%% WebSocket session record shared by every transport.
%%%
%%% Internal to the nhttp app. Included (guarded) by `nhttp_conn.hrl`
%%% (H1/H2) and `nhttp_conn_h3.hrl` so the per-transport WS layers share
%%% one `#ws_state{}` shape without either conn header pulling in the
%%% other's records.
%%%
%%% The `handles_*` flags record which optional callbacks the handler
%%% exports. `nhttp_conn_ws:open/3` sets them once per session.
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_CONN_WS_HRL).
-define(NHTTP_CONN_WS_HRL, true).

-record(ws_state, {
    session :: nhttp_ws:session(),
    ref :: reference(),
    handler_state :: term(),
    deliver_ping = false :: boolean(),
    deliver_pong = false :: boolean(),
    max_message_size = infinity :: pos_integer() | infinity,
    handles_frame = false :: boolean(),
    handles_info = false :: boolean(),
    handles_closed = false :: boolean()
}).

-endif.
