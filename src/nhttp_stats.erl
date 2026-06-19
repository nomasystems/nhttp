-module(nhttp_stats).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    active_connections/0,
    active_ws_sessions/0,
    decr_connection/0,
    decr_ws_session/0,
    incr_connection/0,
    incr_ws_session/0
]).

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(KEY, '$nhttp_stats_counters').
-define(IDX_CONNECTIONS, 1).
-define(IDX_WS_SESSIONS, 2).
-define(NSLOTS, 2).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec active_connections() -> non_neg_integer().
active_connections() ->
    get_counter(?IDX_CONNECTIONS).

-spec active_ws_sessions() -> non_neg_integer().
active_ws_sessions() ->
    get_counter(?IDX_WS_SESSIONS).

-spec decr_connection() -> ok.
decr_connection() ->
    add(?IDX_CONNECTIONS, -1).

-spec decr_ws_session() -> ok.
decr_ws_session() ->
    add(?IDX_WS_SESSIONS, -1).

-spec incr_connection() -> ok.
incr_connection() ->
    add(?IDX_CONNECTIONS, 1).

-spec incr_ws_session() -> ok.
incr_ws_session() ->
    add(?IDX_WS_SESSIONS, 1).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec add(pos_integer(), integer()) -> ok.
add(Idx, N) ->
    Ref = ensure_counters(),
    atomics:add(Ref, Idx, N),
    ok.

-spec ensure_counters() -> atomics:atomics_ref().
ensure_counters() ->
    case persistent_term:get(?KEY, undefined) of
        undefined ->
            Ref = atomics:new(?NSLOTS, [{signed, true}]),
            persistent_term:put(?KEY, Ref),
            persistent_term:get(?KEY);
        Ref ->
            Ref
    end.

-spec get_counter(pos_integer()) -> non_neg_integer().
get_counter(Idx) ->
    case persistent_term:get(?KEY, undefined) of
        undefined -> 0;
        Ref -> max(0, atomics:get(Ref, Idx))
    end.
