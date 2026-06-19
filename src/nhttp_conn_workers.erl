-module(nhttp_conn_workers).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    route/5,
    route_silent/4
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type workers() :: #{pid() => nhttp_lib:stream_id()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Route a worker-originated chunk_ack-bearing message to its owning stream.
When `WPid` is unknown (the stream was already torn down), notify the
worker with `{chunk_ack, Ref, {error, closed}}` and return `State`
unchanged. Otherwise invoke `Cont(StreamId)` to perform the per-stream
action.

`State` is `dynamic()` because callers come from both the H1/H2 family
(`nhttp_conn.hrl`'s `#state{}`) and the H3 family (`nhttp_conn_h3.hrl`'s
`#state{}`). This module is intentionally transport-agnostic.
""".
-spec route(pid(), reference(), workers(), dynamic(), fun((nhttp_lib:stream_id()) -> dynamic())) ->
    dynamic().
route(WPid, Ref, Workers, State, Cont) ->
    case maps:get(WPid, Workers, undefined) of
        undefined ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            State;
        StreamId ->
            Cont(StreamId)
    end.

-doc """
Variant of `route/5` that does not notify the worker when it is unknown.
Used for terminal events (`DOWN`, ack-only) where there is no outstanding
`chunk_ack` to deliver.
""".
-spec route_silent(pid(), workers(), dynamic(), fun((nhttp_lib:stream_id()) -> dynamic())) ->
    dynamic().
route_silent(WPid, Workers, State, Cont) ->
    case maps:get(WPid, Workers, undefined) of
        undefined -> State;
        StreamId -> Cont(StreamId)
    end.
