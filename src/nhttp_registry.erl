-module(nhttp_registry).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    lookup_acceptor_sup/1,
    lookup_advertise_draining/1,
    lookup_advertise_port/1,
    lookup_advertise_tracker/1,
    lookup_conn_sup/1,
    lookup_conn_tracker/1,
    lookup_counter/1,
    lookup_name/1,
    lookup_port/1,
    new/0,
    register_acceptor_sup/2,
    register_advertise_port/2,
    register_advertise_tracker/2,
    register_conn_sup/2,
    register_conn_tracker/2,
    register_counter/2,
    register_name/2,
    register_port/2,
    set_advertise_draining/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([tab/0]).

-type tab() :: ets:table().

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(ACCEPTOR_SUP, acceptor_sup).
-define(ADVERTISE_DRAINING, advertise_draining).
-define(ADVERTISE_PORT, advertise_port).
-define(ADVERTISE_TRACKER, advertise_tracker).
-define(CONN_SUP, conn_sup).
-define(CONN_TRACKER, conn_tracker).
-define(COUNTER, counter).
-define(NAME, name).
-define(PORT, port).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Acceptor supervisor pid for this listener, or `undefined`.".
-spec lookup_acceptor_sup(tab()) -> pid() | undefined.
lookup_acceptor_sup(Tab) ->
    lookup(Tab, ?ACCEPTOR_SUP).

-doc """
Whether the QUIC transport has stopped advertising (draining or shut
down). Absent entry reads as `false` (RFC 7838 §2.1).
""".
-spec lookup_advertise_draining(tab()) -> boolean().
lookup_advertise_draining(Tab) ->
    case lookup(Tab, ?ADVERTISE_DRAINING) of
        true -> true;
        _ -> false
    end.

-doc "QUIC port advertised over `Alt-Svc` on a mixed listener, or `undefined`.".
-spec lookup_advertise_port(tab()) -> inet:port_number() | undefined.
lookup_advertise_port(Tab) ->
    lookup(Tab, ?ADVERTISE_PORT).

-doc """
Liveness handle for the QUIC transport advertised over `Alt-Svc`: the
QUIC connection tracker pid, or `undefined`. A dead pid means the QUIC
transport is down and h3 must no longer be advertised (RFC 7838 §2.1).
""".
-spec lookup_advertise_tracker(tab()) -> pid() | undefined.
lookup_advertise_tracker(Tab) ->
    lookup(Tab, ?ADVERTISE_TRACKER).

-doc "Connection supervisor pid for this listener, or `undefined`.".
-spec lookup_conn_sup(tab()) -> pid() | undefined.
lookup_conn_sup(Tab) ->
    lookup(Tab, ?CONN_SUP).

-doc "Connection tracker pid for this listener, or `undefined`.".
-spec lookup_conn_tracker(tab()) -> pid() | undefined.
lookup_conn_tracker(Tab) ->
    lookup(Tab, ?CONN_TRACKER).

-doc "Connection counter (`atomics:atomics_ref()`) for this listener.".
-spec lookup_counter(tab()) -> nhttp_listener_counter:counter() | undefined.
lookup_counter(Tab) ->
    lookup(Tab, ?COUNTER).

-doc "Listener name (user-supplied or auto-generated).".
-spec lookup_name(tab()) -> term().
lookup_name(Tab) ->
    lookup(Tab, ?NAME).

-doc "Bound listen port for this listener, or `undefined`.".
-spec lookup_port(tab()) -> inet:port_number() | undefined.
lookup_port(Tab) ->
    lookup(Tab, ?PORT).

-doc """
Create a new per-listener registry table.
The calling process becomes the owner. The table dies with it.
""".
-spec new() -> tab().
new() ->
    ets:new(?MODULE, [public, set, {read_concurrency, true}]).

-doc "Register the acceptor supervisor pid for this listener.".
-spec register_acceptor_sup(tab(), pid()) -> ok.
register_acceptor_sup(Tab, Pid) when is_pid(Pid) ->
    insert(Tab, ?ACCEPTOR_SUP, Pid).

-doc "Register the QUIC port advertised over `Alt-Svc` by a mixed listener.".
-spec register_advertise_port(tab(), inet:port_number()) -> ok.
register_advertise_port(Tab, Port) when is_integer(Port) ->
    insert(Tab, ?ADVERTISE_PORT, Port).

-doc "Register the QUIC connection tracker pid used as the `Alt-Svc` liveness handle.".
-spec register_advertise_tracker(tab(), pid()) -> ok.
register_advertise_tracker(Tab, Pid) when is_pid(Pid) ->
    insert(Tab, ?ADVERTISE_TRACKER, Pid).

-doc "Register the connection supervisor pid for this listener.".
-spec register_conn_sup(tab(), pid()) -> ok.
register_conn_sup(Tab, Pid) when is_pid(Pid) ->
    insert(Tab, ?CONN_SUP, Pid).

-doc "Register the connection tracker pid for this listener.".
-spec register_conn_tracker(tab(), pid()) -> ok.
register_conn_tracker(Tab, Pid) when is_pid(Pid) ->
    insert(Tab, ?CONN_TRACKER, Pid).

-doc "Register the connection counter atomics ref for this listener.".
-spec register_counter(tab(), nhttp_listener_counter:counter()) -> ok.
register_counter(Tab, Counter) ->
    insert(Tab, ?COUNTER, Counter).

-doc "Register the listener name (used in logs / otel attributes).".
-spec register_name(tab(), term()) -> ok.
register_name(Tab, Name) ->
    insert(Tab, ?NAME, Name).

-doc "Register the bound listen port for this listener.".
-spec register_port(tab(), inet:port_number()) -> ok.
register_port(Tab, Port) when is_integer(Port) ->
    insert(Tab, ?PORT, Port).

-doc """
Set whether the QUIC transport has stopped advertising over `Alt-Svc`.
Flipped to `true` when the QUIC transport drains, reset to `false` when a
fresh tracker comes up (RFC 7838 §2.1, §4).
""".
-spec set_advertise_draining(tab(), boolean()) -> ok.
set_advertise_draining(Tab, Draining) when is_boolean(Draining) ->
    insert(Tab, ?ADVERTISE_DRAINING, Draining).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec insert(tab(), atom(), term()) -> ok.
insert(Tab, Key, Value) ->
    true = ets:insert(Tab, {Key, Value}),
    ok.

-spec lookup(tab(), atom()) -> term() | undefined.
lookup(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        [{_, Value}] -> Value;
        [] -> undefined
    end.
