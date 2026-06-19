-module(nhttp_conn_tracker).

-moduledoc false.

-behaviour(gen_server).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    drain_all/1,
    listener_port/1,
    start_link/2,
    stop_advertising/1,
    track/2,
    wait_for_drained/2
]).

%%%-----------------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL RECORDS
%%%-----------------------------------------------------------------------------
-record(state, {
    tab :: nhttp_registry:tab(),
    advertise_tab :: nhttp_registry:tab() | undefined,
    counter :: nhttp_listener_counter:counter(),
    monitors = #{} :: #{pid() => reference()},
    pending_drains = [] :: [gen_server:from()]
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Send a drain signal to every tracked connection.
Both `nhttp_conn` and `nhttp_conn_h3` interpret the `shutdown` message as
"close at the next safe point": HTTP/1.1 closes after the in-flight
request, HTTP/2 and HTTP/3 emit GOAWAY, WebSocket sends a close frame.
""".
-spec drain_all(pid()) -> ok.
drain_all(TrackerPid) ->
    gen_server:call(TrackerPid, drain_all).

-doc """
Return the bound listen port for this listener.
The tracker is the surviving non-acceptor child, so `nhttp_listener`
uses it to answer `get_port/1` on the QUIC path where there is no
acceptor process holding the socket.
""".
-spec listener_port(pid()) -> {ok, inet:port_number()} | {error, term()}.
listener_port(TrackerPid) ->
    gen_server:call(TrackerPid, listener_port).

-doc """
Start a connection tracker for one transport.
`AdvertiseTab` is the listener's primary registry table when this tracker
backs a QUIC transport whose readiness gates `Alt-Svc` advertisement, or
`undefined` otherwise. When set, the tracker publishes its liveness and
draining state there so the TCP path advertises h3 only while QUIC is
actually accepting (RFC 7838 §2.1).
""".
-spec start_link(nhttp_registry:tab(), nhttp_registry:tab() | undefined) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Tab, AdvertiseTab) ->
    gen_server:start_link(?MODULE, {Tab, AdvertiseTab}, []).

-doc """
Stop advertising this transport's QUIC endpoint over `Alt-Svc` without
draining its connections. The TCP path emits `Alt-Svc: clear` from this
point (RFC 7838 §2.1, §4). A no-op for trackers that do not gate
advertisement.
""".
-spec stop_advertising(pid()) -> ok.
stop_advertising(TrackerPid) ->
    gen_server:call(TrackerPid, stop_advertising).

-doc """
Begin tracking a connection pid.
Synchronous so the monitor is registered before `start_conn/3` returns:
a fire-and-forget cast can be silently dropped if the tracker is briefly
down (e.g. mid `rest_for_one` restart), which would leak a counter slot.
""".
-spec track(pid(), pid()) -> ok.
track(TrackerPid, ConnPid) ->
    gen_server:call(TrackerPid, {track, ConnPid}).

-doc """
Block the caller until every tracked connection has exited, or until
`Timeout` elapses. Returns `ok` immediately if no connections are
tracked, or if the tracker is no longer alive (nothing to drain).
Used by `nhttp_listener:drain/2` after `drain_all/1` has signalled
existing connections. The reply is driven reactively from the `'DOWN'`
flow, no polling.
""".
-spec wait_for_drained(pid(), timeout()) -> ok | {error, timeout}.
wait_for_drained(TrackerPid, Timeout) ->
    try
        gen_server:call(TrackerPid, wait_for_drained, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> ok
    end.

%%%-----------------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%%-----------------------------------------------------------------------------
-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec handle_call(
    drain_all | listener_port | stop_advertising | {track, pid()} | wait_for_drained,
    gen_server:from(),
    #state{}
) ->
    {reply, ok | {ok, inet:port_number()} | {error, term()}, #state{}} | {noreply, #state{}}.
handle_call(listener_port, _From, #state{tab = Tab} = State) ->
    case nhttp_registry:lookup_port(Tab) of
        undefined -> {reply, {error, {server, no_port}}, State};
        Port -> {reply, {ok, Port}, State}
    end;
handle_call(stop_advertising, _From, State) ->
    ok = mark_advertise_draining(State),
    {reply, ok, State};
handle_call(drain_all, _From, #state{monitors = Monitors} = State) ->
    ok = mark_advertise_draining(State),
    ok = lists:foreach(
        fun(Pid) -> Pid ! shutdown end,
        maps:keys(Monitors)
    ),
    {reply, ok, State};
handle_call({track, Pid}, _From, #state{monitors = Monitors} = State) ->
    MRef = erlang:monitor(process, Pid),
    {reply, ok, State#state{monitors = Monitors#{Pid => MRef}}};
handle_call(
    wait_for_drained, From, #state{monitors = Monitors, pending_drains = Pending} = State
) ->
    case map_size(Monitors) of
        0 ->
            {reply, ok, State};
        _ ->
            {noreply, State#state{pending_drains = [From | Pending]}}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info({'DOWN', reference(), process, pid(), term()}, #state{}) ->
    {noreply, #state{}}.
handle_info(
    {'DOWN', _MRef, process, Pid, _Reason},
    #state{counter = Counter, monitors = Monitors, pending_drains = Pending} = State
) ->
    nhttp_listener_counter:release(Counter),
    NewMonitors = maps:remove(Pid, Monitors),
    NewPending = notify_drain_waiters_if_empty(NewMonitors, Pending),
    {noreply, State#state{monitors = NewMonitors, pending_drains = NewPending}}.

-spec init({nhttp_registry:tab(), nhttp_registry:tab() | undefined}) -> {ok, #state{}}.
init({Tab, AdvertiseTab}) ->
    process_flag(trap_exit, true),
    ok = nhttp_registry:register_conn_tracker(Tab, self()),
    Counter = nhttp_registry:lookup_counter(Tab),
    true = Counter =/= undefined,
    nhttp_listener_counter:reset(Counter),
    ok = init_advertise(AdvertiseTab),
    {ok, #state{tab = Tab, advertise_tab = AdvertiseTab, counter = Counter}}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec init_advertise(nhttp_registry:tab() | undefined) -> ok.
init_advertise(undefined) ->
    ok;
init_advertise(AdvertiseTab) ->
    ok = nhttp_registry:register_advertise_tracker(AdvertiseTab, self()),
    nhttp_registry:set_advertise_draining(AdvertiseTab, false).

-spec mark_advertise_draining(#state{}) -> ok.
mark_advertise_draining(#state{advertise_tab = undefined}) ->
    ok;
mark_advertise_draining(#state{advertise_tab = AdvertiseTab}) ->
    nhttp_registry:set_advertise_draining(AdvertiseTab, true).

-spec notify_drain_waiters_if_empty(#{pid() => reference()}, [gen_server:from()]) ->
    [gen_server:from()].
notify_drain_waiters_if_empty(Monitors, Pending) ->
    case map_size(Monitors) of
        0 ->
            ok = lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Pending),
            [];
        _ ->
            Pending
    end.
