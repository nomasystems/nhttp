-module(nhttp_acceptor_core).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_proc_lib.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    get_listen_port/1,
    start_link/3,
    stop_accepting/1
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    init/1,
    system_code_change/4,
    system_continue/3,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([spawn_ctx/0]).

-type spawn_ctx() :: #{
    name := term(),
    counter := nhttp_listener_counter:counter(),
    opts := nhttp:opts(),
    conn_sup := pid(),
    tracker := pid()
}.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(GET_PORT_REPLY_TIMEOUT, 5000).
-define(ACCEPT_BACKOFF_INITIAL, 100).
-define(ACCEPT_BACKOFF_MAX, 1000).
-define(ACCEPT_ERROR_LOG_INTERVAL, 5000).

%%%-----------------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%%-----------------------------------------------------------------------------
-doc """
Build per-transport state from the listener Opts (e.g. the listen
socket for TCP/TLS, the QUIC listener handle for HTTP/3). Returned
value is stored opaquely by the core and threaded through `do_accept/1`.
""".
-callback init_sub(nhttp:opts()) -> term().

-doc """
Block on the transport-specific accept primitive. The 1000 ms timeout
is shared across transports so the core loop can interleave system
messages cheaply.
""".
-callback do_accept(SubState :: term()) ->
    {ok, term()} | {error, term()}.

-doc """
Release the transport resource for an accepted connection that the
core has decided NOT to spawn (at-capacity). Send any application-
visible refusal here (e.g. HTTP/1.1 503).
""".
-callback reject(Accepted :: term()) -> ok.

-doc """
Spawn the connection process for an accepted resource. Owns the full
lifecycle: counter release on `nhttp_conn_sup:start_conn/3` failure,
ownership handoff (e.g. `controlling_process` + `{socket_ready, _}` for
sockets), and any transport-specific cleanup. Returns `ok` once the
work is done.
""".
-callback spawn_conn(Accepted :: term(), spawn_ctx()) -> ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL RECORDS
%%%-----------------------------------------------------------------------------
-record(state, {
    module :: module(),
    sub_state :: term(),
    name :: term(),
    counter :: nhttp_listener_counter:counter(),
    opts :: nhttp:opts(),
    port :: inet:port_number(),
    parent :: pid(),
    debug :: [sys:debug_option()],
    conn_sup :: pid(),
    tracker :: pid(),
    backoff = 0 :: non_neg_integer(),
    last_error_log = undefined :: integer() | undefined
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Get the port the acceptor is listening on.".
-spec get_listen_port(pid()) -> {ok, inet:port_number()} | nhttp_error:t().
get_listen_port(AcceptorPid) ->
    Ref = make_ref(),
    AcceptorPid ! {get_port, self(), Ref},
    receive
        {port_reply, Ref, Port} -> {ok, Port}
    after ?GET_PORT_REPLY_TIMEOUT ->
        nhttp_error:accept_timeout()
    end.

-spec start_link(module(), nhttp_registry:tab(), nhttp:opts()) -> {ok, pid()}.
start_link(Mod, Tab, Opts) ->
    Args = {Mod, Tab, Opts, self()},
    proc_lib:start_link(?MODULE, init, [Args]).

-doc """
Signal the acceptor to stop accepting new connections. The acceptor
exits normally. In-flight connections are unaffected.
""".
-spec stop_accepting(pid()) -> ok.
stop_accepting(AcceptorPid) ->
    AcceptorPid ! stop_accepting,
    ok.

%%%-----------------------------------------------------------------------------
%% PROC_LIB CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({module(), nhttp_registry:tab(), nhttp:opts(), pid()}) -> no_return().
init({Mod, Tab, Opts, Parent}) ->
    Port = maps:get(actual_port, Opts),
    Debug = sys:debug_options([]),
    ConnSupPid = nhttp_registry:lookup_conn_sup(Tab),
    TrackerPid = nhttp_registry:lookup_conn_tracker(Tab),
    Counter = nhttp_registry:lookup_counter(Tab),
    Name = nhttp_registry:lookup_name(Tab),
    true = is_pid(ConnSupPid),
    true = is_pid(TrackerPid),
    true = Counter =/= undefined,
    SubState = Mod:init_sub(Opts),
    proc_lib:init_ack(Parent, {ok, self()}),
    State = #state{
        module = Mod,
        sub_state = SubState,
        name = Name,
        counter = Counter,
        opts = Opts,
        port = Port,
        parent = Parent,
        debug = Debug,
        conn_sup = ConnSupPid,
        tracker = TrackerPid
    },
    accept_loop(State).

-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
?NHTTP_SYSTEM_CODE_CHANGE_NOOP.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(_Parent, Debug, State) ->
    accept_loop(State#state{debug = Debug}).

-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

%%%-----------------------------------------------------------------------------
%% ACCEPT LOOP
%%%-----------------------------------------------------------------------------
-spec accept_loop(#state{}) -> no_return().
accept_loop(#state{parent = Parent, debug = Debug} = State) ->
    case receive_system_msg(Parent, Debug, State, 0) of
        {ok, NewDebug, NewState} ->
            do_accept(NewState#state{debug = NewDebug});
        {stop, Reason} ->
            exit(Reason)
    end.

-spec do_accept(#state{}) -> no_return().
do_accept(#state{module = Mod, sub_state = SubState} = State) ->
    case Mod:do_accept(SubState) of
        {ok, Accepted} ->
            handle_accepted(Accepted, State),
            accept_loop(State#state{backoff = 0});
        {error, timeout} ->
            accept_loop(State);
        {error, closed} ->
            exit(normal);
        {error, Reason} ->
            accept_loop(handle_accept_error(Reason, State))
    end.

-spec handle_accept_error(term(), #state{}) -> #state{}.
handle_accept_error(Reason, State) ->
    State1 = maybe_log_accept_error(Reason, State),
    Backoff = next_backoff(State1#state.backoff),
    State2 = State1#state{backoff = Backoff},
    case receive_system_msg(State2#state.parent, State2#state.debug, State2, Backoff) of
        {ok, NewDebug, NewState} ->
            NewState#state{debug = NewDebug};
        {stop, StopReason} ->
            exit(StopReason)
    end.

-spec maybe_log_accept_error(term(), #state{}) -> #state{}.
maybe_log_accept_error(Reason, #state{last_error_log = Last, name = Name} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case Last =:= undefined orelse (Now - Last) >= ?ACCEPT_ERROR_LOG_INTERVAL of
        true ->
            nhttp_log:accept_error(#{listener_name => Name}, Reason),
            State#state{last_error_log = Now};
        false ->
            State
    end.

-spec next_backoff(non_neg_integer()) -> pos_integer().
next_backoff(0) ->
    ?ACCEPT_BACKOFF_INITIAL;
next_backoff(Backoff) ->
    min(Backoff * 2, ?ACCEPT_BACKOFF_MAX).

-spec handle_accepted(term(), #state{}) -> ok.
handle_accepted(
    Accepted, #state{module = Mod, name = Name, counter = Counter, opts = Opts} = State
) ->
    case nhttp_listener_counter:acquire(Counter) of
        ok ->
            ok = Mod:spawn_conn(Accepted, spawn_ctx(State));
        {error, {server, #{type := at_capacity}}} ->
            ok = Mod:reject(Accepted),
            OtelConfig = nhttp_otel:config(Opts),
            nhttp_otel:connection_rejected(OtelConfig, #{
                listener_name => Name,
                reason => at_capacity
            }),
            ok
    end.

-spec receive_system_msg(pid(), [sys:debug_option()], #state{}, timeout()) ->
    {ok, [sys:debug_option()], #state{}} | {stop, term()}.
receive_system_msg(Parent, Debug, State, Timeout) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State),
            {ok, Debug, State};
        {'EXIT', Parent, Reason} ->
            {stop, Reason};
        {get_port, From, Ref} ->
            From ! {port_reply, Ref, State#state.port},
            {ok, Debug, State};
        stop_accepting ->
            {stop, normal}
    after Timeout ->
        {ok, Debug, State}
    end.

-spec spawn_ctx(#state{}) -> spawn_ctx().
spawn_ctx(#state{
    name = Name, counter = Counter, opts = Opts, conn_sup = ConnSup, tracker = Tracker
}) ->
    #{
        name => Name,
        counter => Counter,
        opts => Opts,
        conn_sup => ConnSup,
        tracker => Tracker
    }.
