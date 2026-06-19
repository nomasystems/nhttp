-module(nhttp_conn_ws_h1).

-moduledoc false.

-behaviour(nhttp_protocol).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn.hrl").
-include("nhttp_proc_lib.hrl").
-include("nhttp_ws_codes.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    loop/3,
    start/3
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    system_code_change/4,
    system_continue/3,
    system_terminate/4,
    ws_wake/4
]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(WS_CLOSE_DRAIN_TIMEOUT, 200).
-define(WS_CLOSE_DRAIN_MAX, 8).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Entry point for the WebSocket session. Called by
`nhttp_conn_h1:handle_ws_upgrade/4` after the `101 Switching Protocols`
response is on the wire and the socket is in `{packet, raw}, {active, once}`
mode.

Builds the `#ws_state{}` session, fires `handle_ws_open/2` (if exported) via
the dispatcher, applies any runtime opts the handler returned, then enters
the receive loop. If the open callback returns `{close, _, _}` the
dispatcher emits `close_session` and the conn terminates without ever
entering the loop.
""".
-spec start(pid(), [sys:debug_option()], #state{}) -> no_return().
start(Parent, Debug, State) ->
    NewState = open(State),
    loop(Parent, Debug, NewState).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS CALLBACKS
%%%-----------------------------------------------------------------------------
-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
?NHTTP_SYSTEM_CODE_CHANGE_NOOP.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(Parent, Debug, State) ->
    loop(Parent, Debug, State).

-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    nhttp_conn:stop_parent(Reason, State).

-doc """
`proc_lib:hibernate/3` re-entry point for the frame-wait hibernation.
Cancels the idle timer armed before hibernating; when it already fired
the wake-up is an idle timeout unless other traffic is queued (drained
by the zero-timeout receive).
""".
-spec ws_wake(pid(), [sys:debug_option()], #state{}, nhttp_conn:hibernate_timer()) ->
    no_return().
ws_wake(Parent, Debug, State, Timer) ->
    case nhttp_conn:cancel_hibernate_timer(Timer) of
        ok -> ws_receive(Parent, Debug, State, State#state.idle_timeout);
        expired -> ws_receive(Parent, Debug, State, 0)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - WEBSOCKET LOOP
%%%-----------------------------------------------------------------------------
-doc """
Frame-wait block point. A session with no partial frame buffered and an
empty mailbox hibernates here; incoming frames (including ping/pong) and
async-API casts wake the process. The idle deadline is carried across
hibernation by the timer from `nhttp_conn:start_hibernate_timer/1`.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, State) ->
    case hibernate_eligible(State) of
        true -> hibernate(Parent, Debug, State);
        false -> ws_receive(Parent, Debug, State, State#state.idle_timeout)
    end.

-spec hibernate_eligible(#state{}) -> boolean().
hibernate_eligible(#state{protocol_state = #h1_state{ws_buffer = <<>>}}) ->
    nhttp_conn:mailbox_empty();
hibernate_eligible(#state{}) ->
    false.

-spec hibernate(pid(), [sys:debug_option()], #state{}) -> no_return().
hibernate(Parent, Debug, #state{idle_timeout = IdleTimeout} = State) ->
    Timer = nhttp_conn:start_hibernate_timer(IdleTimeout),
    proc_lib:hibernate(?MODULE, ws_wake, [Parent, Debug, State, Timer]).

-spec ws_receive(pid(), [sys:debug_option()], #state{}, timeout()) -> no_return().
ws_receive(Parent, Debug, State, IdleTimeout) ->
    receive
        {tcp, _Socket, Data} ->
            handle_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_data(Parent, Debug, State, Data);
        {tcp_closed, _Socket} ->
            finish({transport, closed}, normal, State);
        {ssl_closed, _Socket} ->
            finish({transport, closed}, normal, State);
        {tcp_error, _Socket, Reason} ->
            finish({transport, Reason}, {socket_error, Reason}, State);
        {ssl_error, _Socket, Reason} ->
            finish({transport, Reason}, {socket_error, Reason}, State);
        {'$gen_call', From, Request} ->
            loop(Parent, Debug, handle_call(State, From, Request));
        {'$gen_cast', Msg} ->
            handle_cast_loop(Parent, Debug, State, Msg);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        shutdown ->
            send_close(State, ?WS_CLOSE_GOING_AWAY, <<"Server shutting down">>),
            finish(shutdown, normal, State);
        {'EXIT', Parent, Reason} ->
            finish_parent({transport, Reason}, Reason, State);
        Info ->
            handle_info_loop(Parent, Debug, State, Info)
    after IdleTimeout ->
        send_close(State, ?WS_CLOSE_GOING_AWAY, <<"Idle timeout">>),
        finish(timeout, idle_timeout, State)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - LIFECYCLE
%%%-----------------------------------------------------------------------------
-spec drain_peer(#state{}) -> ok.
drain_peer(#state{socket = undefined}) ->
    ok;
drain_peer(#state{socket = Socket}) ->
    case nhttp_sock:setopts(Socket, [{active, false}]) of
        ok -> drain_peer_loop(Socket, ?WS_CLOSE_DRAIN_MAX);
        {error, _} -> ok
    end.

-spec drain_peer_loop(nhttp_sock:t(), non_neg_integer()) -> ok.
drain_peer_loop(_Socket, 0) ->
    ok;
drain_peer_loop(Socket, N) ->
    case nhttp_sock:recv(Socket, 0, ?WS_CLOSE_DRAIN_TIMEOUT) of
        {ok, _} -> drain_peer_loop(Socket, N - 1);
        {error, _} -> ok
    end.

-spec finish(nhttp_handler:ws_close_reason(), term(), #state{}) -> no_return().
finish(WsReason, ExitReason, State) ->
    State1 = finish_state(WsReason, State),
    ok = drain_peer(State1),
    nhttp_conn:stop(ExitReason, State1).

-spec finish_parent(nhttp_handler:ws_close_reason(), term(), #state{}) -> no_return().
finish_parent(WsReason, Reason, State) ->
    State1 = finish_state(WsReason, State),
    ok = drain_peer(State1),
    nhttp_conn:stop_parent(Reason, State1).

-spec finish_state(nhttp_handler:ws_close_reason(), #state{}) -> #state{}.
finish_state(
    WsReason, #state{handler = Handler, protocol_state = #h1_state{h1_ws = Ws}} = State
) ->
    View = view(Ws),
    Actions = nhttp_conn_ws:close_lifecycle(nhttp_conn:log_ctx(State), WsReason, View, Handler),
    State1 =
        case interpret(Actions, State) of
            {ok, S} -> S;
            {close, S, _} -> S
        end,
    #state{protocol_state = #h1_state{h1_ws = WsAfter} = H1After} = State1,
    NewSState =
        case WsAfter of
            #ws_state{handler_state = LatestS} -> LatestS;
            undefined -> State1#state.handler_state
        end,
    ok = nhttp_stats:decr_ws_session(),
    State1#state{
        handler_state = NewSState,
        protocol_state = H1After#h1_state{h1_ws = undefined}
    }.

-spec open(#state{}) -> #state{} | no_return().
open(
    #state{handler = Handler, handler_state = HState, protocol_state = #h1_state{} = H1} = State
) ->
    Ref = make_ref(),
    Session = nhttp_ws:new_session(h1, self(), undefined, Ref),
    Ws0 = #ws_state{
        session = Session,
        ref = Ref,
        handler_state = HState,
        deliver_ping = false,
        deliver_pong = false,
        max_message_size = infinity
    },
    State1 = State#state{protocol_state = H1#h1_state{h1_ws = Ws0}},
    ok = nhttp_stats:incr_ws_session(),
    View0 = view(Ws0),
    Actions = nhttp_conn_ws:open(nhttp_conn:log_ctx(State1), View0, Handler),
    apply_actions(State1, Actions).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - INCOMING DATA
%%%-----------------------------------------------------------------------------
-spec handle_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_data(
    Parent, Debug, #state{protocol_state = #h1_state{ws_buffer = Buffer} = H1} = State, Data
) ->
    process(
        Parent,
        Debug,
        State#state{protocol_state = H1#h1_state{ws_buffer = <<Buffer/binary, Data/binary>>}}
    ).

-spec process(pid(), [sys:debug_option()], #state{}) -> no_return().
process(
    Parent,
    Debug,
    #state{protocol_state = #h1_state{ws_buffer = Buffer} = H1, socket = Socket} = State
) ->
    case nhttp_ws:decode(Buffer) of
        {ok, Message, Rest} ->
            State0 = State#state{protocol_state = H1#h1_state{ws_buffer = Rest}},
            #state{handler = Handler, protocol_state = #h1_state{h1_ws = Ws}} = State0,
            View = view(Ws),
            Actions = nhttp_conn_ws:dispatch_frame(
                nhttp_conn:log_ctx(State0), Message, View, Handler
            ),
            State1 = apply_actions(State0, Actions),
            case Rest of
                <<>> ->
                    case nhttp_sock:setopts(Socket, [{active, once}]) of
                        ok -> loop(Parent, Debug, State1);
                        {error, _} -> finish({transport, closed}, normal, State1)
                    end;
                _ ->
                    process(Parent, Debug, State1)
            end;
        {more, _Needed} ->
            case nhttp_sock:setopts(Socket, [{active, once}]) of
                ok -> loop(Parent, Debug, State);
                {error, _} -> finish({transport, closed}, normal, State)
            end;
        {error, Reason} ->
            ReasonBin = nhttp_conn_ws:str_or_atom(Reason),
            send_close(State, ?WS_CLOSE_PROTOCOL_ERROR, ReasonBin),
            finish({fail, ?WS_CLOSE_PROTOCOL_ERROR, ReasonBin}, {ws_error, Reason}, State)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - CALL / CAST / INFO
%%%-----------------------------------------------------------------------------
-spec dispatch_info_loop(pid(), [sys:debug_option()], #state{}, term()) -> no_return().
dispatch_info_loop(
    Parent,
    Debug,
    #state{handler = Handler, protocol_state = #h1_state{h1_ws = Ws}} = State,
    Msg
) ->
    View = view(Ws),
    Actions = nhttp_conn_ws:dispatch_info(nhttp_conn:log_ctx(State), Msg, View, Handler),
    State1 = apply_actions(State, Actions),
    loop(Parent, Debug, State1).

-spec handle_call(#state{}, gen_server:from(), term()) -> #state{}.
handle_call(
    #state{protocol_state = #h1_state{h1_ws = #ws_state{ref = Ref}}} = State,
    From,
    {ws_send, Ref, undefined, Frame}
) ->
    Reply = send_msg(State, Frame),
    gen_server:reply(From, Reply),
    State;
handle_call(
    #state{protocol_state = #h1_state{h1_ws = #ws_state{ref = Ref}}} = State,
    From,
    {ws_send_async, Ref, undefined, Frame}
) ->
    Reply = send_msg(State, Frame),
    gen_server:reply(From, Reply),
    State;
handle_call(State, From, {Tag, _StaleRef, _, _}) when
    Tag =:= ws_send; Tag =:= ws_send_async
->
    gen_server:reply(From, {error, gone}),
    State;
handle_call(State, From, _Other) ->
    gen_server:reply(From, {error, badarg}),
    State.

-spec handle_cast_loop(pid(), [sys:debug_option()], #state{}, term()) -> no_return().
handle_cast_loop(Parent, Debug, State, {ws_info, undefined, Msg}) ->
    dispatch_info_loop(Parent, Debug, State, Msg);
handle_cast_loop(
    _Parent,
    _Debug,
    #state{protocol_state = #h1_state{h1_ws = #ws_state{ref = Ref}}} = State,
    {ws_close, Ref, undefined, Code, Reason, _Opts}
) ->
    send_close(State, Code, Reason),
    finish({local, Code, Reason}, normal, State);
handle_cast_loop(Parent, Debug, State, {ws_close, _StaleRef, _, _, _, _}) ->
    loop(Parent, Debug, State);
handle_cast_loop(Parent, Debug, State, _Other) ->
    loop(Parent, Debug, State).

-spec handle_info_loop(pid(), [sys:debug_option()], #state{}, term()) -> no_return().
handle_info_loop(Parent, Debug, State, Msg) ->
    dispatch_info_loop(Parent, Debug, State, Msg).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - VIEW + ACTION INTERPRETER
%%%-----------------------------------------------------------------------------
-spec apply_actions(#state{}, nhttp_conn_ws:actions()) ->
    #state{} | no_return().
apply_actions(State, Actions) ->
    case interpret(Actions, State) of
        {ok, NewState} ->
            NewState;
        {close, NewState, WsReason} ->
            finish(WsReason, ws_reason_to_exit(WsReason), NewState)
    end.

-spec from_view(nhttp_conn_ws:session_view(), #ws_state{}) -> #ws_state{}.
from_view(View, Ws) ->
    #{handler_state := SState, runtime_opts := Opts} = View,
    #{deliver_ping := DP, deliver_pong := DG, max_message_size := MS} = Opts,
    Ws#ws_state{
        handler_state = SState,
        deliver_ping = DP,
        deliver_pong = DG,
        max_message_size = MS
    }.

-spec interpret(nhttp_conn_ws:actions(), #state{}) ->
    {ok, #state{}} | {close, #state{}, nhttp_handler:ws_close_reason()}.
interpret([], State) ->
    {ok, State};
interpret([{send_frame, Msg} | Rest], State) ->
    send_msg_async(State, Msg),
    interpret(Rest, State);
interpret([{send_close, no_code, _} | Rest], State) ->
    send_close_no_code(State),
    interpret(Rest, State);
interpret([{send_close, Code, Reason} | Rest], State) ->
    send_close(State, Code, Reason),
    interpret(Rest, State);
interpret(
    [{update_view, View} | Rest], #state{protocol_state = #h1_state{h1_ws = Ws} = H1} = State
) ->
    NewWs = from_view(View, Ws),
    interpret(Rest, State#state{protocol_state = H1#h1_state{h1_ws = NewWs}});
interpret([{close_session, WsReason} | _Rest], State) ->
    {close, State, WsReason}.

-spec view(#ws_state{}) -> nhttp_conn_ws:session_view().
view(#ws_state{
    session = Session,
    handler_state = SState,
    deliver_ping = DP,
    deliver_pong = DG,
    max_message_size = MS
}) ->
    #{
        handler_state => SState,
        runtime_opts => #{
            deliver_ping => DP,
            deliver_pong => DG,
            max_message_size => MS
        },
        session => Session
    }.

-spec ws_reason_to_exit(nhttp_handler:ws_close_reason()) -> term().
ws_reason_to_exit({fail, _, ReasonBin}) -> {ws_error, ReasonBin};
ws_reason_to_exit(_) -> normal.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SEND HELPERS
%%%-----------------------------------------------------------------------------
-spec send_close(#state{}, nhttp_ws:close_code(), binary()) -> ok.
send_close(State, Code, Reason) ->
    nhttp_conn:sock_send(State, nhttp_ws:encode({close, Code, Reason})).

-spec send_close_no_code(#state{}) -> ok.
send_close_no_code(State) ->
    nhttp_conn:sock_send(State, nhttp_ws:encode(close)).

-spec send_msg(#state{}, nhttp_ws:ws_message()) -> ok | {error, term()}.
send_msg(#state{socket = Socket}, Msg) ->
    nhttp_sock:send(Socket, nhttp_ws:encode(Msg)).

-spec send_msg_async(#state{}, nhttp_ws:ws_message()) -> ok.
send_msg_async(State, Msg) ->
    case send_msg(State, Msg) of
        ok ->
            ok;
        {error, Reason} ->
            nhttp_log:ws_send_failed(nhttp_conn:log_ctx(State), Reason),
            ok
    end.
