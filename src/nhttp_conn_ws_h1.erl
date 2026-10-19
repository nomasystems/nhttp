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
response is on the wire and the socket is in `{packet, raw}, {active, N}`
mode. The loop arms the socket again on each `tcp_passive` or `ssl_passive`.

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
        ok -> loop(Parent, Debug, State);
        expired -> ws_receive(Parent, Debug, State, nhttp_conn:idle_deadline(0))
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - WEBSOCKET LOOP
%%%-----------------------------------------------------------------------------
-doc """
Frame-wait block point. The receive waits the `hibernate` timeout, capped
by the idle deadline. A session that is exchanging frames re-enters here
per frame and never hibernates. One that saw no traffic for that long
hibernates for the rest of the idle budget, carried across the
hibernation by the timer from `nhttp_conn:start_hibernate_timer/1`.
Incoming frames (including ping/pong) and async-API casts wake it.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, #state{idle_timeout = IdleTimeout} = State) ->
    ws_receive(Parent, Debug, State, nhttp_conn:idle_deadline(IdleTimeout)).

-spec hibernate(pid(), [sys:debug_option()], #state{}, timeout()) -> no_return().
hibernate(Parent, Debug, State, IdleRemaining) ->
    Timer = nhttp_conn:start_hibernate_timer(IdleRemaining),
    proc_lib:hibernate(?MODULE, ws_wake, [Parent, Debug, State, Timer]).

-spec quiescent(pid(), [sys:debug_option()], #state{}, nhttp_conn:deadline()) -> no_return().
quiescent(Parent, Debug, State, IdleDeadline) ->
    case nhttp_conn:deadline_remaining(IdleDeadline) of
        0 ->
            send_close(State, ?WS_CLOSE_GOING_AWAY, <<"Idle timeout">>),
            finish(timeout, idle_timeout, State);
        IdleRemaining ->
            case nhttp_conn:mailbox_empty() of
                true -> hibernate(Parent, Debug, State, IdleRemaining);
                false -> ws_receive(Parent, Debug, State, IdleDeadline)
            end
    end.

-spec ws_receive(pid(), [sys:debug_option()], #state{}, nhttp_conn:deadline()) -> no_return().
ws_receive(Parent, Debug, #state{hibernate_after = HibernateAfter} = State, IdleDeadline) ->
    receive
        {tcp, _Socket, Data} ->
            handle_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_data(Parent, Debug, State, Data);
        {tcp_passive, _Socket} ->
            activate(Parent, Debug, State);
        {ssl_passive, _Socket} ->
            activate(Parent, Debug, State);
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
    after nhttp_conn:quiescence_wait(HibernateAfter, IdleDeadline) ->
        quiescent(Parent, Debug, State, IdleDeadline)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - LIFECYCLE
%%%-----------------------------------------------------------------------------
-doc """
Build the frame decoder from the runtime opts the open callback settled
on. Armed here rather than at session construction because
`max_message_size` bounds what the decoder accumulates, and until
`handle_ws_open/2` returns there is no limit to give it.
""".
-spec arm_decoder(#state{}) -> #state{}.
arm_decoder(#state{protocol_state = #h1_state{h1_ws = #ws_state{} = Ws} = H1} = State) ->
    Dec = nhttp_ws:decoder_new(server, nhttp_conn_ws:runtime_opts(Ws)),
    State#state{protocol_state = H1#h1_state{ws_decoder = Dec}}.

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
    WsReason,
    #state{handler = Handler, protocol_state = #h1_state{h1_ws = #ws_state{} = Ws} = H1} = State
) ->
    #ws_state{handler_state = SState} =
        nhttp_conn_ws:close_lifecycle(nhttp_conn:log_ctx(State), WsReason, Ws, Handler),
    ok = nhttp_stats:decr_ws_session(),
    State#state{handler_state = SState, protocol_state = H1#h1_state{h1_ws = undefined}}.

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
    ok = nhttp_stats:incr_ws_session(),
    {Ws1, Actions} = nhttp_conn_ws:open(nhttp_conn:log_ctx(State), Ws0, Handler),
    arm_decoder(apply_actions(State#state{protocol_state = H1#h1_state{h1_ws = Ws1}}, Actions)).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - INCOMING DATA
%%%-----------------------------------------------------------------------------
-spec activate(pid(), [sys:debug_option()], #state{}) -> no_return().
activate(Parent, Debug, State) ->
    case nhttp_conn:activate_n(State, []) of
        ok -> loop(Parent, Debug, State);
        {stop, Reason} -> finish({transport, closed}, Reason, State)
    end.

-spec dispatch_frames(#state{}, [nhttp_ws:ws_message()], iodata()) -> {#state{}, iodata()}.
dispatch_frames(State, [], Out) ->
    {State, Out};
dispatch_frames(
    #state{handler = Handler, protocol_state = #h1_state{h1_ws = #ws_state{} = Ws} = H1} = State,
    [Message | Rest],
    Out
) ->
    {Ws1, Actions} = nhttp_conn_ws:dispatch_frame(nhttp_conn:log_ctx(State), Message, Ws, Handler),
    State1 = State#state{protocol_state = H1#h1_state{h1_ws = Ws1}},
    case encode_actions(Actions, Out) of
        {ok, Out1} -> dispatch_frames(State1, Rest, Out1);
        {close, Out1, WsReason} -> flush_and_finish(State1, Out1, WsReason)
    end.

-doc """
Decode one socket read and dispatch every complete message in it. The
frames the read produces leave in one socket write after the last
message, so a read that holds many frames costs one send, and control
replies and the close keep their order within it.
""".
-spec handle_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_data(
    Parent, Debug, #state{protocol_state = #h1_state{ws_decoder = Dec} = H1} = State, Data
) ->
    case nhttp_ws:feed(Data, Dec) of
        {ok, Messages, NewDec} ->
            State1 = State#state{protocol_state = H1#h1_state{ws_decoder = NewDec}},
            {State2, Out} = dispatch_frames(State1, Messages, []),
            ok = flush(State2, Out),
            loop(Parent, Debug, State2);
        {error, Reason, Messages} ->
            {State1, Out} = dispatch_frames(State, Messages, []),
            {Code, ReasonBin} = nhttp_conn_ws:decode_error_close(Reason),
            ok = flush(State1, [Out, nhttp_ws:encode({close, Code, ReasonBin})]),
            finish({fail, Code, ReasonBin}, {ws_error, Reason}, State1)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - CALL / CAST / INFO
%%%-----------------------------------------------------------------------------
-spec dispatch_info_loop(pid(), [sys:debug_option()], #state{}, term()) -> no_return().
dispatch_info_loop(
    Parent,
    Debug,
    #state{handler = Handler, protocol_state = #h1_state{h1_ws = #ws_state{} = Ws} = H1} = State,
    Msg
) ->
    {Ws1, Actions} = nhttp_conn_ws:dispatch_info(nhttp_conn:log_ctx(State), Msg, Ws, Handler),
    State1 = apply_actions(State#state{protocol_state = H1#h1_state{h1_ws = Ws1}}, Actions),
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
%% INTERNAL FUNCTIONS - ACTION INTERPRETER
%%%-----------------------------------------------------------------------------
-spec apply_actions(#state{}, nhttp_conn_ws:actions()) ->
    #state{} | no_return().
apply_actions(State, Actions) ->
    case encode_actions(Actions, []) of
        {ok, Out} ->
            ok = flush(State, Out),
            State;
        {close, Out, WsReason} ->
            flush_and_finish(State, Out, WsReason)
    end.

-doc """
Append the wire bytes of each send action to `Out`, in order, up to the
first `close_session`.
""".
-spec encode_actions(nhttp_conn_ws:actions(), iodata()) ->
    {ok, iodata()} | {close, iodata(), nhttp_handler:ws_close_reason()}.
encode_actions([], Out) ->
    {ok, Out};
encode_actions([{send_frame, Msg} | Rest], Out) ->
    encode_actions(Rest, [Out, nhttp_ws:encode(Msg)]);
encode_actions([{send_close, no_code, _} | Rest], Out) ->
    encode_actions(Rest, [Out, nhttp_ws:encode(close)]);
encode_actions([{send_close, Code, Reason} | Rest], Out) ->
    encode_actions(Rest, [Out, nhttp_ws:encode({close, Code, Reason})]);
encode_actions([{close_session, WsReason} | _Rest], Out) ->
    {close, Out, WsReason}.

-spec flush_and_finish(#state{}, iodata(), nhttp_handler:ws_close_reason()) -> no_return().
flush_and_finish(State, Out, WsReason) ->
    ok = flush(State, Out),
    finish(WsReason, ws_reason_to_exit(WsReason), State).

-spec ws_reason_to_exit(nhttp_handler:ws_close_reason()) -> term().
ws_reason_to_exit({fail, _, ReasonBin}) -> {ws_error, ReasonBin};
ws_reason_to_exit(_) -> normal.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SEND HELPERS
%%%-----------------------------------------------------------------------------
-spec flush(#state{}, iodata()) -> ok.
flush(_State, []) ->
    ok;
flush(#state{socket = Socket} = State, Out) ->
    case nhttp_sock:send(Socket, Out) of
        ok ->
            ok;
        {error, Reason} ->
            nhttp_log:ws_send_failed(nhttp_conn:log_ctx(State), Reason),
            ok
    end.

-spec send_close(#state{}, nhttp_ws:close_code(), binary()) -> ok.
send_close(State, Code, Reason) ->
    nhttp_conn:sock_send(State, nhttp_ws:encode({close, Code, Reason})).

-doc """
Write one frame for the session API and name the outcome. A socket that
refuses the write no longer carries the session, so the caller sees
`gone`, the same answer it gets once the connection process is down.
""".
-spec send_msg(#state{}, nhttp_ws:ws_message()) -> ok | {error, gone}.
send_msg(#state{socket = Socket} = State, Msg) ->
    case nhttp_sock:send(Socket, nhttp_ws:encode(Msg)) of
        ok ->
            ok;
        {error, Reason} ->
            nhttp_log:ws_send_failed(nhttp_conn:log_ctx(State), Reason),
            {error, gone}
    end.
