-module(nhttp_conn_h3).

-moduledoc false.

-behaviour(nhttp_protocol).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn_h3.hrl").
-include("nhttp_defaults.hrl").
-include("nhttp_proc_lib.hrl").
-include("nhttp_status_codes.hrl").

-compile({inline, [stream_id_to_type/1]}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    drain/1,
    start_link/1
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS (USED BY NHTTP_CONN_H3_PUSH AND NHTTP_CONN_WS_H3)
%%%-----------------------------------------------------------------------------
-export([
    emit_request_stop/3,
    emit_request_stop_with_size/4,
    emit_stream_start/2,
    h3_error_to_code/1,
    log_ctx/1,
    reset_stream/3,
    send_h3_data/4,
    send_h3_error_response/3,
    send_h3_headers/4
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    h3_wake/2,
    init_handler/1,
    loop/3,
    system_code_change/4,
    system_continue/3,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% LOCAL TYPES
%%%-----------------------------------------------------------------------------
-type h3_init_args() :: #{
    name := term(),
    opts := nhttp:opts(),
    parent := pid(),
    handler := module(),
    handler_args := term()
}.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(MAX_DRAIN_PACKETS, 64).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Signal the connection to drain.
Sends GOAWAY and closes after in-flight streams complete.
""".
-spec drain(pid()) -> ok.
drain(Pid) ->
    Pid ! shutdown,
    ok.

-doc """
`conn_handler` entry point.
nquic starts this process under its partition supervisor for each new
connection (owner-from-first-packet) and forwards the first packet. The
process is the connection owner from packet 1: it seeds the server
context with `nquic_lib:server_accept_init/1`, registers its own CIDs,
and drives the handshake itself before entering the H3 connection loop.
There is no accept queue or `takeover/1`, so the connection's CID never
resolves to a non-owner.
""".
-spec start_link(map()) -> {ok, pid()}.
start_link(NquicOpts) ->
    proc_lib:start_link(?MODULE, init_handler, [NquicOpts]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init_handler(map()) -> no_return().
init_handler(NquicOpts) ->
    process_flag(trap_exit, true),
    HandlerOpts = maps:get(conn_handler_opts, NquicOpts),
    {ok, Ctx} = nquic_lib:server_accept_init(NquicOpts),
    proc_lib:init_ack({ok, self()}),
    ok = maybe_track(HandlerOpts),
    InitArgs = #{
        name => maps:get(name, HandlerOpts, undefined),
        opts => HandlerOpts,
        parent => proc_lib_parent(),
        handler => maps:get(handler, HandlerOpts),
        handler_args => maps:get(handler_args, HandlerOpts, [])
    },
    handshake_loop(InitArgs, Ctx, initial).

-doc """
`nhttp_protocol` loop entry. The H3 conn carries `Parent` and `Debug`
inside its `#state{}` (the loop is entered from `init_handler/1`, not
from `nhttp_conn` classification), so this stores them back and enters
`connection_loop/1`.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, State) ->
    connection_loop(State#state{parent = Parent, debug = Debug}).

-doc """
`proc_lib:hibernate/3` re-entry point for the event-wait hibernation.
Cancels the idle timer armed before hibernating; when it already fired
the wake-up is an idle timeout unless other traffic is queued (drained
by the zero-timeout receive).
""".
-spec h3_wake(#state{}, nhttp_conn:hibernate_timer()) -> no_return().
h3_wake(State, Timer) ->
    case nhttp_conn:cancel_hibernate_timer(Timer) of
        ok -> h3_receive(State, State#state.idle_timeout);
        expired -> h3_receive(State, 0)
    end.

-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
?NHTTP_SYSTEM_CODE_CHANGE_NOOP.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(Parent, Debug, State) ->
    loop(Parent, Debug, State).

-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HANDSHAKE (OWNER-FROM-FIRST-PACKET)
%%%-----------------------------------------------------------------------------
-spec handshake_loop(h3_init_args(), nquic:ctx(), initial | handshake) -> no_return().
handshake_loop(InitArgs, Ctx, Phase) ->
    receive
        {packet, Source, Bin} ->
            hs_packet(InitArgs, nquic_lib:handle_packet(Ctx, Source, Bin), Phase);
        {packet, Source, Bin, ECN} ->
            hs_packet(InitArgs, nquic_lib:handle_packet(Ctx, Source, Bin, ECN), Phase);
        {packet_batch, Source, Buf, GsoSize, ECN} ->
            hs_packet(
                InitArgs, nquic_lib:handle_packet_batch(Ctx, Source, Buf, GsoSize, ECN), Phase
            );
        {quic_timeout, Type} ->
            hs_timeout(InitArgs, nquic_lib:handshake_timeout(Ctx, Phase, Type), Phase);
        {quic_drain, _Listener} ->
            close_and_exit(Ctx)
    end.

-spec hs_dispatch(h3_init_args(), [nquic_protocol:event()], nquic:ctx(), initial | handshake) ->
    no_return().
hs_dispatch(InitArgs, Events, Ctx, Phase) ->
    case lists:member(connected, Events) of
        true ->
            setup_h3_connection(InitArgs, Ctx);
        false ->
            handshake_loop(InitArgs, Ctx, next_phase(Events, Phase))
    end.

-spec hs_packet(
    h3_init_args(),
    {ok, [nquic_protocol:event()], nquic:ctx()} | {error, term(), nquic:ctx()},
    initial | handshake
) -> no_return().
hs_packet(InitArgs, {ok, Events, Ctx1}, Phase) ->
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    hs_dispatch(InitArgs, Events, Ctx2, Phase);
hs_packet(_InitArgs, {error, Reason, Ctx1}, _Phase) ->
    close_and_exit(Ctx1, nquic_protocol:error_code(Reason)).

-spec hs_timeout(
    h3_init_args(),
    {ok, [nquic_protocol:event()], nquic:ctx()} | {error, term(), nquic:ctx()},
    initial | handshake
) -> no_return().
hs_timeout(InitArgs, {ok, Events, Ctx1}, Phase) ->
    hs_dispatch(InitArgs, Events, Ctx1, Phase);
hs_timeout(_InitArgs, {error, Reason, Ctx1}, _Phase) ->
    close_and_exit(Ctx1, nquic_protocol:error_code(Reason)).

-spec maybe_track(map()) -> ok.
maybe_track(#{registry := Tab}) ->
    case nhttp_registry:lookup_conn_tracker(Tab) of
        undefined -> ok;
        TrackerPid -> nhttp_conn_tracker:track(TrackerPid, self())
    end;
maybe_track(_HandlerOpts) ->
    ok.

-spec next_phase([nquic_protocol:event()], initial | handshake) -> initial | handshake.
next_phase(Events, Phase) ->
    case lists:keyfind(state_transition, 1, Events) of
        {state_transition, handshake} -> handshake;
        _ -> Phase
    end.

-spec proc_lib_parent() -> pid().
proc_lib_parent() ->
    case get('$ancestors') of
        [Parent | _] when is_pid(Parent) -> Parent
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ACTION EXECUTION
%%%-----------------------------------------------------------------------------
-spec execute_h3_actions(#state{}, [nhttp_h3:action()]) -> #state{}.
execute_h3_actions(State, []) ->
    State;
execute_h3_actions(State, [{send, StreamId, Data} | Rest]) ->
    case nquic_lib:send(State#state.ctx, StreamId, Data) of
        {ok, Ctx1} ->
            execute_h3_actions(State#state{ctx = Ctx1}, Rest);
        {error, _, Ctx1} ->
            execute_h3_actions(State#state{ctx = Ctx1}, Rest);
        {error, _} ->
            execute_h3_actions(State, Rest)
    end;
execute_h3_actions(State, [{send_fin, StreamId, Data} | Rest]) ->
    case nquic_lib:send_fin_noflush(State#state.ctx, StreamId, Data) of
        {ok, Ctx1} ->
            execute_h3_actions(State#state{ctx = Ctx1}, Rest);
        {error, _, Ctx1} ->
            execute_h3_actions(State#state{ctx = Ctx1}, Rest);
        {error, _} ->
            execute_h3_actions(State, Rest)
    end;
execute_h3_actions(State, [{close_connection, Code, _Reason} | _]) ->
    ok = nquic_lib:shutdown(State#state.ctx, Code, <<>>),
    terminate({h3_connection_error, Code}, State#state{closing = true}).

-spec execute_init_actions(nquic:ctx(), [nhttp_h3:action()]) -> nquic:ctx().
execute_init_actions(Ctx, []) ->
    Ctx;
execute_init_actions(Ctx, [{send, StreamId, Data} | Rest]) ->
    case nquic_lib:send(Ctx, StreamId, Data) of
        {ok, Ctx1} ->
            execute_init_actions(Ctx1, Rest);
        {error, _, Ctx1} ->
            execute_init_actions(Ctx1, Rest);
        {error, _} ->
            execute_init_actions(Ctx, Rest)
    end;
execute_init_actions(Ctx, [{send_fin, StreamId, Data} | Rest]) ->
    case nquic_lib:send_fin_noflush(Ctx, StreamId, Data) of
        {ok, Ctx1} ->
            execute_init_actions(Ctx1, Rest);
        {error, _, Ctx1} ->
            execute_init_actions(Ctx1, Rest);
        {error, _} ->
            execute_init_actions(Ctx, Rest)
    end;
execute_init_actions(Ctx, [{close_connection, Code, _Reason} | _]) ->
    ok = nquic_lib:shutdown(Ctx, Code, <<>>),
    ok = nhttp_stats:decr_connection(),
    cleanup_ctx(Ctx),
    exit(normal).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - COMPRESSION
%%%-----------------------------------------------------------------------------
-spec compress_config(#state{}) -> nhttp_conn_compress:config().
compress_config(#state{
    compression_enabled = Enabled,
    compression_level = Level,
    compression_threshold = Threshold,
    compress_mime_types = MimeTypes
}) ->
    nhttp_conn_compress:config(Enabled, Level, Threshold, MimeTypes).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - CONNECTION LOOP
%%%-----------------------------------------------------------------------------
-spec connection_loop(#state{}) -> no_return().
connection_loop(State) ->
    case nquic_lib:ctx_connected(State#state.ctx) of
        true ->
            connection_loop_connected(State);
        false ->
            connection_loop_receive(State)
    end.

-spec connection_loop_connected(#state{}) -> no_return().
connection_loop_connected(State) ->
    Socket = nquic_lib:ctx_socket(State#state.ctx),
    case socket:recvfrom(Socket, 0, nowait) of
        {ok, {Source, Bin}} ->
            handle_packet_and_drain(State, Source, Bin);
        {select, _} ->
            connection_loop_receive(State);
        {error, _} ->
            connection_loop_receive(State)
    end.

-doc """
Event-wait block point. A quiescent connection (no streams, no push
workers, not closing, empty mailbox) hibernates here. nquic arms its
protocol timers via `erlang:send_after` to this process, so QUIC timer
messages wake a hibernated connection like any packet. The idle deadline
is carried across hibernation by the timer from
`nhttp_conn:start_hibernate_timer/1`.
""".
-spec connection_loop_receive(#state{}) -> no_return().
connection_loop_receive(State) ->
    case hibernate_eligible(State) of
        true -> hibernate(State);
        false -> h3_receive(State, State#state.idle_timeout)
    end.

-spec hibernate_eligible(#state{}) -> boolean().
hibernate_eligible(#state{closing = false, h3_streams = Streams, h3_push_workers = Workers}) ->
    map_size(Streams) =:= 0 andalso map_size(Workers) =:= 0 andalso
        nhttp_conn:mailbox_empty();
hibernate_eligible(#state{}) ->
    false.

-spec hibernate(#state{}) -> no_return().
hibernate(#state{idle_timeout = IdleTimeout} = State) ->
    Timer = nhttp_conn:start_hibernate_timer(IdleTimeout),
    proc_lib:hibernate(?MODULE, h3_wake, [State, Timer]).

-spec h3_receive(#state{}, timeout()) -> no_return().
h3_receive(#state{parent = Parent} = State, IdleTimeout) ->
    receive
        {packet, Source, Bin} ->
            handle_packet_and_drain(State, Source, Bin);
        {packet, Source, Bin, ECN} ->
            handle_packet_ecn_and_drain(State, Source, Bin, ECN);
        {packet_batch, Source, Buf, GsoSize, ECN} ->
            handle_packet_batch_and_drain(State, Source, Buf, GsoSize, ECN);
        {'$socket', _Socket, select, _SelectInfo} ->
            State1 = drain_connected_socket(State, ?MAX_DRAIN_PACKETS),
            flush_batch(State1);
        {quic_timeout, Type} ->
            handle_quic_timeout(State, Type);
        {send_chunk, WPid, Ref, Data} ->
            State1 = nhttp_conn_h3_push:handle_h3_push_send_chunk(State, WPid, Ref, Data, nofin),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {stream_done, WPid, Ref} ->
            State1 = nhttp_conn_h3_push:handle_h3_push_send_chunk(State, WPid, Ref, <<>>, fin),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {send_trailers, WPid, Ref, Trailers} ->
            State1 = nhttp_conn_h3_push:handle_h3_push_send_trailers(State, WPid, Ref, Trailers),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {'DOWN', _MRef, process, WPid, Reason} ->
            State1 = nhttp_conn_h3_push:handle_h3_push_worker_down(State, WPid, Reason),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {'$gen_call', From, Request} ->
            State1 = nhttp_conn_ws_h3:handle_call(State, From, Request),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {'$gen_cast', Msg} ->
            State1 = nhttp_conn_ws_h3:handle_cast(State, Msg),
            State2 = flush_ctx_state(State1),
            connection_loop(State2);
        {system, From, Request} ->
            sys:handle_system_msg(
                Request, From, Parent, ?MODULE, State#state.debug, State
            );
        shutdown ->
            graceful_shutdown(State);
        {quic_drain, _Listener} ->
            graceful_shutdown(State);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State);
        Info ->
            State1 = nhttp_conn_ws_h3:handle_info(State, Info),
            State2 = flush_ctx_state(State1),
            connection_loop(State2)
    after IdleTimeout ->
        terminate(idle_timeout, State)
    end.

-spec drain_connected_socket(#state{}, non_neg_integer()) -> #state{}.
drain_connected_socket(State, 0) ->
    State;
drain_connected_socket(State, N) ->
    Socket = nquic_lib:ctx_socket(State#state.ctx),
    case socket:recvfrom(Socket, 0, nowait) of
        {ok, {Source, Bin}} ->
            handle_drained_packet(State, Source, Bin, N);
        {select, _} ->
            State;
        {error, _} ->
            State
    end.

-spec drain_packets(#state{}, non_neg_integer()) -> #state{}.
drain_packets(State, 0) ->
    State;
drain_packets(State, N) ->
    receive
        {packet, Source, Bin} ->
            case nquic_lib:handle_packet(State#state.ctx, Source, Bin) of
                {ok, Events, Ctx1} ->
                    State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
                    drain_packets(State1, N - 1);
                {error, Reason, Ctx1} ->
                    handle_connection_error(State#state{ctx = Ctx1}, Reason)
            end
    after 0 ->
        case nquic_lib:ctx_connected(State#state.ctx) of
            true -> drain_connected_socket(State, N);
            false -> State
        end
    end.

-spec flush_batch(#state{}) -> no_return().
flush_batch(State) ->
    {ok, Ctx1} = nquic_lib:flush_notimers(State#state.ctx),
    connection_loop(State#state{ctx = Ctx1}).

-spec flush_ctx_state(#state{}) -> #state{}.
flush_ctx_state(State) ->
    {ok, Ctx1} = nquic_lib:flush(State#state.ctx),
    State#state{ctx = Ctx1}.

-spec handle_drained_packet(
    #state{}, nquic_socket:sockaddr(), binary(), non_neg_integer()
) -> #state{}.
handle_drained_packet(State, Source, Bin, N) ->
    case nquic_lib:handle_packet(State#state.ctx, Source, Bin) of
        {ok, Events, Ctx1} ->
            State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
            drain_connected_socket(State1, N - 1);
        {error, Reason, Ctx1} ->
            handle_connection_error(State#state{ctx = Ctx1}, Reason)
    end.

-spec handle_packet_and_drain(#state{}, nquic_socket:sockaddr(), binary()) ->
    no_return().
handle_packet_and_drain(State, Source, Bin) ->
    case nquic_lib:handle_packet(State#state.ctx, Source, Bin) of
        {ok, Events, Ctx1} ->
            State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
            State2 = drain_packets(State1, ?MAX_DRAIN_PACKETS),
            flush_batch(State2);
        {error, Reason, Ctx1} ->
            handle_connection_error(State#state{ctx = Ctx1}, Reason)
    end.

-spec handle_packet_batch_and_drain(
    #state{}, nquic_socket:sockaddr(), binary(), pos_integer(), nquic_socket:ecn_mark()
) -> no_return().
handle_packet_batch_and_drain(State, Source, Buf, GsoSize, ECN) ->
    {ok, Events, Ctx1} = nquic_lib:handle_packet_batch(State#state.ctx, Source, Buf, GsoSize, ECN),
    State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
    State2 = drain_packets(State1, ?MAX_DRAIN_PACKETS),
    flush_batch(State2).

-spec handle_packet_ecn_and_drain(
    #state{}, nquic_socket:sockaddr(), binary(), nquic_socket:ecn_mark()
) -> no_return().
handle_packet_ecn_and_drain(State, Source, Bin, ECN) ->
    case nquic_lib:handle_packet(State#state.ctx, Source, Bin, ECN) of
        {ok, Events, Ctx1} ->
            State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
            State2 = drain_packets(State1, ?MAX_DRAIN_PACKETS),
            flush_batch(State2);
        {error, Reason, Ctx1} ->
            handle_connection_error(State#state{ctx = Ctx1}, Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - DISPATCH
%%%-----------------------------------------------------------------------------
-doc """
Transition a stream into the accept_body state. The conn keeps the
stream entry alive so subsequent DATA / trailer events can drive
`handle_request_body/3` via `apply_body_event/4`. The stream record's
`req_span` is preserved so the eventual terminal result can emit a
single matching `emit_request_stop` (or one of the abort variants).
""".
-spec accept_body_for_stream(
    #state{},
    nhttp_lib:stream_id(),
    term(),
    {nhttp_otel:span_ctx(), integer()},
    term()
) -> #state{}.
accept_body_for_stream(
    #state{h3_streams = Streams} = State, StreamId, BodyState, ReqSpan, NewHState
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{} = Stream0 ->
            Stream1 = Stream0#h3_stream{
                accept_body = true,
                body_state = BodyState,
                req_span = ReqSpan
            },
            State#state{
                h3_streams = Streams#{StreamId => Stream1},
                handler_state = NewHState
            };
        undefined ->
            emit_request_stop(State, ?HTTP_CLIENT_CLOSED_REQUEST, ReqSpan),
            State#state{handler_state = NewHState}
    end.

-doc """
Apply one body event to the handler. Threads the body state across
chunks. On the first non-`accept_body` return the stream finishes via
`apply_h3_request_result/5`.
""".
-spec apply_body_event(
    #state{}, nhttp_lib:stream_id(), #h3_stream{}, nhttp_handler:body_event()
) -> #state{}.
apply_body_event(
    #state{handler = Handler, handler_state = HState} = State,
    StreamId,
    #h3_stream{request = Request, body_state = BodyState, req_span = ReqSpan},
    Event
) ->
    Result = nhttp_handler:safe_handle_request_body(
        Handler, Event, BodyState, HState
    ),
    apply_body_event_result(State, StreamId, Request, ReqSpan, Event, Result).

-spec apply_body_event_result(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:request(),
    {nhttp_otel:span_ctx(), integer()},
    nhttp_handler:body_event(),
    term()
) -> #state{}.
apply_body_event_result(
    State, StreamId, _Request, _ReqSpan, {fin, _}, {accept_body, _NewBodyState, NewHState}
) ->
    reset_stream_with_handler(State, StreamId, NewHState, h3_request_incomplete);
apply_body_event_result(
    State, StreamId, _Request, _ReqSpan, {abort, _}, {accept_body, _NewBodyState, NewHState}
) ->
    reset_stream_with_handler(State, StreamId, NewHState, h3_request_cancelled);
apply_body_event_result(
    #state{h3_streams = Streams} = State,
    StreamId,
    _Request,
    _ReqSpan,
    {data, _},
    {accept_body, NewBodyState, NewHState}
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{} = Stream ->
            Stream1 = Stream#h3_stream{body_state = NewBodyState},
            State#state{
                h3_streams = Streams#{StreamId => Stream1},
                handler_state = NewHState
            };
        undefined ->
            State#state{handler_state = NewHState}
    end;
apply_body_event_result(State, StreamId, Request, ReqSpan, _Event, Result) ->
    State1 = apply_h3_request_result(State, StreamId, Request, ReqSpan, Result),
    maybe_remove_stream_after_terminal(State1, StreamId).

-spec apply_h3_request_result(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:request(),
    {nhttp_otel:span_ctx(), integer()},
    term()
) -> #state{}.
apply_h3_request_result(
    State, StreamId, Request, ReqSpan, {reply, #{status := Status} = Response0, NewHState}
) ->
    Response = nhttp_conn_compress:maybe_compress(
        Response0, Request, compress_config(State)
    ),
    NewState = send_h3_response(State, StreamId, Response),
    emit_request_stop(NewState, Status, ReqSpan),
    NewState#state{
        handler_state = NewHState,
        requests_count = State#state.requests_count + 1
    };
apply_h3_request_result(
    State, StreamId, Request, ReqSpan, {stream, {producer, Status, Headers, Producer}, NewHState}
) ->
    nhttp_conn_h3_push:dispatch_h3_stream_push(
        State, StreamId, Request, Status, Headers, Producer, NewHState, ReqSpan
    );
apply_h3_request_result(State, StreamId, _Request, ReqSpan, {accept_body, BodyState, NewHState}) ->
    accept_body_for_stream(State, StreamId, BodyState, ReqSpan, NewHState);
apply_h3_request_result(State, StreamId, _Request, ReqSpan, {upgrade, websocket, _NewHState}) ->
    State1 = reset_stream(State, StreamId, h3_error_to_code(h3_internal_error)),
    emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State1;
apply_h3_request_result(
    State, StreamId, _Request, ReqSpan, {upgrade, websocket, _SessionOpts, _NewHState}
) ->
    State1 = reset_stream(State, StreamId, h3_error_to_code(h3_internal_error)),
    emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State1;
apply_h3_request_result(State, StreamId, _Request, ReqSpan, {abort, _Reason, NewHState}) ->
    State1 = reset_stream(State, StreamId, h3_error_to_code(h3_internal_error)),
    emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State1#state{handler_state = NewHState};
apply_h3_request_result(
    State,
    StreamId,
    Request,
    ReqSpan,
    {nhttp_handler_exception, Class, Reason}
) ->
    Ctx = nhttp_log:request_ctx(log_ctx(State), Request, StreamId),
    nhttp_log:handler_crashed(Ctx, handle_request, Class, Reason),
    nhttp_conn:emit_handler_exception(ReqSpan, Class, Reason),
    reset_stream(State, StreamId, h3_error_to_code(h3_internal_error)).

-spec dispatch_request(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) -> #state{}.
dispatch_request(#state{limits = Limits} = State, StreamId, Request) ->
    case nhttp_limits:validate_request(Request, Limits) of
        ok ->
            dispatch_request_validated(State, StreamId, Request);
        {error, LimitError} ->
            handle_h3_limit_error(State, StreamId, LimitError)
    end.

-spec dispatch_request_validated(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) ->
    #state{}.
dispatch_request_validated(
    #state{handler = Handler, handler_state = HState} = State,
    StreamId,
    Request
) ->
    ReqSpan = emit_request_start(State, StreamId, Request),
    Result = nhttp_handler:safe_handle_request(Handler, Request, HState),
    apply_h3_request_result(State, StreamId, Request, ReqSpan, Result).

-doc """
Dispatch a request whose body is still in flight. Validates the request,
then drives `handle_request/2` with `body => streaming`. On `accept_body`
the conn keeps the stream alive so subsequent DATA / trailer events feed
`handle_request_body/3` directly via `apply_body_event/4`.
""".
-spec dispatch_streaming_request(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) ->
    #state{}.
dispatch_streaming_request(#state{limits = Limits} = State, StreamId, Request) ->
    case nhttp_limits:validate_request(Request, Limits) of
        ok ->
            dispatch_streaming_request_validated(
                State, StreamId, Request#{body => streaming}
            );
        {error, LimitError} ->
            handle_h3_limit_error(State, StreamId, LimitError)
    end.

-spec dispatch_streaming_request_validated(
    #state{}, nhttp_lib:stream_id(), nhttp_lib:request()
) -> #state{}.
dispatch_streaming_request_validated(
    #state{handler = Handler, handler_state = HState, h3_streams = Streams} = State,
    StreamId,
    Request
) ->
    ReqSpan = emit_request_start(State, StreamId, Request),
    Stream = #h3_stream{request = Request, req_span = ReqSpan},
    State1 = State#state{h3_streams = Streams#{StreamId => Stream}},
    Result = nhttp_handler:safe_handle_request(Handler, Request, HState),
    State2 = apply_h3_request_result(State1, StreamId, Request, ReqSpan, Result),
    maybe_remove_stream_after_terminal(State2, StreamId).

-spec handle_h3_limit_error(#state{}, nhttp_lib:stream_id(), nhttp_limits:limit_error()) ->
    #state{}.
handle_h3_limit_error(State, StreamId, body_too_large = E) ->
    State1 = send_h3_error_response(State, StreamId, nhttp_limits:error_to_status(E)),
    reset_stream(State1, StreamId, h3_error_to_code(h3_request_rejected));
handle_h3_limit_error(State, StreamId, LimitError) ->
    send_h3_error_response(State, StreamId, nhttp_limits:error_to_status(LimitError)).

-spec maybe_remove_stream_after_terminal(#state{}, nhttp_lib:stream_id()) -> #state{}.
maybe_remove_stream_after_terminal(#state{h3_streams = Streams} = State, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h3_stream{type = push} ->
            State;
        #h3_stream{accept_body = true} ->
            State;
        _ ->
            State#state{h3_streams = maps:remove(StreamId, Streams)}
    end.

-spec reset_stream_with_handler(#state{}, nhttp_lib:stream_id(), term(), atom()) ->
    #state{}.
reset_stream_with_handler(#state{h3_streams = Streams} = State, StreamId, NewHState, ErrorAtom) ->
    State1 = reset_stream(State, StreamId, h3_error_to_code(ErrorAtom)),
    State1#state{
        handler_state = NewHState,
        h3_streams = maps:remove(StreamId, Streams)
    }.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ERROR CODES
%%%-----------------------------------------------------------------------------
-spec h3_error_to_code(nhttp_h3:h3_error_code()) -> non_neg_integer().
h3_error_to_code(h3_no_error) -> 16#0100;
h3_error_to_code(h3_general_protocol_error) -> 16#0101;
h3_error_to_code(h3_internal_error) -> 16#0102;
h3_error_to_code(h3_stream_creation_error) -> 16#0103;
h3_error_to_code(h3_closed_critical_stream) -> 16#0104;
h3_error_to_code(h3_frame_unexpected) -> 16#0105;
h3_error_to_code(h3_frame_error) -> 16#0106;
h3_error_to_code(h3_excessive_load) -> 16#0107;
h3_error_to_code(h3_id_error) -> 16#0108;
h3_error_to_code(h3_settings_error) -> 16#0109;
h3_error_to_code(h3_missing_settings) -> 16#010a;
h3_error_to_code(h3_request_rejected) -> 16#010b;
h3_error_to_code(h3_request_cancelled) -> 16#010c;
h3_error_to_code(h3_request_incomplete) -> 16#010d;
h3_error_to_code(h3_message_error) -> 16#010e;
h3_error_to_code(h3_connect_error) -> 16#010f;
h3_error_to_code(h3_version_fallback) -> 16#0110;
h3_error_to_code(qpack_decompression_failed) -> 16#0200;
h3_error_to_code(qpack_encoder_stream_error) -> 16#0201;
h3_error_to_code(qpack_decoder_stream_error) -> 16#0202.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - EVENT HANDLING
%%%-----------------------------------------------------------------------------
-spec deliver_h3_fin(#state{}, nhttp_lib:stream_id(), nhttp_lib:headers()) -> #state{}.
deliver_h3_fin(#state{h3_streams = Streams} = State, StreamId, Trailers) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{accept_body = true} = Stream ->
            apply_body_event(State, StreamId, Stream, {fin, Trailers});
        _ ->
            State
    end.

-spec handle_h3_event(#state{}, nhttp_h3:event()) -> #state{}.
handle_h3_event(State, {request, StreamId, Request, fin}) ->
    dispatch_request(State, StreamId, Request);
handle_h3_event(State, {request, StreamId, Request, nofin}) ->
    case nhttp_req:is_websocket_upgrade(Request) of
        true ->
            nhttp_conn_ws_h3:connect(State, StreamId, Request);
        false ->
            dispatch_streaming_request(State, StreamId, Request)
    end;
handle_h3_event(#state{h3_streams = Streams, limits = Limits} = State, {data, StreamId, Data, Fin}) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h3_stream{type = websocket} ->
            nhttp_conn_ws_h3:handle_data(State, StreamId, Data, Fin);
        #h3_stream{accept_body = true, body_size = Size} = Stream ->
            NewSize = Size + byte_size(Data),
            case nhttp_limits:check_body_size(NewSize, Limits) of
                ok ->
                    Stream1 = Stream#h3_stream{body_size = NewSize},
                    State1 = State#state{h3_streams = Streams#{StreamId => Stream1}},
                    State2 = apply_body_event(State1, StreamId, Stream1, {data, Data}),
                    case Fin of
                        fin -> deliver_h3_fin(State2, StreamId, []);
                        nofin -> State2
                    end;
                {error, body_too_large} ->
                    NewStreams = maps:remove(StreamId, Streams),
                    handle_h3_limit_error(
                        State#state{h3_streams = NewStreams}, StreamId, body_too_large
                    )
            end;
        #h3_stream{} ->
            State
    end;
handle_h3_event(State, {trailers, StreamId, Trailers}) ->
    deliver_h3_fin(State, StreamId, Trailers);
handle_h3_event(State, {goaway, _LastStreamId, _ErrorCode, _DebugData}) ->
    State;
handle_h3_event(State, {settings, _Settings}) ->
    State;
handle_h3_event(#state{h3_streams = Streams} = State, {stream_reset, StreamId, ErrorCode}) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = websocket} ->
            nhttp_conn_ws_h3:handle_stream_reset(State, StreamId, ErrorCode);
        #h3_stream{accept_body = true} = Stream ->
            State1 = apply_body_event(
                State, StreamId, Stream, {abort, {peer_reset, ErrorCode}}
            ),
            State1#state{h3_streams = maps:remove(StreamId, State1#state.h3_streams)};
        _ ->
            State#state{h3_streams = maps:remove(StreamId, Streams)}
    end;
handle_h3_event(State, _Event) ->
    State.

-spec handle_h3_events(#state{}, [nhttp_h3:event()]) -> #state{}.
handle_h3_events(State, []) ->
    State;
handle_h3_events(State, [Event | Rest]) ->
    NewState = handle_h3_event(State, Event),
    handle_h3_events(NewState, Rest).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - OPENTELEMETRY
%%%-----------------------------------------------------------------------------
-spec conn_attrs() -> nhttp_conn_otel:conn_attrs().
conn_attrs() ->
    #{transport => udp, version => http3, scheme => <<"https">>}.

-spec emit_connection_start(#state{}) -> #state{}.
emit_connection_start(#state{otel_config = OtelConfig, peer = Peer} = State) ->
    State#state{
        conn_span = nhttp_conn_otel:connection_start(OtelConfig, Peer, conn_attrs())
    }.

-spec emit_connection_stop(#state{}, term()) -> ok.
emit_connection_stop(#state{conn_span = ConnSpan, requests_count = RequestsCount}, Reason) ->
    nhttp_conn_otel:connection_stop(ConnSpan, RequestsCount, Reason).

-spec emit_request_start(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) ->
    {nhttp_otel:span_ctx(), integer()}.
emit_request_start(
    #state{otel_config = OtelConfig, conn_span = ConnSpan}, StreamId, Request
) ->
    nhttp_conn_otel:request_start(OtelConfig, ConnSpan, StreamId, Request, conn_attrs()).

-spec emit_request_stop(#state{}, nhttp_lib:status(), {nhttp_otel:span_ctx(), integer()}) ->
    ok.
emit_request_stop(#state{otel_config = OtelConfig}, Status, ReqSpanCtx) ->
    nhttp_conn_otel:request_stop(OtelConfig, Status, ReqSpanCtx).

-spec emit_request_stop_with_size(
    #state{},
    nhttp_lib:status(),
    {nhttp_otel:span_ctx(), integer()},
    non_neg_integer()
) -> ok.
emit_request_stop_with_size(#state{otel_config = OtelConfig}, Status, ReqSpanCtx, Size) ->
    nhttp_conn_otel:request_stop_with_size(OtelConfig, Status, ReqSpanCtx, Size).

-spec emit_stream_start(#state{}, {nhttp_otel:span_ctx(), integer()}) -> ok.
emit_stream_start(#state{otel_config = OtelConfig}, ReqSpanCtx) ->
    nhttp_conn_otel:stream_start(OtelConfig, ReqSpanCtx).

-doc """
Project the connection state into the structured-logging context map
consumed by `nhttp_log`.
""".
-spec log_ctx(#state{}) -> nhttp_log:ctx().
log_ctx(#state{name = Name, peer = Peer, handler = Handler}) ->
    nhttp_log:conn_ctx(Name, quic, http3, Peer, Handler).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - QUIC PROTOCOL
%%%-----------------------------------------------------------------------------
-spec handle_connection_error(#state{}, term()) -> no_return().
handle_connection_error(State, {transport_error, Reason}) ->
    ErrorCode = nquic_protocol:error_code(Reason),
    {ok, Ctx1} = nquic_lib:close(State#state.ctx, #{error_code => ErrorCode}),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    terminate({quic_error, Reason}, State#state{ctx = Ctx2, closing = true});
handle_connection_error(State, Reason) ->
    {ok, Ctx1} = nquic_lib:close(State#state.ctx),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    terminate({quic_error, Reason}, State#state{ctx = Ctx2, closing = true}).

-spec handle_quic_timeout(#state{}, nquic_protocol:timer_type()) -> no_return().
handle_quic_timeout(State, Type) ->
    case nquic_lib:timeout(State#state.ctx, Type) of
        {ok, Events, Ctx1} ->
            State1 = process_quic_events(State#state{ctx = Ctx1}, Events),
            connection_loop(State1);
        {error, Reason, Ctx1} ->
            handle_connection_error(State#state{ctx = Ctx1}, Reason)
    end.

-spec process_quic_event(#state{}, nquic_protocol:event()) -> #state{}.
process_quic_event(State, {stream_data, StreamId}) ->
    handle_stream_data_event(State, StreamId);
process_quic_event(State, {stream_opened, StreamId}) ->
    handle_stream_opened(State, StreamId);
process_quic_event(#state{h3_streams = Streams} = State, {stream_reset, StreamId, ErrorCode}) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = push} = Stream ->
            nhttp_conn_h3_push:handle_h3_push_stream_reset(State, StreamId, Stream);
        _ ->
            handle_stream_reset(State, StreamId, ErrorCode)
    end;
process_quic_event(#state{h3_streams = Streams} = State, {stop_sending, StreamId, ErrorCode}) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = websocket} ->
            nhttp_conn_ws_h3:handle_stream_reset(State, StreamId, ErrorCode);
        _ ->
            nhttp_conn_h3_push:handle_h3_push_stop_sending(State, StreamId, ErrorCode)
    end;
process_quic_event(State, {stream_writable, StreamId}) ->
    nhttp_conn_h3_push:flush_h3_stream_buffer(State, StreamId);
process_quic_event(State, connection_closed) ->
    State1 = nhttp_conn_ws_h3:notify_connection_close(State, 0),
    terminate(quic_closed, State1#state{closing = true});
process_quic_event(State, _Event) ->
    State.

-spec process_quic_events(#state{}, [nquic_protocol:event()]) -> #state{}.
process_quic_events(State, []) ->
    State;
process_quic_events(State, [Event | Rest]) ->
    State1 = process_quic_event(State, Event),
    process_quic_events(State1, Rest).

-spec reset_stream(#state{}, nhttp_lib:stream_id(), non_neg_integer()) -> #state{}.
reset_stream(State, StreamId, ErrorCode) ->
    case nquic_lib:reset_stream(State#state.ctx, StreamId, ErrorCode) of
        {ok, Ctx1} ->
            State#state{ctx = Ctx1};
        {error, _} ->
            State
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - MESSAGE HANDLING
%%%-----------------------------------------------------------------------------
-spec handle_stream_data(#state{}, nhttp_lib:stream_id(), binary(), nhttp_h3:fin()) ->
    #state{}.
handle_stream_data(State, StreamId, Data, Fin) ->
    case nhttp_h3:recv(State#state.h3_conn, StreamId, Data, Fin) of
        {ok, Events, NewH3, Actions} ->
            State1 = State#state{h3_conn = NewH3},
            State2 = execute_h3_actions(State1, Actions),
            handle_h3_events(State2, Events);
        {error, {connection_error, Code, _Reason}} ->
            close_with_error(State, Code);
        {error, {stream_error, SId, Code, _Reason}} ->
            reset_stream(State, SId, h3_error_to_code(Code))
    end.

-spec handle_stream_data_event(#state{}, nhttp_lib:stream_id()) -> #state{}.
handle_stream_data_event(State, StreamId) ->
    case nquic_lib:recv(State#state.ctx, StreamId) of
        {ok, Data, IsFin, Ctx1} ->
            Fin = bool_to_fin(IsFin),
            handle_stream_data(State#state{ctx = Ctx1}, StreamId, Data, Fin);
        {error, _} ->
            State
    end.

-spec handle_stream_opened(#state{}, nhttp_lib:stream_id()) -> #state{}.
handle_stream_opened(State, StreamId) ->
    Type = stream_id_to_type(StreamId),
    case nhttp_h3:stream_opened(State#state.h3_conn, StreamId, Type) of
        {ok, NewH3} ->
            State#state{h3_conn = NewH3};
        {error, {stream_error, _SId, Code, _Msg}} ->
            reset_stream(State, StreamId, h3_error_to_code(Code))
    end.

-spec handle_stream_reset(#state{}, nhttp_lib:stream_id(), non_neg_integer()) ->
    #state{}.
handle_stream_reset(State, StreamId, ErrorCode) ->
    {ok, Events, NewH3} = nhttp_h3:stream_reset(
        State#state.h3_conn, StreamId, ErrorCode
    ),
    State1 = State#state{h3_conn = NewH3},
    handle_h3_events(State1, Events).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - RESPONSE SENDING
%%%-----------------------------------------------------------------------------
-spec response_to_h3_headers(nhttp_lib:response()) -> [{binary(), binary()}].
response_to_h3_headers(#{status := Status, headers := Headers}) ->
    [{<<":status">>, integer_to_binary(Status)} | Headers].

-spec send_h3_data(#state{}, nhttp_lib:stream_id(), iodata(), nhttp_h3:fin()) -> #state{}.
send_h3_data(State, StreamId, Data, Fin) ->
    case nhttp_h3:send_data(State#state.h3_conn, StreamId, Data, Fin) of
        {ok, NewH3, Actions} ->
            execute_h3_actions(State#state{h3_conn = NewH3}, Actions);
        {error, _} ->
            State
    end.

-spec send_h3_error_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:status()) -> #state{}.
send_h3_error_response(State, StreamId, Status) ->
    Headers = [{<<":status">>, integer_to_binary(Status)}],
    case nhttp_h3:send_headers(State#state.h3_conn, StreamId, Headers, fin) of
        {ok, NewH3, Actions} ->
            execute_h3_actions(State#state{h3_conn = NewH3}, Actions);
        {error, _} ->
            State
    end.

-spec send_h3_headers(#state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), nhttp_h3:fin()) ->
    #state{}.
send_h3_headers(State, StreamId, Headers, Fin) ->
    case nhttp_h3:send_headers(State#state.h3_conn, StreamId, Headers, Fin) of
        {ok, NewH3, Actions} ->
            execute_h3_actions(State#state{h3_conn = NewH3}, Actions);
        {error, _} ->
            State
    end.

-spec send_h3_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:response()) -> #state{}.
send_h3_response(State, StreamId, Resp) ->
    Headers = response_to_h3_headers(Resp),
    case maps:get(body, Resp, <<>>) of
        <<>> ->
            send_h3_headers(State, StreamId, Headers, fin);
        Body ->
            case nhttp_h3:send_response(State#state.h3_conn, StreamId, Headers, Body) of
                {ok, NewH3, Actions} ->
                    execute_h3_actions(State#state{h3_conn = NewH3}, Actions);
                {error, _} ->
                    State
            end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SETUP
%%%-----------------------------------------------------------------------------
-spec bool_to_fin(boolean()) -> nhttp_h3:fin().
bool_to_fin(true) -> fin;
bool_to_fin(false) -> nofin.

-spec build_state(
    h3_init_args(),
    term(),
    {inet:ip_address(), inet:port_number()},
    nquic:ctx(),
    nhttp_h3:conn()
) -> #state{}.
build_state(InitArgs, HandlerState, Peer, Ctx, H3Conn) ->
    #{
        name := Name,
        opts := Opts,
        parent := Parent,
        handler := Handler
    } = InitArgs,
    Timeouts = maps:get(timeouts, Opts, #{}),
    #state{
        name = Name,
        peer = Peer,
        ctx = Ctx,
        h3_conn = H3Conn,
        handler = Handler,
        handler_state = HandlerState,
        opts = Opts,
        limits = nhttp_limits:from_opts(Opts),
        idle_timeout = maps:get(idle, Timeouts, ?DEFAULT_IDLE_TIMEOUT),
        otel_config = nhttp_otel:config(Opts),
        conn_span = undefined,
        compression_enabled = maps:get(compression, Opts, true),
        compression_level = maps:get(compression_level, Opts, ?DEFAULT_COMPRESSION_LEVEL),
        compression_threshold = maps:get(
            compression_threshold, Opts, ?DEFAULT_COMPRESSION_THRESHOLD
        ),
        compress_mime_types = maps:get(
            compress_mime_types, Opts, nhttp_compress:default_mime_types()
        ),
        parent = Parent,
        debug = sys:debug_options([])
    }.

-spec close_and_exit(nquic:ctx()) -> no_return().
close_and_exit(Ctx) ->
    close_and_exit(Ctx, 0).

-spec close_and_exit(nquic:ctx(), non_neg_integer()) -> no_return().
close_and_exit(Ctx, ErrorCode) ->
    {ok, Ctx1} = nquic_lib:close(Ctx, #{error_code => ErrorCode}),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    cleanup_ctx(Ctx2),
    exit(normal).

-spec drain_buffered_streams(#state{}) -> #state{}.
drain_buffered_streams(#state{ctx = Ctx} = State) ->
    StreamIds = nquic_protocol:pending_stream_ids(nquic_lib:ctx_state(Ctx)),
    lists:foldl(
        fun(StreamId, S) ->
            S1 = handle_stream_opened(S, StreamId),
            handle_stream_data_event(S1, StreamId)
        end,
        State,
        lists:sort(StreamIds)
    ).

-spec open_h3_streams(nquic:ctx(), nhttp_h3:conn()) ->
    {ok, nquic:ctx(), nhttp_h3:conn(), [nhttp_h3:action()]} | {error, term()}.
open_h3_streams(Ctx0, H3Conn) ->
    maybe
        {ok, CtrlId, Ctx1} ?= nquic_lib:open_stream(Ctx0, #{type => uni}),
        {ok, EncId, Ctx2} ?= nquic_lib:open_stream(Ctx1, #{type => uni}),
        {ok, DecId, Ctx3} ?= nquic_lib:open_stream(Ctx2, #{type => uni}),
        {ok, H3Conn1, Actions} ?=
            nhttp_h3:init_local_streams(H3Conn, #{
                control => CtrlId,
                encoder => EncId,
                decoder => DecId
            }),
        {ok, Ctx3, H3Conn1, Actions}
    end.

-spec setup_h3_connection(h3_init_args(), nquic:ctx()) -> no_return().
setup_h3_connection(InitArgs, Ctx0) ->
    Handler = maps:get(handler, InitArgs),
    HandlerArgs = maps:get(handler_args, InitArgs),
    case Handler:init(HandlerArgs) of
        {ok, HandlerState} ->
            ok = nhttp_stats:incr_connection(),
            setup_h3_streams(InitArgs, HandlerState, Ctx0);
        {error, _Reason} ->
            close_and_exit(Ctx0)
    end.

-spec setup_h3_streams(h3_init_args(), term(), nquic:ctx()) -> no_return().
setup_h3_streams(InitArgs, HandlerState, Ctx0) ->
    H3Conn = nhttp_h3:new(server, #{enable_connect_protocol => true}),
    case open_h3_streams(Ctx0, H3Conn) of
        {ok, Ctx1, H3Conn1, InitActions} ->
            Ctx2 = execute_init_actions(Ctx1, InitActions),
            {ok, Ctx3} = nquic_lib:flush(Ctx2),
            Peer = nquic_socket:sockaddr_to_tuple(nquic_lib:ctx_peer(Ctx3)),
            H3Conn2 = nhttp_h3:set_peer(H3Conn1, Peer),
            State = build_state(InitArgs, HandlerState, Peer, Ctx3, H3Conn2),
            State1 = drain_buffered_streams(State),
            {ok, Ctx4} = nquic_lib:flush(State1#state.ctx),
            State2 = emit_connection_start(State1#state{ctx = Ctx4}),
            connection_loop(State2);
        {error, _Reason} ->
            ok = nhttp_stats:decr_connection(),
            close_and_exit(Ctx0)
    end.

-spec stream_id_to_type(nhttp_lib:stream_id()) -> bidi | uni.
stream_id_to_type(StreamId) when StreamId band 2 =:= 0 -> bidi;
stream_id_to_type(_) -> uni.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - TERMINATION
%%%-----------------------------------------------------------------------------
-spec cleanup_ctx(nquic:ctx()) -> ok.
cleanup_ctx(Ctx) ->
    ok = maps:foreach(
        fun(_Type, Ref) ->
            _ = erlang:cancel_timer(Ref, [{async, true}, {info, false}])
        end,
        nquic_lib:ctx_timers(Ctx)
    ),
    _ =
        case nquic_lib:ctx_connected(Ctx) of
            true -> socket:close(nquic_lib:ctx_socket(Ctx));
            false -> ok
        end,
    case nquic_lib:ctx_dispatch(Ctx) of
        undefined ->
            ok;
        Dispatch ->
            CIDs = nquic_protocol:local_cids(nquic_lib:ctx_state(Ctx)),
            lists:foreach(fun(CID) -> nquic_dispatch:unregister(Dispatch, CID) end, CIDs)
    end,
    ok.

-spec close_with_error(#state{}, nhttp_h3:h3_error_code()) -> no_return().
close_with_error(State, Code) ->
    {ok, NewH3, Actions} = nhttp_h3:send_goaway(State#state.h3_conn),
    State1 = execute_h3_actions(State#state{h3_conn = NewH3}, Actions),
    {ok, Ctx1} = nquic_lib:close(State1#state.ctx, #{
        scope => application, error_code => h3_error_to_code(Code)
    }),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    terminate({h3_connection_error, Code}, State1#state{ctx = Ctx2, closing = true}).

-spec graceful_shutdown(#state{}) -> no_return().
graceful_shutdown(State) ->
    State0 = nhttp_conn_ws_h3:close_all(State),
    {ok, NewH3, Actions} = nhttp_h3:send_goaway(State0#state.h3_conn),
    State1 = execute_h3_actions(State0#state{h3_conn = NewH3}, Actions),
    {ok, Ctx1} = nquic_lib:close(State1#state.ctx, #{
        scope => application, error_code => h3_error_to_code(h3_no_error)
    }),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    terminate(normal, State1#state{ctx = Ctx2, closing = true}).

-spec terminate(term(), #state{}) -> no_return().
terminate(
    Reason,
    #state{
        ctx = Ctx,
        handler = Handler,
        handler_state = HState,
        h3_push_workers = Workers
    } = State
) ->
    ok = nhttp_stream_worker:reap(maps:keys(Workers)),
    emit_connection_stop(State, Reason),
    case erlang:function_exported(Handler, terminate, 2) of
        true ->
            case nhttp_handler:safe_terminate(Handler, normal, HState) of
                {nhttp_handler_exception, Class, CrashReason} ->
                    nhttp_log:handler_crashed(
                        log_ctx(State), terminate, Class, CrashReason
                    );
                _ ->
                    ok
            end;
        false ->
            ok
    end,
    case State#state.closing of
        true ->
            cleanup_ctx(Ctx);
        false ->
            ok = nquic_lib:shutdown(Ctx)
    end,
    ok = nhttp_stats:decr_connection(),
    exit(normal).
