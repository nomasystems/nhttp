-module(nhttp_conn_h2).

-moduledoc false.

-behaviour(nhttp_protocol).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn.hrl").
-include("nhttp_status_codes.hrl").
-include("nhttp_proc_lib.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    loop/3
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS (USED BY NHTTP_CONN_H2_PUSH)
%%%-----------------------------------------------------------------------------
-export([
    apply_h2_request_result/3,
    send_h2_data_with_buffer/6,
    send_h2_error_response/3,
    send_h2_headers/4,
    send_h2_rst_stream/2
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    h2_wake/4,
    system_code_change/4,
    system_continue/3,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(DRAIN_IDLE_WAKE_MS, 100).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Entry point for the HTTP/2 receive loop. Called by `nhttp_conn` after
protocol classification picks `http2`.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, State) ->
    h2_loop(Parent, Debug, State).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS CALLBACKS
%%%-----------------------------------------------------------------------------
-doc """
`proc_lib:hibernate/3` re-entry point for the frame-wait hibernation.
Cancels the idle timer armed before hibernating; when it already fired
the wake-up is an idle timeout unless other traffic is queued (drained
by the zero-timeout receive).
""".
-spec h2_wake(pid(), [sys:debug_option()], #state{}, nhttp_conn:hibernate_timer()) ->
    no_return().
h2_wake(Parent, Debug, State, Timer) ->
    case nhttp_conn:cancel_hibernate_timer(Timer) of
        ok -> h2_receive(Parent, Debug, State, receive_timeout(State));
        expired -> h2_receive(Parent, Debug, State, 0)
    end.

-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
?NHTTP_SYSTEM_CODE_CHANGE_NOOP.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(Parent, Debug, State) ->
    loop(Parent, Debug, State).

-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    nhttp_conn:stop_parent(Reason, State).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/2 LOOP
%%%-----------------------------------------------------------------------------
-spec drain_idle(#state{}) -> boolean().
drain_idle(#state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers}}) ->
    map_size(Streams) =:= 0 andalso map_size(Workers) =:= 0.

-spec finalize_goaway(#state{}) -> #state{}.
finalize_goaway(#state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State) ->
    {ok, NewH2Conn, GoawayFrame} = nhttp_h2:send_goaway(H2Conn, no_error, <<>>),
    State1 = State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}},
    ok = nhttp_conn:sock_send(State1, GoawayFrame),
    State1.

-doc """
Frame-wait block point. A quiescent connection (no streams, no workers,
not draining, empty mailbox) hibernates here. With active streams the
loop never hibernates: frames arrive at data rate and a full-sweep GC
per frame is a performance hazard. Protocol PINGs wake the process like
any other socket data. The `after` deadline is carried across
hibernation by the timer from `nhttp_conn:start_hibernate_timer/1`.
""".
-spec h2_loop(pid(), [sys:debug_option()], #state{}) -> no_return().
h2_loop(Parent, Debug, State) ->
    case hibernate_eligible(State) of
        true -> hibernate(Parent, Debug, State);
        false -> h2_receive(Parent, Debug, State, receive_timeout(State))
    end.

-spec hibernate_eligible(#state{}) -> boolean().
hibernate_eligible(#state{protocol_state = #h2_state{drain_deadline = undefined}} = State) ->
    drain_idle(State) andalso nhttp_conn:mailbox_empty();
hibernate_eligible(#state{}) ->
    false.

-spec hibernate(pid(), [sys:debug_option()], #state{}) -> no_return().
hibernate(Parent, Debug, #state{idle_timeout = IdleTimeout} = State) ->
    Timer = nhttp_conn:start_hibernate_timer(IdleTimeout),
    proc_lib:hibernate(?MODULE, h2_wake, [Parent, Debug, State, Timer]).

-spec h2_receive(pid(), [sys:debug_option()], #state{}, timeout()) -> no_return().
h2_receive(Parent, Debug, State, Timeout) ->
    receive
        {tcp, _Socket, Data} ->
            handle_h2_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_h2_data(Parent, Debug, State, Data);
        {tcp_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {ssl_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {tcp_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {ssl_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {request_result, StreamId, Ref, Result} ->
            NewState = handle_h2_request_result(State, StreamId, Ref, Result),
            h2_loop(Parent, Debug, NewState);
        {body_chunk_ack, WPid, Ref} ->
            NewState = handle_h2_body_chunk_ack(State, WPid, Ref),
            h2_loop(Parent, Debug, NewState);
        {send_chunk, WPid, Ref, Data} ->
            NewState = handle_h2_worker_send_chunk(State, WPid, Ref, Data, nofin),
            h2_loop(Parent, Debug, NewState);
        {stream_done, WPid, Ref} ->
            NewState = handle_h2_worker_send_chunk(State, WPid, Ref, <<>>, fin),
            h2_loop(Parent, Debug, NewState);
        {send_trailers, WPid, Ref, Trailers} ->
            NewState = handle_h2_worker_send_trailers(State, WPid, Ref, Trailers),
            h2_loop(Parent, Debug, NewState);
        {'DOWN', _MRef, process, WPid, Reason} ->
            NewState = handle_h2_worker_down(State, WPid, Reason),
            h2_loop(Parent, Debug, NewState);
        {'$gen_call', From, Request} ->
            h2_loop(Parent, Debug, nhttp_conn_ws_h2:handle_call(State, From, Request));
        {'$gen_cast', Msg} ->
            h2_loop(Parent, Debug, nhttp_conn_ws_h2:handle_cast(State, Msg));
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        shutdown ->
            case nhttp_conn:graceful_shutdown(State) of
                #state{} = NewState -> h2_loop(Parent, Debug, NewState)
            end;
        {'EXIT', Parent, Reason} ->
            nhttp_conn:stop_parent(Reason, State);
        Info ->
            h2_loop(Parent, Debug, nhttp_conn_ws_h2:handle_info(State, Info))
    after Timeout ->
        handle_h2_timeout(State)
    end.

-spec handle_h2_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_h2_data(
    Parent,
    Debug,
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2, socket = Socket} = State,
    Data
) ->
    case nhttp_h2:recv(H2Conn, Data) of
        {ok, Events, NewH2Conn} ->
            NewState = handle_h2_events(
                State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}, Events
            ),
            case nhttp_conn:activate(NewState) of
                ok -> h2_loop(Parent, Debug, NewState);
                {stop, Reason} -> nhttp_conn:stop(Reason, NewState)
            end;
        {ok, Events, NewH2Conn, FramesToSend} ->
            case nhttp_sock:send(Socket, FramesToSend) of
                ok ->
                    NewState = handle_h2_events(
                        State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}, Events
                    ),
                    case nhttp_conn:activate(NewState) of
                        ok -> h2_loop(Parent, Debug, NewState);
                        {stop, Reason} -> nhttp_conn:stop(Reason, NewState)
                    end;
                {error, Reason} ->
                    nhttp_conn:stop(
                        nhttp_conn:sock_stop_reason(Reason),
                        State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}
                    )
            end;
        {error, {connection_error, ErrorCode, _Reason}} ->
            {ok, FinalConn, GoawayFrame} = nhttp_h2:send_goaway(H2Conn, ErrorCode, <<>>),
            State1 = State#state{protocol_state = H2#h2_state{h2_conn = FinalConn}},
            ok = nhttp_conn:sock_send(State1, GoawayFrame),
            nhttp_conn:stop({h2_connection_error, ErrorCode}, State1);
        {error, Reason} ->
            nhttp_conn:stop({h2_error, Reason}, State)
    end.

-spec handle_h2_timeout(#state{}) -> no_return().
handle_h2_timeout(#state{protocol_state = #h2_state{drain_deadline = undefined}} = State) ->
    nhttp_conn:stop(idle_timeout, State);
handle_h2_timeout(#state{} = State) ->
    nhttp_conn:stop(normal, finalize_goaway(State)).

-spec receive_timeout(#state{}) -> timeout().
receive_timeout(#state{
    protocol_state = #h2_state{drain_deadline = undefined}, idle_timeout = IdleTimeout
}) ->
    IdleTimeout;
receive_timeout(#state{protocol_state = #h2_state{drain_deadline = Deadline}} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Remaining = max(0, Deadline - Now),
    case drain_idle(State) of
        true -> min(?DRAIN_IDLE_WAKE_MS, Remaining);
        false -> Remaining
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/2 REQUEST HANDLING
%%%-----------------------------------------------------------------------------
-doc """
Apply the result a worker reported for a dispatched request. Exposed as
an internal helper so `nhttp_conn_h2_push` can share the validation
fast-path for `stream_push`.
""".
-spec apply_h2_request_result(#state{}, nhttp_lib:stream_id(), term()) -> #state{}.
apply_h2_request_result(State, StreamId, Result) ->
    apply_request_result(State, StreamId, Result).

-spec apply_request_result(#state{}, nhttp_lib:stream_id(), term()) -> #state{}.
apply_request_result(
    #state{protocol_state = #h2_state{} = H2} = State,
    StreamId,
    {reply, #{status := Status} = Response0, NewHState}
) ->
    Stream = stream(State, StreamId),
    Request = stream_request(Stream),
    Response = nhttp_conn_compress:maybe_compress(
        Response0, Request, nhttp_conn:compress_config(State)
    ),
    {NewH2Conn, NewStreams} = send_h2_response(State, StreamId, Response),
    State1 = State#state{
        protocol_state = H2#h2_state{h2_conn = NewH2Conn, h2_streams = NewStreams}
    },
    State2 = maybe_rst_stream_unread_body(State1, StreamId, Stream),
    State3 = release_request_worker(State2, StreamId, Stream),
    nhttp_conn:emit_request_stop(State3, Status, Stream#h2_stream.req_span),
    State3#state{
        handler_state = NewHState,
        requests_count = State3#state.requests_count + 1
    };
apply_request_result(State, StreamId, {stream, {producer, Status, Headers, _Producer}, NewHState}) ->
    start_h2_stream_push_response(State, StreamId, Status, Headers, NewHState);
apply_request_result(State, StreamId, {accept_body, _BodyState, NewHState}) ->
    enter_h2_accept_body(State, StreamId, NewHState);
apply_request_result(State, StreamId, {upgrade, websocket, _NewHState}) ->
    Stream = stream(State, StreamId),
    send_h2_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
    State1 = release_request_worker(State, StreamId, Stream),
    nhttp_conn:emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span),
    State1;
apply_request_result(State, StreamId, {upgrade, websocket, _SessionOpts, _NewHState}) ->
    Stream = stream(State, StreamId),
    send_h2_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
    State1 = release_request_worker(State, StreamId, Stream),
    nhttp_conn:emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span),
    State1;
apply_request_result(State, StreamId, {abort, _Reason, NewHState}) ->
    Stream = stream(State, StreamId),
    send_h2_rst_stream(State, StreamId),
    State1 = release_request_worker(State, StreamId, Stream),
    nhttp_conn:emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span),
    State1#state{
        handler_state = NewHState,
        requests_count = State1#state.requests_count + 1
    };
apply_request_result(State, StreamId, {nhttp_handler_exception, Class, Reason}) ->
    Stream = stream(State, StreamId),
    Ctx = nhttp_log:request_ctx(
        nhttp_conn:log_ctx(State), Stream#h2_stream.request, StreamId
    ),
    nhttp_log:handler_crashed(Ctx, handle_request, Class, Reason),
    nhttp_conn:emit_handler_exception(Stream#h2_stream.req_span, Class, Reason),
    send_h2_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
    State1 = release_request_worker(State, StreamId, Stream),
    nhttp_conn:emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span),
    State1#state{requests_count = State1#state.requests_count + 1}.

-spec dispatch_h2_request(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) -> #state{}.
dispatch_h2_request(#state{limits = Limits} = State, StreamId, Request) ->
    case nhttp_limits:validate_request(Request, Limits) of
        ok ->
            spawn_h2_request_worker(State, StreamId, Request, true);
        {error, LimitError} ->
            handle_h2_limit_error(State, StreamId, LimitError)
    end.

-doc """
Dispatch a request whose body is still in flight (HEADERS arrived with
END_STREAM clear). The worker runs `handle_request/2` with `body =>
streaming`. The conn buffers DATA / trailer events until the worker
returns `{accept_body, _, _}` (flush to worker) or any other terminal
tag (discard + RST_STREAM(NO_ERROR), RFC 9113 §8.1).
""".
-spec dispatch_h2_streaming_request(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) ->
    #state{}.
dispatch_h2_streaming_request(#state{limits = Limits} = State, StreamId, Request) ->
    case nhttp_limits:validate_request(Request, Limits) of
        ok ->
            spawn_h2_request_worker(State, StreamId, Request#{body => streaming}, false);
        {error, LimitError} ->
            handle_h2_limit_error(State, StreamId, LimitError)
    end.

-spec spawn_h2_request_worker(
    #state{}, nhttp_lib:stream_id(), nhttp_lib:request(), boolean()
) -> #state{}.
spawn_h2_request_worker(
    #state{
        handler = Handler,
        handler_state = HState,
        protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2
    } = State,
    StreamId,
    Request,
    EndStreamRecv
) ->
    ReqSpan = nhttp_conn:emit_request_start(State, StreamId, Request),
    WorkerRef = make_ref(),
    WorkerPid = nhttp_stream_worker:start_request(
        self(), StreamId, WorkerRef, Handler, Request, HState
    ),
    MRef = erlang:monitor(process, WorkerPid),
    StoredRequest =
        case EndStreamRecv of
            true -> Request#{body => <<>>};
            false -> Request
        end,
    Stream = #h2_stream{
        type = request,
        worker = WorkerPid,
        worker_ref = WorkerRef,
        worker_mref = MRef,
        req_span = ReqSpan,
        end_stream = EndStreamRecv,
        request = StoredRequest
    },
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = Streams#{StreamId => Stream},
            h2_workers = Workers#{WorkerPid => StreamId}
        }
    }.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/2 STREAMING REQUEST BODY (PHASE 6 WAVE 2)
%%%-----------------------------------------------------------------------------
-spec abort_stream_worker(#state{}, nhttp_lib:stream_id()) -> #state{}.
abort_stream_worker(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{worker = WPid, worker_ref = Ref, pending_ack = PendingRef} when
            WPid =/= undefined
        ->
            AckRef =
                case PendingRef of
                    undefined -> Ref;
                    _ -> PendingRef
                end,
            WPid ! {chunk_ack, AckRef, {error, closed}},
            State;
        _ ->
            State
    end.

-spec apply_stream_push_validation(
    #state{},
    nhttp_lib:stream_id(),
    #h2_stream{},
    nhttp_lib:request(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    term()
) -> #state{}.
apply_stream_push_validation(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State,
    StreamId,
    Stream0,
    Request,
    Status,
    Headers,
    NewHState
) ->
    case nhttp_conn_h2_push:validate_h2_stream_push(Request, Status) of
        ok ->
            RespHeaders = nhttp_conn:alt_svc_headers(
                State, [{<<":status">>, integer_to_binary(Status)} | Headers]
            ),
            NewH2Conn = send_h2_headers(State, StreamId, RespHeaders, nofin),
            Stream1 = Stream0#h2_stream{type = stream, status = Status},
            State#state{
                protocol_state = H2#h2_state{
                    h2_conn = NewH2Conn,
                    h2_streams = Streams#{StreamId => Stream1}
                },
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        ok_head ->
            RespHeaders = nhttp_conn:alt_svc_headers(
                State, [{<<":status">>, integer_to_binary(Status)} | Headers]
            ),
            NewH2Conn = send_h2_headers(State, StreamId, RespHeaders, fin),
            State1 = State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}},
            State2 = abort_stream_worker(State1, StreamId),
            State3 = release_request_worker(State2, StreamId, Stream0),
            nhttp_conn:emit_request_stop_with_size(State3, Status, Stream0#h2_stream.req_span, 0),
            State3#state{
                handler_state = NewHState,
                requests_count = State3#state.requests_count + 1
            };
        {reject, RejectReason} ->
            #{method := Method, path := Path} = Request,
            nhttp_log:stream_push_rejected(
                nhttp_conn:log_ctx(State),
                Method,
                Path,
                RejectReason
            ),
            send_h2_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
            State1 = abort_stream_worker(State, StreamId),
            State2 = release_request_worker(State1, StreamId, Stream0),
            nhttp_conn:emit_request_stop(
                State2, ?HTTP_INTERNAL_SERVER_ERROR, Stream0#h2_stream.req_span
            ),
            State2#state{
                handler_state = NewHState,
                requests_count = State2#state.requests_count + 1
            }
    end.

-doc """
Bridge accept_body into the streaming body recv loop.
When the worker returns `accept_body` from `handle_request/2`, flush any
DATA frames the conn has buffered while the worker was running, then
mark the stream as streaming so subsequent events forward straight to
the worker. If the peer already sent END_STREAM (HEADERS+nofin followed
by DATA+fin before the worker posted accept_body), also send the
synthesized `{fin, []}` body event so the worker reaches `handle_request_body/3`
with the body-terminating event.
""".
-spec enter_h2_accept_body(#state{}, nhttp_lib:stream_id(), term()) -> #state{}.
enter_h2_accept_body(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId, NewHState
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = request, worker = WPid, worker_ref = Ref} = Stream when
            WPid =/= undefined
        ->
            BufferedChunks = lists:reverse(Stream#h2_stream.body_acc),
            EndStream = Stream#h2_stream.end_stream,
            PendingTrailers = Stream#h2_stream.pending_trailers,
            BufferedSizes = [byte_size(C) || C <- BufferedChunks],
            Stream1 = Stream#h2_stream{
                streaming_body = true,
                body_acc = [],
                pending_trailers = undefined,
                body_window_pending =
                    queue:join(
                        Stream#h2_stream.body_window_pending, queue:from_list(BufferedSizes)
                    )
            },
            State1 = State#state{
                protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}},
                handler_state = NewHState
            },
            forward_body_chunks(WPid, Ref, BufferedChunks),
            forward_terminal_body_event(WPid, Ref, EndStream, PendingTrailers),
            State1;
        _ ->
            State#state{handler_state = NewHState}
    end.

-spec forward_body_chunks(pid(), reference(), [binary()]) -> ok.
forward_body_chunks(_WPid, _Ref, []) ->
    ok;
forward_body_chunks(WPid, Ref, [Chunk | Rest]) ->
    WPid ! {body_chunk, Ref, {data, Chunk}},
    forward_body_chunks(WPid, Ref, Rest).

-spec forward_terminal_body_event(
    pid(), reference(), boolean(), nhttp_lib:headers() | undefined
) -> ok.
forward_terminal_body_event(_WPid, _Ref, false, _PendingTrailers) ->
    ok;
forward_terminal_body_event(WPid, Ref, true, undefined) ->
    WPid ! {body_chunk, Ref, {fin, []}},
    ok;
forward_terminal_body_event(WPid, Ref, true, Trailers) ->
    WPid ! {body_chunk, Ref, {fin, Trailers}},
    ok.

-doc """
Worker acked a body chunk. Pop the oldest pending byte size and emit
WINDOW_UPDATE for that many bytes on both the connection-level and the
stream-level flow-control windows so the peer can resume sending.
Acks for `fin` / `abort` events carry no flow-control debt and are
absorbed silently when the pending queue is empty.
""".
-spec handle_h2_body_chunk_ack(#state{}, pid(), reference()) -> #state{}.
handle_h2_body_chunk_ack(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers}} = State,
    WPid,
    Ref
) ->
    nhttp_conn_workers:route_silent(
        WPid,
        Workers,
        State,
        fun(StreamId) ->
            case maps:get(StreamId, Streams, undefined) of
                #h2_stream{worker = WPid, worker_ref = Ref, body_window_pending = Pending} =
                        Stream ->
                    case queue:out(Pending) of
                        {{value, Size}, Rest} ->
                            Stream1 = Stream#h2_stream{body_window_pending = Rest},
                            State1 = replenish_recv_window(State, StreamId, Size),
                            #state{protocol_state = #h2_state{h2_streams = Streams1} = H2After} =
                                State1,
                            State1#state{
                                protocol_state = H2After#h2_state{
                                    h2_streams = Streams1#{StreamId => Stream1}
                                }
                            };
                        {empty, _Rest} ->
                            State
                    end;
                _ ->
                    State
            end
        end
    ).

-doc """
DATA event on a stream that has been dispatched to a worker. Buffers
into `body_acc` while the worker is still in `handle_request/2`. Once
the worker has returned `accept_body` (streaming_body=true), forwards
each chunk straight through. Enforces `max_body_size` either way.
""".
-spec handle_h2_request_data(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, binary(), nhttp_h2:fin()
) -> #state{}.
handle_h2_request_data(
    #state{limits = Limits, protocol_state = #h2_state{h2_streams = Streams} = H2} = State,
    StreamId,
    Stream,
    Data,
    Fin
) ->
    NewSize = Stream#h2_stream.body_size + byte_size(Data),
    case nhttp_limits:check_body_size(NewSize, Limits) of
        ok ->
            EndStream = Fin =:= fin,
            Stream1 = Stream#h2_stream{
                body_size = NewSize,
                end_stream = EndStream
            },
            case Stream#h2_stream.streaming_body of
                true ->
                    WPid = Stream#h2_stream.worker,
                    Ref = Stream#h2_stream.worker_ref,
                    WPid ! {body_chunk, Ref, {data, Data}},
                    case EndStream of
                        true ->
                            WPid ! {body_chunk, Ref, {fin, []}},
                            ok;
                        false ->
                            ok
                    end,
                    Stream2 = Stream1#h2_stream{
                        body_window_pending =
                            queue:in(byte_size(Data), Stream#h2_stream.body_window_pending)
                    },
                    State#state{
                        protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream2}}
                    };
                false ->
                    Stream2 = Stream1#h2_stream{
                        body_acc = [Data | Stream#h2_stream.body_acc]
                    },
                    State#state{
                        protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream2}}
                    }
            end;
        {error, body_too_large} ->
            handle_h2_streaming_body_too_large(State, StreamId, Stream)
    end.

-spec handle_h2_request_result(#state{}, nhttp_lib:stream_id(), reference(), term()) ->
    #state{}.
handle_h2_request_result(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId, Ref, Result
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = request, worker_ref = Ref} ->
            apply_request_result(State, StreamId, Result);
        _ ->
            State
    end.

-doc "Trailers (HEADERS frame after DATA) on a worker stream.".
-spec handle_h2_request_trailers(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_lib:headers()
) -> #state{}.
handle_h2_request_trailers(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State,
    StreamId,
    Stream,
    Trailers
) ->
    case Stream#h2_stream.streaming_body of
        true ->
            WPid = Stream#h2_stream.worker,
            Ref = Stream#h2_stream.worker_ref,
            WPid ! {body_chunk, Ref, {fin, Trailers}},
            Stream1 = Stream#h2_stream{end_stream = true},
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}}};
        false ->
            Stream1 = Stream#h2_stream{
                end_stream = true,
                pending_trailers = Trailers
            },
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}}}
    end.

-spec handle_h2_streaming_body_too_large(#state{}, nhttp_lib:stream_id(), #h2_stream{}) ->
    #state{}.
handle_h2_streaming_body_too_large(State, StreamId, Stream) ->
    case Stream#h2_stream.streaming_body of
        true ->
            WPid = Stream#h2_stream.worker,
            Ref = Stream#h2_stream.worker_ref,
            WPid ! {body_chunk, Ref, {abort, body_too_large}},
            ok;
        false ->
            ok
    end,
    handle_h2_limit_error(State, StreamId, body_too_large).

-doc """
After a terminal handler result (`reply` / `stream`) on a request whose
body was still in flight, send RST_STREAM(NO_ERROR) to ask the peer to
stop transmitting the remainder (RFC 9113 §8.1). No-op when END_STREAM
was already received.
""".
-spec maybe_rst_stream_unread_body(#state{}, nhttp_lib:stream_id(), #h2_stream{}) -> #state{}.
maybe_rst_stream_unread_body(State, _StreamId, #h2_stream{end_stream = true}) ->
    State;
maybe_rst_stream_unread_body(State, StreamId, #h2_stream{end_stream = false}) ->
    send_h2_rst_stream_with_code(State, StreamId, no_error),
    State.

-spec release_request_worker(#state{}, nhttp_lib:stream_id(), #h2_stream{}) -> #state{}.
release_request_worker(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    StreamId,
    #h2_stream{worker = WPid, worker_mref = MRef}
) ->
    case MRef of
        undefined ->
            ok;
        _ ->
            _ = erlang:demonitor(MRef, [flush]),
            ok
    end,
    Workers1 =
        case WPid of
            undefined -> Workers;
            _ -> maps:remove(WPid, Workers)
        end,
    Streams1 =
        case maps:get(StreamId, Streams, undefined) of
            undefined ->
                Streams;
            #h2_stream{send_buffer = <<>>} ->
                maps:remove(StreamId, Streams);
            #h2_stream{} = Live ->
                Cleared = Live#h2_stream{
                    type = http,
                    worker = undefined,
                    worker_ref = undefined,
                    worker_mref = undefined,
                    pending_ack = undefined,
                    request = undefined
                },
                Streams#{StreamId => Cleared}
        end,
    State#state{protocol_state = H2#h2_state{h2_streams = Streams1, h2_workers = Workers1}}.

-spec replenish_recv_window(#state{}, nhttp_lib:stream_id(), non_neg_integer()) -> #state{}.
replenish_recv_window(State, _StreamId, 0) ->
    State;
replenish_recv_window(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, StreamId, Size
) ->
    State1 =
        case nhttp_h2:send_window_update(H2Conn, connection, Size) of
            {ok, H2Conn1, Frame1} ->
                nhttp_conn:sock_send(State, Frame1),
                State#state{protocol_state = H2#h2_state{h2_conn = H2Conn1}};
            {error, _} ->
                State
        end,
    #state{protocol_state = #h2_state{h2_conn = ConnAfter} = H2After} = State1,
    case nhttp_h2:send_window_update(ConnAfter, StreamId, Size) of
        {ok, H2Conn2, Frame2} ->
            nhttp_conn:sock_send(State1, Frame2),
            State1#state{protocol_state = H2After#h2_state{h2_conn = H2Conn2}};
        {error, _} ->
            State1
    end.

-spec send_h2_rst_stream_with_code(#state{}, nhttp_lib:stream_id(), atom()) -> ok.
send_h2_rst_stream_with_code(State, StreamId, ErrorCode) ->
    {ok, Frame} = nhttp_h2_frame:rst_stream(StreamId, ErrorCode),
    nhttp_conn:sock_send(State, Frame),
    ok.

-spec start_h2_stream_push_response(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    term()
) -> #state{}.
start_h2_stream_push_response(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State,
    StreamId,
    Status,
    Headers,
    NewHState
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = request} = Stream0 ->
            Request = stream_request(Stream0),
            apply_stream_push_validation(
                State, StreamId, Stream0, Request, Status, Headers, NewHState
            );
        _ ->
            State#state{handler_state = NewHState}
    end.

-spec stream(#state{}, nhttp_lib:stream_id()) -> #h2_stream{}.
stream(#state{protocol_state = #h2_state{h2_streams = Streams}}, StreamId) ->
    maps:get(StreamId, Streams, #h2_stream{}).

-spec stream_request(#h2_stream{}) -> nhttp_lib:request().
stream_request(#h2_stream{request = R}) when is_map(R) ->
    R.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - WORKER MESSAGE HANDLERS
%%%-----------------------------------------------------------------------------
-spec cleanup_h2_worker_entry(#state{}, pid()) -> #state{}.
cleanup_h2_worker_entry(
    #state{protocol_state = #h2_state{h2_workers = Workers} = H2} = State, WPid
) ->
    State#state{protocol_state = H2#h2_state{h2_workers = maps:remove(WPid, Workers)}}.

-spec demonitor_worker(#h2_stream{}) -> ok.
demonitor_worker(#h2_stream{worker_mref = undefined}) ->
    ok;
demonitor_worker(#h2_stream{worker_mref = MRef}) ->
    _ = erlang:demonitor(MRef, [flush]),
    ok.

-spec do_h2_worker_send_chunk(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), iodata(), nhttp_h2:fin()
) -> #state{}.
do_h2_worker_send_chunk(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_conn = H2Conn} = H2} = State,
    StreamId,
    WPid,
    Ref,
    Data,
    Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = stream, worker = WPid, worker_ref = Ref} = Stream0 ->
            Stream1 = maybe_emit_h2_stream_start(State, Stream0),
            BytesAdded = iolist_size(Data),
            Stream2 = Stream1#h2_stream{bytes_sent = Stream1#h2_stream.bytes_sent + BytesAdded},
            Streams1 = Streams#{StreamId => Stream2},
            {NewH2Conn, NewStreams} = send_h2_data_with_buffer(
                State, H2Conn, Streams1, StreamId, Data, Fin
            ),
            State1 = State#state{
                protocol_state = H2#h2_state{h2_conn = NewH2Conn, h2_streams = NewStreams}
            },
            finalize_h2_worker_send(State1, StreamId, WPid, Ref, Fin);
        _ ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            cleanup_h2_worker_entry(State, WPid)
    end.

-spec emit_h2_stream_complete(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, atom()
) -> ok.
emit_h2_stream_complete(_State, _StreamId, #h2_stream{req_span = undefined}, _Reason) ->
    ok;
emit_h2_stream_complete(State, _StreamId, Stream, _Reason) ->
    #h2_stream{status = Status, req_span = ReqSpan, bytes_sent = Bytes} = Stream,
    nhttp_conn:emit_request_stop_with_size(State, Status, ReqSpan, Bytes).

-spec finalize_h2_worker_send(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), nhttp_h2:fin()
) -> #state{}.
finalize_h2_worker_send(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    StreamId,
    WPid,
    Ref,
    Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            cleanup_h2_worker_entry(State, WPid);
        #h2_stream{send_buffer = <<>>} = Stream when Fin =:= fin ->
            WPid ! {chunk_ack, Ref, ok},
            emit_h2_stream_complete(State, StreamId, Stream, normal),
            _ = demonitor_worker(Stream),
            State#state{
                protocol_state = H2#h2_state{
                    h2_streams = maps:remove(StreamId, Streams),
                    h2_workers = maps:remove(WPid, Workers)
                }
            };
        #h2_stream{send_buffer = <<>>} = Stream ->
            WPid ! {chunk_ack, Ref, ok},
            Stream1 = Stream#h2_stream{pending_ack = undefined},
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}}};
        #h2_stream{} = Stream ->
            Stream1 = Stream#h2_stream{pending_ack = Ref},
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}}}
    end.

-doc "Try to flush buffered data for a stream after WINDOW_UPDATE.".
-spec flush_single_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
flush_single_stream(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_conn = H2Conn} = H2} = State,
    StreamId
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h2_stream{send_buffer = <<>>} ->
            State;
        #h2_stream{send_buffer = Buffer, send_end_stream = EndStream} = Stream ->
            EndStreamFin =
                case EndStream of
                    true -> fin;
                    false -> nofin
                end,
            ClearedStream = Stream#h2_stream{send_buffer = <<>>, send_end_stream = false},
            Streams1 = Streams#{StreamId => ClearedStream},
            {NewH2Conn, NewStreams} = send_h2_data_with_buffer(
                State, H2Conn, Streams1, StreamId, Buffer, EndStreamFin
            ),
            State1 = State#state{
                protocol_state = H2#h2_state{h2_conn = NewH2Conn, h2_streams = NewStreams}
            },
            post_buffer_drain(State1, StreamId)
    end.

-spec flush_stream_buffer(#state{}, nhttp_lib:stream_id()) -> #state{}.
flush_stream_buffer(#state{protocol_state = #h2_state{h2_streams = Streams}} = State, 0) ->
    StreamIds = [Id || {Id, #h2_stream{send_buffer = Buf}} <- maps:to_list(Streams), Buf =/= <<>>],
    lists:foldl(
        fun(StreamId, AccState) -> flush_single_stream(AccState, StreamId) end, State, StreamIds
    );
flush_stream_buffer(State, StreamId) ->
    flush_single_stream(State, StreamId).

-spec handle_h2_event(#state{}, nhttp_h2:event()) -> #state{}.
handle_h2_event(State, {request, StreamId, Request, fin}) ->
    dispatch_h2_request(State, StreamId, Request);
handle_h2_event(State, {request, StreamId, Request, nofin}) ->
    case nhttp_req:is_websocket_upgrade(Request) of
        true ->
            nhttp_conn_ws_h2:connect(State, StreamId, Request);
        false ->
            dispatch_h2_streaming_request(State, StreamId, Request)
    end;
handle_h2_event(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, {data, StreamId, Data, Fin}
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h2_stream{type = websocket} ->
            nhttp_conn_ws_h2:handle_data(State, StreamId, Data, Fin);
        #h2_stream{type = request} = Stream ->
            handle_h2_request_data(State, StreamId, Stream, Data, Fin);
        _ ->
            State
    end;
handle_h2_event(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, {trailers, StreamId, Trailers}
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = request} = Stream ->
            handle_h2_request_trailers(State, StreamId, Stream, Trailers);
        _ ->
            State
    end;
handle_h2_event(State, {goaway, _LastStreamId, ErrorCode, _DebugData}) ->
    nhttp_conn_ws_h2:notify_goaway(State, ErrorCode);
handle_h2_event(State, {settings, _NewSettings}) ->
    State;
handle_h2_event(State, settings_ack) ->
    State;
handle_h2_event(State, {window_update, StreamId, _Increment}) ->
    flush_stream_buffer(State, StreamId);
handle_h2_event(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State,
    {stream_reset, StreamId, ErrorCode}
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = websocket} ->
            nhttp_conn_ws_h2:handle_stream_reset(State, StreamId, ErrorCode);
        #h2_stream{type = T} = Stream when T =:= request; T =:= stream ->
            handle_h2_worker_stream_reset(State, StreamId, Stream);
        _ ->
            State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}}
    end;
handle_h2_event(State, _Event) ->
    State.

-spec handle_h2_events(#state{}, [nhttp_h2:event()]) -> #state{}.
handle_h2_events(State, []) ->
    State;
handle_h2_events(State, [Event | Rest]) ->
    NewState = handle_h2_event(State, Event),
    handle_h2_events(NewState, Rest).

-spec handle_h2_limit_error(#state{}, nhttp_lib:stream_id(), nhttp_limits:limit_error()) ->
    #state{}.
handle_h2_limit_error(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State,
    StreamId,
    body_too_large = E
) ->
    send_h2_error_response(State, StreamId, nhttp_limits:error_to_status(E)),
    send_h2_rst_stream(State, StreamId),
    State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}};
handle_h2_limit_error(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId, LimitError
) ->
    send_h2_error_response(State, StreamId, nhttp_limits:error_to_status(LimitError)),
    State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}}.

-spec handle_h2_worker_down(#state{}, pid(), term()) -> #state{}.
handle_h2_worker_down(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    WPid,
    Reason
) ->
    nhttp_conn_workers:route_silent(
        WPid,
        Workers,
        State,
        fun(StreamId) ->
            Workers1 = maps:remove(WPid, Workers),
            case maps:get(StreamId, Streams, undefined) of
                undefined ->
                    State#state{protocol_state = H2#h2_state{h2_workers = Workers1}};
                #h2_stream{type = stream} = Stream when Reason =:= normal ->
                    emit_h2_stream_complete(State, StreamId, Stream, normal),
                    NewState = send_h2_data_unbuffered(
                        State#state{protocol_state = H2#h2_state{h2_workers = Workers1}},
                        StreamId,
                        <<>>,
                        fin
                    ),
                    #state{protocol_state = #h2_state{h2_streams = StreamsAfter} = H2After} =
                        NewState,
                    NewState#state{
                        protocol_state = H2After#h2_state{
                            h2_streams = maps:remove(StreamId, StreamsAfter)
                        }
                    };
                #h2_stream{type = stream} = Stream ->
                    nhttp_log:stream_push_producer_crashed(
                        nhttp_conn:log_ctx(State), Reason
                    ),
                    emit_h2_stream_complete(State, StreamId, Stream, producer_crashed),
                    send_h2_rst_stream(State, StreamId),
                    State#state{
                        protocol_state = H2#h2_state{
                            h2_workers = Workers1,
                            h2_streams = maps:remove(StreamId, Streams)
                        }
                    };
                #h2_stream{type = request} = Stream when Reason =:= normal ->
                    send_h2_rst_stream(State, StreamId),
                    State1 = State#state{
                        protocol_state = H2#h2_state{
                            h2_workers = Workers1,
                            h2_streams = maps:remove(StreamId, Streams)
                        }
                    },
                    nhttp_conn:emit_request_stop(
                        State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span
                    ),
                    State1;
                #h2_stream{type = request} = Stream ->
                    Ctx = nhttp_log:request_ctx(
                        nhttp_conn:log_ctx(State), Stream#h2_stream.request, StreamId
                    ),
                    nhttp_log:handler_crashed(Ctx, handle_request, exit, Reason),
                    nhttp_conn:emit_handler_exception(
                        Stream#h2_stream.req_span, exit, Reason
                    ),
                    send_h2_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
                    State1 = State#state{
                        protocol_state = H2#h2_state{
                            h2_workers = Workers1,
                            h2_streams = maps:remove(StreamId, Streams)
                        }
                    },
                    nhttp_conn:emit_request_stop(
                        State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span
                    ),
                    State1#state{requests_count = State1#state.requests_count + 1};
                _ ->
                    State#state{protocol_state = H2#h2_state{h2_workers = Workers1}}
            end
        end
    ).

-spec handle_h2_worker_send_chunk(
    #state{}, pid(), reference(), iodata(), nhttp_h2:fin()
) -> #state{}.
handle_h2_worker_send_chunk(
    #state{protocol_state = #h2_state{h2_workers = Workers}} = State, WPid, Ref, Data, Fin
) ->
    nhttp_conn_workers:route(
        WPid,
        Ref,
        Workers,
        State,
        fun(StreamId) ->
            do_h2_worker_send_chunk(State, StreamId, WPid, Ref, Data, Fin)
        end
    ).

-spec handle_h2_worker_send_trailers(
    #state{}, pid(), reference(), nhttp_lib:headers()
) -> #state{}.
handle_h2_worker_send_trailers(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    WPid,
    Ref,
    Trailers
) ->
    nhttp_conn_workers:route(
        WPid,
        Ref,
        Workers,
        State,
        fun(StreamId) ->
            case maps:get(StreamId, Streams, undefined) of
                #h2_stream{type = stream, worker = WPid, worker_ref = Ref} = Stream ->
                    NewH2Conn = send_h2_headers(State, StreamId, Trailers, fin),
                    WPid ! {chunk_ack, Ref, ok},
                    emit_h2_stream_complete(State, StreamId, Stream, normal),
                    _ = demonitor_worker(Stream),
                    State#state{
                        protocol_state = H2#h2_state{
                            h2_conn = NewH2Conn,
                            h2_streams = maps:remove(StreamId, Streams),
                            h2_workers = maps:remove(WPid, Workers)
                        }
                    };
                _ ->
                    WPid ! {chunk_ack, Ref, {error, closed}},
                    cleanup_h2_worker_entry(State, WPid)
            end
        end
    ).

-spec handle_h2_worker_stream_reset(#state{}, nhttp_lib:stream_id(), #h2_stream{}) -> #state{}.
handle_h2_worker_stream_reset(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    StreamId,
    Stream
) ->
    #h2_stream{
        worker = WPid,
        worker_ref = Ref,
        worker_mref = MRef,
        pending_ack = PendingRef
    } = Stream,
    emit_h2_stream_complete(State, StreamId, Stream, peer_reset),
    case PendingRef of
        undefined when WPid =/= undefined ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            ok;
        _ when WPid =/= undefined ->
            WPid ! {chunk_ack, PendingRef, {error, closed}},
            ok;
        _ ->
            ok
    end,
    case MRef of
        undefined ->
            ok;
        _ ->
            _ = erlang:demonitor(MRef, [flush]),
            ok
    end,
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = maps:remove(StreamId, Streams),
            h2_workers =
                case WPid of
                    undefined -> Workers;
                    _ -> maps:remove(WPid, Workers)
                end
        }
    }.

-spec maybe_emit_h2_stream_start(#state{}, #h2_stream{}) -> #h2_stream{}.
maybe_emit_h2_stream_start(_State, #h2_stream{response_started = true} = Stream) ->
    Stream;
maybe_emit_h2_stream_start(State, #h2_stream{req_span = ReqSpan} = Stream) when
    ReqSpan =/= undefined
->
    nhttp_conn:emit_stream_start(State, ReqSpan),
    Stream#h2_stream{response_started = true};
maybe_emit_h2_stream_start(_State, Stream) ->
    Stream#h2_stream{response_started = true}.

-spec post_buffer_drain(#state{}, nhttp_lib:stream_id()) -> #state{}.
post_buffer_drain(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h2_stream{
            send_buffer = <<>>,
            type = stream,
            worker = WPid,
            pending_ack = Ref
        } = Stream when Ref =/= undefined, WPid =/= undefined ->
            WPid ! {chunk_ack, Ref, ok},
            Stream1 = Stream#h2_stream{pending_ack = undefined},
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream1}}};
        #h2_stream{send_buffer = <<>>, type = Type} when Type =/= stream ->
            State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}};
        _ ->
            State
    end.

-spec response_to_headers(nhttp_lib:response()) -> [{binary(), binary()}].
response_to_headers(#{status := Status, headers := Headers}) ->
    [{<<":status">>, integer_to_binary(Status)} | Headers].

-spec send_h2_data_unbuffered(
    #state{}, nhttp_lib:stream_id(), iodata(), nhttp_h2:fin()
) -> #state{}.
send_h2_data_unbuffered(
    #state{protocol_state = #h2_state{h2_conn = H2Conn, h2_streams = Streams} = H2} = State,
    StreamId,
    Data,
    Fin
) ->
    {NewH2Conn, NewStreams} = send_h2_data_with_buffer(
        State, H2Conn, Streams, StreamId, Data, Fin
    ),
    State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn, h2_streams = NewStreams}}.

-spec send_h2_data_with_buffer(
    #state{},
    nhttp_h2:conn(),
    #{nhttp_lib:stream_id() => #h2_stream{}},
    nhttp_lib:stream_id(),
    iodata(),
    nhttp_h2:fin()
) -> {nhttp_h2:conn(), #{nhttp_lib:stream_id() => #h2_stream{}}}.
send_h2_data_with_buffer(State, H2Conn, Streams, StreamId, Data, EndStream) ->
    case nhttp_h2:send_data(H2Conn, StreamId, Data, EndStream) of
        {ok, NewH2Conn, DataFrame} ->
            ok = nhttp_conn:sock_send(State, DataFrame),
            {NewH2Conn, Streams};
        {partial, NewH2Conn, DataFrame, Remaining, PendingEndStream, _Window} ->
            ok = nhttp_conn:sock_send(State, DataFrame),
            Stream = maps:get(StreamId, Streams, #h2_stream{}),
            NewStream = Stream#h2_stream{
                send_buffer = Remaining,
                send_end_stream = PendingEndStream =:= fin
            },
            {NewH2Conn, Streams#{StreamId => NewStream}};
        {error, {stream_closed, _}} ->
            {H2Conn, maps:remove(StreamId, Streams)};
        {error, {unknown_stream, _}} ->
            {H2Conn, maps:remove(StreamId, Streams)}
    end.

-spec send_h2_error_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:status()) -> ok.
send_h2_error_response(
    #state{protocol_state = #h2_state{h2_conn = H2Conn}} = State, StreamId, Status
) ->
    Headers = [{<<":status">>, integer_to_binary(Status)}],
    case nhttp_h2:send_headers(H2Conn, StreamId, Headers, fin) of
        {ok, _NewConn, Frame} ->
            ok = nhttp_conn:sock_send(State, Frame),
            ok;
        {error, {stream_closed, _}} ->
            ok;
        {error, connection_closing} ->
            ok
    end.

-spec send_h2_headers(#state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), nhttp_h2:fin()) ->
    nhttp_h2:conn().
send_h2_headers(
    #state{protocol_state = #h2_state{h2_conn = H2Conn}} = State, StreamId, Headers, Fin
) ->
    case nhttp_h2:send_headers(H2Conn, StreamId, Headers, Fin) of
        {ok, NewH2Conn, Frame} ->
            nhttp_conn:sock_send(State, Frame),
            NewH2Conn;
        {error, _Reason} ->
            H2Conn
    end.

-spec send_h2_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:response()) ->
    {nhttp_h2:conn(), #{nhttp_lib:stream_id() => #h2_stream{}}}.
send_h2_response(
    #state{protocol_state = #h2_state{h2_conn = H2Conn, h2_streams = Streams}} = State,
    StreamId,
    Response
) ->
    Headers = nhttp_conn:alt_svc_headers(State, response_to_headers(Response)),
    Body = maps:get(body, Response, <<>>),
    case Body of
        <<>> ->
            case nhttp_h2:send_headers(H2Conn, StreamId, Headers, fin) of
                {ok, NewH2Conn, Frame} ->
                    nhttp_conn:sock_send(State, Frame),
                    {NewH2Conn, Streams};
                {error, {stream_closed, _}} ->
                    {H2Conn, maps:remove(StreamId, Streams)};
                {error, connection_closing} ->
                    {H2Conn, Streams}
            end;
        _ ->
            case nhttp_h2:send_headers(H2Conn, StreamId, Headers, nofin) of
                {ok, H2Conn1, HeaderFrame} ->
                    send_h2_response_with_body(
                        State, H2Conn1, Streams, StreamId, HeaderFrame, Body
                    );
                {error, {stream_closed, _}} ->
                    {H2Conn, maps:remove(StreamId, Streams)};
                {error, connection_closing} ->
                    {H2Conn, Streams}
            end
    end.

-doc "Send HEADERS + DATA, combining into single syscall when possible.".
-spec send_h2_response_with_body(
    #state{},
    nhttp_h2:conn(),
    #{nhttp_lib:stream_id() => #h2_stream{}},
    nhttp_lib:stream_id(),
    iodata(),
    binary()
) -> {nhttp_h2:conn(), #{nhttp_lib:stream_id() => #h2_stream{}}}.
send_h2_response_with_body(State, H2Conn, Streams, StreamId, HeaderFrame, Body) ->
    case nhttp_h2:send_data(H2Conn, StreamId, Body, fin) of
        {ok, NewH2Conn, DataFrame} ->
            nhttp_conn:sock_send(State, [HeaderFrame, DataFrame]),
            {NewH2Conn, Streams};
        {partial, NewH2Conn, DataFrame, Remaining, PendingEndStream, _Window} ->
            nhttp_conn:sock_send(State, [HeaderFrame, DataFrame]),
            Stream = maps:get(StreamId, Streams, #h2_stream{}),
            NewStream = Stream#h2_stream{
                send_buffer = Remaining,
                send_end_stream = PendingEndStream =:= fin
            },
            {NewH2Conn, Streams#{StreamId => NewStream}};
        {error, {stream_closed, _}} ->
            nhttp_conn:sock_send(State, HeaderFrame),
            {H2Conn, maps:remove(StreamId, Streams)};
        {error, {unknown_stream, _}} ->
            nhttp_conn:sock_send(State, HeaderFrame),
            {H2Conn, Streams}
    end.

-spec send_h2_rst_stream(#state{}, nhttp_lib:stream_id()) -> ok.
send_h2_rst_stream(State, StreamId) ->
    {ok, Frame} = nhttp_h2_frame:rst_stream(StreamId, internal_error),
    nhttp_conn:sock_send(State, Frame),
    ok.
