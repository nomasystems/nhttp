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
%% INTERNAL EXPORTS (USED BY NHTTP_CONN_H2_PUSH AND NHTTP_CONN_WS_H2)
%%%-----------------------------------------------------------------------------
-export([
    apply_h2_request_result/3,
    offer_h2_data/5,
    reset_h2_stream/3,
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
%% TYPE EXPORTS
%%%-----------------------------------------------------------------------------
-export_type([offer_outcome/0]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-doc """
What the codec did with an offer to its send queue. `sent`: every octet
left. `queued`: the codec holds the rest and drains it as credit arrives.
`send_buffer_full`: the queue bound refused the whole offer and nothing
changed. `stream_gone`: the codec has no open stream with that id.
""".
-type offer_outcome() :: sent | queued | send_buffer_full | stream_gone.

-doc """
How a `{reply, _, _}` response left. `closed` means that the stream is
closed in the codec and nothing more goes out on it.
""".
-type response_outcome() :: sent | queued | closed.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(CREDIT_DELAY_TAG, nhttp_h2_credit_delay).
-define(DRAIN_IDLE_WAKE_MS, 100).
-define(RESPONSE_DELAY_TAG, nhttp_h2_response_delay).

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
        {timeout, TimerRef, {?RESPONSE_DELAY_TAG, StreamId}} ->
            NewState = handle_h2_response_delay(State, StreamId, TimerRef),
            h2_loop(Parent, Debug, NewState);
        {timeout, TimerRef, ?CREDIT_DELAY_TAG} ->
            NewState = handle_h2_credit_delay(State, TimerRef),
            h2_loop(Parent, Debug, NewState);
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
    #state{protocol_state = #h2_state{response_delay = Delay}} = State,
    StreamId,
    {reply, #{status := _} = Response0, NewHState}
) ->
    Stream = stream(State, StreamId),
    Request = stream_request(Stream),
    Response = nhttp_conn_compress:maybe_compress(
        Response0, Request, nhttp_conn:compress_config(State)
    ),
    State1 = State#state{handler_state = NewHState},
    case Delay of
        0 -> complete_h2_reply(State1, StreamId, Stream, Response);
        _ -> hold_h2_reply(State1, StreamId, Stream, Response, Delay)
    end;
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
    State1 = release_request_worker(send_h2_rst_stream(State, StreamId), StreamId, Stream),
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

-spec cancel_held_response(#h2_stream{}) -> ok.
cancel_held_response(#h2_stream{held_response = undefined}) ->
    ok;
cancel_held_response(#h2_stream{held_response = {TimerRef, _Response}}) ->
    ok = erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    ok.

-doc """
Send a `{reply, _, _}` response and close the request bookkeeping: the
unread-body RST_STREAM, the worker release and the request span. With a
response delay this runs when the hold timer fires. An `on_response`
policy releases the credit of the request ahead of the HEADERS, in the
same socket write.
""".
-spec complete_h2_reply(#state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_lib:response()) ->
    #state{}.
complete_h2_reply(
    #state{protocol_state = #h2_state{conn_credit = {on_response, _, _}}} = State,
    StreamId,
    Stream,
    Response
) ->
    {State1, Credit} = release_response_credit(State, StreamId, Stream),
    finish_h2_reply(State1, StreamId, Stream, Response, Credit);
complete_h2_reply(
    #state{protocol_state = #h2_state{stream_credit = on_response}} = State,
    StreamId,
    Stream,
    Response
) ->
    {State1, Credit} = release_response_credit(State, StreamId, Stream),
    finish_h2_reply(State1, StreamId, Stream, Response, Credit);
complete_h2_reply(State, StreamId, Stream, Response) ->
    finish_h2_reply(State, StreamId, Stream, Response, []).

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

-spec finish_h2_reply(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_lib:response(), iodata()
) -> #state{}.
finish_h2_reply(State, StreamId, Stream, #{status := Status} = Response, Credit) ->
    {Outcome, State1} = send_h2_response(State, StreamId, Response, Credit),
    State2 = maybe_rst_stream_unread_body(State1, StreamId, Stream, Outcome),
    State3 = release_request_worker(State2, StreamId, Stream),
    nhttp_conn:emit_request_stop(State3, Status, Stream#h2_stream.req_span),
    State3#state{requests_count = State3#state.requests_count + 1}.

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

-spec apply_conn_credit(#state{}, nhttp_lib:stream_id(), pos_integer()) -> #state{}.
apply_conn_credit(
    #state{protocol_state = #h2_state{conn_credit = eager}} = State, _StreamId, Size
) ->
    credit_connection(State, Size);
apply_conn_credit(
    #state{protocol_state = #h2_state{conn_credit = never}} = State, _StreamId, _Size
) ->
    State;
apply_conn_credit(
    #state{protocol_state = #h2_state{conn_credit = {threshold, N, Acc}} = H2} = State,
    _StreamId,
    Size
) ->
    case Acc + Size of
        Total when Total >= N ->
            Reset = State#state{protocol_state = H2#h2_state{conn_credit = {threshold, N, 0}}},
            credit_connection(Reset, Total);
        Total ->
            State#state{protocol_state = H2#h2_state{conn_credit = {threshold, N, Total}}}
    end;
apply_conn_credit(
    #state{protocol_state = #h2_state{conn_credit = {delay, Ms, Due}} = H2} = State,
    StreamId,
    Size
) ->
    Now = erlang:monotonic_time(millisecond),
    DueAt = Now + Ms,
    Queued = State#state{
        protocol_state = H2#h2_state{
            conn_credit = {delay, Ms, queue:in({DueAt, StreamId, Size}, Due)}
        }
    },
    arm_credit_timer(Queued, DueAt, Now);
apply_conn_credit(
    #state{
        protocol_state = #h2_state{conn_credit = {on_response, _, _}, h2_streams = Streams} = H2
    } = State,
    StreamId,
    Size
) ->
    #h2_stream{conn_uncredited = Acc} = Stream = maps:get(StreamId, Streams),
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = Streams#{StreamId => Stream#h2_stream{conn_uncredited = Acc + Size}}
        }
    }.

-spec apply_stream_credit(#state{}, nhttp_lib:stream_id(), pos_integer()) -> #state{}.
apply_stream_credit(
    #state{protocol_state = #h2_state{stream_credit = eager}} = State, StreamId, Size
) ->
    credit_stream(State, StreamId, Size);
apply_stream_credit(
    #state{protocol_state = #h2_state{stream_credit = never}} = State, _StreamId, _Size
) ->
    State;
apply_stream_credit(
    #state{
        protocol_state = #h2_state{stream_credit = {threshold, N}, h2_streams = Streams} = H2
    } = State,
    StreamId,
    Size
) ->
    #h2_stream{uncredited = Acc} = Stream = maps:get(StreamId, Streams),
    case Acc + Size of
        Total when Total >= N ->
            Reset = State#state{
                protocol_state = H2#h2_state{
                    h2_streams = Streams#{StreamId => Stream#h2_stream{uncredited = 0}}
                }
            },
            credit_stream(Reset, StreamId, Total);
        Total ->
            State#state{
                protocol_state = H2#h2_state{
                    h2_streams = Streams#{StreamId => Stream#h2_stream{uncredited = Total}}
                }
            }
    end;
apply_stream_credit(
    #state{protocol_state = #h2_state{stream_credit = {delay, Ms, Due}} = H2} = State,
    StreamId,
    Size
) ->
    Now = erlang:monotonic_time(millisecond),
    DueAt = Now + Ms,
    Queued = State#state{
        protocol_state = H2#h2_state{
            stream_credit = {delay, Ms, queue:in({DueAt, StreamId, Size}, Due)}
        }
    },
    arm_credit_timer(Queued, DueAt, Now);
apply_stream_credit(
    #state{protocol_state = #h2_state{stream_credit = on_response, h2_streams = Streams} = H2} =
        State,
    StreamId,
    Size
) ->
    #h2_stream{uncredited = Acc} = Stream = maps:get(StreamId, Streams),
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = Streams#{StreamId => Stream#h2_stream{uncredited = Acc + Size}}
        }
    }.

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
Arm the one credit timer of the connection for `DueAt` unless it is
already armed for an earlier or equal instant. A later timer is
cancelled first. Its stale message is ignored by the reference check in
`handle_h2_credit_delay/2`.
""".
-spec arm_credit_timer(#state{}, integer(), integer()) -> #state{}.
arm_credit_timer(
    #state{protocol_state = #h2_state{credit_timer = {_Ref, Armed}}} = State, DueAt, _Now
) when
    Armed =< DueAt
->
    State;
arm_credit_timer(
    #state{protocol_state = #h2_state{credit_timer = Timer} = H2} = State, DueAt, Now
) ->
    ok = cancel_credit_timer(Timer),
    Ref = erlang:start_timer(max(0, DueAt - Now), self(), ?CREDIT_DELAY_TAG),
    State#state{protocol_state = H2#h2_state{credit_timer = {Ref, DueAt}}}.

-spec cancel_credit_timer({reference(), integer()} | undefined) -> ok.
cancel_credit_timer(undefined) ->
    ok;
cancel_credit_timer({Ref, _DueAt}) ->
    ok = erlang:cancel_timer(Ref, [{async, true}, {info, false}]),
    ok.

-spec credit_connection(#state{}, pos_integer()) -> #state{}.
credit_connection(#state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, Size) ->
    case nhttp_h2:send_window_update(H2Conn, connection, Size) of
        {ok, H2Conn1, Frame} ->
            ok = nhttp_conn:sock_send(State, Frame),
            State#state{protocol_state = H2#h2_state{h2_conn = H2Conn1}};
        {error, _} ->
            State
    end.

-doc """
Turn every full batch of the `on_response` connection accumulator into
one WINDOW_UPDATE frame. A batch of 0 sends the whole accumulator.
""".
-spec credit_connection_batches(#state{}, iodata()) -> {#state{}, iodata()}.
credit_connection_batches(
    #state{protocol_state = #h2_state{conn_credit = {on_response, Batch, Acc}} = H2} = State,
    Frames
) when Acc > 0, Acc >= Batch ->
    Increment =
        case Batch of
            0 -> Acc;
            _ -> Batch
        end,
    Popped = State#state{
        protocol_state = H2#h2_state{conn_credit = {on_response, Batch, Acc - Increment}}
    },
    {State1, Frame} = window_update_frame(Popped, connection, Increment),
    credit_connection_batches(State1, [Frames, Frame]);
credit_connection_batches(State, Frames) ->
    {State, Frames}.

-spec credit_stream(#state{}, nhttp_lib:stream_id(), pos_integer()) -> #state{}.
credit_stream(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, StreamId, Size
) ->
    case nhttp_h2:send_window_update(H2Conn, StreamId, Size) of
        {ok, _UnknownStream, []} ->
            State;
        {ok, H2Conn1, Frame} ->
            ok = nhttp_conn:sock_send(State, Frame),
            State#state{protocol_state = H2#h2_state{h2_conn = H2Conn1}};
        {error, _} ->
            State
    end.

-spec draw_response_delay(nhttp:h2_response_delay()) -> non_neg_integer().
draw_response_delay(Ms) when is_integer(Ms) ->
    Ms;
draw_response_delay({uniform, MinMs, MaxMs}) ->
    MinMs + rand:uniform(MaxMs - MinMs + 1) - 1.

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
Worker acked a body chunk. Pop the oldest pending byte size and hand it
to the credit policies of the connection and the stream receive windows.
Acks for `fin` / `abort` events carry no flow-control debt and are
absorbed silently when the pending queue is empty.
""".
-spec handle_h2_body_chunk_ack(#state{}, pid(), reference()) -> #state{}.
handle_h2_body_chunk_ack(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} =
        State,
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
                            Popped = State#state{
                                protocol_state = H2#h2_state{
                                    h2_streams = Streams#{StreamId => Stream1}
                                }
                            },
                            replenish_recv_window(Popped, StreamId, Size);
                        {empty, _Rest} ->
                            State
                    end;
                _ ->
                    State
            end
        end
    ).

-doc """
Credit timer fired. Sends every delayed credit that is due on both
windows, oldest first, and re-arms the timer for the earliest entry
left. A timer reference that no longer matches is ignored.
""".
-spec handle_h2_credit_delay(#state{}, reference()) -> #state{}.
handle_h2_credit_delay(
    #state{protocol_state = #h2_state{credit_timer = {TimerRef, _DueAt}} = H2} = State, TimerRef
) ->
    Now = erlang:monotonic_time(millisecond),
    Disarmed = State#state{protocol_state = H2#h2_state{credit_timer = undefined}},
    Released = release_due_stream_credit(release_due_conn_credit(Disarmed, Now), Now),
    rearm_credit_timer(Released, Now);
handle_h2_credit_delay(State, _TimerRef) ->
    State.

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

-doc """
Hold timer fired for a delayed reply. A stream that was reset or closed
during the hold, or a timer reference that no longer matches, is ignored.
""".
-spec handle_h2_response_delay(#state{}, nhttp_lib:stream_id(), reference()) -> #state{}.
handle_h2_response_delay(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId, TimerRef
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = request, held_response = {TimerRef, Response}} = Stream ->
            Released = Stream#h2_stream{held_response = undefined},
            complete_h2_reply(State, StreamId, Released, Response);
        _ ->
            State
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

-spec head_credit_due(h2_conn_credit() | h2_stream_credit()) -> integer() | undefined.
head_credit_due(eager) ->
    undefined;
head_credit_due(never) ->
    undefined;
head_credit_due({threshold, _N}) ->
    undefined;
head_credit_due({threshold, _N, _Acc}) ->
    undefined;
head_credit_due(on_response) ->
    undefined;
head_credit_due({on_response, _Batch, _Acc}) ->
    undefined;
head_credit_due({delay, _Ms, Due}) ->
    case queue:peek(Due) of
        {value, {DueAt, _StreamId, _Size}} -> DueAt;
        empty -> undefined
    end.

-doc """
Park a compressed `{reply, _, _}` response on its stream until the
response-delay timer fires. The worker exits right after it posts the
result, so it is released here and its `DOWN` is flushed. The stream
keeps `type = request` and drops any DATA that arrives during the hold.
""".
-spec hold_h2_reply(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_lib:response(), nhttp:h2_response_delay()
) -> #state{}.
hold_h2_reply(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    StreamId,
    #h2_stream{worker = WPid} = Stream,
    Response,
    Delay
) ->
    TimerRef = erlang:start_timer(
        draw_response_delay(Delay), self(), {?RESPONSE_DELAY_TAG, StreamId}
    ),
    ok = demonitor_worker(Stream),
    Held = Stream#h2_stream{
        worker = undefined,
        worker_ref = undefined,
        worker_mref = undefined,
        pending_ack = undefined,
        streaming_body = false,
        held_response = {TimerRef, Response}
    },
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = Streams#{StreamId => Held},
            h2_workers = maps:remove(WPid, Workers)
        }
    }.

-spec join_credit(iodata(), iodata()) -> [iodata()].
join_credit([], []) ->
    [];
join_credit(ConnFrames, StreamFrames) ->
    [ConnFrames, StreamFrames].

-doc """
After a `{reply, _, _}` on a request whose body is still in flight, send
RST_STREAM(NO_ERROR) to ask the peer to stop the remainder (RFC 9113
Section 8.1). The reset is allowed only after a complete response, so a
queued body defers it to the `data_sent` event that carries its
END_STREAM, and a closed stream takes none. No-op when END_STREAM was
already received.
""".
-spec maybe_rst_stream_unread_body(
    #state{}, nhttp_lib:stream_id(), #h2_stream{}, response_outcome()
) -> #state{}.
maybe_rst_stream_unread_body(State, _StreamId, #h2_stream{end_stream = true}, _Outcome) ->
    State;
maybe_rst_stream_unread_body(State, StreamId, #h2_stream{end_stream = false}, sent) ->
    reset_h2_stream(State, StreamId, no_error);
maybe_rst_stream_unread_body(State, _StreamId, #h2_stream{end_stream = false}, queued) ->
    State;
maybe_rst_stream_unread_body(State, _StreamId, #h2_stream{end_stream = false}, closed) ->
    State.

-spec next_credit_due(integer() | undefined, integer() | undefined) -> integer() | undefined.
next_credit_due(undefined, StreamDue) ->
    StreamDue;
next_credit_due(ConnDue, undefined) ->
    ConnDue;
next_credit_due(ConnDue, StreamDue) ->
    min(ConnDue, StreamDue).

-spec rearm_credit_timer(#state{}, integer()) -> #state{}.
rearm_credit_timer(
    #state{protocol_state = #h2_state{conn_credit = Conn, stream_credit = Stream}} = State, Now
) ->
    case next_credit_due(head_credit_due(Conn), head_credit_due(Stream)) of
        undefined -> State;
        DueAt -> arm_credit_timer(State, DueAt, Now)
    end.

-spec release_conn_response_credit(#state{}, non_neg_integer()) -> {#state{}, iodata()}.
release_conn_response_credit(
    #state{protocol_state = #h2_state{conn_credit = {on_response, Batch, Acc}} = H2} = State,
    Octets
) ->
    Queued = State#state{
        protocol_state = H2#h2_state{conn_credit = {on_response, Batch, Acc + Octets}}
    },
    credit_connection_batches(Queued, []);
release_conn_response_credit(State, _Octets) ->
    {State, []}.

-spec release_due_conn_credit(#state{}, integer()) -> #state{}.
release_due_conn_credit(
    #state{protocol_state = #h2_state{conn_credit = {delay, Ms, Due}} = H2} = State, Now
) ->
    case queue:out(Due) of
        {{value, {DueAt, _StreamId, Size}}, Rest} when DueAt =< Now ->
            Popped = State#state{protocol_state = H2#h2_state{conn_credit = {delay, Ms, Rest}}},
            release_due_conn_credit(credit_connection(Popped, Size), Now);
        {{value, _NotDue}, _Rest} ->
            State;
        {empty, _Due} ->
            State
    end;
release_due_conn_credit(State, _Now) ->
    State.

-spec release_due_stream_credit(#state{}, integer()) -> #state{}.
release_due_stream_credit(
    #state{protocol_state = #h2_state{stream_credit = {delay, Ms, Due}} = H2} = State, Now
) ->
    case queue:out(Due) of
        {{value, {DueAt, StreamId, Size}}, Rest} when DueAt =< Now ->
            Popped = State#state{
                protocol_state = H2#h2_state{stream_credit = {delay, Ms, Rest}}
            },
            release_due_stream_credit(credit_stream(Popped, StreamId, Size), Now);
        {{value, _NotDue}, _Rest} ->
            State;
        {empty, _Due} ->
            State
    end;
release_due_stream_credit(State, _Now) ->
    State.

-doc """
Release the worker of a request stream. The stream entry is removed
unless the codec still holds octets of its response, in which case it
stays as a cleared `type = http` entry until the `data_sent` event with
END_STREAM retires it.
""".
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
    Streams1 =
        case maps:get(StreamId, Streams, undefined) of
            undefined ->
                Streams;
            #h2_stream{} = Live ->
                case stream_send_pending(State, StreamId) of
                    false ->
                        maps:remove(StreamId, Streams);
                    true ->
                        Cleared = Live#h2_stream{
                            type = http,
                            worker = undefined,
                            worker_ref = undefined,
                            worker_mref = undefined,
                            pending_ack = undefined,
                            request = undefined,
                            held_response = undefined
                        },
                        Streams#{StreamId => Cleared}
                end
        end,
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = Streams1, h2_workers = remove_worker(WPid, Workers)
        }
    }.

-doc """
Release the credit an `on_response` policy held for the request of
`Stream`. The connection octets move into the batch accumulator and
every full batch becomes one WINDOW_UPDATE. The stream octets become one
WINDOW_UPDATE. The frames go ahead of the response HEADERS, because
HEADERS with END_STREAM closes the stream and the codec refuses credit
on a closed stream.
""".
-spec release_response_credit(#state{}, nhttp_lib:stream_id(), #h2_stream{}) ->
    {#state{}, iodata()}.
release_response_credit(
    State, StreamId, #h2_stream{uncredited = StreamOctets, conn_uncredited = ConnOctets}
) ->
    {State1, ConnFrames} = release_conn_response_credit(State, ConnOctets),
    {State2, StreamFrames} = release_stream_response_credit(State1, StreamId, StreamOctets),
    {State2, join_credit(ConnFrames, StreamFrames)}.

-spec release_stream_response_credit(#state{}, nhttp_lib:stream_id(), non_neg_integer()) ->
    {#state{}, iodata()}.
release_stream_response_credit(
    #state{protocol_state = #h2_state{stream_credit = on_response}} = State, StreamId, Octets
) when Octets > 0 ->
    window_update_frame(State, StreamId, Octets);
release_stream_response_credit(State, _StreamId, _Octets) ->
    {State, []}.

-doc """
Credit the peer for `Size` consumed octets on both receive windows
through the resolved policies. The `eager` pair is the default and sends
both WINDOW_UPDATE frames at once.
""".
-spec replenish_recv_window(#state{}, nhttp_lib:stream_id(), non_neg_integer()) -> #state{}.
replenish_recv_window(State, _StreamId, 0) ->
    State;
replenish_recv_window(
    #state{protocol_state = #h2_state{conn_credit = eager, stream_credit = eager}} = State,
    StreamId,
    Size
) ->
    credit_stream(credit_connection(State, Size), StreamId, Size);
replenish_recv_window(State, StreamId, Size) ->
    apply_stream_credit(apply_conn_credit(State, StreamId, Size), StreamId, Size).

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

-doc """
Record the END_STREAM of the peer on a stream whose request body the
server discards: a reply held by the response delay, or a reply whose
body the codec still holds.
""".
-spec track_stream_end(#state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_h2:fin()) ->
    #state{}.
track_stream_end(State, _StreamId, _Stream, nofin) ->
    State;
track_stream_end(State, StreamId, Stream, fin) ->
    put_h2_stream(State, StreamId, Stream#h2_stream{end_stream = true}).

-doc """
Build one WINDOW_UPDATE for `Target` without sending it. A stream the
codec no longer has yields no frame. An overflow keeps the codec state
and yields no frame, which is the codec contract.
""".
-spec window_update_frame(#state{}, connection | nhttp_lib:stream_id(), pos_integer()) ->
    {#state{}, iodata()}.
window_update_frame(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, Target, Size
) ->
    case nhttp_h2:send_window_update(H2Conn, Target, Size) of
        {ok, _UnknownStream, []} ->
            {State, []};
        {ok, H2Conn1, Frame} ->
            {State#state{protocol_state = H2#h2_state{h2_conn = H2Conn1}}, Frame};
        {error, _} ->
            {State, []}
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - WORKER MESSAGE HANDLERS
%%%-----------------------------------------------------------------------------
-spec cleanup_h2_worker_entry(#state{}, pid()) -> #state{}.
cleanup_h2_worker_entry(
    #state{protocol_state = #h2_state{h2_workers = Workers} = H2} = State, WPid
) ->
    State#state{protocol_state = H2#h2_state{h2_workers = maps:remove(WPid, Workers)}}.

-doc """
End a producer stream: emit the request-stop telemetry, drop the monitor
of its worker and remove the stream and the worker entries.
""".
-spec complete_h2_stream(#state{}, nhttp_lib:stream_id(), atom()) -> #state{}.
complete_h2_stream(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers} = H2} = State,
    StreamId,
    Reason
) ->
    #h2_stream{worker = WPid} = Stream = maps:get(StreamId, Streams),
    emit_h2_stream_complete(State, StreamId, Stream, Reason),
    ok = demonitor_worker(Stream),
    State#state{
        protocol_state = H2#h2_state{
            h2_streams = maps:remove(StreamId, Streams),
            h2_workers = remove_worker(WPid, Workers)
        }
    }.

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
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId, WPid, Ref, Data, Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = stream, worker = WPid, worker_ref = Ref} = Stream0 ->
            Stream1 = maybe_emit_h2_stream_start(State, Stream0),
            BytesAdded = iolist_size(Data),
            Stream2 = Stream1#h2_stream{bytes_sent = Stream1#h2_stream.bytes_sent + BytesAdded},
            State1 = put_h2_stream(State, StreamId, Stream2),
            {Outcome, State2} = offer_h2_data(State1, StreamId, Data, Fin, []),
            finalize_h2_worker_send(State2, StreamId, WPid, Ref, Fin, Outcome);
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

-doc """
Settle a producer chunk after its offer. A chunk that left in full is
acked at once. A queued chunk sets `pending_ack`, and the ack is paid
when the codec holds nothing for the stream. A chunk the queue bound
refused ends the stream with RST_STREAM(ENHANCE_YOUR_CALM) and the
producer sees `{error, closed}`.
""".
-spec finalize_h2_worker_send(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), nhttp_h2:fin(), offer_outcome()
) -> #state{}.
finalize_h2_worker_send(State, StreamId, WPid, Ref, fin, sent) ->
    WPid ! {chunk_ack, Ref, ok},
    complete_h2_stream(State, StreamId, normal);
finalize_h2_worker_send(State, _StreamId, WPid, Ref, nofin, sent) ->
    WPid ! {chunk_ack, Ref, ok},
    State;
finalize_h2_worker_send(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State,
    StreamId,
    _WPid,
    Ref,
    _Fin,
    queued
) ->
    Stream = maps:get(StreamId, Streams),
    put_h2_stream(State, StreamId, Stream#h2_stream{pending_ack = Ref});
finalize_h2_worker_send(State, StreamId, WPid, Ref, _Fin, send_buffer_full) ->
    State1 = reset_h2_stream(State, StreamId, enhance_your_calm),
    WPid ! {chunk_ack, Ref, {error, closed}},
    complete_h2_stream(State1, StreamId, send_buffer_full);
finalize_h2_worker_send(State, StreamId, WPid, Ref, _Fin, stream_gone) ->
    WPid ! {chunk_ack, Ref, {error, closed}},
    cleanup_h2_worker_entry(remove_h2_stream(State, StreamId), WPid).

-doc """
The producer of `StreamId` exited before its END_STREAM went out. An
empty `fin` closes the stream. Behind pending octets the codec queues it
and the entry stays, without a worker, until the `data_sent` event with
END_STREAM retires it.
""".
-spec finish_orphan_h2_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
finish_orphan_h2_stream(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId
) ->
    Stream = maps:get(StreamId, Streams),
    Orphan = Stream#h2_stream{
        worker = undefined,
        worker_ref = undefined,
        worker_mref = undefined,
        pending_ack = undefined
    },
    case offer_h2_data(put_h2_stream(State, StreamId, Orphan), StreamId, <<>>, fin, []) of
        {sent, State1} ->
            complete_h2_stream(State1, StreamId, normal);
        {queued, State1} ->
            State1;
        {send_buffer_full, State1} ->
            State2 = reset_h2_stream(State1, StreamId, enhance_your_calm),
            complete_h2_stream(State2, StreamId, send_buffer_full);
        {stream_gone, State1} ->
            complete_h2_stream(State1, StreamId, normal)
    end.

-doc """
The codec drained `StreamId`. Pay a producer the ack it is owed once the
codec holds nothing for the stream, and retire the entry when the
END_STREAM frame went out. A missing entry is a WebSocket stream that
`nhttp_conn_ws_h2` dropped after it queued its CLOSE frame, or a stream
the peer reset. A WebSocket session owns nothing in the queue. A `request`
entry is unreachable, because the reply path clears the type before the
body queues.
""".
-spec handle_h2_data_sent(#state{}, nhttp_lib:stream_id(), nhttp_h2:fin()) -> #state{}.
handle_h2_data_sent(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId, Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h2_stream{type = stream} = Stream ->
            handle_h2_stream_drained(State, StreamId, Stream, Fin);
        #h2_stream{type = http} = Stream ->
            handle_h2_reply_drained(State, StreamId, Stream, Fin);
        #h2_stream{type = websocket} ->
            State;
        #h2_stream{type = request} ->
            State
    end.

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
        #h2_stream{type = request, held_response = {_, _}} = Stream ->
            track_stream_end(State, StreamId, Stream, Fin);
        #h2_stream{type = request} = Stream ->
            handle_h2_request_data(State, StreamId, Stream, Data, Fin);
        #h2_stream{type = http} = Stream ->
            track_stream_end(State, StreamId, Stream, Fin);
        #h2_stream{type = stream} ->
            State
    end;
handle_h2_event(
    #state{protocol_state = #h2_state{h2_streams = Streams}} = State, {trailers, StreamId, Trailers}
) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            State;
        #h2_stream{type = request} = Stream ->
            handle_h2_request_trailers(State, StreamId, Stream, Trailers);
        #h2_stream{type = http} = Stream ->
            track_stream_end(State, StreamId, Stream, fin);
        #h2_stream{type = stream} ->
            State;
        #h2_stream{type = websocket} ->
            State
    end;
handle_h2_event(State, {goaway, _LastStreamId, ErrorCode, _DebugData}) ->
    nhttp_conn_ws_h2:notify_goaway(State, ErrorCode);
handle_h2_event(State, {settings, _NewSettings}) ->
    State;
handle_h2_event(State, settings_ack) ->
    State;
handle_h2_event(State, {window_update, _StreamId, _Increment}) ->
    State;
handle_h2_event(State, {data_sent, StreamId, _Bytes, Fin}) ->
    handle_h2_data_sent(State, StreamId, Fin);
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
handle_h2_limit_error(State, StreamId, body_too_large = E) ->
    send_h2_error_response(State, StreamId, nhttp_limits:error_to_status(E)),
    remove_h2_stream(send_h2_rst_stream(State, StreamId), StreamId);
handle_h2_limit_error(State, StreamId, LimitError) ->
    send_h2_error_response(State, StreamId, nhttp_limits:error_to_status(LimitError)),
    remove_h2_stream(State, StreamId).

-spec handle_h2_reply_drained(#state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_h2:fin()) ->
    #state{}.
handle_h2_reply_drained(State, _StreamId, _Stream, nofin) ->
    State;
handle_h2_reply_drained(State, StreamId, #h2_stream{end_stream = true}, fin) ->
    remove_h2_stream(State, StreamId);
handle_h2_reply_drained(State, StreamId, #h2_stream{end_stream = false}, fin) ->
    remove_h2_stream(reset_h2_stream(State, StreamId, no_error), StreamId).

-spec handle_h2_stream_drained(#state{}, nhttp_lib:stream_id(), #h2_stream{}, nhttp_h2:fin()) ->
    #state{}.
handle_h2_stream_drained(State, StreamId, Stream, nofin) ->
    pay_pending_ack(State, StreamId, Stream);
handle_h2_stream_drained(State, StreamId, Stream, fin) ->
    complete_h2_stream(pay_pending_ack(State, StreamId, Stream), StreamId, normal).

-spec handle_h2_worker_down(#state{}, pid(), term()) -> #state{}.
handle_h2_worker_down(
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers}} = State,
    WPid,
    Reason
) ->
    nhttp_conn_workers:route_silent(
        WPid,
        Workers,
        State,
        fun(StreamId) ->
            Released = cleanup_h2_worker_entry(State, WPid),
            case maps:get(StreamId, Streams, undefined) of
                undefined ->
                    Released;
                #h2_stream{type = stream} when Reason =:= normal ->
                    finish_orphan_h2_stream(Released, StreamId);
                #h2_stream{type = stream} = Stream ->
                    nhttp_log:stream_push_producer_crashed(
                        nhttp_conn:log_ctx(State), Reason
                    ),
                    emit_h2_stream_complete(State, StreamId, Stream, producer_crashed),
                    remove_h2_stream(send_h2_rst_stream(Released, StreamId), StreamId);
                #h2_stream{type = request} = Stream when Reason =:= normal ->
                    State1 = remove_h2_stream(send_h2_rst_stream(Released, StreamId), StreamId),
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
                    State1 = remove_h2_stream(Released, StreamId),
                    nhttp_conn:emit_request_stop(
                        State1, ?HTTP_INTERNAL_SERVER_ERROR, Stream#h2_stream.req_span
                    ),
                    State1#state{requests_count = State1#state.requests_count + 1};
                _ ->
                    Released
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
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers}} = State,
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
                #h2_stream{type = stream, worker = WPid, worker_ref = Ref} ->
                    send_h2_trailers(State, StreamId, WPid, Ref, Trailers);
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
    ok = cancel_held_response(Stream),
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

-doc """
Offer `Data` on `StreamId` to the codec send queue and write the frames
that the credit paid for, behind `Prefix` in one socket write. The
outcome names what the codec did with the offer. `Prefix` goes out on
every outcome, because the caller already encoded it.
""".
-spec offer_h2_data(#state{}, nhttp_lib:stream_id(), iodata(), nhttp_h2:fin(), iodata()) ->
    {offer_outcome(), #state{}}.
offer_h2_data(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, StreamId, Data, Fin, Prefix
) ->
    case nhttp_h2:send_data(H2Conn, StreamId, Data, Fin) of
        {ok, NewH2Conn, Frames} ->
            ok = write_frames(State, Prefix, Frames),
            {sent, State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}};
        {queued, NewH2Conn, Frames, _Buffered} ->
            ok = write_frames(State, Prefix, Frames),
            {queued, State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}};
        {error, send_buffer_full} ->
            ok = write_frames(State, Prefix, []),
            {send_buffer_full, State};
        {error, {stream_closed, _}} ->
            ok = write_frames(State, Prefix, []),
            {stream_gone, State};
        {error, {unknown_stream, _}} ->
            ok = write_frames(State, Prefix, []),
            {stream_gone, State}
    end.

-doc """
Pay the ack a producer is owed for its queued chunk once the codec holds
nothing for the stream. A stream without a worker owes no ack.
""".
-spec pay_pending_ack(#state{}, nhttp_lib:stream_id(), #h2_stream{}) -> #state{}.
pay_pending_ack(State, StreamId, #h2_stream{worker = WPid, pending_ack = Ref} = Stream) when
    is_pid(WPid), is_reference(Ref)
->
    case stream_send_pending(State, StreamId) of
        true ->
            State;
        false ->
            WPid ! {chunk_ack, Ref, ok},
            put_h2_stream(State, StreamId, Stream#h2_stream{pending_ack = undefined})
    end;
pay_pending_ack(State, _StreamId, #h2_stream{}) ->
    State.

-spec put_h2_stream(#state{}, nhttp_lib:stream_id(), #h2_stream{}) -> #state{}.
put_h2_stream(
    #state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId, Stream
) ->
    State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => Stream}}}.

-doc """
Name every refusal of `nhttp_h2:send_headers/4` on the reply path. A
stream the codec closed is removed. HEADERS behind pending DATA cannot
happen on a request stream, so it is a broken server invariant and the
stream is reset with INTERNAL_ERROR. After GOAWAY the connection stops,
so nothing more goes out on the stream.
""".
-spec refused_h2_response(
    #state{},
    nhttp_lib:stream_id(),
    connection_closing
    | {stream_closed, nhttp_lib:stream_id()}
    | {data_pending, nhttp_lib:stream_id()}
) -> {closed, #state{}}.
refused_h2_response(State, _StreamId, connection_closing) ->
    {closed, State};
refused_h2_response(State, StreamId, {stream_closed, _}) ->
    {closed, remove_h2_stream(State, StreamId)};
refused_h2_response(State, StreamId, {data_pending, _}) ->
    {closed, remove_h2_stream(reset_h2_stream(State, StreamId, internal_error), StreamId)}.

-spec remove_h2_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
remove_h2_stream(#state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId) ->
    State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}}.

-spec remove_worker(pid() | undefined, #{pid() => nhttp_lib:stream_id()}) ->
    #{pid() => nhttp_lib:stream_id()}.
remove_worker(undefined, Workers) ->
    Workers;
remove_worker(WPid, Workers) ->
    maps:remove(WPid, Workers).

-doc """
Send RST_STREAM with `ErrorCode` through the codec, which closes the
stream and purges the octets it holds for it, so the drain never emits
DATA on a stream the server reset (RFC 9113 Section 5.1).
""".
-spec reset_h2_stream(#state{}, nhttp_lib:stream_id(), nhttp_h2:error_code()) -> #state{}.
reset_h2_stream(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, StreamId, ErrorCode
) ->
    {ok, NewH2Conn, Frame} = nhttp_h2:send_rst_stream(H2Conn, StreamId, ErrorCode),
    ok = nhttp_conn:sock_send(State, Frame),
    State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}.

-spec response_to_headers(nhttp_lib:response()) -> [{binary(), binary()}].
response_to_headers(#{status := Status, headers := Headers}) ->
    [{<<":status">>, integer_to_binary(Status)} | Headers].

-spec send_credit_only(#state{}, iodata()) -> ok.
send_credit_only(_State, []) ->
    ok;
send_credit_only(State, Credit) ->
    nhttp_conn:sock_send(State, Credit).

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

-doc """
Send a HEADERS frame and return the codec state. A refusal keeps the
codec state: the stream is closed or the connection sent GOAWAY, and
`{data_pending, _}` cannot happen on a stream that has not started its
body.
""".
-spec send_h2_headers(#state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), nhttp_h2:fin()) ->
    nhttp_h2:conn().
send_h2_headers(
    #state{protocol_state = #h2_state{h2_conn = H2Conn}} = State, StreamId, Headers, Fin
) ->
    case nhttp_h2:send_headers(H2Conn, StreamId, Headers, Fin) of
        {ok, NewH2Conn, Frame} ->
            ok = nhttp_conn:sock_send(State, Frame),
            NewH2Conn;
        {error, connection_closing} ->
            H2Conn;
        {error, {stream_closed, _}} ->
            H2Conn;
        {error, {data_pending, _}} ->
            H2Conn
    end.

-doc """
Send a `{reply, _, _}` response with `Credit`, the WINDOW_UPDATE frames
of an `on_response` policy, ahead of it in the same socket write. The
body goes through the codec send queue, and the outcome says whether it
left in full, whether the codec holds part of it, or whether the stream
is closed. When the codec refuses the HEADERS the credit still goes out,
because the codec already counted it.
""".
-spec send_h2_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:response(), iodata()) ->
    {response_outcome(), #state{}}.
send_h2_response(State, StreamId, Response, Credit) ->
    Headers = nhttp_conn:alt_svc_headers(State, response_to_headers(Response)),
    case maps:get(body, Response, <<>>) of
        <<>> -> send_h2_response_headers_only(State, StreamId, Headers, Credit);
        Body -> send_h2_response_with_body(State, StreamId, Headers, Body, Credit)
    end.

-spec send_h2_response_headers_only(
    #state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), iodata()
) -> {response_outcome(), #state{}}.
send_h2_response_headers_only(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State, StreamId, Headers, Credit
) ->
    case nhttp_h2:send_headers(H2Conn, StreamId, Headers, fin) of
        {ok, NewH2Conn, Frame} ->
            ok = send_with_credit(State, Credit, Frame),
            {sent, State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}}};
        {error, Reason} ->
            ok = send_credit_only(State, Credit),
            refused_h2_response(State, StreamId, Reason)
    end.

-doc """
Send HEADERS and the DATA frames the credit pays for in one socket
write, `Credit` first. A body the queue bound refuses ends the stream
with RST_STREAM(ENHANCE_YOUR_CALM) behind the HEADERS that already went
out.
""".
-spec send_h2_response_with_body(
    #state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), iodata(), iodata()
) -> {response_outcome(), #state{}}.
send_h2_response_with_body(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State,
    StreamId,
    Headers,
    Body,
    Credit
) ->
    case nhttp_h2:send_headers(H2Conn, StreamId, Headers, nofin) of
        {ok, H2Conn1, HeaderFrame} ->
            State1 = State#state{protocol_state = H2#h2_state{h2_conn = H2Conn1}},
            case offer_h2_data(State1, StreamId, Body, fin, [Credit, HeaderFrame]) of
                {sent, State2} ->
                    {sent, State2};
                {queued, State2} ->
                    {queued, State2};
                {send_buffer_full, State2} ->
                    State3 = reset_h2_stream(State2, StreamId, enhance_your_calm),
                    {closed, remove_h2_stream(State3, StreamId)};
                {stream_gone, State2} ->
                    {closed, remove_h2_stream(State2, StreamId)}
            end;
        {error, Reason} ->
            ok = send_credit_only(State, Credit),
            refused_h2_response(State, StreamId, Reason)
    end.

-spec send_h2_rst_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
send_h2_rst_stream(State, StreamId) ->
    reset_h2_stream(State, StreamId, internal_error).

-doc """
Send the trailers of a producer and end its stream. HEADERS behind
pending DATA cannot happen here, because a producer's `SendChunk` returns
only when its chunk left, so `{data_pending, _}` is a broken server
invariant: the stream is reset with INTERNAL_ERROR and the producer sees
`{error, closed}`.
""".
-spec send_h2_trailers(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), nhttp_lib:headers()
) -> #state{}.
send_h2_trailers(
    #state{protocol_state = #h2_state{h2_conn = H2Conn} = H2} = State,
    StreamId,
    WPid,
    Ref,
    Trailers
) ->
    case nhttp_h2:send_headers(H2Conn, StreamId, Trailers, fin) of
        {ok, NewH2Conn, Frame} ->
            ok = nhttp_conn:sock_send(State, Frame),
            WPid ! {chunk_ack, Ref, ok},
            State1 = State#state{protocol_state = H2#h2_state{h2_conn = NewH2Conn}},
            complete_h2_stream(State1, StreamId, normal);
        {error, {data_pending, _}} ->
            State1 = reset_h2_stream(State, StreamId, internal_error),
            WPid ! {chunk_ack, Ref, {error, closed}},
            complete_h2_stream(State1, StreamId, internal_error);
        {error, connection_closing} ->
            WPid ! {chunk_ack, Ref, ok},
            complete_h2_stream(State, StreamId, normal);
        {error, {stream_closed, _}} ->
            WPid ! {chunk_ack, Ref, ok},
            complete_h2_stream(State, StreamId, normal)
    end.

-spec send_with_credit(#state{}, iodata(), iodata()) -> ok.
send_with_credit(State, [], Frames) ->
    nhttp_conn:sock_send(State, Frames);
send_with_credit(State, Credit, Frames) ->
    nhttp_conn:sock_send(State, [Credit, Frames]).

-doc """
The codec holds octets of `StreamId` in its send queue. A `#h2_stream{}`
stays in `h2_streams` while this holds, so that `drain_idle/1` and
`hibernate_eligible/1` see the stream, and it leaves on the `data_sent`
event that carries END_STREAM.
""".
-spec stream_send_pending(#state{}, nhttp_lib:stream_id()) -> boolean().
stream_send_pending(#state{protocol_state = #h2_state{h2_conn = H2Conn}}, StreamId) ->
    nhttp_h2:send_buffer_bytes(H2Conn, StreamId) > 0.

-spec write_frames(#state{}, iodata(), iodata()) -> ok.
write_frames(_State, [], []) ->
    ok;
write_frames(State, Prefix, Frames) ->
    nhttp_conn:sock_send(State, [Prefix, Frames]).
