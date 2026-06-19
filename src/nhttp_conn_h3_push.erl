-module(nhttp_conn_h3_push).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn_h3.hrl").
-include("nhttp_status_codes.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    dispatch_h3_stream_push/8,
    flush_h3_stream_buffer/2,
    handle_h3_push_send_chunk/5,
    handle_h3_push_send_trailers/4,
    handle_h3_push_stop_sending/3,
    handle_h3_push_stream_reset/3,
    handle_h3_push_worker_down/3
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec dispatch_h3_stream_push(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:request(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    nhttp_stream:producer(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> #state{}.
dispatch_h3_stream_push(
    State, StreamId, Request, Status, Headers, Producer, NewHState, ReqSpan
) ->
    case validate_h3_stream_push(Request, Status) of
        ok_head ->
            RespHeaders = [{<<":status">>, integer_to_binary(Status)} | Headers],
            State1 = nhttp_conn_h3:send_h3_headers(State, StreamId, RespHeaders, fin),
            nhttp_conn_h3:emit_request_stop_with_size(State1, Status, ReqSpan, 0),
            State1#state{
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        {reject, RejectReason} ->
            #{method := Method, path := Path} = Request,
            nhttp_log:stream_push_rejected(
                nhttp_conn_h3:log_ctx(State),
                Method,
                Path,
                RejectReason
            ),
            State1 = nhttp_conn_h3:send_h3_error_response(
                State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR
            ),
            nhttp_conn_h3:emit_request_stop(State1, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
            State1#state{
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        ok ->
            start_h3_stream_push(
                State, StreamId, Status, Headers, Producer, NewHState, ReqSpan
            )
    end.

-spec flush_h3_stream_buffer(#state{}, nhttp_lib:stream_id()) -> #state{}.
flush_h3_stream_buffer(#state{h3_streams = Streams} = State, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{
            type = push,
            push =
                #h3_push_ctx{
                    send_buffer = Buf,
                    send_end_stream = EndStream,
                    worker = WPid,
                    ref = Ref0,
                    pending_ack = PendingRef
                } = Ctx
        } = Stream when Buf =/= <<>> ->
            Ref =
                case PendingRef of
                    undefined -> Ref0;
                    _ -> PendingRef
                end,
            Fin =
                case EndStream of
                    true -> fin;
                    false -> nofin
                end,
            Stream1 = Stream#h3_stream{
                push = Ctx#h3_push_ctx{
                    send_buffer = <<>>,
                    send_end_stream = false,
                    pending_ack = undefined
                }
            },
            Streams1 = Streams#{StreamId => Stream1},
            State1 = State#state{h3_streams = Streams1},
            Action =
                case Fin of
                    fin -> {send_fin, StreamId, Buf};
                    nofin -> {send, StreamId, Buf}
                end,
            execute_h3_push_action(State1, StreamId, WPid, Ref, Action, Fin);
        _ ->
            State
    end.

-spec handle_h3_push_send_chunk(
    #state{}, pid(), reference(), iodata(), nhttp_h3:fin()
) -> #state{}.
handle_h3_push_send_chunk(
    #state{h3_push_workers = Workers} = State, WPid, Ref, Data, Fin
) ->
    nhttp_conn_workers:route(
        WPid,
        Ref,
        Workers,
        State,
        fun(StreamId) ->
            h3_push_send_chunk(State, StreamId, WPid, Ref, Data, Fin)
        end
    ).

-spec handle_h3_push_send_trailers(
    #state{}, pid(), reference(), nhttp_lib:headers()
) -> #state{}.
handle_h3_push_send_trailers(
    #state{h3_push_workers = Workers, h3_streams = Streams} = State,
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
                #h3_stream{
                    type = push, push = #h3_push_ctx{ref = Ref, worker = WPid, mref = MRef}
                } = Stream0 ->
                    Stream1 = maybe_emit_h3_stream_start(State, Stream0),
                    State1 = nhttp_conn_h3:send_h3_headers(
                        State#state{h3_streams = Streams#{StreamId => Stream1}},
                        StreamId,
                        Trailers,
                        fin
                    ),
                    WPid ! {chunk_ack, Ref, ok},
                    emit_h3_stream_complete(State1, StreamId, Stream1, normal),
                    _ = erlang:demonitor(MRef, [flush]),
                    State1#state{
                        h3_streams = maps:remove(StreamId, State1#state.h3_streams),
                        h3_push_workers = maps:remove(WPid, Workers)
                    };
                _ ->
                    WPid ! {chunk_ack, Ref, {error, closed}},
                    cleanup_h3_push_worker_entry(State, WPid)
            end
        end
    ).

-spec handle_h3_push_stop_sending(
    #state{}, nhttp_lib:stream_id(), non_neg_integer()
) -> #state{}.
handle_h3_push_stop_sending(
    #state{h3_streams = Streams} = State, StreamId, _ErrorCode
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = push} = Stream ->
            handle_h3_push_stream_reset(State, StreamId, Stream);
        _ ->
            State
    end.

-spec handle_h3_push_stream_reset(
    #state{}, nhttp_lib:stream_id(), #h3_stream{}
) -> #state{}.
handle_h3_push_stream_reset(
    #state{h3_streams = Streams, h3_push_workers = Workers} = State, StreamId, Stream
) ->
    #h3_stream{
        push = #h3_push_ctx{
            worker = WPid,
            ref = Ref0,
            mref = MRef,
            pending_ack = PendingRef
        }
    } = Stream,
    Ref =
        case PendingRef of
            undefined -> Ref0;
            _ -> PendingRef
        end,
    emit_h3_stream_complete(State, StreamId, Stream, peer_reset),
    WPid ! {chunk_ack, Ref, {error, closed}},
    _ = erlang:demonitor(MRef, [flush]),
    State#state{
        h3_streams = maps:remove(StreamId, Streams),
        h3_push_workers = maps:remove(WPid, Workers)
    }.

-spec handle_h3_push_worker_down(#state{}, pid(), term()) -> #state{}.
handle_h3_push_worker_down(
    #state{h3_push_workers = Workers, h3_streams = Streams} = State, WPid, Reason
) ->
    nhttp_conn_workers:route_silent(
        WPid,
        Workers,
        State,
        fun(StreamId) ->
            Workers1 = maps:remove(WPid, Workers),
            case maps:get(StreamId, Streams, undefined) of
                #h3_stream{type = push} = Stream when Reason =:= normal ->
                    emit_h3_stream_complete(State, StreamId, Stream, normal),
                    State1 = State#state{h3_push_workers = Workers1},
                    send_h3_push_empty_fin(State1, StreamId);
                #h3_stream{type = push} = Stream ->
                    nhttp_log:stream_push_producer_crashed(
                        nhttp_conn_h3:log_ctx(State), Reason
                    ),
                    emit_h3_stream_complete(State, StreamId, Stream, producer_crashed),
                    State1 = nhttp_conn_h3:reset_stream(
                        State,
                        StreamId,
                        nhttp_conn_h3:h3_error_to_code(h3_internal_error)
                    ),
                    State1#state{
                        h3_push_workers = Workers1,
                        h3_streams = maps:remove(StreamId, Streams)
                    };
                _ ->
                    State#state{h3_push_workers = Workers1}
            end
        end
    ).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/3 PUSH STREAMING
%%%-----------------------------------------------------------------------------
-spec cleanup_h3_push_stream(
    #state{}, nhttp_lib:stream_id(), pid()
) -> #state{}.
cleanup_h3_push_stream(
    #state{h3_streams = Streams, h3_push_workers = Workers} = State, StreamId, WPid
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{push = #h3_push_ctx{mref = MRef}} ->
            _ = erlang:demonitor(MRef, [flush]);
        _ ->
            ok
    end,
    State#state{
        h3_streams = maps:remove(StreamId, Streams),
        h3_push_workers = maps:remove(WPid, Workers)
    }.

-spec cleanup_h3_push_worker_entry(#state{}, pid()) -> #state{}.
cleanup_h3_push_worker_entry(#state{h3_push_workers = Workers} = State, WPid) ->
    State#state{h3_push_workers = maps:remove(WPid, Workers)}.

-spec emit_h3_stream_complete(
    #state{}, nhttp_lib:stream_id(), #h3_stream{}, atom()
) -> ok.
emit_h3_stream_complete(_State, _StreamId, #h3_stream{push = undefined}, _Reason) ->
    ok;
emit_h3_stream_complete(State, _StreamId, #h3_stream{push = Ctx}, _Reason) ->
    #h3_push_ctx{status = Status, req_span = ReqSpan, bytes = Bytes} = Ctx,
    nhttp_conn_h3:emit_request_stop_with_size(State, Status, ReqSpan, Bytes).

-spec execute_h3_push_action(
    #state{},
    nhttp_lib:stream_id(),
    pid(),
    reference(),
    nhttp_h3:action(),
    nhttp_h3:fin()
) -> #state{}.
execute_h3_push_action(State, StreamId, WPid, Ref, {send, StreamId, Frame}, Fin) ->
    case nquic_lib:send(State#state.ctx, StreamId, Frame) of
        {ok, Ctx1} ->
            State1 = State#state{ctx = Ctx1},
            finalize_h3_push_send(State1, StreamId, WPid, Ref, Fin);
        {error, _BlockedReason, Ctx1} ->
            stash_h3_push_frame(
                State#state{ctx = Ctx1}, StreamId, Ref, Frame, Fin
            );
        {error, Reason} ->
            handle_h3_push_send_fatal(State, StreamId, WPid, Ref, Reason)
    end;
execute_h3_push_action(State, StreamId, WPid, Ref, {send_fin, StreamId, Frame}, Fin) ->
    case nquic_lib:send_fin_noflush(State#state.ctx, StreamId, Frame) of
        {ok, Ctx1} ->
            State1 = State#state{ctx = Ctx1},
            finalize_h3_push_send(State1, StreamId, WPid, Ref, Fin);
        {error, _BlockedReason, Ctx1} ->
            stash_h3_push_frame(
                State#state{ctx = Ctx1}, StreamId, Ref, Frame, Fin
            );
        {error, Reason} ->
            handle_h3_push_send_fatal(State, StreamId, WPid, Ref, Reason)
    end.

-spec finalize_h3_push_send(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), nhttp_h3:fin()
) -> #state{}.
finalize_h3_push_send(State, StreamId, WPid, Ref, fin) ->
    WPid ! {chunk_ack, Ref, ok},
    #state{h3_streams = Streams, h3_push_workers = Workers} = State,
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = push, push = #h3_push_ctx{mref = MRef}} = Stream ->
            emit_h3_stream_complete(State, StreamId, Stream, normal),
            _ = erlang:demonitor(MRef, [flush]),
            State#state{
                h3_streams = maps:remove(StreamId, Streams),
                h3_push_workers = maps:remove(WPid, Workers)
            };
        _ ->
            State#state{h3_push_workers = maps:remove(WPid, Workers)}
    end;
finalize_h3_push_send(State, _StreamId, WPid, Ref, nofin) ->
    WPid ! {chunk_ack, Ref, ok},
    State.

-spec h3_push_send_chunk(
    #state{},
    nhttp_lib:stream_id(),
    pid(),
    reference(),
    iodata(),
    nhttp_h3:fin()
) -> #state{}.
h3_push_send_chunk(
    #state{h3_streams = Streams, h3_conn = H3Conn} = State,
    StreamId,
    WPid,
    Ref,
    Data,
    Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = push, push = #h3_push_ctx{ref = Ref, worker = WPid}} = Stream0 ->
            Stream1 = maybe_emit_h3_stream_start(State, Stream0),
            #h3_stream{push = Ctx1} = Stream1,
            BytesAdded = iolist_size(Data),
            Stream2 = Stream1#h3_stream{
                push = Ctx1#h3_push_ctx{bytes = Ctx1#h3_push_ctx.bytes + BytesAdded}
            },
            case nhttp_h3:send_data(H3Conn, StreamId, Data, Fin) of
                {ok, NewH3Conn, [Action]} ->
                    State1 = State#state{
                        h3_conn = NewH3Conn,
                        h3_streams = Streams#{StreamId => Stream2}
                    },
                    execute_h3_push_action(State1, StreamId, WPid, Ref, Action, Fin);
                {error, _} ->
                    WPid ! {chunk_ack, Ref, {error, closed}},
                    cleanup_h3_push_stream(State, StreamId, WPid)
            end;
        _ ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            cleanup_h3_push_worker_entry(State, WPid)
    end.

-spec handle_h3_push_send_fatal(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), term()
) -> #state{}.
handle_h3_push_send_fatal(State, StreamId, WPid, Ref, _Reason) ->
    WPid ! {chunk_ack, Ref, {error, closed}},
    State1 = nhttp_conn_h3:reset_stream(
        State, StreamId, nhttp_conn_h3:h3_error_to_code(h3_internal_error)
    ),
    cleanup_h3_push_stream(State1, StreamId, WPid).

-spec maybe_emit_h3_stream_start(#state{}, #h3_stream{}) -> #h3_stream{}.
maybe_emit_h3_stream_start(_State, #h3_stream{push = #h3_push_ctx{started = true}} = Stream) ->
    Stream;
maybe_emit_h3_stream_start(
    State, #h3_stream{push = #h3_push_ctx{req_span = ReqSpan} = Ctx} = Stream
) ->
    nhttp_conn_h3:emit_stream_start(State, ReqSpan),
    Stream#h3_stream{push = Ctx#h3_push_ctx{started = true}}.

-spec send_h3_push_empty_fin(#state{}, nhttp_lib:stream_id()) -> #state{}.
send_h3_push_empty_fin(#state{h3_streams = Streams} = State, StreamId) ->
    State1 = nhttp_conn_h3:send_h3_data(State, StreamId, <<>>, fin),
    State1#state{h3_streams = maps:remove(StreamId, Streams)}.

-spec start_h3_stream_push(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    nhttp_stream:producer(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> #state{}.
start_h3_stream_push(State, StreamId, Status, Headers, Producer, NewHState, ReqSpan) ->
    RespHeaders = [{<<":status">>, integer_to_binary(Status)} | Headers],
    State1 = nhttp_conn_h3:send_h3_headers(State, StreamId, RespHeaders, nofin),
    Ref = make_ref(),
    WorkerPid = nhttp_stream_worker:start(self(), Ref, Producer),
    MRef = erlang:monitor(process, WorkerPid),
    PushStream = #h3_stream{
        type = push,
        push = #h3_push_ctx{
            worker = WorkerPid,
            ref = Ref,
            mref = MRef,
            status = Status,
            req_span = ReqSpan
        }
    },
    State1#state{
        handler_state = NewHState,
        h3_streams = (State1#state.h3_streams)#{StreamId => PushStream},
        h3_push_workers = (State1#state.h3_push_workers)#{WorkerPid => StreamId},
        requests_count = State#state.requests_count + 1
    }.

-spec stash_h3_push_frame(
    #state{},
    nhttp_lib:stream_id(),
    reference(),
    iodata(),
    nhttp_h3:fin()
) -> #state{}.
stash_h3_push_frame(
    #state{h3_streams = Streams} = State, StreamId, Ref, Frame, Fin
) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = push, push = #h3_push_ctx{} = Ctx} = Stream ->
            Stream1 = Stream#h3_stream{
                push = Ctx#h3_push_ctx{
                    send_buffer = iolist_to_binary(Frame),
                    send_end_stream = Fin =:= fin,
                    pending_ack = Ref
                }
            },
            State#state{h3_streams = Streams#{StreamId => Stream1}};
        _ ->
            State
    end.

-spec validate_h3_stream_push(nhttp_lib:request(), nhttp_lib:status()) ->
    ok | ok_head | {reject, term()}.
validate_h3_stream_push(_Request, Status) when
    Status =:= ?HTTP_NO_CONTENT; Status =:= ?HTTP_NOT_MODIFIED
->
    {reject, {no_body_status, Status}};
validate_h3_stream_push(#{method := head}, _Status) ->
    ok_head;
validate_h3_stream_push(_Request, _Status) ->
    ok.
