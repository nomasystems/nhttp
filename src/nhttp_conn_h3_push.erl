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
    handle_h3_push_flushed/2,
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

-doc """
Settle a push stream after its send buffer drained or failed. A drained
buffer pays the pending chunk ack, and finishes the stream when that
chunk carried FIN. A failed flush resets the stream.
""".
-spec handle_h3_push_flushed(nhttp_lib:stream_id(), nhttp_conn_h3:offer_result()) -> #state{}.
handle_h3_push_flushed(_StreamId, {stashed, State}) ->
    State;
handle_h3_push_flushed(StreamId, {sent, #state{h3_streams = Streams} = State}) ->
    case maps:get(StreamId, Streams) of
        #h3_stream{push = #h3_push_ctx{pending = undefined}} ->
            State;
        #h3_stream{push = #h3_push_ctx{worker = WPid, pending = {Ref, Fin}} = Ctx} = Stream ->
            Stream1 = Stream#h3_stream{push = Ctx#h3_push_ctx{pending = undefined}},
            State1 = State#state{h3_streams = Streams#{StreamId => Stream1}},
            finalize_h3_push_send(State1, StreamId, WPid, Ref, Fin)
    end;
handle_h3_push_flushed(StreamId, {error, Reason, #state{h3_streams = Streams} = State}) ->
    #h3_stream{push = #h3_push_ctx{worker = WPid} = Ctx} = maps:get(StreamId, Streams),
    handle_h3_push_send_fatal(State, StreamId, WPid, ack_ref(Ctx), Reason).

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
    #h3_stream{push = #h3_push_ctx{worker = WPid, mref = MRef} = Ctx} = Stream,
    Ref = ack_ref(Ctx),
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
-spec ack_ref(#h3_push_ctx{}) -> reference().
ack_ref(#h3_push_ctx{ref = Ref, pending = undefined}) -> Ref;
ack_ref(#h3_push_ctx{pending = {Ref, _Fin}}) -> Ref.

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
    #state{h3_streams = Streams} = State,
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
            State1 = State#state{h3_streams = Streams#{StreamId => Stream2}},
            case nhttp_conn_h3:send_h3_data(State1, StreamId, Data, Fin) of
                {sent, State2} ->
                    finalize_h3_push_send(State2, StreamId, WPid, Ref, Fin);
                {stashed, State2} ->
                    set_pending(State2, StreamId, {Ref, Fin});
                {error, {h3, _Reason}, State2} ->
                    WPid ! {chunk_ack, Ref, {error, closed}},
                    cleanup_h3_push_stream(State2, StreamId, WPid);
                {error, {quic, _} = Reason, State2} ->
                    handle_h3_push_send_fatal(State2, StreamId, WPid, Ref, Reason)
            end;
        _ ->
            WPid ! {chunk_ack, Ref, {error, closed}},
            cleanup_h3_push_worker_entry(State, WPid)
    end.

-spec handle_h3_push_send_fatal(
    #state{}, nhttp_lib:stream_id(), pid(), reference(), nhttp_conn_h3:send_error()
) -> #state{}.
handle_h3_push_send_fatal(State, StreamId, WPid, Ref, Reason) ->
    nhttp_log:sock_send_failed(nhttp_conn_h3:log_ctx(State), Reason),
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

-doc """
End a push stream whose producer returned. A FIN that flow control holds
leaves from the send buffer after the stream entry is gone.
""".
-spec send_h3_push_empty_fin(#state{}, nhttp_lib:stream_id()) -> #state{}.
send_h3_push_empty_fin(State, StreamId) ->
    State1 =
        case nhttp_conn_h3:send_h3_data(State, StreamId, <<>>, fin) of
            {sent, S} ->
                S;
            {stashed, S} ->
                S;
            {error, Reason, S} ->
                nhttp_log:sock_send_failed(nhttp_conn_h3:log_ctx(S), Reason),
                S
        end,
    State1#state{h3_streams = maps:remove(StreamId, State1#state.h3_streams)}.

-spec set_pending(#state{}, nhttp_lib:stream_id(), {reference(), nhttp_h3:fin()}) -> #state{}.
set_pending(#state{h3_streams = Streams} = State, StreamId, Pending) ->
    #h3_stream{push = Ctx} = Stream = maps:get(StreamId, Streams),
    Stream1 = Stream#h3_stream{push = Ctx#h3_push_ctx{pending = Pending}},
    State#state{h3_streams = Streams#{StreamId => Stream1}}.

-spec start_h3_stream_push(
    #state{},
    nhttp_lib:stream_id(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    nhttp_stream:producer(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> #state{}.
start_h3_stream_push(
    #state{opts = Opts} = State, StreamId, Status, Headers, Producer, NewHState, ReqSpan
) ->
    RespHeaders = [{<<":status">>, integer_to_binary(Status)} | Headers],
    State1 = nhttp_conn_h3:send_h3_headers(State, StreamId, RespHeaders, nofin),
    Ref = make_ref(),
    WorkerPid = nhttp_stream_worker:start(self(), Ref, Producer, nhttp_conn:spawn_opts(Opts)),
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
