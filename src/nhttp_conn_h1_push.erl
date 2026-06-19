-module(nhttp_conn_h1_push).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn.hrl").
-include("nhttp_status_codes.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    dispatch_h1_stream_push/7,
    h1_stream_push_loop/4
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([h1_push_ctx/0]).

-define(H1_WORKER_EXIT_TIMEOUT, 5000).
-type h1_push_ctx() :: #h1_push_ctx{}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec dispatch_h1_stream_push(
    #state{},
    nhttp_lib:request(),
    nhttp_lib:status(),
    nhttp_lib:headers(),
    nhttp_stream:producer(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> #state{} | {stream_push, #state{}, h1_push_ctx()}.
dispatch_h1_stream_push(
    #state{protocol_state = #h1_state{} = H1} = State,
    Request,
    Status,
    Headers,
    Producer,
    NewHState,
    ReqSpan
) ->
    case validate_h1_stream_push(Request, Status) of
        ok_head ->
            KeepAlive =
                case nhttp_conn_h1:send_h1_response_head(State, Status, Headers) of
                    ok -> H1#h1_state.keep_alive;
                    {error, _Reason} -> false
                end,
            nhttp_conn:emit_request_stop(State, Status, ReqSpan),
            State#state{
                protocol_state = H1#h1_state{keep_alive = KeepAlive},
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        {reject, RejectReason} ->
            #{method := Method, path := Path} = Request,
            nhttp_log:stream_push_rejected(
                nhttp_conn:log_ctx(State),
                Method,
                Path,
                RejectReason
            ),
            nhttp_conn_h1:send_h1_error_response(
                State, ?HTTP_INTERNAL_SERVER_ERROR, <<"Internal Server Error">>
            ),
            nhttp_conn:emit_request_stop(State, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
            State#state{
                protocol_state = H1#h1_state{keep_alive = false},
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        ok ->
            StreamHeaders = nhttp_conn_h1:add_chunked_header(Headers),
            case nhttp_conn_h1:send_h1_response_head(State, Status, StreamHeaders) of
                ok ->
                    start_h1_stream_push(State, Status, Producer, NewHState, ReqSpan);
                {error, _Reason} ->
                    nhttp_conn:emit_request_stop(State, Status, ReqSpan),
                    State#state{
                        protocol_state = H1#h1_state{keep_alive = false},
                        handler_state = NewHState,
                        requests_count = State#state.requests_count + 1
                    }
            end
    end.

-spec h1_stream_push_loop(pid(), [sys:debug_option()], #state{}, h1_push_ctx()) -> no_return().
h1_stream_push_loop(Parent, Debug, #state{socket = Socket} = State, Ctx) ->
    case nhttp_sock:setopts(Socket, [{active, once}]) of
        ok ->
            h1_stream_push_recv(Parent, Debug, State, Ctx);
        {error, _} ->
            ok = release_h1_worker(Ctx),
            nhttp_conn:stop(normal, State)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/1.1 PUSH STREAMING
%%%-----------------------------------------------------------------------------
-spec await_h1_worker_exit(#state{}, reference(), pid()) -> ok.
await_h1_worker_exit(State, MRef, WPid) ->
    receive
        {'DOWN', MRef, process, WPid, _Reason} -> ok
    after ?H1_WORKER_EXIT_TIMEOUT ->
        nhttp_log:h1_push_worker_stuck(nhttp_conn:log_ctx(State), WPid),
        ok = nhttp_stream_worker:stop(WPid),
        receive
            {'DOWN', MRef, process, WPid, _Reason} -> ok
        after ?H1_WORKER_EXIT_TIMEOUT ->
            ok
        end
    end.

-spec continue_after_h1_push(pid(), [sys:debug_option()], #state{}) -> no_return().
continue_after_h1_push(
    _Parent, _Debug, #state{protocol_state = #h1_state{keep_alive = false}} = State
) ->
    nhttp_conn:stop(normal, State);
continue_after_h1_push(
    Parent, Debug, #state{socket = Socket, protocol_state = #h1_state{buffer = Buffer}} = State
) ->
    case Buffer of
        <<>> ->
            ok = nhttp_sock:setopts(Socket, [{active, once}]),
            nhttp_conn_h1:h1_loop(Parent, Debug, State);
        _ ->
            nhttp_conn_h1:process_h1_pipeline(Parent, Debug, State, 0)
    end.

-spec finalize_h1_stream_push(#state{}, h1_push_ctx(), atom()) -> #state{}.
finalize_h1_stream_push(State, Ctx, _Reason) ->
    #h1_push_ctx{status = Status, req_span = ReqSpan, bytes = Bytes} = Ctx,
    nhttp_conn:emit_request_stop_with_size(State, Status, ReqSpan, Bytes),
    State.

-spec h1_stash_tcp_data(#state{}, binary()) -> #state{}.
h1_stash_tcp_data(#state{protocol_state = #h1_state{buffer = Buffer} = H1} = State, Data) ->
    State#state{protocol_state = H1#h1_state{buffer = <<Buffer/binary, Data/binary>>}}.

-spec h1_stream_push_recv(pid(), [sys:debug_option()], #state{}, h1_push_ctx()) -> no_return().
h1_stream_push_recv(Parent, Debug, #state{idle_timeout = IdleTimeout} = State, Ctx) ->
    #h1_push_ctx{worker = WPid, ref = Ref, mref = MRef} = Ctx,
    receive
        {send_chunk, WPid, Ref, Data} ->
            Ctx1 = maybe_emit_h1_stream_start(State, Ctx),
            case nhttp_conn_h1:send_h1_chunk(State, Data) of
                ok ->
                    WPid ! {chunk_ack, Ref, ok},
                    BytesAdded = iolist_size(Data),
                    Ctx2 = Ctx1#h1_push_ctx{bytes = Ctx1#h1_push_ctx.bytes + BytesAdded},
                    h1_stream_push_loop(Parent, Debug, State, Ctx2);
                {error, _Reason} ->
                    WPid ! {chunk_ack, Ref, {error, closed}},
                    FinalState = finalize_h1_stream_push(State, Ctx1, peer_closed),
                    nhttp_conn:stop(normal, FinalState)
            end;
        {stream_done, WPid, Ref} ->
            Ctx1 = maybe_emit_h1_stream_start(State, Ctx),
            nhttp_conn_h1:send_h1_last_chunk(State),
            WPid ! {chunk_ack, Ref, ok},
            await_h1_worker_exit(State, MRef, WPid),
            FinalState = finalize_h1_stream_push(State, Ctx1, normal),
            continue_after_h1_push(Parent, Debug, FinalState);
        {send_trailers, WPid, Ref, _Trailers} ->
            Ctx1 = maybe_emit_h1_stream_start(State, Ctx),
            nhttp_conn_h1:send_h1_last_chunk(State),
            WPid ! {chunk_ack, Ref, ok},
            await_h1_worker_exit(State, MRef, WPid),
            FinalState = finalize_h1_stream_push(State, Ctx1, normal),
            continue_after_h1_push(Parent, Debug, FinalState);
        {'DOWN', MRef, process, WPid, Reason} ->
            nhttp_log:stream_push_producer_crashed(nhttp_conn:log_ctx(State), Reason),
            FinalState = finalize_h1_stream_push(State, Ctx, producer_crashed),
            #state{protocol_state = #h1_state{} = H1} = FinalState,
            nhttp_conn:stop(
                normal, FinalState#state{protocol_state = H1#h1_state{keep_alive = false}}
            );
        {tcp_closed, _Socket} ->
            ok = release_h1_worker(Ctx),
            nhttp_conn:stop(normal, State);
        {ssl_closed, _Socket} ->
            ok = release_h1_worker(Ctx),
            nhttp_conn:stop(normal, State);
        {tcp_error, _Socket, Reason} ->
            ok = release_h1_worker(Ctx),
            nhttp_conn:stop({socket_error, Reason}, State);
        {ssl_error, _Socket, Reason} ->
            ok = release_h1_worker(Ctx),
            nhttp_conn:stop({socket_error, Reason}, State);
        {tcp, _Socket, Data} ->
            NewState = h1_stash_tcp_data(State, Data),
            h1_stream_push_loop(Parent, Debug, NewState, Ctx);
        {ssl, _Socket, Data} ->
            NewState = h1_stash_tcp_data(State, Data),
            h1_stream_push_loop(Parent, Debug, NewState, Ctx)
    after IdleTimeout ->
        nhttp_log:h1_push_idle_timeout(nhttp_conn:log_ctx(State), WPid),
        ok = release_h1_worker(Ctx),
        FinalState = finalize_h1_stream_push(State, Ctx, idle_timeout),
        nhttp_conn:stop(idle_timeout, FinalState)
    end.

-spec maybe_emit_h1_stream_start(#state{}, h1_push_ctx()) -> h1_push_ctx().
maybe_emit_h1_stream_start(_State, #h1_push_ctx{started = true} = Ctx) ->
    Ctx;
maybe_emit_h1_stream_start(State, #h1_push_ctx{req_span = ReqSpan} = Ctx) ->
    nhttp_conn:emit_stream_start(State, ReqSpan),
    Ctx#h1_push_ctx{started = true}.

-spec release_h1_worker(h1_push_ctx()) -> ok.
release_h1_worker(#h1_push_ctx{worker = WPid, ref = Ref, mref = MRef}) ->
    _ = erlang:demonitor(MRef, [flush]),
    WPid ! {chunk_ack, Ref, {error, closed}},
    nhttp_stream_worker:reap([WPid]).

-spec start_h1_stream_push(
    #state{},
    nhttp_lib:status(),
    nhttp_stream:producer(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> {stream_push, #state{}, h1_push_ctx()}.
start_h1_stream_push(State, Status, Producer, NewHState, ReqSpan) ->
    Ref = make_ref(),
    WorkerPid = nhttp_stream_worker:start(self(), Ref, Producer),
    MRef = erlang:monitor(process, WorkerPid),
    Ctx = #h1_push_ctx{
        worker = WorkerPid,
        ref = Ref,
        mref = MRef,
        status = Status,
        req_span = ReqSpan
    },
    NewState = State#state{
        handler_state = NewHState,
        requests_count = State#state.requests_count + 1
    },
    {stream_push, NewState, Ctx}.

-spec validate_h1_stream_push(nhttp_lib:request(), nhttp_lib:status()) ->
    ok | ok_head | {reject, term()}.
validate_h1_stream_push(#{version := http1_0}, _Status) ->
    {reject, http_1_0_not_supported};
validate_h1_stream_push(_Request, Status) when
    Status =:= ?HTTP_NO_CONTENT; Status =:= ?HTTP_NOT_MODIFIED
->
    {reject, {no_body_status, Status}};
validate_h1_stream_push(#{method := head}, _Status) ->
    ok_head;
validate_h1_stream_push(_Request, _Status) ->
    ok.
