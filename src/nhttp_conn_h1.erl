-module(nhttp_conn_h1).

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
%% INTERNAL EXPORTS (USED BY NHTTP_CONN_H1_PUSH)
%%%-----------------------------------------------------------------------------
-export([
    add_chunked_header/1,
    h1_loop/3,
    process_h1_pipeline/4,
    send_h1_chunk/2,
    send_h1_error_response/3,
    send_h1_last_chunk/1,
    send_h1_response_head/3
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    h1_wake/4,
    system_code_change/4,
    system_continue/3,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_MAX_PIPELINE_DEPTH, 10).
-define(H1_REQUEST_LINE_ALLOWANCE, 8192).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Entry point for the HTTP/1.1 loop.

Called by `nhttp_conn` after protocol classification picks `http1`. After
a successful WebSocket upgrade the conn switches to `nhttp_conn_ws_h1`,
which owns its own loop and `system_*` callbacks. This loop never sees a
WebSocket session.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, State) ->
    h1_loop(Parent, Debug, State).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS CALLBACKS
%%%-----------------------------------------------------------------------------
-doc """
`proc_lib:hibernate/3` re-entry point for the head-wait hibernation.
Cancels the idle timer armed before hibernating; when it already fired
the wake-up is an idle timeout unless other traffic is queued (drained
by the zero-timeout receive).
""".
-spec h1_wake(pid(), [sys:debug_option()], #state{}, nhttp_conn:hibernate_timer()) ->
    no_return().
h1_wake(Parent, Debug, State, Timer) ->
    case nhttp_conn:cancel_hibernate_timer(Timer) of
        ok -> h1_receive(Parent, Debug, State, h1_head_timeout(State));
        expired -> h1_receive(Parent, Debug, State, 0)
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
%% INTERNAL FUNCTIONS - HTTP/1.1 LOOP
%%%-----------------------------------------------------------------------------
-doc """
Head-wait block point. A quiescent connection (empty buffer, no head in
flight, empty mailbox) hibernates here: this is the canonical idle
keep-alive connection, and the full-sweep GC releases the heap and any
refc binary holds for the price of one wake-up GC amortized by network
latency. The `after` deadline is carried across hibernation by the timer
from `nhttp_conn:start_hibernate_timer/1`.
""".
-spec h1_loop(pid(), [sys:debug_option()], #state{}) -> no_return().
h1_loop(Parent, Debug, State) ->
    case hibernate_eligible(State) of
        true -> hibernate(Parent, Debug, State);
        false -> h1_receive(Parent, Debug, State, h1_head_timeout(State))
    end.

-spec hibernate_eligible(#state{}) -> boolean().
hibernate_eligible(#state{
    protocol_state = #h1_state{buffer = <<>>, head_deadline_at = undefined}
}) ->
    nhttp_conn:mailbox_empty();
hibernate_eligible(#state{}) ->
    false.

-spec hibernate(pid(), [sys:debug_option()], #state{}) -> no_return().
hibernate(Parent, Debug, #state{idle_timeout = IdleTimeout} = State) ->
    Timer = nhttp_conn:start_hibernate_timer(IdleTimeout),
    proc_lib:hibernate(?MODULE, h1_wake, [Parent, Debug, State, Timer]).

-spec h1_receive(pid(), [sys:debug_option()], #state{}, timeout()) -> no_return().
h1_receive(Parent, Debug, State, HeadTimeout) ->
    receive
        {tcp, _Socket, Data} ->
            handle_h1_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_h1_data(Parent, Debug, State, Data);
        {tcp_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {ssl_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {tcp_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {ssl_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        shutdown ->
            nhttp_conn:stop(normal, State);
        {'EXIT', Parent, Reason} ->
            nhttp_conn:stop_parent(Reason, State)
    after HeadTimeout ->
        handle_h1_head_timeout(State)
    end.

-doc """
Effective wait while receiving a request head. With no head in flight the
keep-alive `idle_timeout` governs; once a head is partially buffered the
absolute `request_timeout` deadline caps total receive time, so a
per-packet drip (slowloris) cannot re-arm the wait forever.
""".
-spec h1_head_timeout(#state{}) -> timeout().
h1_head_timeout(#state{
    protocol_state = #h1_state{head_deadline_at = undefined}, idle_timeout = IdleTimeout
}) ->
    IdleTimeout;
h1_head_timeout(#state{
    protocol_state = #h1_state{head_deadline_at = Deadline}, idle_timeout = IdleTimeout
}) ->
    Now = erlang:monotonic_time(millisecond),
    min(IdleTimeout, max(0, Deadline - Now)).

-spec handle_h1_head_timeout(#state{}) -> no_return().
handle_h1_head_timeout(
    #state{protocol_state = #h1_state{head_deadline_at = undefined}} = State
) ->
    nhttp_conn:stop(idle_timeout, State);
handle_h1_head_timeout(
    #state{protocol_state = #h1_state{head_deadline_at = Deadline} = H1} = State
) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            send_h1_error_response(State, ?HTTP_REQUEST_TIMEOUT, <<"Request Timeout">>),
            nhttp_conn:stop(
                request_timeout, State#state{protocol_state = H1#h1_state{keep_alive = false}}
            );
        false ->
            nhttp_conn:stop(idle_timeout, State)
    end.

-doc """
Arm the absolute request-head deadline the first time a head is buffered
incomplete. Persists across packets (and pipelined leftover bytes) so the
deadline is measured from the first byte of the head, not re-armed per
arrival. Cleared on a complete head parse.
""".
-spec arm_head_deadline(#state{}) -> #state{}.
arm_head_deadline(#state{protocol_state = #h1_state{buffer = <<>>}} = State) ->
    State;
arm_head_deadline(
    #state{protocol_state = #h1_state{head_deadline_at = undefined, request_timeout = infinity}} =
        State
) ->
    State;
arm_head_deadline(
    #state{
        protocol_state =
            #h1_state{head_deadline_at = undefined, request_timeout = RequestTimeout} = H1
    } = State
) ->
    State#state{
        protocol_state = H1#h1_state{
            head_deadline_at = erlang:monotonic_time(millisecond) + RequestTimeout
        }
    };
arm_head_deadline(#state{} = State) ->
    State.

-spec handle_h1_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_h1_data(Parent, Debug, #state{protocol_state = #h1_state{buffer = <<>>} = H1} = State, Data) ->
    process_h1_pipeline(Parent, Debug, State#state{protocol_state = H1#h1_state{buffer = Data}}, 0);
handle_h1_data(
    Parent, Debug, #state{protocol_state = #h1_state{buffer = Buffer} = H1} = State, Data
) ->
    Combined = <<Buffer/binary, Data/binary>>,
    process_h1_pipeline(
        Parent, Debug, State#state{protocol_state = H1#h1_state{buffer = Combined}}, 0
    ).

-doc "Process pipelined HTTP/1.1 requests.".
-spec process_h1_pipeline(pid(), [sys:debug_option()], #state{}, non_neg_integer()) ->
    no_return().
process_h1_pipeline(
    Parent,
    Debug,
    #state{
        protocol_state = #h1_state{buffer = Buffer} = H1, opts = Opts
    } = State,
    PipelineDepth
) ->
    MaxPipelineDepth = maps:get(max_pipeline_depth, Opts, ?DEFAULT_MAX_PIPELINE_DEPTH),
    Limits = State#state.limits,
    ParseOpts = (nhttp_limits:h1_parse_opts(Limits))#{
        scheme => nhttp_conn:transport_to_scheme(State#state.transport),
        peer => State#state.peer
    },
    case nhttp_h1:parse_request_headers(Buffer, ParseOpts) of
        {ok, Request, BodyStream, Consumed} ->
            Rest = nhttp_h1:split_at(Buffer, Consumed),
            State1 = State#state{
                protocol_state = H1#h1_state{
                    buffer = Rest, body_stream = BodyStream, head_deadline_at = undefined
                }
            },
            case dispatch_h1_request(State1, Request) of
                {upgrade, websocket, NewState, UpgradeRequest} ->
                    handle_ws_upgrade(Parent, Debug, NewState, UpgradeRequest, undefined);
                {upgrade, websocket, NewState, UpgradeRequest, SessionOpts} ->
                    handle_ws_upgrade(Parent, Debug, NewState, UpgradeRequest, SessionOpts);
                {stream_push, NewState, PushCtx} ->
                    nhttp_conn_h1_push:h1_stream_push_loop(Parent, Debug, NewState, PushCtx);
                {accept_body, NewState, BodyState, ReqSpan} ->
                    enter_h1_body_loop(Parent, Debug, NewState, Request, BodyState, ReqSpan);
                NewState when is_record(NewState, state) ->
                    NewState1 = clear_body_stream_or_close(NewState),
                    finish_h1_request(Parent, Debug, NewState1, PipelineDepth, MaxPipelineDepth)
            end;
        {more, _} ->
            case byte_size(Buffer) > head_buffer_cap(ParseOpts) of
                true ->
                    send_h1_error(State, header_too_large),
                    nhttp_conn:stop({protocol_error, header_too_large}, State);
                false ->
                    case nhttp_conn:activate(State) of
                        ok -> h1_loop(Parent, Debug, arm_head_deadline(State));
                        {stop, Reason} -> nhttp_conn:stop(Reason, State)
                    end
            end;
        {error, Reason} ->
            send_h1_error(State, Reason),
            nhttp_conn:stop({protocol_error, Reason}, State)
    end.

-doc "Yield to scheduler and then continue processing buffered H1 requests.".
-spec yield_and_continue_h1(pid(), [sys:debug_option()], #state{}) -> no_return().
yield_and_continue_h1(
    Parent, Debug, #state{protocol_state = #h1_state{buffer = Buffer}} = State
) ->
    receive
        {tcp, _Socket, Data} ->
            handle_h1_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_h1_data(Parent, Debug, State, Data);
        {tcp_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {ssl_closed, _Socket} ->
            nhttp_conn:stop(normal, State);
        {tcp_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {ssl_error, _Socket, Reason} ->
            nhttp_conn:stop({socket_error, Reason}, State);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        shutdown ->
            nhttp_conn:stop(normal, State);
        {'EXIT', Parent, Reason} ->
            nhttp_conn:stop_parent(Reason, State)
    after 0 ->
        case Buffer of
            <<>> ->
                h1_loop(Parent, Debug, State);
            _ ->
                process_h1_pipeline(Parent, Debug, State, 0)
        end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HTTP/1.1 REQUEST HANDLING
%%%-----------------------------------------------------------------------------
-spec abort_h1_body_phase(
    #state{}, {nhttp_otel:span_ctx(), integer()}, term()
) -> no_return().
abort_h1_body_phase(#state{protocol_state = #h1_state{} = H1} = State, ReqSpan, Reason) ->
    nhttp_conn:emit_request_stop(State, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    nhttp_conn:stop(
        {body_phase_unterminated, Reason},
        State#state{protocol_state = H1#h1_state{keep_alive = false}}
    ).

-spec add_chunked_header(nhttp_lib:headers()) -> nhttp_lib:headers().
add_chunked_header(Headers) ->
    [{<<"transfer-encoding">>, <<"chunked">>} | Headers].

-spec apply_h1_body_chunks(
    pid(),
    [sys:debug_option()],
    #state{},
    nhttp_lib:request(),
    term(),
    {nhttp_otel:span_ctx(), integer()},
    [nhttp_handler:body_event()]
) -> no_return().
apply_h1_body_chunks(Parent, Debug, State, Request, BodyState, ReqSpan, []) ->
    drain_h1_body_buffer(Parent, Debug, State, Request, BodyState, ReqSpan);
apply_h1_body_chunks(Parent, Debug, State, Request, BodyState, ReqSpan, [Event | Rest]) ->
    Handler = State#state.handler,
    HState = State#state.handler_state,
    Result = nhttp_handler:safe_handle_request_body(
        Handler, Event, BodyState, HState
    ),
    Applied = apply_h1_request_result(State, Request, ReqSpan, Result),
    handle_h1_body_event_result(Parent, Debug, Request, ReqSpan, Event, Rest, Applied).

-spec apply_h1_request_result(
    #state{}, nhttp_lib:request(), {nhttp_otel:span_ctx(), integer()}, term()
) ->
    #state{}
    | {upgrade, websocket, #state{}, nhttp_lib:request()}
    | {upgrade, websocket, #state{}, nhttp_lib:request(), nhttp_ws:ws_session_opts()}
    | {stream_push, #state{}, nhttp_conn_h1_push:h1_push_ctx()}
    | {accept_body, #state{}, term(), term()}.
apply_h1_request_result(
    #state{protocol_state = #h1_state{} = H1} = State,
    Request,
    ReqSpan,
    {reply, #{status := Status} = Response0, NewHState}
) ->
    Response = nhttp_conn_compress:maybe_compress(
        Response0, Request, nhttp_conn:compress_config(State)
    ),
    case send_h1_response(State, Response) of
        ok ->
            nhttp_conn:emit_request_stop(State, Status, ReqSpan),
            KeepAlive = should_keep_alive(Request, Response),
            State#state{
                protocol_state = H1#h1_state{keep_alive = KeepAlive},
                handler_state = NewHState,
                requests_count = State#state.requests_count + 1
            };
        {error, Reason} ->
            reply_encode_failed(State, Request, ReqSpan, Reason, NewHState)
    end;
apply_h1_request_result(
    State, Request, ReqSpan, {stream, {producer, Status, Headers, Producer}, NewHState}
) ->
    nhttp_conn_h1_push:dispatch_h1_stream_push(
        State, Request, Status, Headers, Producer, NewHState, ReqSpan
    );
apply_h1_request_result(State, _Request, ReqSpan, {accept_body, BodyState, NewHState}) ->
    {accept_body, State#state{handler_state = NewHState}, BodyState, ReqSpan};
apply_h1_request_result(State, Request, ReqSpan, {upgrade, websocket, NewHState}) ->
    nhttp_conn:emit_request_stop(State, ?HTTP_SWITCHING_PROTOCOLS, ReqSpan),
    {upgrade, websocket,
        State#state{
            handler_state = NewHState,
            requests_count = State#state.requests_count + 1
        },
        Request};
apply_h1_request_result(State, Request, ReqSpan, {upgrade, websocket, SessionOpts, NewHState}) ->
    nhttp_conn:emit_request_stop(State, ?HTTP_SWITCHING_PROTOCOLS, ReqSpan),
    {upgrade, websocket,
        State#state{
            handler_state = NewHState,
            requests_count = State#state.requests_count + 1
        },
        Request, SessionOpts};
apply_h1_request_result(
    #state{protocol_state = #h1_state{} = H1} = State,
    _Request,
    ReqSpan,
    {abort, _Reason, NewHState}
) ->
    nhttp_conn:emit_request_stop(State, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State#state{
        protocol_state = H1#h1_state{keep_alive = false},
        handler_state = NewHState,
        requests_count = State#state.requests_count + 1
    };
apply_h1_request_result(
    #state{protocol_state = #h1_state{} = H1} = State,
    Request,
    ReqSpan,
    {nhttp_handler_exception, Class, Reason}
) ->
    Ctx = nhttp_log:request_ctx(nhttp_conn:log_ctx(State), Request, undefined),
    nhttp_log:handler_crashed(Ctx, handle_request, Class, Reason),
    nhttp_conn:emit_handler_exception(ReqSpan, Class, Reason),
    send_h1_error_response(State, ?HTTP_INTERNAL_SERVER_ERROR, <<"Internal Server Error">>),
    nhttp_conn:emit_request_stop(State, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State#state{protocol_state = H1#h1_state{keep_alive = false}}.

-spec await_h1_body_data(
    pid(),
    [sys:debug_option()],
    #state{},
    nhttp_lib:request(),
    term(),
    {nhttp_otel:span_ctx(), integer()}
) -> no_return().
await_h1_body_data(
    Parent,
    Debug,
    #state{protocol_state = #h1_state{buffer = Buffer} = H1} = State,
    Request,
    BodyState,
    ReqSpan
) ->
    BodyTimeout = h1_body_timeout(State),
    receive
        {tcp, _Socket, Data} ->
            drain_h1_body_buffer(
                Parent,
                Debug,
                State#state{
                    protocol_state = H1#h1_state{buffer = <<Buffer/binary, Data/binary>>}
                },
                Request,
                BodyState,
                ReqSpan
            );
        {ssl, _Socket, Data} ->
            drain_h1_body_buffer(
                Parent,
                Debug,
                State#state{
                    protocol_state = H1#h1_state{buffer = <<Buffer/binary, Data/binary>>}
                },
                Request,
                BodyState,
                ReqSpan
            );
        {tcp_closed, _Socket} ->
            deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, peer_closed);
        {ssl_closed, _Socket} ->
            deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, peer_closed);
        {tcp_error, _Socket, Reason} ->
            deliver_h1_body_abort(
                Parent, Debug, State, Request, BodyState, ReqSpan, {socket_error, Reason}
            );
        {ssl_error, _Socket, Reason} ->
            deliver_h1_body_abort(
                Parent, Debug, State, Request, BodyState, ReqSpan, {socket_error, Reason}
            );
        {system, From, Req} ->
            sys:handle_system_msg(Req, From, Parent, ?MODULE, Debug, State);
        shutdown ->
            deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, shutdown);
        {'EXIT', Parent, Reason} ->
            nhttp_conn:stop_parent(Reason, State)
    after BodyTimeout ->
        Reason = h1_body_timeout_reason(State),
        deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, Reason)
    end.

-doc """
Effective wait for the next body chunk: the per-chunk `body_timeout`
capped by the absolute `body_deadline` (when armed), so a per-chunk drip
cannot re-arm the wait past the deadline.
""".
-spec h1_body_timeout(#state{}) -> timeout().
h1_body_timeout(#state{
    protocol_state = #h1_state{body_deadline_at = undefined, body_timeout = BodyTimeout}
}) ->
    BodyTimeout;
h1_body_timeout(#state{
    protocol_state = #h1_state{body_deadline_at = Deadline, body_timeout = BodyTimeout}
}) ->
    Now = erlang:monotonic_time(millisecond),
    min(BodyTimeout, max(0, Deadline - Now)).

-spec h1_body_timeout_reason(#state{}) -> body_timeout | request_timeout.
h1_body_timeout_reason(#state{protocol_state = #h1_state{body_deadline_at = undefined}}) ->
    body_timeout;
h1_body_timeout_reason(#state{protocol_state = #h1_state{body_deadline_at = Deadline}}) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true -> request_timeout;
        false -> body_timeout
    end.

-spec clear_body_stream_or_close(#state{}) -> #state{}.
clear_body_stream_or_close(#state{protocol_state = #h1_state{body_stream = undefined}} = State) ->
    State;
clear_body_stream_or_close(
    #state{protocol_state = #h1_state{body_stream = none} = H1} = State
) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined}};
clear_body_stream_or_close(
    #state{protocol_state = #h1_state{body_stream = {length, 0}} = H1} = State
) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined}};
clear_body_stream_or_close(#state{protocol_state = #h1_state{} = H1} = State) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined, keep_alive = false}}.

-spec count_host_headers(nhttp_lib:headers(), 0..1) -> ok | {error, bad_host}.
count_host_headers([{<<"host">>, _} | _], 1) -> {error, bad_host};
count_host_headers([{<<"host">>, _} | Rest], 0) -> count_host_headers(Rest, 1);
count_host_headers([_ | Rest], Seen) -> count_host_headers(Rest, Seen);
count_host_headers([], 1) -> ok;
count_host_headers([], 0) -> {error, bad_host}.

-spec deliver_h1_body_abort(
    pid(),
    [sys:debug_option()],
    #state{},
    nhttp_lib:request(),
    term(),
    {nhttp_otel:span_ctx(), integer()},
    term()
) -> no_return().
deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, Reason) ->
    apply_h1_body_chunks(Parent, Debug, State, Request, BodyState, ReqSpan, [{abort, Reason}]).

-spec dispatch_h1_request(#state{}, nhttp_lib:request()) ->
    #state{}
    | {upgrade, websocket, #state{}, nhttp_lib:request()}
    | {upgrade, websocket, #state{}, nhttp_lib:request(), nhttp_ws:ws_session_opts()}
    | {stream_push, #state{}, nhttp_conn_h1_push:h1_push_ctx()}
    | {accept_body, #state{}, term(), term()}.
dispatch_h1_request(
    #state{limits = Limits, protocol_state = #h1_state{} = H1} = State, Request
) ->
    case validate_h1_host(Request) of
        ok ->
            case nhttp_limits:validate_request(Request, Limits) of
                ok ->
                    dispatch_h1_request_validated(State, Request);
                {error, LimitError} ->
                    handle_h1_limit_error(State, LimitError)
            end;
        {error, bad_host} ->
            send_h1_error_response(State, ?HTTP_BAD_REQUEST, <<"Bad Request">>),
            State#state{protocol_state = H1#h1_state{keep_alive = false}}
    end.

-spec dispatch_h1_request_validated(#state{}, nhttp_lib:request()) ->
    #state{}
    | {upgrade, websocket, #state{}, nhttp_lib:request()}
    | {upgrade, websocket, #state{}, nhttp_lib:request(), nhttp_ws:ws_session_opts()}
    | {stream_push, #state{}, nhttp_conn_h1_push:h1_push_ctx()}
    | {accept_body, #state{}, term(), term()}.
dispatch_h1_request_validated(#state{handler = Handler, handler_state = HState} = State, Request) ->
    ReqSpan = nhttp_conn:emit_request_start(State, 0, Request),
    Result = nhttp_handler:safe_handle_request(Handler, Request, HState),
    apply_h1_request_result(State, Request, ReqSpan, Result).

-spec drain_h1_body_buffer(
    pid(), [sys:debug_option()], #state{}, nhttp_lib:request(), term(), {
        nhttp_otel:span_ctx(), integer()
    }
) -> no_return().
drain_h1_body_buffer(
    Parent,
    Debug,
    #state{protocol_state = #h1_state{buffer = Buffer, body_stream = Stream} = H1} = State,
    Request,
    BodyState,
    ReqSpan
) ->
    case nhttp_h1:parse_request_body(Buffer, Stream) of
        {ok, Chunks, NewStream, Consumed} ->
            Rest = nhttp_h1:split_at(Buffer, Consumed),
            State1 = State#state{
                protocol_state = H1#h1_state{buffer = Rest, body_stream = NewStream}
            },
            apply_h1_body_chunks(Parent, Debug, State1, Request, BodyState, ReqSpan, Chunks);
        {more, _MinBytes, NewStream} ->
            State1 = State#state{protocol_state = H1#h1_state{body_stream = NewStream}},
            case nhttp_conn:activate(State1) of
                ok ->
                    await_h1_body_data(Parent, Debug, State1, Request, BodyState, ReqSpan);
                {stop, normal} ->
                    deliver_h1_body_abort(
                        Parent, Debug, State1, Request, BodyState, ReqSpan, peer_closed
                    );
                {stop, Reason} ->
                    deliver_h1_body_abort(
                        Parent, Debug, State1, Request, BodyState, ReqSpan, Reason
                    )
            end;
        {error, Reason} ->
            deliver_h1_body_abort(Parent, Debug, State, Request, BodyState, ReqSpan, Reason)
    end.

-spec enter_h1_body_loop(
    pid(), [sys:debug_option()], #state{}, nhttp_lib:request(), term(), {
        nhttp_otel:span_ctx(), integer()
    }
) -> no_return().
enter_h1_body_loop(Parent, Debug, State, Request, BodyState, ReqSpan) ->
    drain_h1_body_buffer(Parent, Debug, arm_body_deadline(State), Request, BodyState, ReqSpan).

-doc """
Arm the absolute body-receive deadline when the body phase starts.
`infinity` (the default) leaves it disarmed, so legitimate large or slow
uploads are bounded only by the per-chunk `body_timeout`.
""".
-spec arm_body_deadline(#state{}) -> #state{}.
arm_body_deadline(#state{protocol_state = #h1_state{body_deadline = infinity} = H1} = State) ->
    State#state{protocol_state = H1#h1_state{body_deadline_at = undefined}};
arm_body_deadline(#state{protocol_state = #h1_state{body_deadline = BodyDeadline} = H1} = State) ->
    State#state{
        protocol_state = H1#h1_state{
            body_deadline_at = erlang:monotonic_time(millisecond) + BodyDeadline
        }
    }.

-spec finalize_h1_body_terminal(#state{}, nhttp_handler:body_event()) -> #state{}.
finalize_h1_body_terminal(#state{protocol_state = #h1_state{} = H1} = State, {fin, _}) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined}};
finalize_h1_body_terminal(#state{protocol_state = #h1_state{} = H1} = State, {abort, _}) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined, keep_alive = false}};
finalize_h1_body_terminal(#state{protocol_state = #h1_state{} = H1} = State, {data, _}) ->
    State#state{protocol_state = H1#h1_state{body_stream = undefined, keep_alive = false}}.

-spec finish_h1_request(
    pid(), [sys:debug_option()], #state{}, non_neg_integer(), pos_integer()
) -> no_return().
finish_h1_request(
    Parent,
    Debug,
    #state{protocol_state = #h1_state{keep_alive = KeepAlive}} = State,
    PipelineDepth,
    MaxPipelineDepth
) ->
    case KeepAlive of
        true when PipelineDepth < MaxPipelineDepth ->
            case nhttp_conn:activate(State) of
                ok ->
                    receive
                        {system, From, Request} ->
                            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
                        shutdown ->
                            nhttp_conn:stop(normal, State);
                        {'EXIT', Parent, Reason} ->
                            nhttp_conn:stop_parent(Reason, State)
                    after 0 ->
                        process_h1_pipeline(Parent, Debug, State, PipelineDepth + 1)
                    end;
                {stop, Reason} ->
                    nhttp_conn:stop(Reason, State)
            end;
        true ->
            case nhttp_conn:activate(State) of
                ok -> yield_and_continue_h1(Parent, Debug, State);
                {stop, Reason} -> nhttp_conn:stop(Reason, State)
            end;
        false ->
            nhttp_conn:stop(normal, State)
    end.

-spec handle_h1_body_event_result(
    pid(),
    [sys:debug_option()],
    nhttp_lib:request(),
    {nhttp_otel:span_ctx(), integer()},
    nhttp_handler:body_event(),
    [nhttp_handler:body_event()],
    term()
) -> no_return().
handle_h1_body_event_result(
    Parent,
    Debug,
    Request,
    ReqSpan,
    Event,
    Rest,
    {accept_body, NewState, NewBodyState, _NewReqSpan}
) ->
    case Event of
        {fin, _} ->
            abort_h1_body_phase(NewState, ReqSpan, body_unconsumed);
        {abort, _} ->
            abort_h1_body_phase(NewState, ReqSpan, peer_aborted);
        _ ->
            apply_h1_body_chunks(Parent, Debug, NewState, Request, NewBodyState, ReqSpan, Rest)
    end;
handle_h1_body_event_result(
    Parent, Debug, _Request, _ReqSpan, Event, _Rest, NewState
) when is_record(NewState, state) ->
    NewState1 = finalize_h1_body_terminal(NewState, Event),
    finish_h1_request(Parent, Debug, NewState1, 0, max_pipeline_depth(NewState1));
handle_h1_body_event_result(
    Parent,
    Debug,
    _Request,
    _ReqSpan,
    _Event,
    _Rest,
    {upgrade, websocket, NewState, UpgradeRequest}
) ->
    handle_ws_upgrade(Parent, Debug, NewState, UpgradeRequest, undefined);
handle_h1_body_event_result(
    Parent,
    Debug,
    _Request,
    _ReqSpan,
    _Event,
    _Rest,
    {upgrade, websocket, NewState, UpgradeRequest, SessionOpts}
) ->
    handle_ws_upgrade(Parent, Debug, NewState, UpgradeRequest, SessionOpts);
handle_h1_body_event_result(
    Parent, Debug, _Request, _ReqSpan, _Event, _Rest, {stream_push, NewState, PushCtx}
) ->
    nhttp_conn_h1_push:h1_stream_push_loop(Parent, Debug, NewState, PushCtx).

-spec handle_h1_limit_error(#state{}, nhttp_limits:limit_error()) -> #state{}.
handle_h1_limit_error(#state{protocol_state = #h1_state{} = H1} = State, LimitError) ->
    Status = nhttp_limits:error_to_status(LimitError),
    send_h1_error_response(State, Status, limit_error_body(Status)),
    State#state{protocol_state = H1#h1_state{keep_alive = false}}.

-doc "Handle WebSocket upgrade request.".
-spec handle_ws_upgrade(
    pid(),
    [sys:debug_option()],
    #state{},
    nhttp_lib:request(),
    nhttp_ws:ws_session_opts() | undefined
) -> no_return().
handle_ws_upgrade(
    Parent,
    Debug,
    #state{socket = Socket, protocol_state = #h1_state{} = H1} = State,
    Request,
    SessionOpts
) ->
    case nhttp_ws:validate_upgrade(Request) of
        {ok, Key} ->
            case SessionOpts of
                undefined ->
                    Response = nhttp_ws:handshake_response(Key);
                _ ->
                    Response = nhttp_ws:handshake_response(Key, SessionOpts)
            end,
            case nhttp_sock:send(Socket, Response) of
                ok ->
                    case nhttp_conn:activate(State, [{packet, raw}]) of
                        ok ->
                            nhttp_conn_ws_h1:start(
                                Parent,
                                Debug,
                                State#state{
                                    family = websocket,
                                    protocol_state = H1#h1_state{buffer = <<>>}
                                }
                            );
                        {stop, Reason} ->
                            nhttp_conn:stop(Reason, State)
                    end;
                {error, Reason} ->
                    nhttp_conn:stop(nhttp_conn:sock_stop_reason(Reason), State)
            end;
        {error, Reason} ->
            send_h1_error_response(
                State, ?HTTP_BAD_REQUEST, <<"Bad Request: ", (atom_to_binary(Reason))/binary>>
            ),
            nhttp_conn:stop({ws_upgrade_error, Reason}, State)
    end.

-doc """
Cap on the buffered bytes of an incomplete request head. Belt-and-braces
behind the parser's own `{more, _}` cap: bounds the buffer even if the
parser accepts the input as incomplete.
""".
-spec head_buffer_cap(nhttp_h1:opts()) -> pos_integer().
head_buffer_cap(#{max_header_size := MaxHeaderSize}) ->
    MaxHeaderSize + ?H1_REQUEST_LINE_ALLOWANCE.

-spec limit_error_body(nhttp_lib:status()) -> binary().
limit_error_body(?HTTP_PAYLOAD_TOO_LARGE) -> <<"Payload Too Large">>;
limit_error_body(?HTTP_URI_TOO_LONG) -> <<"URI Too Long">>;
limit_error_body(?HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE) -> <<"Request Header Fields Too Large">>.

-spec lower_or_undefined(binary() | undefined) -> binary() | undefined.
lower_or_undefined(undefined) -> undefined;
lower_or_undefined(Value) -> nhttp_headers:to_lower(Value).

-spec max_pipeline_depth(#state{}) -> non_neg_integer().
max_pipeline_depth(#state{opts = Opts}) ->
    maps:get(max_pipeline_depth, Opts, ?DEFAULT_MAX_PIPELINE_DEPTH).

-spec reply_encode_failed(
    #state{},
    nhttp_lib:request(),
    {nhttp_otel:span_ctx(), integer()},
    nhttp_h1:encode_error(),
    term()
) -> #state{}.
reply_encode_failed(
    #state{protocol_state = #h1_state{} = H1} = State, Request, ReqSpan, Reason, NewHState
) ->
    Ctx = nhttp_log:request_ctx(nhttp_conn:log_ctx(State), Request, undefined),
    nhttp_log:response_encode_failed(Ctx, Reason),
    send_h1_error_response(State, ?HTTP_INTERNAL_SERVER_ERROR, <<"Internal Server Error">>),
    nhttp_conn:emit_request_stop(State, ?HTTP_INTERNAL_SERVER_ERROR, ReqSpan),
    State#state{
        protocol_state = H1#h1_state{keep_alive = false},
        handler_state = NewHState,
        requests_count = State#state.requests_count + 1
    }.

-spec send_h1_chunk(#state{}, iodata()) -> ok | {error, term()}.
send_h1_chunk(#state{socket = Socket}, Data) ->
    Chunk = nhttp_h1:encode_chunk(Data),
    nhttp_sock:send(Socket, Chunk).

-spec send_h1_error(#state{}, term()) -> ok.
send_h1_error(State, {body_too_large, _Size, _Max}) ->
    send_h1_error_response(State, ?HTTP_PAYLOAD_TOO_LARGE, <<"Payload Too Large">>);
send_h1_error(State, LimitError) when
    LimitError =:= header_too_large; LimitError =:= too_many_headers
->
    send_h1_error_response(
        State, ?HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE, <<"Request Header Fields Too Large">>
    );
send_h1_error(State, _Reason) ->
    Response = #{
        status => ?HTTP_BAD_REQUEST,
        reason => <<"Bad Request">>,
        headers => [],
        body => <<"Bad Request">>
    },
    {ok, IOList} = nhttp_h1:encode_response(Response),
    nhttp_conn:sock_send(State, IOList).

-spec send_h1_error_response(#state{}, nhttp_lib:status(), binary()) -> ok.
send_h1_error_response(State, Status, Body) ->
    Response = #{
        status => Status,
        reason => nhttp_resp:reason_phrase(Status),
        headers => [{<<"connection">>, <<"close">>}],
        body => Body
    },
    {ok, IOList} = nhttp_h1:encode_response(Response),
    nhttp_conn:sock_send(State, IOList).

-spec send_h1_last_chunk(#state{}) -> ok.
send_h1_last_chunk(State) ->
    LastChunk = nhttp_h1:encode_last_chunk(),
    nhttp_conn:sock_send(State, LastChunk).

-spec send_h1_response(#state{}, nhttp_lib:response()) -> ok | {error, nhttp_h1:encode_error()}.
send_h1_response(State, Response0) ->
    Headers = nhttp_conn:alt_svc_headers(State, maps:get(headers, Response0, [])),
    Response = Response0#{headers => Headers},
    case nhttp_h1:encode_response(Response) of
        {ok, IOList} -> nhttp_conn:sock_send(State, IOList);
        {error, _Reason} = Error -> Error
    end.

-spec send_h1_response_head(#state{}, nhttp_lib:status(), nhttp_lib:headers()) ->
    ok | {error, term()}.
send_h1_response_head(#state{socket = Socket} = State, Status, Headers0) ->
    Headers = nhttp_conn:alt_svc_headers(State, Headers0),
    case nhttp_h1:encode_response_head(http1_1, Status, Headers) of
        {ok, IOList} -> nhttp_sock:send(Socket, IOList);
        {error, _Reason} = Error -> Error
    end.

-spec should_keep_alive(nhttp_lib:request(), nhttp_lib:response()) -> boolean().
should_keep_alive(
    #{headers := ReqHeaders} = Request,
    #{headers := RespHeaders}
) ->
    Version = maps:get(version, Request, http1_1),
    Default = Version =:= http1_1,
    ConnHeader = lower_or_undefined(
        nhttp_headers:get(<<"connection">>, ReqHeaders)
    ),
    RespConnHeader = lower_or_undefined(
        nhttp_headers:get(<<"connection">>, RespHeaders)
    ),
    case {ConnHeader, RespConnHeader} of
        {_, <<"close">>} -> false;
        {<<"close">>, _} -> false;
        {<<"keep-alive">>, _} -> true;
        _ -> Default
    end.

-spec validate_h1_host(nhttp_lib:request()) -> ok | {error, bad_host}.
validate_h1_host(#{version := http1_1, headers := Headers}) ->
    count_host_headers(Headers, 0);
validate_h1_host(_Request) ->
    ok.
