-module(nhttp_conn_ws_h3).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn_h3.hrl").
-include("nhttp_status_codes.hrl").
-include("nhttp_ws_codes.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    close_all/1,
    connect/3,
    handle_call/3,
    handle_cast/2,
    handle_data/3,
    handle_flushed/2,
    handle_info/2,
    handle_stream_reset/3,
    notify_connection_close/2
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Close every active WebSocket session on the connection. Called from
`nhttp_conn_h3:graceful_shutdown/1` before the H3 GOAWAY is sent.
""".
-spec close_all(#state{}) -> #state{}.
close_all(State) ->
    fold(State, fun(StreamId, _Ws, Acc) ->
        close_session(Acc, StreamId, ?WS_CLOSE_GOING_AWAY, <<"Server shutting down">>, shutdown)
    end).

-doc """
Handle Extended CONNECT with `:protocol = websocket` over HTTP/3
(RFC 9220). The handler's `handle_request/2` callback decides whether
to accept the upgrade. On `{upgrade, websocket, _}` the open lifecycle
runs and the session enters the connection's stream map under
`type = websocket`.
""".
-spec connect(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) -> #state{}.
connect(#state{handler = Handler, handler_state = HState} = State, StreamId, Request) ->
    case nhttp_handler:safe_handle_request(Handler, Request, HState) of
        {upgrade, websocket, NewHState} ->
            open(State#state{handler_state = NewHState}, StreamId, #{});
        {upgrade, websocket, SessionOpts, NewHState} ->
            open(State#state{handler_state = NewHState}, StreamId, SessionOpts);
        {nhttp_handler_exception, Class, Reason} ->
            Ctx = nhttp_log:request_ctx(nhttp_conn_h3:log_ctx(State), Request, StreamId),
            nhttp_log:handler_crashed(Ctx, handle_request, Class, Reason),
            nhttp_conn_h3:send_h3_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR);
        _ ->
            nhttp_conn_h3:send_h3_error_response(State, StreamId, ?HTTP_BAD_REQUEST)
    end.

-doc """
Dispatch an incoming `'$gen_call'` envelope. Currently `ws_send` and
`ws_send_async` are the only call paths.
""".
-spec handle_call(#state{}, gen_server:from(), term()) -> #state{}.
handle_call(State, From, {ws_send, Ref, StreamId, Frame}) ->
    call_send(State, From, Ref, StreamId, Frame);
handle_call(State, From, {ws_send_async, Ref, StreamId, Frame}) ->
    call_send(State, From, Ref, StreamId, Frame);
handle_call(State, From, _Other) ->
    gen_server:reply(From, {error, badarg}),
    State.

-doc """
Dispatch an incoming `'$gen_cast'` envelope. `ws_info` delivers a message
to a specific session via `dispatch_info/3`. `ws_close` closes a session
locally with the given code/reason.
""".
-spec handle_cast(#state{}, term()) -> #state{}.
handle_cast(State, {ws_info, StreamId, Msg}) ->
    case get_ws(State, StreamId) of
        undefined -> State;
        #ws_state{} -> dispatch_info(State, StreamId, Msg)
    end;
handle_cast(State, {ws_close, Ref, StreamId, Code, Reason, _Opts}) ->
    case get_ws(State, StreamId) of
        #ws_state{ref = Ref} ->
            close_session(State, StreamId, Code, Reason, {local, Code, Reason});
        _ ->
            State
    end;
handle_cast(State, _Other) ->
    State.

-doc """
Process incoming WebSocket frame data for a stream. Feeds it to the
stream decoder and dispatches each complete message through `nhttp_conn_ws`.
""".
-spec handle_data(#state{}, nhttp_lib:stream_id(), binary()) -> #state{}.
handle_data(#state{h3_streams = Streams} = State, StreamId, Data) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = websocket, ws_decoder = Dec} ->
            feed(State, StreamId, Data, Dec);
        _ ->
            State
    end.

-doc """
Settle a WebSocket stream after its send buffer drained or failed. A
failed flush resets the stream and ends the session if it is still open.
""".
-spec handle_flushed(nhttp_lib:stream_id(), nhttp_conn_h3:offer_result()) -> #state{}.
handle_flushed(_StreamId, {sent, State}) ->
    State;
handle_flushed(_StreamId, {stashed, State}) ->
    State;
handle_flushed(StreamId, {error, Reason, State}) ->
    abort(State, StreamId, Reason).

-doc """
Broadcast an untagged Erlang message to every active WS session on the
connection (proposal §4.4). For non-WS conns this is a no-op.
""".
-spec handle_info(#state{}, term()) -> #state{}.
handle_info(State, Msg) ->
    fold(State, fun(StreamId, _Ws, Acc) ->
        dispatch_info(Acc, StreamId, Msg)
    end).

-doc """
Handle a peer-initiated stream reset (RESET_STREAM) or STOP_SENDING on a
WebSocket stream. Fires `handle_ws_closed/3` with `{h3_reset, ErrorCode}`
and drops the stream entry.
""".
-spec handle_stream_reset(#state{}, nhttp_lib:stream_id(), non_neg_integer()) -> #state{}.
handle_stream_reset(State, StreamId, ErrorCode) ->
    case get_ws(State, StreamId) of
        undefined ->
            drop_stream(State, StreamId);
        #ws_state{} ->
            finish(State, StreamId, {h3_reset, ErrorCode})
    end.

-doc """
Notify every active WS session that the QUIC connection is going away.
The transport is already gone, so no CLOSE frame is sent. The close
lifecycle just fires `handle_ws_closed/3` (when exported) and drops the
stream.
""".
-spec notify_connection_close(#state{}, non_neg_integer()) -> #state{}.
notify_connection_close(State, ErrorCode) ->
    fold(State, fun(StreamId, _Ws, Acc) ->
        finish(Acc, StreamId, {transport, {connection_close, ErrorCode}})
    end).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - LIFECYCLE
%%%-----------------------------------------------------------------------------
-doc """
Build the frame decoder from the runtime opts the open callback settled
on. Armed here rather than at stream construction because
`max_message_size` bounds what the decoder accumulates, and until
`handle_ws_open/2` returns there is no limit to give it.
""".
-spec arm_decoder(#state{}, nhttp_lib:stream_id()) -> #state{}.
arm_decoder(State, StreamId) ->
    case get_ws(State, StreamId) of
        undefined ->
            State;
        #ws_state{} = Ws ->
            Dec = nhttp_ws:decoder_new(server, nhttp_conn_ws:runtime_opts(Ws)),
            set_decoder(State, StreamId, Dec)
    end.

-spec call_send(
    #state{}, gen_server:from(), reference(), nhttp_lib:stream_id(), nhttp_ws:ws_message()
) -> #state{}.
call_send(State, From, Ref, StreamId, Frame) ->
    case get_ws(State, StreamId) of
        #ws_state{ref = Ref} ->
            case send_frame(State, StreamId, Frame) of
                {error, Reason, State1} ->
                    gen_server:reply(From, {error, gone}),
                    abort(State1, StreamId, Reason);
                {_SentOrStashed, State1} ->
                    gen_server:reply(From, ok),
                    State1
            end;
        #ws_state{} ->
            gen_server:reply(From, {error, gone}),
            State;
        undefined ->
            gen_server:reply(From, {error, gone}),
            State
    end.

-spec dispatch_frame(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_message()) -> #state{}.
dispatch_frame(#state{handler = Handler} = State, StreamId, Message) ->
    case get_ws(State, StreamId) of
        undefined ->
            State;
        #ws_state{} = Ws ->
            {Ws1, Actions} = nhttp_conn_ws:dispatch_frame(
                nhttp_conn_h3:log_ctx(State), Message, Ws, Handler
            ),
            interpret(put_ws(State, StreamId, Ws1), StreamId, Actions)
    end.

-spec dispatch_info(#state{}, nhttp_lib:stream_id(), term()) -> #state{}.
dispatch_info(#state{handler = Handler} = State, StreamId, Msg) ->
    case get_ws(State, StreamId) of
        undefined ->
            State;
        #ws_state{} = Ws ->
            {Ws1, Actions} = nhttp_conn_ws:dispatch_info(
                nhttp_conn_h3:log_ctx(State), Msg, Ws, Handler
            ),
            interpret(put_ws(State, StreamId, Ws1), StreamId, Actions)
    end.

-spec finish(#state{}, nhttp_lib:stream_id(), nhttp_handler:ws_close_reason()) -> #state{}.
finish(#state{handler = Handler} = State, StreamId, WsReason) ->
    case get_ws(State, StreamId) of
        undefined ->
            drop_stream(State, StreamId);
        #ws_state{} = Ws ->
            #ws_state{} = nhttp_conn_ws:close_lifecycle(
                nhttp_conn_h3:log_ctx(State), WsReason, Ws, Handler
            ),
            drop_stream(State, StreamId)
    end.

-spec open(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_session_opts()) -> #state{}.
open(
    #state{handler = Handler, handler_state = HState} = State,
    StreamId,
    SessionOpts
) ->
    SubprotocolHeaders =
        case maps:get(subprotocol, SessionOpts, undefined) of
            undefined -> [];
            P -> [{<<"sec-websocket-protocol">>, P}]
        end,
    ExtensionHeaders =
        case maps:get(extensions, SessionOpts, []) of
            [] -> [];
            Exts -> [{<<"sec-websocket-extensions">>, iolist_to_binary(lists:join(<<", ">>, Exts))}]
        end,
    RespHeaders = [{<<":status">>, <<"200">>}] ++ SubprotocolHeaders ++ ExtensionHeaders,
    State1 = nhttp_conn_h3:send_h3_headers(State, StreamId, RespHeaders, nofin),
    Ref = make_ref(),
    Session = nhttp_ws:new_session(h3, self(), StreamId, Ref),
    Ws0 = #ws_state{
        session = Session,
        ref = Ref,
        handler_state = HState,
        deliver_ping = false,
        deliver_pong = false,
        max_message_size = infinity
    },
    Stream0 = #h3_stream{
        type = websocket,
        ws_decoder = nhttp_ws:decoder_new(server),
        ws = Ws0
    },
    State2 = State1#state{
        h3_streams = (State1#state.h3_streams)#{StreamId => Stream0},
        requests_count = State#state.requests_count + 1
    },
    ok = nhttp_stats:incr_ws_session(),
    {Ws1, Actions} = nhttp_conn_ws:open(nhttp_conn_h3:log_ctx(State2), Ws0, Handler),
    arm_decoder(interpret(put_ws(State2, StreamId, Ws1), StreamId, Actions), StreamId).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - FRAME PROCESSING
%%%-----------------------------------------------------------------------------
-spec dispatch_frames(#state{}, nhttp_lib:stream_id(), [nhttp_ws:ws_message()]) -> #state{}.
dispatch_frames(State, _StreamId, []) ->
    State;
dispatch_frames(State, StreamId, [Message | Rest]) ->
    dispatch_frames(dispatch_frame(State, StreamId, Message), StreamId, Rest).

-spec fail(#state{}, nhttp_lib:stream_id(), nhttp_ws:decode_error()) -> #state{}.
fail(State, StreamId, Reason) ->
    case get_ws(State, StreamId) of
        undefined ->
            State;
        #ws_state{} ->
            {Code, ReasonBin} = nhttp_conn_ws:decode_error_close(Reason),
            close_session(State, StreamId, Code, ReasonBin, {fail, Code, ReasonBin})
    end.

-spec feed(#state{}, nhttp_lib:stream_id(), binary(), nhttp_ws:ws_decoder()) -> #state{}.
feed(State, StreamId, Data, Dec) ->
    case nhttp_ws:feed(Data, Dec) of
        {ok, Messages, NewDec} ->
            dispatch_frames(set_decoder(State, StreamId, NewDec), StreamId, Messages);
        {error, Reason, Messages} ->
            fail(dispatch_frames(State, StreamId, Messages), StreamId, Reason)
    end.

-spec set_decoder(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_decoder()) -> #state{}.
set_decoder(State, StreamId, Dec) ->
    case maps:get(StreamId, State#state.h3_streams, undefined) of
        #h3_stream{} = LiveStream ->
            State#state{
                h3_streams = (State#state.h3_streams)#{
                    StreamId => LiveStream#h3_stream{ws_decoder = Dec}
                }
            };
        undefined ->
            State
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ACTION INTERPRETER
%%%-----------------------------------------------------------------------------
-spec interpret(#state{}, nhttp_lib:stream_id(), nhttp_conn_ws:actions()) -> #state{}.
interpret(State, _StreamId, []) ->
    State;
interpret(State, StreamId, [{send_frame, Msg} | Rest]) ->
    interpret_next(send_frame(State, StreamId, Msg), StreamId, Rest);
interpret(State, StreamId, [{send_close, Code, Reason} | Rest]) ->
    interpret_next(send_close(State, StreamId, Code, Reason), StreamId, Rest);
interpret(State, StreamId, [{close_session, WsReason} | _Rest]) ->
    finish(State, StreamId, WsReason).

-spec interpret_next(nhttp_conn_h3:offer_result(), nhttp_lib:stream_id(), nhttp_conn_ws:actions()) ->
    #state{}.
interpret_next({error, Reason, State}, StreamId, _Rest) ->
    abort(State, StreamId, Reason);
interpret_next({_SentOrStashed, State}, StreamId, Rest) ->
    interpret(State, StreamId, Rest).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - STREAM MAP HELPERS
%%%-----------------------------------------------------------------------------
-spec drop_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
drop_stream(#state{h3_streams = Streams} = State, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{type = websocket} ->
            ok = nhttp_stats:decr_ws_session();
        _ ->
            ok
    end,
    State#state{h3_streams = maps:remove(StreamId, Streams)}.

-spec fold(#state{}, fun((nhttp_lib:stream_id(), #ws_state{}, #state{}) -> #state{})) -> #state{}.
fold(#state{h3_streams = Streams} = State, F) ->
    Pairs = [
        {Id, Ws}
     || {Id, #h3_stream{type = websocket, ws = #ws_state{} = Ws}} <- maps:to_list(Streams)
    ],
    lists:foldl(fun({Id, Ws}, Acc) -> F(Id, Ws, Acc) end, State, Pairs).

-spec get_ws(#state{}, nhttp_lib:stream_id()) -> #ws_state{} | undefined.
get_ws(#state{h3_streams = Streams}, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{ws = #ws_state{} = Ws} -> Ws;
        _ -> undefined
    end.

-spec put_ws(#state{}, nhttp_lib:stream_id(), #ws_state{}) -> #state{}.
put_ws(#state{h3_streams = Streams} = State, StreamId, Ws) ->
    case maps:get(StreamId, Streams, undefined) of
        #h3_stream{ws = #ws_state{}} = Stream ->
            NewStream = Stream#h3_stream{ws = Ws},
            State#state{h3_streams = Streams#{StreamId => NewStream}};
        _ ->
            State
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SEND HELPERS
%%%-----------------------------------------------------------------------------
-doc """
End a session whose frame QUIC refused for another reason than flow
control. The stream is reset and the close lifecycle runs with
`{transport, Reason}`.
""".
-spec abort(#state{}, nhttp_lib:stream_id(), nhttp_conn_h3:send_error()) -> #state{}.
abort(State, StreamId, Reason) ->
    nhttp_log:ws_send_failed(nhttp_conn_h3:log_ctx(State), Reason),
    State1 = nhttp_conn_h3:reset_stream(
        State, StreamId, nhttp_conn_h3:h3_error_to_code(h3_internal_error)
    ),
    finish(State1, StreamId, {transport, Reason}).

-doc """
Write the CLOSE frame with FIN and run the close lifecycle. The CLOSE
leaves after the frames already buffered for the stream.
""".
-spec close_session(
    #state{},
    nhttp_lib:stream_id(),
    no_code | nhttp_ws:close_code(),
    binary(),
    nhttp_handler:ws_close_reason()
) -> #state{}.
close_session(State, StreamId, Code, Reason, WsReason) ->
    case send_close(State, StreamId, Code, Reason) of
        {error, SendError, State1} -> abort(State1, StreamId, SendError);
        {_SentOrStashed, State1} -> finish(State1, StreamId, WsReason)
    end.

-spec send_close(#state{}, nhttp_lib:stream_id(), no_code | nhttp_ws:close_code(), binary()) ->
    nhttp_conn_h3:offer_result().
send_close(State, StreamId, no_code, _Reason) ->
    nhttp_conn_h3:send_h3_data(State, StreamId, nhttp_ws:encode(close), fin);
send_close(State, StreamId, Code, Reason) ->
    nhttp_conn_h3:send_h3_data(State, StreamId, nhttp_ws:encode({close, Code, Reason}), fin).

-spec send_frame(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_message()) ->
    nhttp_conn_h3:offer_result().
send_frame(State, StreamId, Message) ->
    nhttp_conn_h3:send_h3_data(State, StreamId, nhttp_ws:encode(Message), nofin).
