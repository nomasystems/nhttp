-module(nhttp_conn_ws_h2).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn.hrl").
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
    handle_info/2,
    handle_stream_reset/3,
    notify_goaway/2
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Close every active WebSocket session on the connection. Called from
`nhttp_conn:graceful_shutdown/1` before the H2 GOAWAY is sent.
""".
-spec close_all(#state{}) -> #state{}.
close_all(State) ->
    fold(State, fun(StreamId, _Ws, Acc) ->
        Acc1 = send_close(Acc, StreamId, ?WS_CLOSE_GOING_AWAY, <<"Server shutting down">>),
        finish(Acc1, StreamId, shutdown)
    end).

-doc """
Handle Extended CONNECT with `:protocol = websocket` over HTTP/2. The
handler's `handle_request/2` callback decides whether to accept the
upgrade. On `{upgrade, websocket, _}` the open lifecycle runs and the
session enters the connection's stream map under `type = websocket`.
""".
-spec connect(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) -> #state{}.
connect(#state{handler = Handler, handler_state = HState} = State, StreamId, Request) ->
    case nhttp_handler:safe_handle_request(Handler, Request, HState) of
        {upgrade, websocket, NewHState} ->
            open(State#state{handler_state = NewHState}, StreamId, #{});
        {upgrade, websocket, SessionOpts, NewHState} ->
            open(State#state{handler_state = NewHState}, StreamId, SessionOpts);
        {nhttp_handler_exception, Class, Reason} ->
            Ctx = nhttp_log:request_ctx(nhttp_conn:log_ctx(State), Request, StreamId),
            nhttp_log:handler_crashed(Ctx, handle_request, Class, Reason),
            send_error_response(State, StreamId, ?HTTP_INTERNAL_SERVER_ERROR),
            State;
        _ ->
            send_error_response(State, StreamId, ?HTTP_BAD_REQUEST),
            State
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
to a specific session via `handle_ws_info/3`. `ws_close` closes a session
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
            State1 = send_close(State, StreamId, Code, Reason),
            finish(State1, StreamId, {local, Code, Reason});
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
handle_data(#state{protocol_state = #h2_state{h2_streams = Streams}} = State, StreamId, Data) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = websocket, ws_decoder = Dec} ->
            feed(State, StreamId, Data, Dec);
        _ ->
            State
    end.

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
Handle a peer-initiated stream reset (RST_STREAM) on a WebSocket stream.
Runs the shared close lifecycle with `{h2_reset, ErrorCode}` and drops
the stream entry. RST_STREAM means the peer stopped listening, so no
CLOSE frame is sent.
""".
-spec handle_stream_reset(#state{}, nhttp_lib:stream_id(), nhttp_h2:error_code()) -> #state{}.
handle_stream_reset(State, StreamId, ErrorCode) ->
    case get_ws(State, StreamId) of
        undefined ->
            drop_stream(State, StreamId);
        #ws_state{} ->
            finish(State, StreamId, {h2_reset, ErrorCode})
    end.

-doc """
Notify every active WS session that the connection is going away. Sends a
no-code CLOSE on each stream and finishes the session with a transport
reason.
""".
-spec notify_goaway(#state{}, nhttp_h2:error_code()) -> #state{}.
notify_goaway(State, ErrorCode) ->
    fold(State, fun(StreamId, _Ws, Acc) ->
        Acc1 = send_close(Acc, StreamId, no_code, <<>>),
        finish(Acc1, StreamId, {transport, {goaway, ErrorCode}})
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
            State1 = send_frame(State, StreamId, Frame),
            gen_server:reply(From, ok),
            State1;
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
                nhttp_conn:log_ctx(State), Message, Ws, Handler
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
                nhttp_conn:log_ctx(State), Msg, Ws, Handler
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
                nhttp_conn:log_ctx(State), WsReason, Ws, Handler
            ),
            drop_stream(State, StreamId)
    end.

-spec open(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_session_opts()) -> #state{}.
open(
    #state{
        handler = Handler,
        handler_state = HState,
        protocol_state = #h2_state{h2_streams = Streams} = H2
    } = State,
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
    NewH2Conn = send_headers(State, StreamId, RespHeaders, nofin),
    Ref = make_ref(),
    Session = nhttp_ws:new_session(h2, self(), StreamId, Ref),
    Ws0 = #ws_state{
        session = Session,
        ref = Ref,
        handler_state = HState,
        deliver_ping = false,
        deliver_pong = false,
        max_message_size = infinity
    },
    Stream0 = #h2_stream{
        type = websocket,
        ws_decoder = nhttp_ws:decoder_new(server),
        ws = Ws0
    },
    State1 = State#state{
        protocol_state = H2#h2_state{
            h2_conn = NewH2Conn,
            h2_streams = Streams#{StreamId => Stream0}
        },
        requests_count = State#state.requests_count + 1
    },
    ok = nhttp_stats:incr_ws_session(),
    {Ws1, Actions} = nhttp_conn_ws:open(nhttp_conn:log_ctx(State1), Ws0, Handler),
    arm_decoder(interpret(put_ws(State1, StreamId, Ws1), StreamId, Actions), StreamId).

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
            State1 = send_close(State, StreamId, Code, ReasonBin),
            finish(State1, StreamId, {fail, Code, ReasonBin})
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
set_decoder(#state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId, Dec) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{} = LiveStream ->
            State#state{
                protocol_state = H2#h2_state{
                    h2_streams = Streams#{StreamId => LiveStream#h2_stream{ws_decoder = Dec}}
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
    State1 = send_frame(State, StreamId, Msg),
    interpret(State1, StreamId, Rest);
interpret(State, StreamId, [{send_close, Code, Reason} | Rest]) ->
    State1 = send_close(State, StreamId, Code, Reason),
    interpret(State1, StreamId, Rest);
interpret(State, StreamId, [{close_session, WsReason} | _Rest]) ->
    finish(State, StreamId, WsReason).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - STREAM MAP HELPERS
%%%-----------------------------------------------------------------------------
-spec drop_stream(#state{}, nhttp_lib:stream_id()) -> #state{}.
drop_stream(#state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{type = websocket} ->
            ok = nhttp_stats:decr_ws_session();
        _ ->
            ok
    end,
    State#state{protocol_state = H2#h2_state{h2_streams = maps:remove(StreamId, Streams)}}.

-spec fold(#state{}, fun((nhttp_lib:stream_id(), #ws_state{}, #state{}) -> #state{})) -> #state{}.
fold(#state{protocol_state = #h2_state{h2_streams = Streams}} = State, F) ->
    Pairs = [
        {Id, Ws}
     || {Id, #h2_stream{type = websocket, ws = #ws_state{} = Ws}} <- maps:to_list(Streams)
    ],
    lists:foldl(fun({Id, Ws}, Acc) -> F(Id, Ws, Acc) end, State, Pairs).

-spec get_ws(#state{}, nhttp_lib:stream_id()) -> #ws_state{} | undefined.
get_ws(#state{protocol_state = #h2_state{h2_streams = Streams}}, StreamId) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{ws = #ws_state{} = Ws} -> Ws;
        _ -> undefined
    end.

-spec put_ws(#state{}, nhttp_lib:stream_id(), #ws_state{}) -> #state{}.
put_ws(#state{protocol_state = #h2_state{h2_streams = Streams} = H2} = State, StreamId, Ws) ->
    case maps:get(StreamId, Streams, undefined) of
        #h2_stream{ws = #ws_state{}} = Stream ->
            NewStream = Stream#h2_stream{ws = Ws},
            State#state{protocol_state = H2#h2_state{h2_streams = Streams#{StreamId => NewStream}}};
        _ ->
            State
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SEND HELPERS
%%%-----------------------------------------------------------------------------
-spec offer(#state{}, nhttp_lib:stream_id(), iodata(), nhttp_h2:fin()) -> #state{}.
offer(State, StreamId, Frame, Fin) ->
    case nhttp_conn_h2:offer_h2_data(State, StreamId, Frame, Fin, []) of
        {sent, State1} ->
            State1;
        {queued, State1} ->
            State1;
        {send_buffer_full, State1} ->
            State2 = nhttp_conn_h2:reset_h2_stream(State1, StreamId, enhance_your_calm),
            finish(State2, StreamId, {transport, send_buffer_full});
        {stream_gone, State1} ->
            State1
    end.

-doc """
Offer the CLOSE frame with END_STREAM to the codec send queue. A queued
frame leaves with the drain and the caller drops the stream entry at once,
so the `data_sent` event for it finds no entry. A frame the queue bound
refuses ends the stream with RST_STREAM(ENHANCE_YOUR_CALM) and runs the
close lifecycle with `{transport, send_buffer_full}`.
""".
-spec send_close(#state{}, nhttp_lib:stream_id(), no_code | nhttp_ws:close_code(), binary()) ->
    #state{}.
send_close(State, StreamId, Code, Reason) ->
    CloseFrame =
        case Code of
            no_code -> nhttp_ws:encode(close);
            _ -> nhttp_ws:encode({close, Code, Reason})
        end,
    offer(State, StreamId, CloseFrame, fin).

-spec send_error_response(#state{}, nhttp_lib:stream_id(), nhttp_lib:status()) -> ok.
send_error_response(
    #state{protocol_state = #h2_state{h2_conn = H2Conn}} = State, StreamId, Status
) ->
    Headers = [{<<":status">>, integer_to_binary(Status)}],
    {ok, _, HeaderFrame} = nhttp_h2:send_headers(H2Conn, StreamId, Headers, fin),
    nhttp_conn:sock_send(State, HeaderFrame).

-doc """
Offer one WebSocket frame to the codec send queue. A frame that outruns
the send window is queued and leaves with the drain. A frame the queue
bound refuses ends the stream with RST_STREAM(ENHANCE_YOUR_CALM) and runs
the close lifecycle with `{transport, send_buffer_full}`.
""".
-spec send_frame(#state{}, nhttp_lib:stream_id(), nhttp_ws:ws_message()) -> #state{}.
send_frame(State, StreamId, Message) ->
    offer(State, StreamId, nhttp_ws:encode(Message), nofin).

-spec send_headers(#state{}, nhttp_lib:stream_id(), nhttp_lib:headers(), nhttp_h2:fin()) ->
    nhttp_h2:conn().
send_headers(#state{protocol_state = #h2_state{h2_conn = H2Conn}} = State, StreamId, Headers, Fin) ->
    {ok, NewH2Conn, EncodedFrame} = nhttp_h2:send_headers(H2Conn, StreamId, Headers, Fin),
    nhttp_conn:sock_send(State, EncodedFrame),
    NewH2Conn.
