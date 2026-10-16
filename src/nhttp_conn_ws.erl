-module(nhttp_conn_ws).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn_ws.hrl").
-include("nhttp_ws_codes.hrl").

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(MESSAGE_TOO_BIG, <<"Message Too Big">>).

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    apply_handler_result/2,
    apply_runtime_opts/2,
    close_lifecycle/4,
    decode_error_close/1,
    default_runtime_opts/0,
    dispatch_frame/4,
    dispatch_info/4,
    open/3,
    oversize/2,
    runtime_opts/1,
    str_or_atom/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    action/0,
    actions/0,
    runtime_opts/0
]).

-type runtime_opts() :: #{
    deliver_ping := boolean(),
    deliver_pong := boolean(),
    max_message_size := pos_integer() | infinity
}.

-type action() ::
    {send_frame, nhttp_ws:ws_message()}
    | {send_close, no_code | nhttp_ws:close_code(), binary()}
    | {close_session, nhttp_handler:ws_close_reason()}.

-type actions() :: [action()].

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Apply the result of a handler callback (`handle_ws_frame/3` or
`handle_ws_info/3`) to the session. Returns the session with the new
handler state and the actions the transport must interpret, in order.
""".
-spec apply_handler_result(term(), #ws_state{}) -> {#ws_state{}, actions()}.
apply_handler_result({ok, NewSState}, Ws) ->
    {Ws#ws_state{handler_state = NewSState}, []};
apply_handler_result({reply, Frames, NewSState}, Ws) when is_list(Frames) ->
    {Ws#ws_state{handler_state = NewSState}, [{send_frame, F} || F <- Frames]};
apply_handler_result({reply, Frame, NewSState}, Ws) ->
    {Ws#ws_state{handler_state = NewSState}, [{send_frame, Frame}]};
apply_handler_result({close, Code, Reason, NewSState}, Ws) ->
    {Ws#ws_state{handler_state = NewSState}, [
        {send_close, Code, Reason},
        {close_session, {local, Code, Reason}}
    ]};
apply_handler_result({stop, Reason, NewSState}, Ws) ->
    {Ws#ws_state{handler_state = NewSState}, [
        {send_close, ?WS_CLOSE_GOING_AWAY, <<"Server Going Away">>},
        {close_session, {handler_stop, Reason}}
    ]};
apply_handler_result({nhttp_handler_exception, Class, Reason}, Ws) ->
    {Ws, [
        {send_close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>},
        {close_session, {handler_crash, {Class, Reason}}}
    ]}.

-doc """
Apply user-provided runtime opts on top of the current opts map.
Unknown keys are ignored. Missing keys preserve the current value.
""".
-spec apply_runtime_opts(map(), runtime_opts()) -> runtime_opts().
apply_runtime_opts(Opts, Current) ->
    #{
        deliver_ping := DP,
        deliver_pong := DG,
        max_message_size := MS
    } = Current,
    #{
        deliver_ping => maps:get(deliver_ping, Opts, DP),
        deliver_pong => maps:get(deliver_pong, Opts, DG),
        max_message_size => maps:get(max_message_size, Opts, MS)
    }.

-doc """
Run the close lifecycle for a session. Fires `handle_ws_closed/3` if the
handler exports it and returns the session with the handler state it
left. The transport completes the close (terminate conn or drop stream).
""".
-spec close_lifecycle(nhttp_log:ctx(), nhttp_handler:ws_close_reason(), #ws_state{}, module()) ->
    #ws_state{}.
close_lifecycle(_Ctx, _Reason, #ws_state{handles_closed = false} = Ws, _Handler) ->
    Ws;
close_lifecycle(Ctx, Reason, #ws_state{handler_state = SState, session = Session} = Ws, Handler) ->
    Result = nhttp_handler:safe_handle_ws_closed(Handler, Reason, Session, SState),
    log_crash_if_any(Result, Ctx, handle_ws_closed),
    case Result of
        {ok, NewSState} -> Ws#ws_state{handler_state = NewSState};
        _ -> Ws
    end.

-doc """
Map a frame decoder error to the CLOSE code and reason RFC 6455 gives it.
A text payload or CLOSE reason that is not valid UTF-8 is 1007 (§8.1), a
message over the session limit is 1009 (§7.4.1), a frame header that
declares a length over that limit is 1009 too (§10.4), and every other framing
error is a protocol error. The set of reasons the decoder can return is
open, so an unknown one is a protocol error too.

`message_too_large` carries the same reason text as the message-level
check in `dispatch_frame/4`, because the peer sees one limit whichever
side of reassembly trips it.
""".
-spec decode_error_close(term()) -> {nhttp_ws:close_code(), binary()}.
decode_error_close(message_too_large) -> {?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG};
decode_error_close({frame_too_large, _Declared}) -> {?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG};
decode_error_close(invalid_utf8 = R) -> {?WS_CLOSE_INVALID_PAYLOAD, str_or_atom(R)};
decode_error_close(invalid_close_reason = R) -> {?WS_CLOSE_INVALID_PAYLOAD, str_or_atom(R)};
decode_error_close(R) -> {?WS_CLOSE_PROTOCOL_ERROR, str_or_atom(R)}.

-doc "Default runtime opts (everything off / unbounded).".
-spec default_runtime_opts() -> runtime_opts().
default_runtime_opts() ->
    #{deliver_ping => false, deliver_pong => false, max_message_size => infinity}.

-doc """
Dispatch a single decoded WebSocket frame. May call `handle_ws_frame/3`
synchronously (always for text/binary, opt-in for ping/pong via
`deliver_ping` / `deliver_pong`). CLOSE frames never reach the handler.
The dispatcher reciprocates the close per RFC 6455 §5.5.1 / §7.1.5.
""".
-spec dispatch_frame(nhttp_log:ctx(), nhttp_ws:ws_message(), #ws_state{}, module()) ->
    {#ws_state{}, actions()}.
dispatch_frame(Ctx, ping, #ws_state{deliver_ping = Deliver} = Ws, Handler) ->
    control_frame_actions(Ctx, [{send_frame, pong}], Deliver, {ping, <<>>}, Ws, Handler);
dispatch_frame(Ctx, {ping, Data}, #ws_state{deliver_ping = Deliver} = Ws, Handler) ->
    control_frame_actions(Ctx, [{send_frame, {pong, Data}}], Deliver, {ping, Data}, Ws, Handler);
dispatch_frame(Ctx, pong, #ws_state{deliver_pong = Deliver} = Ws, Handler) ->
    control_frame_actions(Ctx, [], Deliver, {pong, <<>>}, Ws, Handler);
dispatch_frame(Ctx, {pong, Data}, #ws_state{deliver_pong = Deliver} = Ws, Handler) ->
    control_frame_actions(Ctx, [], Deliver, {pong, Data}, Ws, Handler);
dispatch_frame(_Ctx, close, Ws, _Handler) ->
    {Ws, [{send_close, no_code, <<>>}, {close_session, {peer, ?WS_CLOSE_NO_STATUS, <<>>}}]};
dispatch_frame(_Ctx, {close, Code, Reason}, Ws, _Handler) ->
    {Ws, [{send_close, Code, Reason}, {close_session, {peer, Code, Reason}}]};
dispatch_frame(Ctx, {Type, Data}, #ws_state{max_message_size = Max} = Ws, Handler) when
    Type =:= text orelse Type =:= binary
->
    case oversize(Data, Max) of
        true ->
            {Ws, [
                {send_close, ?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG},
                {close_session, {fail, ?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG}}
            ]};
        false ->
            handler_frame_actions(Ctx, {Type, Data}, Ws, Handler)
    end.

-doc """
Dispatch an Erlang message addressed to the session via
`gen_server:cast/2` ({ws_info, ...}) or via untagged broadcast. Calls
`handle_ws_info/3` if the handler exports it.
""".
-spec dispatch_info(nhttp_log:ctx(), term(), #ws_state{}, module()) -> {#ws_state{}, actions()}.
dispatch_info(_Ctx, _Msg, #ws_state{handles_info = false} = Ws, _Handler) ->
    {Ws, []};
dispatch_info(Ctx, Msg, #ws_state{handler_state = SState, session = Session} = Ws, Handler) ->
    Result = nhttp_handler:safe_handle_ws_info(Handler, Msg, Session, SState),
    log_crash_if_any(Result, Ctx, handle_ws_info),
    apply_handler_result(Result, Ws).

-doc """
Run the open lifecycle. Records which optional callbacks the handler
exports, fires `handle_ws_open/2` if exported, applies the runtime opts it
returns, and translates the result into actions. The handler module is
loaded at this point, because `handle_request/2` ran to accept the upgrade.
""".
-spec open(nhttp_log:ctx(), #ws_state{}, module()) -> {#ws_state{}, actions()}.
open(Ctx, Ws0, Handler) ->
    Ws = Ws0#ws_state{
        handles_frame = erlang:function_exported(Handler, handle_ws_frame, 3),
        handles_info = erlang:function_exported(Handler, handle_ws_info, 3),
        handles_closed = erlang:function_exported(Handler, handle_ws_closed, 3)
    },
    case erlang:function_exported(Handler, handle_ws_open, 2) of
        false ->
            {Ws, []};
        true ->
            #ws_state{handler_state = SState, session = Session} = Ws,
            Result = nhttp_handler:safe_handle_ws_open(Handler, Session, SState),
            log_crash_if_any(Result, Ctx, handle_ws_open),
            case Result of
                {ok, NewSState} ->
                    {Ws#ws_state{handler_state = NewSState}, []};
                {ok, NewSState, Opts} when is_map(Opts) ->
                    NewOpts = apply_runtime_opts(Opts, runtime_opts(Ws)),
                    {put_runtime_opts(NewOpts, Ws#ws_state{handler_state = NewSState}), []};
                {close, Code, Reason} ->
                    {Ws, [
                        {send_close, Code, Reason},
                        {close_session, {local, Code, Reason}}
                    ]};
                {nhttp_handler_exception, Class, Reason} ->
                    {Ws, [
                        {send_close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>},
                        {close_session, {handler_crash, {Class, Reason}}}
                    ]}
            end
    end.

-doc "Check whether a payload exceeds the max message size.".
-spec oversize(binary(), pos_integer() | infinity) -> boolean().
oversize(_Data, infinity) -> false;
oversize(Data, Max) -> byte_size(Data) > Max.

-doc "The runtime opts of a session, the input of `nhttp_ws:decoder_new/2`.".
-spec runtime_opts(#ws_state{}) -> runtime_opts().
runtime_opts(#ws_state{deliver_ping = DP, deliver_pong = DG, max_message_size = MS}) ->
    #{deliver_ping => DP, deliver_pong => DG, max_message_size => MS}.

-doc """
Render an arbitrary term as a binary suitable for use in CLOSE reason
fields. Atoms become their printable name. Everything else is rendered
via `~p` formatting.
""".
-spec str_or_atom(term()) -> binary().
str_or_atom(R) when is_atom(R) -> atom_to_binary(R);
str_or_atom(R) -> iolist_to_binary(io_lib:format("~p", [R])).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec control_frame_actions(
    nhttp_log:ctx(), actions(), boolean(), nhttp_ws:ws_frame(), #ws_state{}, module()
) ->
    {#ws_state{}, actions()}.
control_frame_actions(_Ctx, Reply, false, _Frame, Ws, _Handler) ->
    {Ws, Reply};
control_frame_actions(Ctx, Reply, true, Frame, Ws, Handler) ->
    {Ws1, Actions} = handler_frame_actions(Ctx, Frame, Ws, Handler),
    {Ws1, Reply ++ Actions}.

-spec handler_frame_actions(nhttp_log:ctx(), nhttp_ws:ws_frame(), #ws_state{}, module()) ->
    {#ws_state{}, actions()}.
handler_frame_actions(_Ctx, _Frame, #ws_state{handles_frame = false} = Ws, _Handler) ->
    {Ws, []};
handler_frame_actions(
    Ctx, Frame, #ws_state{handler_state = SState, session = Session} = Ws, Handler
) ->
    Result = nhttp_handler:safe_handle_ws_frame(Handler, Frame, Session, SState),
    log_crash_if_any(Result, Ctx, handle_ws_frame),
    apply_handler_result(Result, Ws).

-spec log_crash_if_any(term(), nhttp_log:ctx(), atom()) -> ok.
log_crash_if_any({nhttp_handler_exception, Class, Reason}, Ctx, Callback) ->
    nhttp_log:handler_crashed(Ctx, Callback, Class, Reason);
log_crash_if_any(_Result, _Ctx, _Callback) ->
    ok.

-spec put_runtime_opts(runtime_opts(), #ws_state{}) -> #ws_state{}.
put_runtime_opts(#{deliver_ping := DP, deliver_pong := DG, max_message_size := MS}, Ws) ->
    Ws#ws_state{deliver_ping = DP, deliver_pong = DG, max_message_size = MS}.
