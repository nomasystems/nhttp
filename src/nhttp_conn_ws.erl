-module(nhttp_conn_ws).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
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
    str_or_atom/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    action/0,
    actions/0,
    runtime_opts/0,
    session_view/0
]).

-type runtime_opts() :: #{
    deliver_ping := boolean(),
    deliver_pong := boolean(),
    max_message_size := pos_integer() | infinity
}.

-type session_view() :: #{
    handler_state := term(),
    runtime_opts := runtime_opts(),
    session := nhttp_ws:session()
}.

-type action() ::
    {send_frame, nhttp_ws:ws_message()}
    | {send_close, no_code | nhttp_ws:close_code(), binary()}
    | {update_view, session_view()}
    | {close_session, nhttp_handler:ws_close_reason()}.

-type actions() :: [action()].

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Apply the result of a handler callback (`handle_ws_frame/3` or
`handle_ws_info/3`) to the session view. Returns the actions the
transport must interpret.
""".
-spec apply_handler_result(term(), session_view()) -> actions().
apply_handler_result({ok, NewSState}, View) ->
    [{update_view, View#{handler_state := NewSState}}];
apply_handler_result({reply, Frames, NewSState}, View) when is_list(Frames) ->
    [
        {update_view, View#{handler_state := NewSState}}
        | [{send_frame, F} || F <- Frames]
    ];
apply_handler_result({reply, Frame, NewSState}, View) ->
    [
        {update_view, View#{handler_state := NewSState}},
        {send_frame, Frame}
    ];
apply_handler_result({close, Code, Reason, NewSState}, View) ->
    [
        {update_view, View#{handler_state := NewSState}},
        {send_close, Code, Reason},
        {close_session, {local, Code, Reason}}
    ];
apply_handler_result({stop, Reason, NewSState}, View) ->
    [
        {update_view, View#{handler_state := NewSState}},
        {send_close, ?WS_CLOSE_GOING_AWAY, <<"Server Going Away">>},
        {close_session, {handler_stop, Reason}}
    ];
apply_handler_result({nhttp_handler_exception, Class, Reason}, _View) ->
    [
        {send_close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>},
        {close_session, {handler_crash, {Class, Reason}}}
    ].

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
handler exports it, otherwise returns no actions. The transport completes
the close (terminate conn or drop stream) after interpreting the result.
""".
-spec close_lifecycle(
    nhttp_log:ctx(), nhttp_handler:ws_close_reason(), session_view(), module()
) -> actions().
close_lifecycle(Ctx, Reason, View, Handler) ->
    case erlang:function_exported(Handler, handle_ws_closed, 3) of
        false ->
            [];
        true ->
            #{handler_state := SState, session := Session} = View,
            Result = nhttp_handler:safe_handle_ws_closed(Handler, Reason, Session, SState),
            log_crash_if_any(Result, Ctx, handle_ws_closed),
            case Result of
                {ok, NewSState} ->
                    [{update_view, View#{handler_state := NewSState}}];
                _ ->
                    []
            end
    end.

-doc """
Map a frame decoder error to the CLOSE code and reason RFC 6455 gives it.
A text payload or CLOSE reason that is not valid UTF-8 is 1007 (§8.1), a
message over the session limit is 1009 (§7.4.1), and every other framing
error is a protocol error. The set of reasons the decoder can return is
open, so an unknown one is a protocol error too.

`message_too_large` carries the same reason text as the message-level
check in `dispatch_frame/4`, because the peer sees one limit whichever
side of reassembly trips it.
""".
-spec decode_error_close(term()) -> {nhttp_ws:close_code(), binary()}.
decode_error_close(message_too_large) -> {?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG};
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
-spec dispatch_frame(nhttp_log:ctx(), nhttp_ws:ws_message(), session_view(), module()) ->
    actions().
dispatch_frame(Ctx, ping, View, Handler) ->
    [{send_frame, pong} | maybe_handler_frame(Ctx, deliver_ping, {ping, <<>>}, View, Handler)];
dispatch_frame(Ctx, {ping, Data}, View, Handler) ->
    [
        {send_frame, {pong, Data}}
        | maybe_handler_frame(Ctx, deliver_ping, {ping, Data}, View, Handler)
    ];
dispatch_frame(Ctx, pong, View, Handler) ->
    maybe_handler_frame(Ctx, deliver_pong, {pong, <<>>}, View, Handler);
dispatch_frame(Ctx, {pong, Data}, View, Handler) ->
    maybe_handler_frame(Ctx, deliver_pong, {pong, Data}, View, Handler);
dispatch_frame(_Ctx, close, _View, _Handler) ->
    [{send_close, no_code, <<>>}, {close_session, {peer, ?WS_CLOSE_NO_STATUS, <<>>}}];
dispatch_frame(_Ctx, {close, Code, Reason}, _View, _Handler) ->
    [{send_close, Code, Reason}, {close_session, {peer, Code, Reason}}];
dispatch_frame(Ctx, {Type, Data}, View, Handler) when Type =:= text orelse Type =:= binary ->
    case oversize(Data, maps:get(runtime_opts, View)) of
        true ->
            [
                {send_close, ?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG},
                {close_session, {fail, ?WS_CLOSE_MESSAGE_TOO_BIG, ?MESSAGE_TOO_BIG}}
            ];
        false ->
            handler_frame_actions(Ctx, {Type, Data}, View, Handler)
    end.

-doc """
Dispatch an Erlang message addressed to the session via
`gen_server:cast/2` ({ws_info, ...}) or via untagged broadcast. Calls
`handle_ws_info/3` if the handler exports it.
""".
-spec dispatch_info(nhttp_log:ctx(), term(), session_view(), module()) -> actions().
dispatch_info(Ctx, Msg, View, Handler) ->
    case erlang:function_exported(Handler, handle_ws_info, 3) of
        false ->
            [];
        true ->
            #{handler_state := SState, session := Session} = View,
            Result = nhttp_handler:safe_handle_ws_info(Handler, Msg, Session, SState),
            log_crash_if_any(Result, Ctx, handle_ws_info),
            apply_handler_result(Result, View)
    end.

-doc """
Run the open lifecycle. Fires `handle_ws_open/2` if exported and
translates the result into actions.
""".
-spec open(nhttp_log:ctx(), session_view(), module()) -> actions().
open(Ctx, View, Handler) ->
    case erlang:function_exported(Handler, handle_ws_open, 2) of
        false ->
            [];
        true ->
            #{handler_state := SState, session := Session} = View,
            Result = nhttp_handler:safe_handle_ws_open(Handler, Session, SState),
            log_crash_if_any(Result, Ctx, handle_ws_open),
            case Result of
                {ok, NewSState} ->
                    [{update_view, View#{handler_state := NewSState}}];
                {ok, NewSState, Opts} when is_map(Opts) ->
                    NewOpts = apply_runtime_opts(Opts, maps:get(runtime_opts, View)),
                    [{update_view, View#{handler_state := NewSState, runtime_opts := NewOpts}}];
                {close, Code, Reason} ->
                    [
                        {send_close, Code, Reason},
                        {close_session, {local, Code, Reason}}
                    ];
                {nhttp_handler_exception, Class, Reason} ->
                    [
                        {send_close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>},
                        {close_session, {handler_crash, {Class, Reason}}}
                    ]
            end
    end.

-doc "Check whether a payload exceeds the max message size in the runtime opts.".
-spec oversize(binary(), runtime_opts()) -> boolean().
oversize(_Data, #{max_message_size := infinity}) -> false;
oversize(Data, #{max_message_size := Max}) -> byte_size(Data) > Max.

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
-spec handler_frame_actions(nhttp_log:ctx(), nhttp_ws:ws_frame(), session_view(), module()) ->
    actions().
handler_frame_actions(Ctx, Frame, View, Handler) ->
    case erlang:function_exported(Handler, handle_ws_frame, 3) of
        false ->
            [];
        true ->
            #{handler_state := SState, session := Session} = View,
            Result = nhttp_handler:safe_handle_ws_frame(Handler, Frame, Session, SState),
            log_crash_if_any(Result, Ctx, handle_ws_frame),
            apply_handler_result(Result, View)
    end.

-spec log_crash_if_any(term(), nhttp_log:ctx(), atom()) -> ok.
log_crash_if_any({nhttp_handler_exception, Class, Reason}, Ctx, Callback) ->
    nhttp_log:handler_crashed(Ctx, Callback, Class, Reason);
log_crash_if_any(_Result, _Ctx, _Callback) ->
    ok.

-spec maybe_handler_frame(
    nhttp_log:ctx(),
    deliver_ping | deliver_pong,
    nhttp_ws:ws_frame(),
    session_view(),
    module()
) ->
    actions().
maybe_handler_frame(Ctx, Flag, Frame, View, Handler) ->
    Opts = maps:get(runtime_opts, View),
    case maps:get(Flag, Opts) of
        true -> handler_frame_actions(Ctx, Frame, View, Handler);
        false -> []
    end.
