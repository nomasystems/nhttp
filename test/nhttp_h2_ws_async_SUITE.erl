-module(nhttp_h2_ws_async_SUITE).

-moduledoc """
Phase-4 verification suite for the H2 (RFC 8441 Extended CONNECT)
WebSocket dispatch in `nhttp_conn`. Mirrors `nhttp_conn_ws_async_SUITE`
for the H1 path so the new lifecycle, external send/info/close, multi-
session broadcast, RST_STREAM, and GOAWAY paths are covered end-to-end.

Reuses the transport-agnostic `nhttp_h1_ws_async_handler` test handler.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    open_event/1,
    frame_echo_text/1,
    frame_echo_binary/1,
    peer_close_with_code/1,
    peer_close_no_code/1,
    local_close_via_callback/1,
    external_send/1,
    external_info_push/1,
    external_close/1,
    oversized_message_close/1,
    handler_crash_close/1,
    deliver_ping_observable/1,
    deliver_pong_observable/1,
    send_async_returns_ok/1,
    stale_session_handle/1,
    stream_reset_h2/1,
    multi_session_broadcast/1,
    multi_session_addressed_info/1,
    goaway_notifies_sessions/1,
    multi_reply_list/1,
    plain_open_no_opts/1,
    ws_h2_two_frames_one_data/1,
    ws_h2_split_frame/1,
    ws_h2_protocol_error/1,
    session_opts_upgrade_headers/1,
    stale_ws_info_no_session/1,
    stale_ws_close_mismatched_ref/1,
    unknown_cast_ignored/1,
    stream_reset_handler_closed_crash/1,
    stream_reset_no_closed_callback/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [{group, h2_ws_async}].

groups() ->
    [
        {h2_ws_async, [], [
            open_event,
            frame_echo_text,
            frame_echo_binary,
            peer_close_with_code,
            peer_close_no_code,
            local_close_via_callback,
            external_send,
            external_info_push,
            external_close,
            oversized_message_close,
            handler_crash_close,
            deliver_ping_observable,
            deliver_pong_observable,
            send_async_returns_ok,
            stale_session_handle,
            stream_reset_h2,
            multi_session_broadcast,
            multi_session_addressed_info,
            goaway_notifies_sessions,
            multi_reply_list,
            plain_open_no_opts,
            ws_h2_two_frames_one_data,
            ws_h2_split_frame,
            ws_h2_protocol_error,
            session_opts_upgrade_headers,
            stale_ws_info_no_session,
            stale_ws_close_mismatched_ref,
            unknown_cast_ignored,
            stream_reset_handler_closed_crash,
            stream_reset_no_closed_callback
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(h2_ws_async, Config) ->
    TestConfDir = filename:join(filename:dirname(code:which(?MODULE)), "conf"),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found"}
    end.

end_per_group(_Group, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

open_event(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ?assertEqual(h2, nhttp_ws:transport(Session)),
    ?assertEqual(1, nhttp_ws:stream_id(Session)),
    ?assert(is_pid(nhttp_ws:owner(Session))),
    teardown_ws(Ctx).

frame_echo_text(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"hello">>}),
    ?assertEqual({text, <<"hello">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

frame_echo_binary(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Payload = crypto:strong_rand_bytes(64),
    ok = h2_ws_send_frame(Ctx, 1, {binary, Payload}),
    ?assertEqual({binary, Payload}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

peer_close_with_code(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    ok = h2_ws_send_frame(Ctx, 1, {close, ?WS_CLOSE_NORMAL, <<"bye">>}),
    ?assertEqual({close, ?WS_CLOSE_NORMAL, <<"bye">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch(
        {closed, {peer, ?WS_CLOSE_NORMAL, <<"bye">>}}, recv_observer_event(closed)
    ),
    teardown_ws(Ctx).

peer_close_no_code(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    ok = h2_ws_send_frame(Ctx, 1, close),
    ?assertEqual(close, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({closed, {peer, 1005, <<>>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

local_close_via_callback(Config) ->
    Ctx = setup_ws(Config, close_on_text, 1),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"trigger">>}),
    ?assertEqual({close, 4000, <<"closing on text">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({closed, {local, 4000, <<"closing on text">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

external_send(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:send(Session, {text, <<"pushed">>}),
    ?assertEqual({text, <<"pushed">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

external_info_push(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:info(Session, {push_text, <<"via info">>}),
    ?assertEqual({text, <<"via info">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

external_close(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:close(Session, 4001, <<"external">>),
    ?assertEqual({close, 4001, <<"external">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({closed, {local, 4001, <<"external">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

oversized_message_close(Config) ->
    Ctx = setup_ws(Config, max_msg_256, 1),
    ok = h2_ws_send_frame(Ctx, 1, {text, binary:copy(<<"x">>, 1024)}),
    ?assertEqual({close, 1009, <<"Message Too Big">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({closed, {fail, 1009, <<"Message Too Big">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

handler_crash_close(Config) ->
    Ctx = setup_ws(Config, crash_text, 1),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"boom">>}),
    ?assertEqual({close, 1011, <<"Internal Server Error">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({closed, {handler_crash, _}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

deliver_ping_observable(Config) ->
    Ctx = setup_ws(Config, deliver_ping, 1),
    ok = h2_ws_send_frame(Ctx, 1, {ping, <<"hi">>}),
    ?assertEqual({pong, <<"hi">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertMatch({frame, {ping, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Ctx).

send_async_returns_ok(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ?assertEqual(ok, nhttp_ws:send_async(Session, {text, <<"async">>})),
    ?assertEqual({text, <<"async">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

stale_session_handle(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    Stale = nhttp_ws:new_session(
        nhttp_ws:transport(Session),
        nhttp_ws:owner(Session),
        nhttp_ws:stream_id(Session),
        make_ref()
    ),
    ?assertEqual({error, gone}, nhttp_ws:send(Stale, ping)),
    teardown_ws(Ctx).

stream_reset_h2(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    ok = ssl:send(ctx_sock(Ctx), rst_stream_frame(1, 8)),
    ?assertMatch({closed, {h2_reset, _}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

multi_session_broadcast(Config) ->
    Ctx0 = setup_ws(Config, echo, 1),
    Ctx = open_extra_stream(Ctx0, 3),
    Session1 = ctx_session(Ctx),
    ok = nhttp_ws:broadcast(Session1, {observe, broadcast_msg}),
    ?assertMatch({info, {observe, broadcast_msg}}, recv_observer_event(info)),
    ?assertMatch({info, {observe, broadcast_msg}}, recv_observer_event(info)),
    teardown_ws(Ctx).

multi_session_addressed_info(Config) ->
    Ctx0 = setup_ws(Config, echo, 1),
    Ctx = open_extra_stream(Ctx0, 3),
    Session3 = pick_session(ctx_sessions(Ctx), 3),
    ok = nhttp_ws:info(Session3, {observe, only_3}),
    ?assertMatch({info, {observe, only_3}}, recv_observer_event(info)),
    ?assertEqual(timeout, recv_observer_event_or_timeout(200)),
    teardown_ws(Ctx).

goaway_notifies_sessions(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    ok = ssl:send(ctx_sock(Ctx), goaway_frame(0, 0)),
    ?assertMatch({closed, {transport, {goaway, _}}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

deliver_pong_observable(Config) ->
    Ctx = setup_ws(Config, deliver_pong, 1),
    ok = h2_ws_send_frame(Ctx, 1, {pong, <<"hi">>}),
    ?assertMatch({frame, {pong, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Ctx).

multi_reply_list(Config) ->
    Ctx = setup_ws(Config, multi_reply, 1),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"x">>}),
    ?assertEqual({text, <<"first:x">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertEqual({text, <<"second:x">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

plain_open_no_opts(Config) ->
    Ctx = setup_ws(Config, plain_open, 1),
    ok = h2_ws_send_frame(Ctx, 1, {ping, <<"p">>}),
    ?assertEqual({pong, <<"p">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

ws_h2_two_frames_one_data(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    FrameA = ws_masked_bytes({text, <<"a">>}),
    FrameB = ws_masked_bytes({text, <<"b">>}),
    ok = h2_send_data(Ctx, 1, <<FrameA/binary, FrameB/binary>>),
    ?assertEqual({text, <<"a">>}, h2_ws_recv_frame(Ctx, 1)),
    ?assertEqual({text, <<"b">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

ws_h2_split_frame(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Frame = ws_masked_bytes({text, <<"split-me">>}),
    Half = byte_size(Frame) div 2,
    <<Part1:Half/binary, Part2/binary>> = Frame,
    ok = h2_send_data(Ctx, 1, Part1),
    ok = h2_send_data(Ctx, 1, Part2),
    ?assertEqual({text, <<"split-me">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

ws_h2_protocol_error(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    <<First:8, Rest/binary>> = ws_masked_bytes({text, <<"x">>}),
    Malformed = <<(First bor 16#40):8, Rest/binary>>,
    ok = h2_send_data(Ctx, 1, Malformed),
    ?assertEqual(
        {close, 1002, <<"decode_failed">>},
        h2_ws_recv_frame(Ctx, 1)
    ),
    ?assertMatch(
        {closed, {fail, 1002, <<"decode_failed">>}},
        recv_observer_event(closed)
    ),
    teardown_ws(Ctx).

session_opts_upgrade_headers(Config) ->
    Ctx = setup_ws(Config, session_opts_upgrade, 1),
    Session = ctx_session(Ctx),
    ?assertEqual(h2, nhttp_ws:transport(Session)),
    ?assertEqual(1, nhttp_ws:stream_id(Session)),
    ok = h2_ws_send_frame(Ctx, 1, {ping, <<"p">>}),
    ?assertEqual({pong, <<"p">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

stale_ws_info_no_session(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    Stale = nhttp_ws:new_session(
        nhttp_ws:transport(Session),
        nhttp_ws:owner(Session),
        99,
        make_ref()
    ),
    ok = nhttp_ws:info(Stale, {push_text, <<"ignored">>}),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"alive">>}),
    ?assertEqual({text, <<"alive">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

stale_ws_close_mismatched_ref(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    Stale = nhttp_ws:new_session(
        nhttp_ws:transport(Session),
        nhttp_ws:owner(Session),
        nhttp_ws:stream_id(Session),
        make_ref()
    ),
    ok = nhttp_ws:close(Stale, ?WS_CLOSE_NORMAL, <<"stale">>),
    ?assertEqual(timeout, recv_observer_event_or_timeout(200)),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"alive">>}),
    ?assertEqual({text, <<"alive">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

unknown_cast_ignored(Config) ->
    Ctx = setup_ws(Config, echo, 1),
    Session = ctx_session(Ctx),
    ConnPid = nhttp_ws:owner(Session),
    gen_server:cast(ConnPid, {totally_unknown_cast, make_ref()}),
    ok = h2_ws_send_frame(Ctx, 1, {text, <<"alive">>}),
    ?assertEqual({text, <<"alive">>}, h2_ws_recv_frame(Ctx, 1)),
    teardown_ws(Ctx).

stream_reset_handler_closed_crash(Config) ->
    Ctx0 = setup_ws(Config, crash_on_closed, 1),
    ok = ssl:send(ctx_sock(Ctx0), rst_stream_frame(1, 8)),
    Ctx = open_extra_stream(Ctx0, 3),
    Session3 = pick_session(ctx_sessions(Ctx), 3),
    ?assertEqual(3, nhttp_ws:stream_id(Session3)),
    teardown_ws(Ctx).

stream_reset_no_closed_callback(Config) ->
    Ctx0 = setup_ws_handler(Config, nhttp_h2_ws_no_closed_handler, undefined, 1),
    ok = ssl:send(ctx_sock(Ctx0), rst_stream_frame(1, 8)),
    Ctx = open_extra_stream(Ctx0, 3),
    Session3 = pick_session(ctx_sessions(Ctx), 3),
    ?assertEqual(3, nhttp_ws:stream_id(Session3)),
    teardown_ws(Ctx).

%%%-----------------------------------------------------------------------------
%%% H2 CONNECTION / WS UPGRADE / FRAMING HELPERS
%%%-----------------------------------------------------------------------------

-record(ctx, {
    sock :: ssl:sslsocket(),
    server :: pid(),
    sessions = [] :: [{pos_integer(), nhttp_ws:session()}],
    rxbuf = #{} :: #{pos_integer() => binary()}
}).

ctx_sock(#ctx{sock = S}) -> S.
ctx_session(#ctx{sessions = [{_, S} | _]}) -> S.
ctx_sessions(#ctx{sessions = Ss}) -> Ss.

pick_session(Sessions, StreamId) ->
    case lists:keyfind(StreamId, 1, Sessions) of
        {StreamId, Session} -> Session;
        false -> error({no_session, StreamId})
    end.

setup_ws(Config, Mode, StreamId) ->
    Observer = self(),
    flush_observer(),
    {Sock, Server} = h2_connect(Config, Mode, Observer),
    Ctx0 = #ctx{sock = Sock, server = Server},
    open_extra_stream(Ctx0, StreamId).

setup_ws_handler(Config, Handler, Mode, StreamId) ->
    Observer = self(),
    flush_observer(),
    {Sock, Server} = h2_connect_handler(Config, Handler, Mode, Observer),
    Ctx0 = #ctx{sock = Sock, server = Server},
    open_extra_stream(Ctx0, StreamId).

open_extra_stream(#ctx{sock = Sock, sessions = Sessions} = Ctx, StreamId) ->
    ok = ssl:send(Sock, h2_ws_upgrade_frame(StreamId)),
    Session =
        receive
            {ws, _Conn, {open, S}} -> S
        after 2000 ->
            ct:fail({no_open_event, StreamId})
        end,
    Ctx#ctx{sessions = Sessions ++ [{StreamId, Session}]}.

teardown_ws(#ctx{sock = Sock, server = Server}) ->
    catch ssl:close(Sock),
    catch nhttp:stop(Server),
    flush_observer().

h2_connect(Config, Mode, Observer) ->
    h2_connect_handler(Config, nhttp_h1_ws_async_handler, Mode, Observer).

h2_connect_handler(Config, Handler, Mode, Observer) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    HandlerArgs =
        case Mode of
            undefined -> #{observer => Observer};
            _ -> #{observer => Observer, mode => Mode}
        end,
    {ok, Server} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => Handler,
        handler_args => HandlerArgs,
        versions => [http2]
    }),
    {ok, Port} = nhttp:get_port(Server),
    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [
            binary,
            {active, false},
            {verify, verify_none},
            {alpn_advertised_protocols, [<<"h2">>]}
        ],
        5000
    ),
    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, settings_frame()),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, settings_ack_frame()),
    {Sock, Server}.

h2_ws_upgrade_frame(StreamId) ->
    HeaderBlock = <<
        16#42,
        7,
        "CONNECT",
        16#40,
        9,
        ":protocol",
        9,
        "websocket",
        16#44,
        3,
        "/ws",
        16#87,
        16#41,
        9,
        "localhost"
    >>,
    Len = byte_size(HeaderBlock),
    <<Len:24, 1, 4, 0:1, StreamId:31, HeaderBlock/binary>>.

h2_ws_send_frame(#ctx{sock = Sock}, StreamId, Msg) ->
    Payload = iolist_to_binary(nhttp_ws:encode_masked(Msg)),
    Len = byte_size(Payload),
    ssl:send(Sock, <<Len:24, 0, 0, 0:1, StreamId:31, Payload/binary>>).

ws_masked_bytes(Msg) ->
    iolist_to_binary(nhttp_ws:encode_masked(Msg)).

h2_send_data(#ctx{sock = Sock}, StreamId, Payload) ->
    Len = byte_size(Payload),
    ssl:send(Sock, <<Len:24, 0, 0, 0:1, StreamId:31, Payload/binary>>).

h2_ws_recv_frame(Ctx, StreamId) ->
    case h2_try_recv_frame(Ctx, StreamId, 2000) of
        timeout -> ct:fail({recv_frame_timeout, StreamId});
        Frame -> Frame
    end.

h2_try_recv_frame(#ctx{sock = Sock}, StreamId, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    h2_try_recv_frame_loop(Sock, StreamId, Deadline, <<>>).

h2_try_recv_frame_loop(Sock, StreamId, Deadline, Acc) ->
    case h2_parse_one_frame(Acc) of
        {ok, {data, FrameStreamId, Payload}, Rest} ->
            decode_and_push(FrameStreamId, Payload),
            h2_try_recv_frame_loop(Sock, StreamId, Deadline, Rest);
        {ok, {headers, _, _}, Rest} ->
            h2_try_recv_frame_loop(Sock, StreamId, Deadline, Rest);
        {ok, {goaway, _, _}, Rest} ->
            h2_try_recv_frame_loop(Sock, StreamId, Deadline, Rest);
        {ok, {other, _}, Rest} ->
            h2_try_recv_frame_loop(Sock, StreamId, Deadline, Rest);
        more ->
            case pop_inbox(StreamId) of
                {ok, Frame} ->
                    Frame;
                none ->
                    Now = erlang:monotonic_time(millisecond),
                    Remaining = max(Deadline - Now, 0),
                    case Remaining of
                        0 ->
                            timeout;
                        _ ->
                            case ssl:recv(Sock, 0, Remaining) of
                                {ok, Bin} ->
                                    h2_try_recv_frame_loop(
                                        Sock, StreamId, Deadline, <<Acc/binary, Bin/binary>>
                                    );
                                {error, _} ->
                                    timeout
                            end
                    end
            end
    end.

h2_parse_one_frame(
    <<Len:24, Type:8, _Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>>
) ->
    Tagged =
        case Type of
            0 -> {data, StreamId, Payload};
            1 -> {headers, StreamId, Payload};
            7 -> {goaway, StreamId, Payload};
            _ -> {other, Type}
        end,
    {ok, Tagged, Rest};
h2_parse_one_frame(_) ->
    more.

decode_and_push(StreamId, Data) ->
    case nhttp_ws:decode_unmasked(Data) of
        {ok, Frame, <<>>} ->
            push_inbox(StreamId, Frame);
        {ok, Frame, Rest} ->
            push_inbox(StreamId, Frame),
            decode_and_push(StreamId, Rest);
        _ ->
            ok
    end.

push_inbox(StreamId, Frame) ->
    Key = {?MODULE, inbox, StreamId},
    Q =
        case erlang:get(Key) of
            undefined -> [];
            L -> L
        end,
    erlang:put(Key, Q ++ [Frame]).

pop_inbox(StreamId) ->
    Key = {?MODULE, inbox, StreamId},
    case erlang:get(Key) of
        [Frame | Rest] ->
            erlang:put(Key, Rest),
            {ok, Frame};
        _ ->
            none
    end.

rst_stream_frame(StreamId, ErrorCode) ->
    <<4:24, 3, 0, 0:1, StreamId:31, ErrorCode:32>>.

goaway_frame(LastStreamId, ErrorCode) ->
    <<8:24, 7, 0, 0:1, 0:31, 0:1, LastStreamId:31, ErrorCode:32>>.

settings_frame() ->
    <<0, 0, 0, 4, 0, 0, 0, 0, 0>>.

settings_ack_frame() ->
    <<0, 0, 0, 4, 1, 0, 0, 0, 0>>.

%%%-----------------------------------------------------------------------------
%%% OBSERVER MAILBOX HELPERS
%%%-----------------------------------------------------------------------------

recv_observer_event(Tag) ->
    receive
        {ws, _Conn, {Tag, _} = Event} -> Event;
        {ws, _Conn, Tag = Event} -> Event
    after 2000 ->
        ct:fail({timeout_waiting_for, Tag})
    end.

recv_observer_event_or_timeout(Timeout) ->
    receive
        {ws, _Conn, _} = Event -> Event
    after Timeout ->
        timeout
    end.

flush_observer() ->
    receive
        {ws, _, _} -> flush_observer()
    after 0 -> ok
    end.
