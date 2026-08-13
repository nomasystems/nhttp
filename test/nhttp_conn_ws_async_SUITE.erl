-module(nhttp_conn_ws_async_SUITE).

-moduledoc """
Phase-3 verification suite for the H1 async WebSocket dispatch in
`nhttp_conn`. Exercises the new `handle_ws_open/2`, `handle_ws_frame/3`,
`handle_ws_info/3`, `handle_ws_closed/3` callbacks plus the
`gen_server:call`/`cast`-based external send/info/close API.

A full cross-transport suite (`nhttp_ws_async_SUITE`, parametrised over
H1/H2/H3) lands in phase 6.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
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
    protocol_error_close/1,
    oversized_message_close/1,
    oversized_fragment_close/1,
    idle_timeout_close/1,
    handler_crash_close/1,
    handler_stop_close/1,
    deliver_ping_observable/1,
    deliver_pong_observable/1,
    send_async_returns_ok/1,
    stale_session_handle/1,
    multi_reply_list/1,
    plain_open_no_opts/1,
    ws_h1_two_frames_one_send/1,
    ws_h1_partial_frame/1,
    ws_h1_stale_close_cast/1,
    ws_h1_unknown_cast/1,
    ws_h1_session_opts_upgrade/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [{group, h1_ws_async}].

groups() ->
    [
        {h1_ws_async, [], [
            open_event,
            frame_echo_text,
            frame_echo_binary,
            peer_close_with_code,
            peer_close_no_code,
            local_close_via_callback,
            external_send,
            external_info_push,
            external_close,
            protocol_error_close,
            oversized_message_close,
            oversized_fragment_close,
            idle_timeout_close,
            handler_crash_close,
            handler_stop_close,
            deliver_ping_observable,
            deliver_pong_observable,
            send_async_returns_ok,
            stale_session_handle,
            multi_reply_list,
            plain_open_no_opts,
            ws_h1_two_frames_one_send,
            ws_h1_partial_frame,
            ws_h1_stale_close_cast,
            ws_h1_unknown_cast,
            ws_h1_session_opts_upgrade
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

open_event(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ?assert(is_pid(nhttp_ws:owner(Session))),
    ?assertEqual(h1, nhttp_ws:transport(Session)),
    ?assertEqual(undefined, nhttp_ws:stream_id(Session)),
    teardown_ws(Sock, Server).

frame_echo_text(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    Payload = <<"hello async ws">>,
    ok = send_text(Sock, Payload),
    ?assertEqual({text, Payload}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

frame_echo_binary(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    Payload = crypto:strong_rand_bytes(64),
    ok = send_binary(Sock, Payload),
    ?assertEqual({binary, Payload}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

peer_close_with_code(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    ok = send_close(Sock, ?WS_CLOSE_NORMAL, <<"bye">>),
    ?assertEqual({close, ?WS_CLOSE_NORMAL, <<"bye">>}, recv_frame(Sock)),
    ?assertMatch(
        {closed, {peer, ?WS_CLOSE_NORMAL, <<"bye">>}}, recv_observer_event(closed)
    ),
    teardown_ws(Sock, Server).

peer_close_no_code(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    ok = send_close_no_code(Sock),
    ?assertEqual(close, recv_frame(Sock)),
    ?assertMatch({closed, {peer, 1005, <<>>}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

local_close_via_callback(_Config) ->
    {Sock, Server, _Session} = setup_ws(close_on_text, #{}),
    ok = send_text(Sock, <<"trigger">>),
    ?assertEqual({close, 4000, <<"closing on text">>}, recv_frame(Sock)),
    ?assertMatch({closed, {local, 4000, <<"closing on text">>}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

external_send(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ok = nhttp_ws:send(Session, {text, <<"pushed">>}),
    ?assertEqual({text, <<"pushed">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

external_info_push(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ok = nhttp_ws:info(Session, {push_text, <<"via info">>}),
    ?assertEqual({text, <<"via info">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

external_close(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ok = nhttp_ws:close(Session, 4001, <<"external">>),
    ?assertEqual({close, 4001, <<"external">>}, recv_frame(Sock)),
    ?assertMatch({closed, {local, 4001, <<"external">>}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

protocol_error_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    Bad = <<16#D1, 16#80, 0, 0, 0, 0>>,
    ok = gen_tcp:send(Sock, Bad),
    {close, 1002, _} = recv_frame(Sock),
    ?assertMatch({closed, {fail, 1002, _}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

oversized_message_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(max_msg_256, #{}),
    ok = send_text(Sock, binary:copy(<<"x">>, 1024)),
    ?assertEqual({close, 1009, <<"Message Too Big">>}, recv_frame(Sock)),
    ?assertMatch({closed, {fail, 1009, <<"Message Too Big">>}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

oversized_fragment_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(max_msg_256, #{}),
    Payload = binary:copy(<<"x">>, 1024),
    MaskKey = <<1, 2, 3, 4>>,
    Masked = crypto:exor(Payload, binary:copy(MaskKey, byte_size(Payload) div 4)),
    ok = gen_tcp:send(Sock, <<0:1, 0:3, 1:4, 1:1, 127:7, 1024:64, MaskKey/binary, Masked/binary>>),
    ?assertEqual({close, 1009, <<"Message Too Big">>}, recv_frame(Sock)),
    ?assertMatch({closed, {fail, 1009, <<"Message Too Big">>}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

idle_timeout_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{timeouts => #{idle => 300}}),
    ?assertEqual({close, 1001, <<"Idle timeout">>}, recv_frame(Sock, 1500)),
    ?assertMatch({closed, timeout}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

handler_crash_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(crash_text, #{}),
    ok = send_text(Sock, <<"boom">>),
    ?assertEqual(
        {close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>}, recv_frame(Sock)
    ),
    ?assertMatch({closed, {handler_crash, _}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

handler_stop_close(_Config) ->
    {Sock, Server, _Session} = setup_ws(stop_on_text, #{}),
    ok = send_text(Sock, <<"halt">>),
    ?assertEqual({close, ?WS_CLOSE_GOING_AWAY, <<"Server Going Away">>}, recv_frame(Sock)),
    ?assertMatch({closed, {handler_stop, stop_requested}}, recv_observer_event(closed)),
    teardown_ws(Sock, Server).

deliver_ping_observable(_Config) ->
    {Sock, Server, _Session} = setup_ws(deliver_ping, #{}),
    ok = send_ping(Sock, <<"hi">>),
    ?assertEqual({pong, <<"hi">>}, recv_frame(Sock)),
    ?assertMatch({frame, {ping, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Sock, Server).

send_async_returns_ok(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ?assertEqual(ok, nhttp_ws:send_async(Session, {text, <<"async">>})),
    ?assertEqual({text, <<"async">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

stale_session_handle(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    Stale = nhttp_ws:new_session(
        nhttp_ws:transport(Session),
        nhttp_ws:owner(Session),
        nhttp_ws:stream_id(Session),
        make_ref()
    ),
    ?assertEqual({error, gone}, nhttp_ws:send(Stale, ping)),
    teardown_ws(Sock, Server).

deliver_pong_observable(_Config) ->
    {Sock, Server, _Session} = setup_ws(deliver_pong, #{}),
    ok = send_pong(Sock, <<"hi">>),
    ?assertMatch({frame, {pong, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Sock, Server).

multi_reply_list(_Config) ->
    {Sock, Server, _Session} = setup_ws(multi_reply, #{}),
    ok = send_text(Sock, <<"x">>),
    {F1, F2} = recv_two_frames(Sock),
    ?assertEqual({text, <<"first:x">>}, F1),
    ?assertEqual({text, <<"second:x">>}, F2),
    teardown_ws(Sock, Server).

plain_open_no_opts(_Config) ->
    {Sock, Server, _Session} = setup_ws(plain_open, #{}),
    ok = send_ping(Sock, <<"p">>),
    ?assertEqual({pong, <<"p">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

ws_h1_two_frames_one_send(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    Frame1 = nhttp_ws:encode_masked({text, <<"one">>}),
    Frame2 = nhttp_ws:encode_masked({text, <<"two">>}),
    ok = gen_tcp:send(Sock, [Frame1, Frame2]),
    {F1, F2} = recv_two_frames(Sock),
    ?assertEqual({text, <<"one">>}, F1),
    ?assertEqual({text, <<"two">>}, F2),
    teardown_ws(Sock, Server).

ws_h1_partial_frame(_Config) ->
    {Sock, Server, _Session} = setup_ws(echo, #{}),
    Frame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"partial frame">>})),
    Split = byte_size(Frame) div 2,
    <<Head:Split/binary, Tail/binary>> = Frame,
    ok = gen_tcp:send(Sock, Head),
    ok = gen_tcp:send(Sock, Tail),
    ?assertEqual({text, <<"partial frame">>}, recv_frame(Sock, 3000)),
    teardown_ws(Sock, Server).

ws_h1_stale_close_cast(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ConnPid = nhttp_ws:owner(Session),
    gen_server:cast(ConnPid, {ws_close, make_ref(), undefined, ?WS_CLOSE_NORMAL, <<>>, #{}}),
    ok = send_text(Sock, <<"still alive">>),
    ?assertEqual({text, <<"still alive">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

ws_h1_unknown_cast(_Config) ->
    {Sock, Server, Session} = setup_ws(echo, #{}),
    ConnPid = nhttp_ws:owner(Session),
    gen_server:cast(ConnPid, some_garbage_atom),
    ok = send_text(Sock, <<"still alive">>),
    ?assertEqual({text, <<"still alive">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

ws_h1_session_opts_upgrade(_Config) ->
    {Sock, Server, Session} = setup_ws(session_opts_upgrade, #{}),
    ?assertEqual(h1, nhttp_ws:transport(Session)),
    ok = gen_tcp:send(Sock, nhttp_ws:encode_masked({ping, <<"p">>})),
    ?assertEqual({pong, <<"p">>}, recv_frame(Sock)),
    teardown_ws(Sock, Server).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

setup_ws(Mode, ExtraOpts) ->
    Observer = self(),
    register_unique(Observer),
    BaseOpts = #{
        port => 0,
        handler => nhttp_h1_ws_async_handler,
        handler_args => #{observer => Observer, mode => Mode},
        versions => [http1_1]
    },
    {ok, Server} = nhttp:start_link(maps:merge(BaseOpts, ExtraOpts)),
    {ok, Port} = nhttp:get_port(Server),
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _RespHeader} = gen_tcp:recv(Sock, 0, 1000),
    Session =
        receive
            {ws, _ConnPid, {open, S}} -> S
        after 1500 ->
            ct:fail(no_open_event)
        end,
    {Sock, Server, Session}.

teardown_ws(Sock, Server) ->
    try
        gen_tcp:close(Sock)
    catch
        _:_ -> ok
    end,
    try
        nhttp:stop(Server)
    catch
        _:_ -> ok
    end,
    flush_observer().

recv_observer_event(Tag) ->
    receive
        {ws, _Conn, {Tag, _} = Event} -> Event;
        {ws, _Conn, Tag = Event} -> Event
    after 2000 ->
        ct:fail({timeout_waiting_for, Tag})
    end.

flush_observer() ->
    receive
        {ws, _, _} -> flush_observer()
    after 0 -> ok
    end.

register_unique(_Pid) ->
    ok.

ws_upgrade_request() ->
    [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ].

send_text(Sock, Data) -> gen_tcp:send(Sock, nhttp_ws:encode_masked({text, Data})).
send_binary(Sock, Data) -> gen_tcp:send(Sock, nhttp_ws:encode_masked({binary, Data})).
send_ping(Sock, Data) -> gen_tcp:send(Sock, nhttp_ws:encode_masked({ping, Data})).
send_pong(Sock, Data) -> gen_tcp:send(Sock, nhttp_ws:encode_masked({pong, Data})).
send_close(Sock, Code, Reason) ->
    gen_tcp:send(Sock, nhttp_ws:encode_masked({close, Code, Reason})).
send_close_no_code(Sock) -> gen_tcp:send(Sock, nhttp_ws:encode_masked(close)).

recv_frame(Sock) -> recv_frame(Sock, 1500).

recv_frame(Sock, Timeout) ->
    Buf = recv_until_decodable(Sock, <<>>, Timeout),
    case nhttp_ws:decode_unmasked(Buf) of
        {ok, Msg, _Rest} -> Msg;
        Other -> ct:fail({bad_frame, Other, Buf})
    end.

recv_until_decodable(Sock, Acc, Timeout) ->
    case nhttp_ws:decode_unmasked(Acc) of
        {ok, _, _} ->
            Acc;
        _ ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Bin} -> recv_until_decodable(Sock, <<Acc/binary, Bin/binary>>, Timeout);
                {error, Reason} -> ct:fail({recv_failed, Reason, Acc})
            end
    end.

recv_two_frames(Sock) ->
    Buf1 = recv_until_decodable(Sock, <<>>, 1500),
    {ok, F1, Rest1} = nhttp_ws:decode_unmasked(Buf1),
    Buf2 = recv_until_decodable(Sock, Rest1, 1500),
    {ok, F2, _Rest2} = nhttp_ws:decode_unmasked(Buf2),
    {F1, F2}.
