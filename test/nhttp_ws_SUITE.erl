%%%-----------------------------------------------------------------------------
%%% @doc WebSocket test suite.
%%%
%%% Tests WebSocket frame encoding/decoding, handshake validation, and
%%% integration with the nhttp server.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_ws_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    validate_upgrade_valid/1,
    validate_upgrade_missing_upgrade/1,
    validate_upgrade_missing_connection/1,
    validate_upgrade_missing_key/1,
    validate_upgrade_wrong_version/1,
    validate_upgrade_wrong_upgrade_value/1,
    validate_upgrade_connection_not_upgrade/1,
    validate_version_missing/1,
    accept_key_generation/1,
    handshake_response/1,
    encode_text/1,
    encode_binary/1,
    encode_ping/1,
    encode_ping_with_data/1,
    encode_pong/1,
    encode_pong_empty/1,
    encode_close/1,
    encode_close_with_code/1,
    encode_large_payload/1,
    encode_extended_16/1,
    encode_extended_64/1,
    encode_text_helper/1,
    encode_binary_helper/1,
    encode_ping_helper/1,
    encode_pong_helper/1,
    encode_close_helper/1,
    encode_close_with_code_helper/1,
    decode_text/1,
    decode_binary/1,
    decode_ping/1,
    decode_ping_with_data/1,
    decode_pong/1,
    decode_pong_empty/1,
    decode_close/1,
    decode_close_with_code/1,
    decode_large_payload/1,
    decode_extended_64/1,
    decode_incomplete/1,
    decode_unmasked_error/1,
    decode_reserved_bits_error/1,
    decode_fragmentation_error/1,
    decode_unknown_opcode_error/1,
    decode_partial_masked_frame/1,
    decode_partial_extended_16/1,
    decode_partial_extended_64/1,
    decode_invalid_frame/1,
    ws_upgrade_and_echo/1,
    ws_ping_pong/1,
    ws_close_handshake/1,
    ws_binary_message/1,
    ws_sys_get_status/1,
    ws_server_drained_during_session/1
]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------
-export([init/1, handle_request/2, handle_ws_frame/3, handle_ws_closed/3]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, handshake},
        {group, encoding},
        {group, decoding},
        {group, integration}
    ].

groups() ->
    [
        {handshake, [parallel], [
            validate_upgrade_valid,
            validate_upgrade_missing_upgrade,
            validate_upgrade_missing_connection,
            validate_upgrade_missing_key,
            validate_upgrade_wrong_version,
            validate_upgrade_wrong_upgrade_value,
            validate_upgrade_connection_not_upgrade,
            validate_version_missing,
            accept_key_generation,
            handshake_response
        ]},
        {encoding, [parallel], [
            encode_text,
            encode_binary,
            encode_ping,
            encode_ping_with_data,
            encode_pong,
            encode_pong_empty,
            encode_close,
            encode_close_with_code,
            encode_large_payload,
            encode_extended_16,
            encode_extended_64,
            encode_text_helper,
            encode_binary_helper,
            encode_ping_helper,
            encode_pong_helper,
            encode_close_helper,
            encode_close_with_code_helper
        ]},
        {decoding, [parallel], [
            decode_text,
            decode_binary,
            decode_ping,
            decode_ping_with_data,
            decode_pong,
            decode_pong_empty,
            decode_close,
            decode_close_with_code,
            decode_large_payload,
            decode_extended_64,
            decode_incomplete,
            decode_unmasked_error,
            decode_reserved_bits_error,
            decode_fragmentation_error,
            decode_unknown_opcode_error,
            decode_partial_masked_frame,
            decode_partial_extended_16,
            decode_partial_extended_64,
            decode_invalid_frame
        ]},
        {integration, [parallel], [
            ws_upgrade_and_echo,
            ws_ping_pong,
            ws_close_handshake,
            ws_binary_message,
            ws_sys_get_status,
            ws_server_drained_during_session
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(nhttp),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(integration, Config) ->
    process_flag(trap_exit, true),
    Config;
init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER IMPLEMENTATION
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

handle_ws_closed(_Reason, _Session, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HANDSHAKE TESTS
%%%-----------------------------------------------------------------------------

validate_upgrade_valid(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({ok, <<"dGhlIHNhbXBsZSBub25jZQ==">>}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_missing_upgrade(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({error, invalid_upgrade}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_missing_connection(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({error, invalid_connection}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_missing_key(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({error, missing_key}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_wrong_version(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"8">>}
        ]
    },
    ?assertMatch({error, unsupported_version}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_wrong_upgrade_value(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"http/2.0">>},
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({error, invalid_upgrade}, nhttp_ws:validate_upgrade(Request)).

validate_upgrade_connection_not_upgrade(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"connection">>, <<"keep-alive">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>},
            {<<"sec-websocket-version">>, <<"13">>}
        ]
    },
    ?assertMatch({error, invalid_connection}, nhttp_ws:validate_upgrade(Request)).

validate_version_missing(_Config) ->
    Request = #{
        method => get,
        path => <<"/ws">>,
        headers => [
            {<<"upgrade">>, <<"websocket">>},
            {<<"connection">>, <<"Upgrade">>},
            {<<"sec-websocket-key">>, <<"dGhlIHNhbXBsZSBub25jZQ==">>}
        ]
    },
    ?assertMatch({error, unsupported_version}, nhttp_ws:validate_upgrade(Request)).

accept_key_generation(_Config) ->
    Key = <<"dGhlIHNhbXBsZSBub25jZQ==">>,
    Expected = <<"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=">>,
    ?assertEqual(Expected, nhttp_ws:accept_key(Key)).

handshake_response(_Config) ->
    Key = <<"dGhlIHNhbXBsZSBub25jZQ==">>,
    Response = iolist_to_binary(nhttp_ws:handshake_response(Key)),
    ?assertMatch(<<"HTTP/1.1 101 Switching Protocols\r\n", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Upgrade: websocket\r\n">>) =/= nomatch),
    ?assert(binary:match(Response, <<"Connection: Upgrade\r\n">>) =/= nomatch),
    ?assert(
        binary:match(Response, <<"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=">>) =/= nomatch
    ).

%%%-----------------------------------------------------------------------------
%%% ENCODING TESTS
%%%-----------------------------------------------------------------------------

encode_text(_Config) ->
    Frame = nhttp_ws:encode({text, <<"Hello">>}),
    ?assertEqual(<<16#81, 5, "Hello">>, iolist_to_binary(Frame)).

encode_binary(_Config) ->
    Frame = nhttp_ws:encode({binary, <<1, 2, 3, 4>>}),
    ?assertEqual(<<16#82, 4, 1, 2, 3, 4>>, iolist_to_binary(Frame)).

encode_ping(_Config) ->
    Frame = nhttp_ws:encode(ping),
    ?assertEqual(<<16#89, 0>>, iolist_to_binary(Frame)).

encode_ping_with_data(_Config) ->
    Frame = nhttp_ws:encode({ping, <<"test">>}),
    ?assertEqual(<<16#89, 4, "test">>, iolist_to_binary(Frame)).

encode_pong_empty(_Config) ->
    Frame = nhttp_ws:encode(pong),
    ?assertEqual(<<16#8A, 0>>, iolist_to_binary(Frame)).

encode_pong(_Config) ->
    Frame = nhttp_ws:encode({pong, <<"data">>}),
    ?assertEqual(<<16#8A, 4, "data">>, iolist_to_binary(Frame)).

encode_close(_Config) ->
    Frame = nhttp_ws:encode(close),
    ?assertEqual(<<16#88, 0>>, iolist_to_binary(Frame)).

encode_close_with_code(_Config) ->
    Frame = nhttp_ws:encode({close, ?WS_CLOSE_NORMAL, <<"Normal">>}),
    ?assertEqual(<<16#88, 8, ?WS_CLOSE_NORMAL:16, "Normal">>, iolist_to_binary(Frame)).

encode_large_payload(_Config) ->
    Data = binary:copy(<<"X">>, 200),
    Frame = iolist_to_binary(nhttp_ws:encode({text, Data})),
    ?assertMatch(<<16#81, 126, 200:16, _:200/binary>>, Frame).

encode_extended_16(_Config) ->
    Data = binary:copy(<<"X">>, 1000),
    Frame = iolist_to_binary(nhttp_ws:encode({text, Data})),
    ?assertMatch(<<16#81, 126, 1000:16, _:1000/binary>>, Frame).

encode_extended_64(_Config) ->
    Data = binary:copy(<<"X">>, 70000),
    Frame = iolist_to_binary(nhttp_ws:encode({text, Data})),
    ?assertMatch(<<16#81, 127, 70000:64, _:70000/binary>>, Frame).

encode_text_helper(_Config) ->
    Frame = nhttp_ws:encode_text(<<"Test message">>),
    ?assertEqual(<<16#81, 12, "Test message">>, iolist_to_binary(Frame)).

encode_binary_helper(_Config) ->
    Frame = nhttp_ws:encode_binary(<<1, 2, 3>>),
    ?assertEqual(<<16#82, 3, 1, 2, 3>>, iolist_to_binary(Frame)).

encode_ping_helper(_Config) ->
    Frame = nhttp_ws:encode_ping(),
    ?assertEqual(<<16#89, 0>>, iolist_to_binary(Frame)),
    FrameWithData = nhttp_ws:encode_ping(<<"ping">>),
    ?assertEqual(<<16#89, 4, "ping">>, iolist_to_binary(FrameWithData)).

encode_pong_helper(_Config) ->
    Frame = nhttp_ws:encode_pong(<<"data">>),
    ?assertEqual(<<16#8A, 4, "data">>, iolist_to_binary(Frame)).

encode_close_helper(_Config) ->
    Frame = nhttp_ws:encode_close(),
    ?assertEqual(<<16#88, 0>>, iolist_to_binary(Frame)).

encode_close_with_code_helper(_Config) ->
    Frame = nhttp_ws:encode_close(1001, <<"Going away">>),
    ?assertEqual(<<16#88, 12, 1001:16, "Going away">>, iolist_to_binary(Frame)).

%%%-----------------------------------------------------------------------------
%%% DECODING TESTS
%%%-----------------------------------------------------------------------------

decode_text(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Payload = mask(<<"Hello">>, MaskKey),
    Frame = <<16#81, 16#85, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {text, <<"Hello">>}, <<>>}, nhttp_ws:decode(Frame)).

decode_binary(_Config) ->
    MaskKey = <<5, 6, 7, 8>>,
    Payload = mask(<<1, 2, 3, 4>>, MaskKey),
    Frame = <<16#82, 16#84, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {binary, <<1, 2, 3, 4>>}, <<>>}, nhttp_ws:decode(Frame)).

decode_ping(_Config) ->
    MaskKey = <<9, 10, 11, 12>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ?assertMatch({ok, ping, <<>>}, nhttp_ws:decode(Frame)).

decode_ping_with_data(_Config) ->
    MaskKey = <<9, 10, 11, 12>>,
    Payload = mask(<<"data">>, MaskKey),
    Frame = <<16#89, 16#84, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {ping, <<"data">>}, <<>>}, nhttp_ws:decode(Frame)).

decode_pong_empty(_Config) ->
    MaskKey = <<13, 14, 15, 16>>,
    Frame = <<16#8A, 16#80, MaskKey/binary>>,
    ?assertMatch({ok, pong, <<>>}, nhttp_ws:decode(Frame)).

decode_pong(_Config) ->
    MaskKey = <<13, 14, 15, 16>>,
    Payload = mask(<<"pong">>, MaskKey),
    Frame = <<16#8A, 16#84, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {pong, <<"pong">>}, <<>>}, nhttp_ws:decode(Frame)).

decode_close(_Config) ->
    MaskKey = <<17, 18, 19, 20>>,
    Frame = <<16#88, 16#80, MaskKey/binary>>,
    ?assertMatch({ok, close, <<>>}, nhttp_ws:decode(Frame)).

decode_close_with_code(_Config) ->
    MaskKey = <<21, 22, 23, 24>>,
    Payload = mask(<<?WS_CLOSE_NORMAL:16, "bye">>, MaskKey),
    Frame = <<16#88, 16#85, MaskKey/binary, Payload/binary>>,
    ?assertMatch(
        {ok, {close, ?WS_CLOSE_NORMAL, <<"bye">>}, <<>>}, nhttp_ws:decode(Frame)
    ).

decode_large_payload(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Data = binary:copy(<<"X">>, 1000),
    Payload = mask(Data, MaskKey),
    Frame = <<16#81, 16#FE, 1000:16, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {text, Data}, <<>>}, nhttp_ws:decode(Frame)).

decode_extended_64(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Data = binary:copy(<<"Y">>, 70000),
    Payload = mask(Data, MaskKey),
    Frame = <<16#81, 16#FF, 70000:64, MaskKey/binary, Payload/binary>>,
    ?assertMatch({ok, {text, Data}, <<>>}, nhttp_ws:decode(Frame)).

decode_incomplete(_Config) ->
    ?assertMatch({more, _}, nhttp_ws:decode(<<16#81>>)),
    MaskKey = <<1, 2, 3, 4>>,
    ?assertMatch({more, _}, nhttp_ws:decode(<<16#81, 16#85, MaskKey/binary, "He">>)).

decode_unmasked_error(_Config) ->
    Frame = <<16#81, 5, "Hello">>,
    ?assertMatch({error, unmasked_client_frame}, nhttp_ws:decode(Frame)).

decode_reserved_bits_error(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Payload = mask(<<"Hi">>, MaskKey),
    Frame = <<16#C1, 16#82, MaskKey/binary, Payload/binary>>,
    ?assertMatch({error, reserved_bits_set}, nhttp_ws:decode(Frame)).

decode_fragmentation_error(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Payload = mask(<<"Hi">>, MaskKey),
    Frame = <<16#01, 16#82, MaskKey/binary, Payload/binary>>,
    ?assertMatch({error, fragmentation_not_supported}, nhttp_ws:decode(Frame)).

decode_unknown_opcode_error(_Config) ->
    MaskKey = <<1, 2, 3, 4>>,
    Payload = mask(<<"Hi">>, MaskKey),
    Frame = <<16#8F, 16#82, MaskKey/binary, Payload/binary>>,
    ?assertMatch({error, {unknown_opcode, _}}, nhttp_ws:decode(Frame)).

decode_partial_masked_frame(_Config) ->
    PartialFrame = <<16#81, 16#85>>,
    ?assertMatch({more, _}, nhttp_ws:decode(PartialFrame)),
    PartialExt16 = <<16#81, 16#FE>>,
    ?assertMatch({more, _}, nhttp_ws:decode(PartialExt16)),
    PartialExt64 = <<16#81, 16#FF>>,
    ?assertMatch({more, _}, nhttp_ws:decode(PartialExt64)).

decode_partial_extended_16(_Config) ->
    Frame = <<16#81, 16#FE, 200:16, 1, 2, 3>>,
    ?assertMatch({more, _}, nhttp_ws:decode(Frame)).

decode_partial_extended_64(_Config) ->
    Frame = <<16#81, 16#FF, 0:1, 200:63, 1, 2>>,
    ?assertMatch({more, _}, nhttp_ws:decode(Frame)).

decode_invalid_frame(_Config) ->
    InvalidFrame = <<16#80, 16#85, 1, 2, 3, 4, "Hello">>,
    Result = nhttp_ws:decode(InvalidFrame),
    ?assertMatch({error, _}, Result).

%%%-----------------------------------------------------------------------------
%%% INTEGRATION TESTS
%%%-----------------------------------------------------------------------------

ws_upgrade_and_echo(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Socket, Request),

    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 101 ", _/binary>>, Response),

    MaskKey = crypto:strong_rand_bytes(4),
    Payload = mask(<<"Hello WebSocket">>, MaskKey),
    Frame = <<16#81, (16#80 bor 15), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Socket, Frame),

    {ok, EchoFrame} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<16#81, 15, "Hello WebSocket">>, EchoFrame),

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

ws_ping_pong(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request = ws_upgrade_request(),
    ok = gen_tcp:send(Socket, Request),
    {ok, _Response} = gen_tcp:recv(Socket, 0, 5000),

    MaskKey = crypto:strong_rand_bytes(4),
    PingData = <<"ping-test">>,
    Payload = mask(PingData, MaskKey),
    Frame = <<16#89, (16#80 bor 9), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Socket, Frame),

    {ok, PongFrame} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<16#8A, 9, "ping-test">>, PongFrame),

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

ws_close_handshake(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request = ws_upgrade_request(),
    ok = gen_tcp:send(Socket, Request),
    {ok, _Response} = gen_tcp:recv(Socket, 0, 5000),

    MaskKey = crypto:strong_rand_bytes(4),
    ClosePayload = <<?WS_CLOSE_NORMAL:16, "bye">>,
    Payload = mask(ClosePayload, MaskKey),
    Frame = <<16#88, (16#80 bor 5), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Socket, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<16#88, _Len, ?WS_CLOSE_NORMAL:16, _/binary>>, CloseFrame),

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

ws_binary_message(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request = ws_upgrade_request(),
    ok = gen_tcp:send(Socket, Request),
    {ok, _Response} = gen_tcp:recv(Socket, 0, 5000),

    MaskKey = crypto:strong_rand_bytes(4),
    BinaryData = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    Payload = mask(BinaryData, MaskKey),
    Frame = <<16#82, (16#80 bor 8), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Socket, Frame),

    {ok, EchoFrame} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<16#82, 8, 1, 2, 3, 4, 5, 6, 7, 8>>, EchoFrame),

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

ws_sys_get_status(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, ws_upgrade_request()),
    {ok, _Response} = gen_tcp:recv(Socket, 0, 5000),

    ConnPid = find_ws_conn_pid(Pid),
    {status, ConnPid, _Mod, _Items} = sys:get_status(ConnPid, 5000),

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

ws_server_drained_during_session(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, ws_upgrade_request()),
    {ok, _Response} = gen_tcp:recv(Socket, 0, 5000),

    Self = self(),
    spawn(fun() -> Self ! {drain_done, nhttp:drain(Pid)} end),

    {ok, CloseFrame} = gen_tcp:recv(Socket, 0, 5000),
    ?assertMatch(<<16#88, _Len, 16#03, 16#E9, _Reason/binary>>, CloseFrame),

    receive
        {drain_done, ok} -> ok
    after 5000 ->
        ct:fail(drain_timeout)
    end,

    gen_tcp:close(Socket),
    nhttp:stop(Pid).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

-spec mask(binary(), binary()) -> binary().
mask(Data, <<K1, K2, K3, K4>>) ->
    mask_loop(Data, <<K1, K2, K3, K4>>, 0, <<>>).

mask_loop(<<>>, _Key, _Idx, Acc) ->
    Acc;
mask_loop(<<B, Rest/binary>>, <<K1, K2, K3, K4>> = Key, Idx, Acc) ->
    KeyByte =
        case Idx rem 4 of
            0 -> K1;
            1 -> K2;
            2 -> K3;
            3 -> K4
        end,
    mask_loop(Rest, Key, Idx + 1, <<Acc/binary, (B bxor KeyByte)>>).

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

find_ws_conn_pid(ListenerPid) ->
    nhttp_test_helpers:wait_until(
        fun() -> find_ws_conn_pid_now(ListenerPid) =/= undefined end, 2000
    ),
    find_ws_conn_pid_now(ListenerPid).

find_ws_conn_pid_now(ListenerPid) ->
    case nhttp_test_helpers:conn_pids(ListenerPid) of
        [Pid | _] -> Pid;
        [] -> undefined
    end.
