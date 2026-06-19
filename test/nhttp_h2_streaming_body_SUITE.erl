%%%-----------------------------------------------------------------------------
%%% @doc HTTP/2 streaming request body integration tests (Phase 6 Wave 2).
%%%
%%% Validates the conn-side wiring of `accept_body` over HTTP/2:
%%%   * Worker is spawned on HEADERS+nofin with `body => streaming`.
%%%   * DATA frames are forwarded to the worker via `{body_chunk, _, _}`.
%%%   * Trailers are delivered as `{fin, Trailers}`.
%%%   * Handler returning a terminal result mid-body emits the response
%%%     and RST_STREAM(NO_ERROR) for the unread tail (RFC 9113 §8.1).
%%%   * `max_body_size` enforcement returns 413 + RST_STREAM.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_h2_streaming_body_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    echo_post/1,
    echo_post_split_data/1,
    echo_post_many_data_frames/1,
    reply_mid_body_rst_no_error/1,
    max_body_size_413/1,
    trailers_streaming/1,
    trailers_buffered/1,
    bad_handler_return/1,
    ws_upgrade_on_h2_rejected/1,
    ws_upgrade_on_h2_session_rejected/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/reply-mid-body">>}, State) ->
    {accept_body, reply_on_data, State};
handle_request(#{path := <<"/trailers-stream">>}, State) ->
    {accept_body, {trailers, []}, State};
handle_request(#{path := <<"/trailers-buffered">>}, State) ->
    timer:sleep(300),
    {accept_body, {trailers, []}, State};
handle_request(#{path := <<"/bad-return">>}, _State) ->
    not_a_handler_return;
handle_request(#{path := <<"/ws-upgrade">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/ws-upgrade-session">>}, State) ->
    {upgrade, websocket, #{}, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_request_body({data, _Chunk}, reply_on_data, State) ->
    {reply, nhttp_resp:ok(<<"early">>), State};
handle_request_body({fin, _}, reply_on_data, State) ->
    {reply, nhttp_resp:ok(<<"empty">>), State};
handle_request_body({abort, Reason}, reply_on_data, State) ->
    {abort, Reason, State};
handle_request_body({data, Chunk}, {trailers, Acc}, State) ->
    {accept_body, {trailers, [Chunk | Acc]}, State};
handle_request_body({fin, Trailers}, {trailers, Acc}, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    TrailerPart = encode_trailers(Trailers),
    {reply, nhttp_resp:ok(<<Body/binary, "|", TrailerPart/binary>>), State};
handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

encode_trailers(Trailers) ->
    iolist_to_binary(
        lists:join(
            <<";">>,
            [<<Name/binary, "=", Value/binary>> || {Name, Value} <- Trailers]
        )
    ).

%%%-----------------------------------------------------------------------------
%%% SUITE
%%%-----------------------------------------------------------------------------

all() ->
    [
        echo_post,
        echo_post_split_data,
        echo_post_many_data_frames,
        reply_mid_body_rst_no_error,
        max_body_size_413,
        trailers_streaming,
        trailers_buffered,
        bad_handler_return,
        ws_upgrade_on_h2_rejected,
        ws_upgrade_on_h2_session_rejected
    ].

init_per_suite(Config) ->
    _ = application:ensure_all_started(ssl),
    ConfDir = find_test_conf_dir(),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_regular(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

echo_post(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        Body = <<"hello-streamed-h2-body">>,
        {ok, Sock} = h2_connect(Port),
        ok = send_post(Sock, 1, <<"/echo">>, Body),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(Body, response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

echo_post_split_data(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        Part1 = <<"first-part-">>,
        Part2 = <<"second-part">>,
        Body = <<Part1/binary, Part2/binary>>,
        {ok, Sock} = h2_connect(Port),
        ok = send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = send_data(Sock, 1, Part1, false),
        timer:sleep(50),
        ok = send_data(Sock, 1, Part2, true),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(Body, response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

echo_post_many_data_frames(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        Parts = [
            <<"chunk-", (integer_to_binary(N))/binary, "|">>
         || N <- lists:seq(1, 128)
        ],
        Body = iolist_to_binary(Parts),
        {ok, Sock} = h2_connect(Port),
        ok = send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = send_data_frames(Sock, 1, Parts),
        Frames = recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(Body, response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

reply_mid_body_rst_no_error(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        Body = <<"the-handler-replies-on-the-first-chunk">>,
        ok = send_headers(Sock, 1, <<"/reply-mid-body">>, byte_size(Body), false),
        Chunk1 = binary:part(Body, 0, 8),
        ok = send_data(Sock, 1, Chunk1, false),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(<<"early">>, response_body(Frames, 1)),
        ?assert(has_rst_no_error(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

max_body_size_413(Config) ->
    {Pid, Port} = start_server(Config, #{max_body_size => 8}),
    try
        {ok, Sock} = h2_connect(Port),
        Body = <<"way-too-many-bytes-for-the-cap">>,
        ok = send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = send_data(Sock, 1, Body, true),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"413">>, response_status(Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

trailers_streaming(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        Body = <<"hello">>,
        ok = send_headers(Sock, 1, <<"/trailers-stream">>, byte_size(Body), false),
        timer:sleep(100),
        ok = send_data(Sock, 1, Body, false),
        timer:sleep(50),
        ok = send_trailers(Sock, 1, [{<<"x-trailer">>, <<"abc">>}]),
        Frames = recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(<<"hello|x-trailer=abc">>, response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

trailers_buffered(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        Body = <<"hello">>,
        ok = send_headers(Sock, 1, <<"/trailers-buffered">>, byte_size(Body), false),
        ok = send_data(Sock, 1, Body, false),
        ok = send_trailers(Sock, 1, [{<<"x-trailer">>, <<"abc">>}]),
        Frames = recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, response_status(Frames)),
        ?assertEqual(<<"hello|x-trailer=abc">>, response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

bad_handler_return(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        ok = send_headers(Sock, 1, <<"/bad-return">>, 0, true),
        Frames = recv_stream(Sock, 1, 3000),
        ?assert(has_rst_stream(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

ws_upgrade_on_h2_rejected(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        ok = send_headers(Sock, 1, <<"/ws-upgrade">>, 0, true),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"500">>, response_status(Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

ws_upgrade_on_h2_session_rejected(Config) ->
    {Pid, Port} = start_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        ok = send_headers(Sock, 1, <<"/ws-upgrade-session">>, 0, true),
        Frames = recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"500">>, response_status(Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

has_rst_stream(Frames, StreamId) ->
    lists:any(
        fun
            ({rst_stream, SId, _}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

%%%-----------------------------------------------------------------------------
%%% SERVER HELPERS
%%%-----------------------------------------------------------------------------

start_server(Config, Extra) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = maps:merge(
        #{
            port => 0,
            handler => ?MODULE,
            tls => #{certfile => CertFile, keyfile => KeyFile},
            versions => [http2],
            timeouts => #{idle => 5000}
        },
        Extra
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

%%%-----------------------------------------------------------------------------
%%% H2 CLIENT HELPERS
%%%-----------------------------------------------------------------------------

h2_connect(Port) ->
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
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    _ = ssl:recv(Sock, 0, 1000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    {ok, Sock}.

send_post(Sock, StreamId, Path, Body) ->
    ok = send_headers(Sock, StreamId, Path, byte_size(Body), false),
    send_data(Sock, StreamId, Body, true).

send_headers(Sock, StreamId, Path, Length, EndStream) ->
    {ok, Enc} = nhttp_hpack:new(),
    Headers = [
        {<<":method">>, <<"POST">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path},
        {<<"content-length">>, integer_to_binary(Length)}
    ],
    {ok, IOList, _Enc1} = nhttp_hpack:encode(Headers, Enc),
    Block = iolist_to_binary(IOList),
    Flags =
        case EndStream of
            true -> 16#05;
            false -> 16#04
        end,
    Frame = <<(byte_size(Block)):24, 1, Flags, 0:1, StreamId:31, Block/binary>>,
    ssl:send(Sock, Frame).

send_data(Sock, StreamId, Data, EndStream) ->
    Flags =
        case EndStream of
            true -> 16#01;
            false -> 16#00
        end,
    Frame = <<(byte_size(Data)):24, 0, Flags, 0:1, StreamId:31, Data/binary>>,
    ssl:send(Sock, Frame).

send_data_frames(Sock, StreamId, [Last]) ->
    send_data(Sock, StreamId, Last, true);
send_data_frames(Sock, StreamId, [Part | Rest]) ->
    ok = send_data(Sock, StreamId, Part, false),
    send_data_frames(Sock, StreamId, Rest).

send_trailers(Sock, StreamId, Trailers) ->
    {ok, Enc} = nhttp_hpack:new(),
    {ok, IOList, _Enc1} = nhttp_hpack:encode(Trailers, Enc),
    Block = iolist_to_binary(IOList),
    Flags = 16#05,
    Frame = <<(byte_size(Block)):24, 1, Flags, 0:1, StreamId:31, Block/binary>>,
    ssl:send(Sock, Frame).

recv_stream(Sock, StreamId, Timeout) ->
    Frames = recv_stream_until_done(Sock, StreamId, Timeout, <<>>, []),
    Tail = recv_stream_tail(Sock, 200, <<>>, []),
    Frames ++ Tail.

recv_stream_until_done(Sock, StreamId, Timeout, Buf, Acc) ->
    case stream_done(Acc, StreamId) of
        true ->
            Acc;
        false ->
            case ssl:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    NewBuf = <<Buf/binary, Data/binary>>,
                    {Frames, Rest} = decode_frames(NewBuf, []),
                    recv_stream_until_done(Sock, StreamId, Timeout, Rest, Acc ++ Frames);
                {error, _} ->
                    Acc
            end
    end.

recv_stream_tail(Sock, Timeout, Buf, Acc) ->
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            {Frames, Rest} = decode_frames(NewBuf, []),
            recv_stream_tail(Sock, Timeout, Rest, Acc ++ Frames);
        {error, _} ->
            Acc
    end.

stream_done(Frames, StreamId) ->
    lists:any(
        fun
            ({data, SId, _, fin}) when SId =:= StreamId -> true;
            ({headers, SId, _, fin}) when SId =:= StreamId -> true;
            ({rst_stream, SId, _}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

response_status(Frames) ->
    case [P || {headers, _, P, _} <- Frames] of
        [Block | _] ->
            {ok, Dec} = nhttp_hpack:new(),
            case nhttp_hpack:decode(Block, Dec) of
                {ok, Headers, _} -> proplists:get_value(<<":status">>, Headers);
                _ -> undefined
            end;
        [] ->
            undefined
    end.

response_body(Frames, StreamId) ->
    iolist_to_binary([P || {data, SId, P, _} <- Frames, SId =:= StreamId]).

has_rst_no_error(Frames, StreamId) ->
    lists:any(
        fun
            ({rst_stream, SId, no_error}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

decode_frames(<<Len:24, Type:8, Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>>, Acc) ->
    Frame = decode_frame(Type, Flags, StreamId, Payload),
    decode_frames(Rest, [Frame | Acc]);
decode_frames(Other, Acc) ->
    {lists:reverse(Acc), Other}.

decode_frame(0, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            0 -> nofin
        end,
    {data, StreamId, Payload, Fin};
decode_frame(1, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            0 -> nofin
        end,
    {headers, StreamId, Payload, Fin};
decode_frame(3, _Flags, StreamId, <<Code:32>>) ->
    {rst_stream, StreamId, error_code(Code)};
decode_frame(Type, _Flags, StreamId, Payload) ->
    {other, StreamId, Type, Payload}.

error_code(0) -> no_error;
error_code(1) -> protocol_error;
error_code(2) -> internal_error;
error_code(3) -> flow_control_error;
error_code(4) -> settings_timeout;
error_code(5) -> stream_closed;
error_code(6) -> frame_size_error;
error_code(7) -> refused_stream;
error_code(8) -> cancel;
error_code(9) -> compression_error;
error_code(10) -> connect_error;
error_code(11) -> enhance_your_calm;
error_code(12) -> inadequate_security;
error_code(13) -> http_1_1_required;
error_code(N) -> N.
