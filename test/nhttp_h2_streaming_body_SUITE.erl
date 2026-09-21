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

echo_post(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        Body = <<"hello-streamed-h2-body">>,
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, Body),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

echo_post_split_data(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        Part1 = <<"first-part-">>,
        Part2 = <<"second-part">>,
        Body = <<Part1/binary, Part2/binary>>,
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Part1, false),
        timer:sleep(50),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Part2, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

echo_post_many_data_frames(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        Parts = [
            <<"chunk-", (integer_to_binary(N))/binary, "|">>
         || N <- lists:seq(1, 128)
        ],
        Body = iolist_to_binary(Parts),
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = send_data_frames(Sock, 1, Parts),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

reply_mid_body_rst_no_error(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"the-handler-replies-on-the-first-chunk">>,
        ok = nhttp_test_helpers:h2_send_headers(
            Sock, 1, <<"/reply-mid-body">>, byte_size(Body), false
        ),
        Chunk1 = binary:part(Body, 0, 8),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Chunk1, false),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(<<"early">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ?assert(has_rst_no_error(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

max_body_size_413(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{max_body_size => 8}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"way-too-many-bytes-for-the-cap">>,
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Body, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"413">>, nhttp_test_helpers:h2_response_status(Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

trailers_streaming(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"hello">>,
        ok = nhttp_test_helpers:h2_send_headers(
            Sock, 1, <<"/trailers-stream">>, byte_size(Body), false
        ),
        timer:sleep(100),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Body, false),
        timer:sleep(50),
        ok = send_trailers(Sock, 1, [{<<"x-trailer">>, <<"abc">>}]),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(<<"hello|x-trailer=abc">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

trailers_buffered(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"hello">>,
        ok = nhttp_test_helpers:h2_send_headers(
            Sock, 1, <<"/trailers-buffered">>, byte_size(Body), false
        ),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Body, false),
        ok = send_trailers(Sock, 1, [{<<"x-trailer">>, <<"abc">>}]),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 5000),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(<<"hello|x-trailer=abc">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

bad_handler_return(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/bad-return">>, 0, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assert(has_rst_stream(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

ws_upgrade_on_h2_rejected(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/ws-upgrade">>, 0, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"500">>, nhttp_test_helpers:h2_response_status(Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

ws_upgrade_on_h2_session_rejected(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/ws-upgrade-session">>, 0, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"500">>, nhttp_test_helpers:h2_response_status(Frames)),
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

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

%%%-----------------------------------------------------------------------------
%%% H2 CLIENT HELPERS
%%%-----------------------------------------------------------------------------

send_data_frames(Sock, StreamId, [Last]) ->
    nhttp_test_helpers:h2_send_data(Sock, StreamId, Last, true);
send_data_frames(Sock, StreamId, [Part | Rest]) ->
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, Part, false),
    send_data_frames(Sock, StreamId, Rest).

send_trailers(Sock, StreamId, Trailers) ->
    {ok, Enc} = nhttp_hpack:new(),
    {ok, IOList, _Enc1} = nhttp_hpack:encode(Trailers, Enc),
    Block = iolist_to_binary(IOList),
    Flags = 16#05,
    Frame = <<(byte_size(Block)):24, 1, Flags, 0:1, StreamId:31, Block/binary>>,
    ssl:send(Sock, Frame).

has_rst_no_error(Frames, StreamId) ->
    lists:any(
        fun
            ({rst_stream, SId, 0}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).
