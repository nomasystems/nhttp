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
-include("../src/nhttp_conn.hrl").

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
    bad_stream_spec/1,
    ws_upgrade_on_h2_rejected/1,
    ws_upgrade_on_h2_session_rejected/1,
    abort_on_peer_reset_in_body_phase/1,
    abort_on_peer_reset_before_accept_body/1,
    abort_on_body_too_large_before_accept_body/1,
    reply_from_abort_sends_no_frame/1,
    stream_from_abort_gets_closed/1,
    duplex_ping_pong/1,
    duplex_ping_pong_from_body_phase/1,
    duplex_on_complete_request/1,
    duplex_data_before_stream_result/1,
    duplex_fin_trailers/1,
    duplex_no_read_no_credit/1,
    duplex_send_blocks_reads_kept/1,
    duplex_reset_in_recv/1,
    duplex_reset_in_send/1,
    duplex_reset_after_request_end/1,
    duplex_body_too_large/1,
    duplex_response_end_resets_open_request/1,
    duplex_producer_crash_resets_stream/1,
    duplex_on_response_policy_credits_reads/1,
    simplex_request_body_is_credited/1,
    simplex_response_end_resets_open_request/1
]).

-define(PROBE, nhttp_h2_streaming_body_probe).
-define(PROBE_TIMEOUT, 2000).
-define(H2_CANCEL, 8).
-define(H2_NO_ERROR, 0).
-define(H2_INTERNAL_ERROR, 2).
-define(H2_SETTINGS_INITIAL_WINDOW_SIZE, 4).
-define(H2_FRAME_PING, 6).
-define(H2_DEFAULT_WINDOW, 65535).
-define(H2_DEFAULT_FRAME, 16384).
-define(FRAME_TIMEOUT, 2000).
-define(SMALL_WINDOW, 10).

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
handle_request(#{path := <<"/bad-stream-spec">>}, State) ->
    {stream, not_a_stream_spec, State};
handle_request(#{path := <<"/probe">>}, State) ->
    probe(request),
    {accept_body, probe, State};
handle_request(#{path := <<"/probe-blocked">>}, State) ->
    probe(request),
    receive
        release -> ok
    end,
    {accept_body, probe, State};
handle_request(#{path := <<"/probe-reply">>}, State) ->
    probe(request),
    {accept_body, probe_reply, State};
handle_request(#{path := <<"/probe-stream">>}, State) ->
    probe(request),
    {accept_body, probe_stream, State};
handle_request(#{path := <<"/duplex">>}, State) ->
    {stream, duplex_spec(), State};
handle_request(#{path := <<"/duplex-blocked">>}, State) ->
    probe(request),
    receive
        release -> ok
    end,
    {stream, duplex_spec(), State};
handle_request(#{path := <<"/duplex-body">>}, State) ->
    {accept_body, duplex_body, State};
handle_request(#{path := <<"/simplex">>}, State) ->
    {stream, simplex_spec(), State};
handle_request(#{path := <<"/ws-upgrade">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/ws-upgrade-session">>}, State) ->
    {upgrade, websocket, #{}, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_request_body({data, _} = Event, duplex_body, State) ->
    probe(Event),
    {stream, duplex_spec(), State};
handle_request_body({abort, _} = Event, probe_reply, State) ->
    probe(Event),
    {reply, nhttp_resp:ok(<<"late">>), State};
handle_request_body({abort, _} = Event, probe_stream, State) ->
    probe(Event),
    Producer = fun(SendChunk) ->
        probe({send_chunk, SendChunk(<<"late">>)}),
        ok
    end,
    {stream, nhttp_stream:producer(200, [], Producer), State};
handle_request_body({data, _} = Event, Probe, State) when
    Probe =:= probe; Probe =:= probe_reply; Probe =:= probe_stream
->
    probe(Event),
    {accept_body, Probe, State};
handle_request_body({fin, _} = Event, Probe, State) when
    Probe =:= probe; Probe =:= probe_reply; Probe =:= probe_stream
->
    probe(Event),
    {reply, nhttp_resp:ok(<<"fin">>), State};
handle_request_body({abort, Reason} = Event, probe, State) ->
    probe(Event),
    {abort, Reason, State};
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

probe(Event) ->
    ?PROBE ! {probe, self(), Event},
    ok.

duplex_spec() ->
    Producer = fun(SendChunk, RecvBody) ->
        probe(producer),
        run_script(SendChunk, RecvBody)
    end,
    nhttp_stream:producer(200, [], Producer).

simplex_spec() ->
    Producer = fun(SendChunk) ->
        probe(producer),
        run_script(SendChunk, undefined)
    end,
    nhttp_stream:producer(200, [], Producer).

run_script(SendChunk, RecvBody) ->
    receive
        {script, {recv, Timeout}} ->
            probe({recv, RecvBody(Timeout)}),
            run_script(SendChunk, RecvBody);
        {script, {send, Data}} ->
            probe({send, SendChunk(Data)}),
            run_script(SendChunk, RecvBody);
        {script, {return, Value}} ->
            Value;
        {script, crash} ->
            exit(scripted_crash)
    end.

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
        bad_stream_spec,
        ws_upgrade_on_h2_rejected,
        ws_upgrade_on_h2_session_rejected,
        abort_on_peer_reset_in_body_phase,
        abort_on_peer_reset_before_accept_body,
        abort_on_body_too_large_before_accept_body,
        reply_from_abort_sends_no_frame,
        stream_from_abort_gets_closed,
        duplex_ping_pong,
        duplex_ping_pong_from_body_phase,
        duplex_on_complete_request,
        duplex_data_before_stream_result,
        duplex_fin_trailers,
        duplex_no_read_no_credit,
        duplex_send_blocks_reads_kept,
        duplex_reset_in_recv,
        duplex_reset_in_send,
        duplex_reset_after_request_end,
        duplex_body_too_large,
        duplex_response_end_resets_open_request,
        duplex_producer_crash_resets_stream,
        duplex_on_response_policy_credits_reads,
        simplex_request_body_is_credited,
        simplex_response_end_resets_open_request
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

bad_stream_spec(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/bad-stream-spec">>, 0, true),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assert(has_rst_stream(Frames, 1)),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 3, <<"/missing">>, 0, true),
        Frames3 = nhttp_test_helpers:h2_recv_stream(Sock, 3, 3000),
        ?assertEqual(<<"404">>, nhttp_test_helpers:h2_response_status(Frames3)),
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

abort_on_peer_reset_in_body_phase(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Worker = open_body_phase(Sock, 1, <<"/probe">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ?assertEqual({abort, {peer_reset, cancel}}, probe_event(Worker)),
        ok = await_down(MRef, Worker),
        ok = no_probe_event(Worker),
        ok = assert_echo(Sock, 3),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

abort_on_peer_reset_before_accept_body(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/probe-blocked">>, 16, false),
        Worker = probe_worker(),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ok = assert_echo(Sock, 3),
        Worker ! release,
        ?assertEqual({abort, {peer_reset, cancel}}, probe_event(Worker)),
        ok = await_down(MRef, Worker),
        ok = no_probe_event(Worker),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

abort_on_body_too_large_before_accept_body(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{max_body_size => 8}),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"way-too-many-bytes-for-the-cap">>,
        ok = nhttp_test_helpers:h2_send_headers(
            Sock, 1, <<"/probe-blocked">>, byte_size(Body), false
        ),
        Worker = probe_worker(),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Body, false),
        Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, 3000),
        ?assertEqual(<<"413">>, nhttp_test_helpers:h2_response_status(Frames)),
        Worker ! release,
        ?assertEqual({abort, body_too_large}, probe_event(Worker)),
        ok = await_down(MRef, Worker),
        ok = no_probe_event(Worker),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

reply_from_abort_sends_no_frame(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Worker = open_body_phase(Sock, 1, <<"/probe-reply">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ?assertEqual({abort, {peer_reset, cancel}}, probe_event(Worker)),
        ok = await_down(MRef, Worker),
        Frames = echo_frames(Sock, 3),
        ?assertEqual([], response_frames(Frames, 1)),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

stream_from_abort_gets_closed(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Worker = open_body_phase(Sock, 1, <<"/probe-stream">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ?assertEqual({abort, {peer_reset, cancel}}, probe_event(Worker)),
        ?assertEqual({send_chunk, {error, closed}}, probe_event(Worker)),
        ok = await_down(MRef, Worker),
        Frames = echo_frames(Sock, 3),
        ?assertEqual([], response_frames(Frames, 1)),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

duplex_ping_pong(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        ?assertEqual({recv, timeout}, script(Worker, {recv, 0})),
        ok = ping_pong(Sock, 1, Worker, 4),
        ok = end_request_then_response(Sock, 1, Worker),
        ?assertEqual([], [F || {rst_stream, 1, _} = F <- ping_frames(Sock)]),
        ok = await_conn_idle(Server)
    end).

duplex_ping_pong_from_body_phase(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = open_stream(Sock, 1, <<"/duplex-body">>),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"first">>, false),
        Worker = probe_pid({data, <<"first">>}),
        ?assertEqual(producer, probe_event(Worker)),
        {headers, 1, _, nofin} = await_frame(Sock, is_headers(1)),
        ok = ping_pong(Sock, 1, Worker, 4),
        ok = end_request_then_response(Sock, 1, Worker),
        ok = await_conn_idle(Server)
    end).

duplex_on_complete_request(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/duplex">>, 0, true),
        Worker = probe_pid(producer),
        MRef = erlang:monitor(process, Worker),
        ?assertEqual({recv, {fin, []}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ?assertEqual({send, ok}, script(Worker, {send, <<"only">>})),
        ?assertEqual({data, 1, <<"only">>, nofin}, await_frame(Sock, is_data(1))),
        ok = script_async(Worker, {return, ok}),
        ?assertEqual({data, 1, <<>>, fin}, await_frame(Sock, is_data(1))),
        ok = await_down(MRef, Worker),
        ?assertEqual([], [F || {rst_stream, 1, _} = F <- ping_frames(Sock)]),
        ok = await_conn_idle(Server)
    end).

duplex_data_before_stream_result(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = open_stream(Sock, 1, <<"/duplex-blocked">>),
        Worker = probe_worker(),
        MRef = erlang:monitor(process, Worker),
        Chunks = [<<"one">>, <<"two">>, <<"three">>],
        ok = send_open_data(Sock, 1, Chunks),
        Trailers = [{<<"x-trailer">>, <<"abc">>}],
        ok = send_trailers(Sock, 1, Trailers),
        ?assertEqual(0, credit(ping_frames(Sock), 1)),
        Worker ! release,
        ?assertEqual(producer, probe_event(Worker)),
        ?assertEqual(
            [{recv, {data, C}} || C <- Chunks],
            [script(Worker, {recv, ?PROBE_TIMEOUT}) || _ <- Chunks]
        ),
        ?assertEqual({recv, {fin, Trailers}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

duplex_fin_trailers(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"body">>, false),
        ?assertEqual({recv, {data, <<"body">>}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        Trailers = [{<<"x-trailer">>, <<"abc">>}],
        ok = send_trailers(Sock, 1, Trailers),
        ?assertEqual({recv, {fin, Trailers}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ?assertEqual({recv, {fin, Trailers}}, script(Worker, {recv, 0})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

duplex_no_read_no_credit(_Config) ->
    with_probe_server(#{}, fun(_Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        [First | _] = Chunks = window_chunks(),
        ok = send_open_data(Sock, 1, Chunks),
        Quiet = ping_frames(Sock),
        ?assertEqual({0, 0}, {credit(Quiet, 0), credit(Quiet, 1)}),
        ?assertEqual({recv, {data, First}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        Credit = [await_frame(Sock, fun is_window_update/1) || _ <- [connection, stream]],
        ?assertEqual(
            [{window_update, 0, byte_size(First)}, {window_update, 1, byte_size(First)}],
            lists:sort(Credit)
        ),
        After = ping_frames(Sock),
        ?assertEqual({0, 0}, {credit(After, 0), credit(After, 1)}),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker)
    end).

duplex_send_blocks_reads_kept(_Config) ->
    with_probe_server(#{}, fun(_Server, Sock) ->
        ok = nhttp_test_helpers:h2_send_settings(
            Sock, [{?H2_SETTINGS_INITIAL_WINDOW_SIZE, ?SMALL_WINDOW}]
        ),
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = script_async(Worker, {send, binary:copy(<<"r">>, 2 * ?SMALL_WINDOW)}),
        Half = binary:copy(<<"r">>, ?SMALL_WINDOW),
        ?assertEqual({data, 1, Half, nofin}, await_frame(Sock, is_data(1))),
        Chunks = [<<"one">>, <<"two">>, <<"three">>],
        ok = send_open_data(Sock, 1, Chunks),
        _ = ping_frames(Sock),
        ok = no_probe_event(Worker),
        ok = nhttp_test_helpers:h2_send_window_update(Sock, 1, ?SMALL_WINDOW),
        ?assertEqual({send, ok}, probe_event(Worker)),
        ?assertEqual({data, 1, Half, nofin}, await_frame(Sock, is_data(1))),
        ?assertEqual(
            [{recv, {data, C}} || C <- Chunks],
            [script(Worker, {recv, ?PROBE_TIMEOUT}) || _ <- Chunks]
        ),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker)
    end).

duplex_reset_in_recv(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = script_async(Worker, {recv, infinity}),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ?assertEqual({recv, {abort, {peer_reset, cancel}}}, probe_event(Worker)),
        ?assertEqual({recv, {abort, {peer_reset, cancel}}}, script(Worker, {recv, 0})),
        ?assertEqual({send, {error, closed}}, script(Worker, {send, <<"late">>})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = assert_echo(Sock, 3),
        ok = await_conn_idle(Server)
    end).

duplex_reset_in_send(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = nhttp_test_helpers:h2_send_settings(
            Sock, [{?H2_SETTINGS_INITIAL_WINDOW_SIZE, ?SMALL_WINDOW}]
        ),
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = script_async(Worker, {send, binary:copy(<<"r">>, 2 * ?SMALL_WINDOW)}),
        {data, 1, _, nofin} = await_frame(Sock, is_data(1)),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        ?assertEqual({send, {error, closed}}, probe_event(Worker)),
        ?assertEqual({recv, {abort, {peer_reset, cancel}}}, script(Worker, {recv, 0})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

duplex_reset_after_request_end(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"all">>, true),
        ?assertEqual({recv, {data, <<"all">>}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ?assertEqual({recv, {fin, []}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        _ = ping_frames(Sock),
        ?assertEqual({send, {error, closed}}, script(Worker, {send, <<"late">>})),
        ?assertEqual({recv, {fin, []}}, script(Worker, {recv, 0})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = assert_echo(Sock, 3),
        ok = await_conn_idle(Server)
    end).

duplex_body_too_large(_Config) ->
    with_probe_server(#{max_body_size => 8}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"12345">>, false),
        ?assertEqual({recv, {data, <<"12345">>}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"6789012345">>, false),
        ?assertEqual(
            {rst_stream, 1, ?H2_INTERNAL_ERROR}, await_frame(Sock, is_stream_end(1))
        ),
        ?assertEqual({recv, {abort, body_too_large}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        ?assertEqual({send, {error, closed}}, script(Worker, {send, <<"late">>})),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

duplex_response_end_resets_open_request(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"unread">>, false),
        _ = ping_frames(Sock),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"late">>, false),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?H2_CANCEL),
        Frames = ping_frames(Sock),
        ?assertEqual(
            [{data, 1, <<>>, fin}, {rst_stream, 1, ?H2_NO_ERROR}], response_frames(Frames, 1)
        ),
        ?assertEqual(byte_size(<<"unread">>) + byte_size(<<"late">>), credit(Frames, 0)),
        ok = assert_echo(Sock, 3),
        ok = await_conn_idle(Server)
    end).

duplex_producer_crash_resets_stream(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        Worker = open_duplex(Sock, 1, <<"/duplex">>),
        MRef = erlang:monitor(process, Worker),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<"unread">>, false),
        _ = ping_frames(Sock),
        ok = script_async(Worker, crash),
        ok = await_down(MRef, Worker),
        Frames = ping_frames(Sock),
        ?assertEqual([{rst_stream, 1, ?H2_INTERNAL_ERROR}], response_frames(Frames, 1)),
        ?assertEqual(byte_size(<<"unread">>), credit(Frames, 0)),
        ok = await_conn_idle(Server)
    end).

duplex_on_response_policy_credits_reads(_Config) ->
    Opts = #{h2_connection_window_policy => on_response, h2_stream_window_policy => on_response},
    with_probe_server(Opts, fun(Server, Sock) ->
        ok = open_stream(Sock, 1, <<"/duplex-body">>),
        First = <<"first">>,
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, First, false),
        Worker = probe_pid({data, First}),
        MRef = erlang:monitor(process, Worker),
        ?assertEqual(producer, probe_event(Worker)),
        Held = frames_before(Sock, is_headers(1)),
        ?assertEqual({byte_size(First), byte_size(First)}, {credit(Held, 0), credit(Held, 1)}),
        Second = <<"second">>,
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Second, false),
        Quiet = ping_frames(Sock),
        ?assertEqual({0, 0}, {credit(Quiet, 0), credit(Quiet, 1)}),
        ?assertEqual({recv, {data, Second}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
        Read = ping_frames(Sock),
        ?assertEqual({byte_size(Second), byte_size(Second)}, {credit(Read, 0), credit(Read, 1)}),
        ok = script_async(Worker, {return, ok}),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

simplex_request_body_is_credited(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = open_stream(Sock, 1, <<"/simplex">>),
        Worker = probe_pid(producer),
        MRef = erlang:monitor(process, Worker),
        {headers, 1, _, nofin} = await_frame(Sock, is_headers(1)),
        ok = send_open_data(Sock, 1, window_chunks()),
        Frames = ping_frames(Sock),
        ?assertEqual(
            {?H2_DEFAULT_WINDOW, ?H2_DEFAULT_WINDOW}, {credit(Frames, 0), credit(Frames, 1)}
        ),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<>>, true),
        ?assertEqual({send, ok}, script(Worker, {send, <<"out">>})),
        ok = script_async(Worker, {return, ok}),
        ?assertEqual({data, 1, <<>>, fin}, await_frame(Sock, is_stream_end(1))),
        ok = await_down(MRef, Worker),
        ?assertEqual([], [F || {rst_stream, 1, _} = F <- ping_frames(Sock)]),
        ok = await_conn_idle(Server)
    end).

simplex_response_end_resets_open_request(_Config) ->
    with_probe_server(#{}, fun(Server, Sock) ->
        ok = open_stream(Sock, 1, <<"/simplex">>),
        Worker = probe_pid(producer),
        MRef = erlang:monitor(process, Worker),
        ok = script_async(Worker, {return, ok}),
        ?assertEqual({data, 1, <<>>, fin}, await_frame(Sock, is_stream_end(1))),
        ?assertEqual({rst_stream, 1, ?H2_NO_ERROR}, await_frame(Sock, is_rst_stream(1))),
        ok = await_down(MRef, Worker),
        ok = await_conn_idle(Server)
    end).

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

assert_echo(Sock, StreamId) ->
    _Frames = echo_frames(Sock, StreamId),
    ok.

await_down(MRef, Worker) ->
    receive
        {'DOWN', MRef, process, Worker, _Reason} -> ok
    after ?PROBE_TIMEOUT ->
        {error, {worker_alive, erlang:process_info(Worker, [current_stacktrace, messages])}}
    end.

echo_frames(Sock, StreamId) ->
    Body = <<"after-the-reset">>,
    ok = nhttp_test_helpers:h2_send_post(Sock, StreamId, <<"/echo">>, Body),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, StreamId, 3000),
    ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Frames, StreamId)),
    Frames.

response_frames(Frames, StreamId) ->
    [
        F
     || F <- Frames,
        lists:member(element(1, F), [headers, data, rst_stream]),
        element(2, F) =:= StreamId
    ].

no_probe_event(Worker) ->
    receive
        {probe, Worker, Event} -> {error, {extra_event, Event}}
    after 0 ->
        ok
    end.

open_body_phase(Sock, StreamId, Path) ->
    ok = nhttp_test_helpers:h2_send_headers(Sock, StreamId, Path, 16, false),
    Worker = probe_worker(),
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, <<"partial">>, false),
    ?assertEqual({data, <<"partial">>}, probe_event(Worker)),
    Worker.

probe_event(Worker) ->
    receive
        {probe, Worker, Event} -> Event
    after ?PROBE_TIMEOUT ->
        {error, {no_event, erlang:process_info(Worker, [current_stacktrace, messages])}}
    end.

probe_worker() ->
    receive
        {probe, Worker, request} -> Worker
    after ?PROBE_TIMEOUT ->
        error(no_request_event)
    end.

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

%%%-----------------------------------------------------------------------------
%%% DUPLEX HELPERS
%%%-----------------------------------------------------------------------------

with_probe_server(Opts, Fun) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, Opts),
    true = register(?PROBE, self()),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Fun(Pid, Sock),
        ssl:close(Sock)
    after
        unregister(?PROBE),
        nhttp:stop(Pid)
    end.

open_stream(Sock, StreamId, Path) ->
    {ok, Enc} = nhttp_hpack:new(),
    Headers = [
        {<<":method">>, <<"POST">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path}
    ],
    {ok, IOList, _Enc1} = nhttp_hpack:encode(Headers, Enc),
    Block = iolist_to_binary(IOList),
    ssl:send(Sock, <<(byte_size(Block)):24, 1, 16#04, 0:1, StreamId:31, Block/binary>>).

open_duplex(Sock, StreamId, Path) ->
    ok = open_stream(Sock, StreamId, Path),
    Worker = probe_pid(producer),
    {headers, StreamId, _, nofin} = await_frame(Sock, is_headers(StreamId)),
    Worker.

ping_pong(_Sock, _StreamId, _Worker, 0) ->
    ok;
ping_pong(Sock, StreamId, Worker, Round) ->
    Ping = <<"ping-", (integer_to_binary(Round))/binary>>,
    Pong = <<"pong-", (integer_to_binary(Round))/binary>>,
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, Ping, false),
    ?assertEqual({recv, {data, Ping}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
    ?assertEqual({send, ok}, script(Worker, {send, Pong})),
    ?assertEqual({data, StreamId, Pong, nofin}, await_frame(Sock, is_data(StreamId))),
    ping_pong(Sock, StreamId, Worker, Round - 1).

end_request_then_response(Sock, StreamId, Worker) ->
    MRef = erlang:monitor(process, Worker),
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, <<>>, true),
    ?assertEqual({recv, {data, <<>>}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
    ?assertEqual({recv, {fin, []}}, script(Worker, {recv, ?PROBE_TIMEOUT})),
    ?assertEqual({recv, {fin, []}}, script(Worker, {recv, 0})),
    ok = script_async(Worker, {return, {trailers, [{<<"x-status">>, <<"0">>}]}}),
    {headers, StreamId, _, fin} = await_frame(Sock, is_headers(StreamId)),
    await_down(MRef, Worker).

script(Worker, Step) ->
    ok = script_async(Worker, Step),
    probe_event(Worker).

script_async(Worker, Step) ->
    Worker ! {script, Step},
    ok.

probe_pid(Event) ->
    receive
        {probe, Worker, Event} -> Worker
    after ?PROBE_TIMEOUT ->
        error({no_probe_event, Event})
    end.

send_open_data(_Sock, _StreamId, []) ->
    ok;
send_open_data(Sock, StreamId, [Chunk | Rest]) ->
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, Chunk, false),
    send_open_data(Sock, StreamId, Rest).

window_chunks() ->
    Full = binary:copy(<<"w">>, ?H2_DEFAULT_FRAME),
    Last = binary:copy(<<"w">>, ?H2_DEFAULT_WINDOW - 3 * ?H2_DEFAULT_FRAME),
    [Full, Full, Full, Last].

credit(Frames, StreamId) ->
    lists:sum([Inc || {window_update, Id, Inc} <- Frames, Id =:= StreamId]).

recv_frame(Sock) ->
    {ok, <<Len:24, _/binary>> = Head} = ssl:recv(Sock, 9, ?FRAME_TIMEOUT),
    Payload = recv_payload(Sock, Len),
    {[Frame], <<>>} = nhttp_test_helpers:decode_h2_frames(<<Head/binary, Payload/binary>>),
    Frame.

recv_payload(_Sock, 0) ->
    <<>>;
recv_payload(Sock, Len) ->
    {ok, Payload} = ssl:recv(Sock, Len, ?FRAME_TIMEOUT),
    Payload.

await_frame(Sock, Pred) ->
    Frame = recv_frame(Sock),
    case Pred(Frame) of
        true -> Frame;
        false -> await_frame(Sock, Pred)
    end.

%% The server answers a PING ahead of the frames that the events of the same
%% socket read produce. The second PING leaves after the first answer, so the
%% server reads it in a later read and its answer follows those frames.
ping_frames(Sock) ->
    ping_round(Sock) ++ ping_round(Sock).

frames_before(Sock, Pred) ->
    frames_before(Sock, Pred, []).

frames_before(Sock, Pred, Acc) ->
    Frame = recv_frame(Sock),
    case Pred(Frame) of
        true -> lists:reverse(Acc);
        false -> frames_before(Sock, Pred, [Frame | Acc])
    end.

ping_round(Sock) ->
    Opaque = <<(erlang:unique_integer([positive])):64>>,
    ok = nhttp_test_helpers:h2_send_raw(Sock, <<8:24, ?H2_FRAME_PING, 0, 0:32, Opaque/binary>>),
    frames_until_ping(Sock, Opaque, []).

frames_until_ping(Sock, Opaque, Acc) ->
    case recv_frame(Sock) of
        {other, 0, ?H2_FRAME_PING, Opaque} -> lists:reverse(Acc);
        Frame -> frames_until_ping(Sock, Opaque, [Frame | Acc])
    end.

is_data(StreamId) ->
    fun
        ({data, Id, _, _}) -> Id =:= StreamId;
        (_) -> false
    end.

is_headers(StreamId) ->
    fun
        ({headers, Id, _, _}) -> Id =:= StreamId;
        (_) -> false
    end.

is_rst_stream(StreamId) ->
    fun
        ({rst_stream, Id, _}) -> Id =:= StreamId;
        (_) -> false
    end.

is_stream_end(StreamId) ->
    fun(Frame) -> nhttp_test_helpers:h2_stream_done([Frame], StreamId) end.

is_window_update({window_update, _, _}) -> true;
is_window_update(_) -> false.

await_conn_idle(Server) ->
    [Conn] = nhttp_test_helpers:conn_pids(Server),
    nhttp_test_helpers:wait_until(fun() -> conn_idle(Conn) end, ?PROBE_TIMEOUT).

conn_idle(Conn) ->
    #state{protocol_state = #h2_state{h2_streams = Streams, h2_workers = Workers}} =
        sys:get_state(Conn),
    {monitors, Monitors} = erlang:process_info(Conn, monitors),
    {Streams, Workers, Monitors} =:= {#{}, #{}, []}.
