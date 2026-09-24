-module(nhttp_h2_send_queue_SUITE).

-moduledoc """
Response bodies on HTTP/2 go through the codec send queue. The cases
pin what a client sees on the wire when its windows are spent: one
connection grant reaches every blocked stream, a body that the queue
bound refuses ends its stream with RST_STREAM(ENHANCE_YOUR_CALM), and the
NO_ERROR RST_STREAM that asks the client to stop an unread request body
follows the last DATA frame of the response. The suite doubles as its own
`nhttp_handler`.
""".

-behaviour(nhttp_handler).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    h2_connection_grant_reaches_every_blocked_stream/1,
    h2_producer_chunk_acked_when_drained/1,
    h2_producer_exit_ends_stream/1,
    h2_send_buffer_full_resets_producer/1,
    h2_send_buffer_full_resets_reply/1,
    h2_unread_body_end_stream_skips_reset/1,
    h2_unread_body_reset_follows_queued_reply/1
]).

-export([
    init/1,
    handle_request/2,
    terminate/2
]).

-define(BIG_BODY, 1048576).
-define(BIG_CHUNK, 100000).
-define(CHUNK, 32768).
-define(DEFAULT_WINDOW, 65535).
-define(ENHANCE_YOUR_CALM, 11).
-define(GRANT, 3 * ?MAX_FRAME_SIZE).
-define(MAX_FRAME_SIZE, 16384).
-define(NO_ERROR, 0).
-define(PRODUCER_MS, 2000).
-define(RECV_MS, 500).
-define(SETTINGS_INITIAL_WINDOW_SIZE, 4).
-define(SMALL_BODY, 1024).
-define(SMALL_SEND_BUFFER, 1024).
-define(STREAM_MS, 5000).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        h2_connection_grant_reaches_every_blocked_stream,
        h2_producer_chunk_acked_when_drained,
        h2_producer_exit_ends_stream,
        h2_send_buffer_full_resets_producer,
        h2_send_buffer_full_resets_reply,
        h2_unread_body_end_stream_skips_reset,
        h2_unread_body_reset_follows_queued_reply
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    {CertFile, _KeyFile} = nhttp_test_helpers:certs(),
    case filelib:is_file(CertFile) of
        true -> Config;
        false -> {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

h2_connection_grant_reaches_every_blocked_stream(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/big">>),
    ?assertEqual(
        ?DEFAULT_WINDOW,
        nhttp_test_helpers:stream_data_size(nhttp_test_helpers:h2_recv(Sock, ?RECV_MS), 1)
    ),
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 1, ?BIG_BODY),
    ?assertEqual(
        0, nhttp_test_helpers:stream_data_size(nhttp_test_helpers:h2_recv(Sock, ?RECV_MS), 1)
    ),
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/small">>),
    ok = nhttp_test_helpers:h2_send_request(Sock, 5, <<"/small">>),
    Blocked = nhttp_test_helpers:h2_recv(Sock, ?RECV_MS),
    ?assertEqual(<<"200">>, status_of(Blocked, 3)),
    ?assertEqual(<<"200">>, status_of(Blocked, 5)),
    ?assertEqual(0, nhttp_test_helpers:stream_data_size(Blocked, 3)),
    ?assertEqual(0, nhttp_test_helpers:stream_data_size(Blocked, 5)),
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 0, ?GRANT),
    Granted = nhttp_test_helpers:h2_recv(Sock, ?RECV_MS),
    ?assertEqual(?SMALL_BODY, nhttp_test_helpers:stream_data_size(Granted, 3)),
    ?assert(nhttp_test_helpers:h2_stream_done(Granted, 3)),
    ?assertEqual(?SMALL_BODY, nhttp_test_helpers:stream_data_size(Granted, 5)),
    ?assert(nhttp_test_helpers:h2_stream_done(Granted, 5)),
    ?assertEqual(?GRANT - 2 * ?SMALL_BODY, nhttp_test_helpers:stream_data_size(Granted, 1)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_producer_chunk_acked_when_drained(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/two-chunks">>),
    Head = nhttp_test_helpers:h2_recv(Sock, ?RECV_MS),
    ?assertEqual(?DEFAULT_WINDOW, nhttp_test_helpers:stream_data_size(Head, 1)),
    ?assertNot(nhttp_test_helpers:h2_stream_done(Head, 1)),
    Tail = credit_and_finish(Sock),
    ?assertEqual(
        ?BIG_CHUNK + ?SMALL_BODY - ?DEFAULT_WINDOW, nhttp_test_helpers:stream_data_size(Tail, 1)
    ),
    ?assertMatch({data, 1, _, fin}, lists:nth(last_data_index(Tail, 1), Tail)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_producer_exit_ends_stream(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/exit-normal">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?STREAM_MS),
    ?assertEqual(?SMALL_BODY, nhttp_test_helpers:stream_data_size(Frames, 1)),
    ?assertMatch({data, 1, _, fin}, lists:nth(last_data_index(Frames, 1), Frames)),
    ?assertNot(lists:keymember(rst_stream, 1, Frames)),
    assert_small_answered(Sock, 3),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_send_buffer_full_resets_reply(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, small_send_buffer()),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/big">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?STREAM_MS),
    ?assertEqual(<<"200">>, status_of(Frames, 1)),
    ?assertEqual(0, nhttp_test_helpers:stream_data_size(Frames, 1)),
    ?assert(frame_index(Frames, headers, 1) < frame_index(Frames, rst_stream, 1)),
    ?assert(lists:member({rst_stream, 1, ?ENHANCE_YOUR_CALM}, Frames)),
    assert_small_answered(Sock, 3),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_send_buffer_full_resets_producer(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(
        ?MODULE, (small_send_buffer())#{handler_args => #{observer => self()}}
    ),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_settings(Sock, [{?SETTINGS_INITIAL_WINDOW_SIZE, 0}]),
    ?assert(lists:member({settings, 0, <<>>}, nhttp_test_helpers:h2_recv(Sock, ?RECV_MS))),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/chunks">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?STREAM_MS),
    ?assertEqual(<<"200">>, status_of(Frames, 1)),
    ?assertEqual(0, nhttp_test_helpers:stream_data_size(Frames, 1)),
    ?assert(lists:member({rst_stream, 1, ?ENHANCE_YOUR_CALM}, Frames)),
    ?assertEqual({error, closed}, producer_result()),
    ok = nhttp_test_helpers:h2_send_settings(Sock, [
        {?SETTINGS_INITIAL_WINDOW_SIZE, ?DEFAULT_WINDOW}
    ]),
    assert_small_answered(Sock, 3),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_unread_body_reset_follows_queued_reply(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    Head = open_big_with_unread_body(Sock),
    ?assertNot(lists:keymember(rst_stream, 1, Head)),
    Tail = credit_and_finish(Sock),
    ?assertEqual(?BIG_BODY - ?DEFAULT_WINDOW, nhttp_test_helpers:stream_data_size(Tail, 1)),
    ?assert(last_data_index(Tail, 1) < frame_index(Tail, rst_stream, 1)),
    ?assert(lists:member({rst_stream, 1, ?NO_ERROR}, Tail)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_unread_body_end_stream_skips_reset(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    Head = open_big_with_unread_body(Sock),
    ?assertNot(lists:keymember(rst_stream, 1, Head)),
    ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<>>, true),
    Tail = credit_and_finish(Sock),
    ?assertEqual(?BIG_BODY - ?DEFAULT_WINDOW, nhttp_test_helpers:stream_data_size(Tail, 1)),
    ?assertMatch({data, 1, _, fin}, lists:nth(last_data_index(Tail, 1), Tail)),
    ?assertNot(lists:keymember(rst_stream, 1, Tail)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/big">>}, State) ->
    {reply, nhttp_resp:ok(binary:copy(<<"x">>, ?BIG_BODY)), State};
handle_request(#{path := <<"/small">>}, State) ->
    {reply, nhttp_resp:ok(binary:copy(<<"y">>, ?SMALL_BODY)), State};
handle_request(#{path := <<"/two-chunks">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun two_chunks/1), State};
handle_request(#{path := <<"/exit-normal">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun exit_after_chunk/1), State};
handle_request(#{path := <<"/chunks">>}, #{observer := Observer} = State) ->
    Producer = fun(SendChunk) -> report_first_chunk(Observer, SendChunk) end,
    {stream, nhttp_stream:producer(200, [], Producer), State}.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

assert_small_answered(Sock, StreamId) ->
    ok = nhttp_test_helpers:h2_send_request(Sock, StreamId, <<"/small">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, StreamId, ?STREAM_MS),
    ?assertEqual(<<"200">>, status_of(Frames, StreamId)),
    ?assertEqual(?SMALL_BODY, nhttp_test_helpers:stream_data_size(Frames, StreamId)),
    ok.

credit_and_finish(Sock) ->
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 0, ?BIG_BODY),
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 1, ?BIG_BODY),
    nhttp_test_helpers:h2_recv_stream(Sock, 1, ?STREAM_MS).

exit_after_chunk(SendChunk) ->
    ok = SendChunk(binary:copy(<<"e">>, ?SMALL_BODY)),
    exit(normal).

frame_index(Frames, Type, StreamId) ->
    Indexed = lists:zip(lists:seq(1, length(Frames)), Frames),
    [Index | _] = [I || {I, F} <- Indexed, element(1, F) =:= Type, element(2, F) =:= StreamId],
    Index.

last_data_index(Frames, StreamId) ->
    Indexed = lists:zip(lists:seq(1, length(Frames)), Frames),
    lists:last([I || {I, {data, Sid, _, _}} <- Indexed, Sid =:= StreamId]).

open_big_with_unread_body(Sock) ->
    ok = nhttp_test_helpers:h2_open_stream(Sock, 1, <<"/big">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, ?RECV_MS),
    ?assertEqual(<<"200">>, status_of(Frames, 1)),
    ?assertEqual(?DEFAULT_WINDOW, nhttp_test_helpers:stream_data_size(Frames, 1)),
    Frames.

producer_result() ->
    receive
        {producer_result, Result} -> Result
    after ?PRODUCER_MS ->
        ct:fail(producer_result_timeout)
    end.

report_first_chunk(Observer, SendChunk) ->
    Observer ! {producer_result, SendChunk(binary:copy(<<0>>, ?CHUNK))},
    ok.

small_send_buffer() ->
    #{h2_settings => #{max_send_buffer => ?SMALL_SEND_BUFFER}}.

status_of(Frames, StreamId) ->
    nhttp_test_helpers:h2_response_status([F || F <- Frames, element(2, F) =:= StreamId]).

two_chunks(SendChunk) ->
    ok = SendChunk(binary:copy(<<"a">>, ?BIG_CHUNK)),
    ok = SendChunk(binary:copy(<<"b">>, ?SMALL_BODY)),
    ok.
