%%%-----------------------------------------------------------------------------
%%% HTTP/2 credit-policy affordances: the listener options that make nhttp
%%% behave like a slow or stingy peer, observed on the wire with the raw
%%% HTTP/2 client in `nhttp_test_helpers'.
%%%-----------------------------------------------------------------------------
-module(nhttp_h2_credit_policy_SUITE).

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
    connection_never_credits_nothing/1,
    connection_threshold_credits_accumulated_total/1,
    credit_sum_bounded_by_connection_policy/1,
    credit_sum_bounded_by_stream_policy/1,
    delay_credits_after_the_delay/1,
    initial_window_size_alias_reaches_codec/1,
    max_frame_size_alias_reaches_codec/1,
    response_delay_holds_concurrent_streams/1,
    response_delay_holds_reply/1,
    response_delay_reply_only/1,
    response_delay_reset_drops_held_reply/1,
    response_delay_uniform_draws_per_response/1,
    stream_and_connection_policies_are_independent/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

-define(CANCEL, 8).
-define(CHUNK, 134).
-define(CHUNK_SPACING_MS, 40).
-define(CREDIT_DELAY_MS, 200).
-define(DELAY_CHUNKS, 5).
-define(DELAY_MS, 300).
-define(FRAME_SIZE_ERROR, 6).
-define(MEASURE_SLACK_MS, 50).
-define(NEVER_POSTS, 200).
-define(POLICY_POSTS, 20).
-define(RECV_TIMEOUT, 3000).
-define(RESET_AFTER_MS, 100).
-define(SETTINGS_INITIAL_WINDOW_SIZE, 4).
-define(SETTLE_MS, 300).
-define(SUM_CHUNK, 100).
-define(SUM_CHUNKS, 4).
-define(SUM_DELAY_MS, 100).
-define(SUM_STREAMS, 5).
-define(SUM_THRESHOLD, 200).
-define(THRESHOLD_CYCLES, 2).
-define(THRESHOLD_POSTS, 10).
-define(UNIFORM_DRAWS, 20).
-define(UNIFORM_MAX_MS, 200).
-define(UNIFORM_MIN_MS, 100).
-define(WINDOW, 1000).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/stream">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun send_one_chunk/1), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.

handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    {reply, nhttp_resp:ok(iolist_to_binary(lists:reverse(Acc))), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

send_one_chunk(SendChunk) ->
    SendChunk(<<"chunk">>).

%%%-----------------------------------------------------------------------------
%%% SUITE
%%%-----------------------------------------------------------------------------

all() ->
    [
        connection_never_credits_nothing,
        connection_threshold_credits_accumulated_total,
        credit_sum_bounded_by_connection_policy,
        credit_sum_bounded_by_stream_policy,
        delay_credits_after_the_delay,
        initial_window_size_alias_reaches_codec,
        max_frame_size_alias_reaches_codec,
        response_delay_holds_concurrent_streams,
        response_delay_holds_reply,
        response_delay_reply_only,
        response_delay_reset_drops_held_reply,
        response_delay_uniform_draws_per_response,
        stream_and_connection_policies_are_independent
    ].

init_per_suite(Config) ->
    _ = application:ensure_all_started(ssl),
    {CertFile, _KeyFile} = nhttp_test_helpers:certs(),
    case filelib:is_regular(CertFile) of
        true -> Config;
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

connection_never_credits_nothing(_Config) ->
    Body = binary:copy(<<$n>>, ?CHUNK),
    StreamIds = stream_ids(?NEVER_POSTS),
    All = all_frames(run_posts(#{h2_connection_window_policy => never}, StreamIds, Body)),
    ?assertEqual([], window_updates(All, 0)),
    ?assertEqual([], [Id || Id <- StreamIds, response_body(All, Id) =/= Body]).

connection_threshold_credits_accumulated_total(_Config) ->
    Threshold = ?THRESHOLD_POSTS * ?CHUNK,
    Body = binary:copy(<<$t>>, ?CHUNK),
    StreamIds = stream_ids(?THRESHOLD_CYCLES * ?THRESHOLD_POSTS),
    {PerStream, Tail} = run_posts(
        #{h2_connection_window_policy => {threshold, Threshold}}, StreamIds, Body
    ),
    Credited = [{Id, window_updates(Frames, 0)} || {Id, Frames} <- PerStream],
    ?assertEqual(
        [{stream_id(N * ?THRESHOLD_POSTS), [Threshold]} || N <- lists:seq(1, ?THRESHOLD_CYCLES)],
        [Entry || {_, Increments} = Entry <- Credited, Increments =/= []]
    ),
    ?assertEqual([], window_updates(Tail, 0)).

credit_sum_bounded_by_connection_policy(_Config) ->
    lists:foreach(
        fun(Shape) ->
            assert_credit_sums(#{h2_connection_window_policy => Shape}, Shape, eager)
        end,
        shapes()
    ).

credit_sum_bounded_by_stream_policy(_Config) ->
    lists:foreach(
        fun(Shape) ->
            assert_credit_sums(#{h2_stream_window_policy => Shape}, eager, Shape)
        end,
        shapes()
    ).

delay_credits_after_the_delay(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_connection_window_policy => {delay, ?CREDIT_DELAY_MS},
        h2_stream_window_policy => {delay, ?CREDIT_DELAY_MS}
    }),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Chunk = binary:copy(<<$d>>, ?CHUNK),
        Total = ?DELAY_CHUNKS * ?CHUNK,
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/echo">>, Total, false),
        SentAt = send_spaced_chunks(Sock, 1, Chunk, ?DELAY_CHUNKS),
        {Timed, Rest} = collect_window_updates(Sock, <<>>, [0, 1], ?DELAY_CHUNKS, ?RECV_TIMEOUT),
        ConnAt = [At || {At, {window_update, 0, _}} <- Timed],
        StreamAt = [At || {At, {window_update, 1, _}} <- Timed],
        ?assertEqual([], early_credits(ConnAt, SentAt)),
        ?assertEqual([], early_credits(StreamAt, SentAt)),
        Frames = [F || {_, F} <- Timed],
        ?assertEqual(Total, lists:sum(window_updates(Frames, 0))),
        ?assertEqual(Total, lists:sum(window_updates(Frames, 1))),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, <<>>, true),
        {Reply, _, _} = await_streams(Sock, [1], Rest, ?RECV_TIMEOUT),
        ?assertEqual(binary:copy(Chunk, ?DELAY_CHUNKS), response_body(Reply, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

initial_window_size_alias_reaches_codec(_Config) ->
    {PidAlias, PortAlias} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_initial_window_size => ?WINDOW
    }),
    {PidSetting, PortSetting} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_settings => #{initial_window_size => ?WINDOW}
    }),
    try
        Advertised = nhttp_test_helpers:h2_server_settings(PortAlias),
        ?assertEqual([?WINDOW], [V || {?SETTINGS_INITIAL_WINDOW_SIZE, V} <- Advertised]),
        ?assertEqual(nhttp_test_helpers:h2_server_settings(PortSetting), Advertised),
        {ok, Sock} = nhttp_test_helpers:h2_connect(PortAlias),
        Fits = binary:copy(<<$a>>, ?WINDOW),
        ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, Fits),
        Echoed = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Echoed)),
        ?assertEqual(Fits, nhttp_test_helpers:h2_response_body(Echoed, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(PidAlias),
        nhttp:stop(PidSetting)
    end.

max_frame_size_alias_reaches_codec(_Config) ->
    {PidMin, PortMin} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_max_frame_size => 16384
    }),
    try
        {ok, SockMin} = nhttp_test_helpers:h2_connect(PortMin),
        TooBig = binary:copy(<<$a>>, 16385),
        ok = nhttp_test_helpers:h2_send_post(SockMin, 1, <<"/echo">>, TooBig),
        Refused = nhttp_test_helpers:h2_recv_stream(SockMin, 1, ?RECV_TIMEOUT),
        ?assertEqual(connection_error, error_scope(Refused, 1, ?FRAME_SIZE_ERROR)),
        ssl:close(SockMin)
    after
        nhttp:stop(PidMin)
    end,
    {PidBig, PortBig} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_max_frame_size => 32768
    }),
    try
        {ok, SockBig} = nhttp_test_helpers:h2_connect(PortBig),
        Body = binary:copy(<<$b>>, 20000),
        ok = nhttp_test_helpers:h2_send_post(SockBig, 1, <<"/echo">>, Body),
        Echoed = nhttp_test_helpers:h2_recv_stream(SockBig, 1, ?RECV_TIMEOUT),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Echoed)),
        ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Echoed, 1)),
        ssl:close(SockBig)
    after
        nhttp:stop(PidBig)
    end.

response_delay_holds_concurrent_streams(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{h2_response_delay => ?DELAY_MS}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        T1 = now_ms(),
        ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, <<"one">>),
        T3 = now_ms(),
        ok = nhttp_test_helpers:h2_send_post(Sock, 3, <<"/echo">>, <<"three">>),
        {Frames, #{1 := At1, 3 := At3}, _Rest} = await_streams(Sock, [1, 3], <<>>, ?RECV_TIMEOUT),
        ?assertEqual([], rst_streams(Frames)),
        ?assert(At1 - T1 >= ?DELAY_MS),
        ?assert(At3 - T3 >= ?DELAY_MS),
        ?assert(max(At1, At3) - T1 < 2 * ?DELAY_MS),
        ?assertEqual(<<"one">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ?assertEqual(<<"three">>, nhttp_test_helpers:h2_response_body(Frames, 3)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

response_delay_holds_reply(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{h2_response_delay => ?DELAY_MS}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        Body = <<"held-until-the-delay-elapses">>,
        ok = nhttp_test_helpers:h2_send_headers(Sock, 1, <<"/echo">>, byte_size(Body), false),
        T0 = now_ms(),
        ok = nhttp_test_helpers:h2_send_data(Sock, 1, Body, true),
        {Frames, #{1 := At}, _Rest} = await_streams(Sock, [1], <<>>, ?RECV_TIMEOUT),
        ?assertEqual([], rst_streams(frames_before_headers(Frames, 1))),
        ?assert(At - T0 >= ?DELAY_MS),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
        ?assertEqual(Body, nhttp_test_helpers:h2_response_body(Frames, 1)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

response_delay_reply_only(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_response_delay => ?DELAY_MS, max_body_size => 8
    }),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        TooLarge = binary:copy(<<$x>>, 32),
        TError = now_ms(),
        ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, TooLarge),
        {Error, #{1 := AtError}, Rest1} = await_streams(Sock, [1], <<>>, ?RECV_TIMEOUT),
        ?assertEqual(<<"413">>, nhttp_test_helpers:h2_response_status(Error)),
        ?assert(AtError - TError < ?DELAY_MS),
        TStream = now_ms(),
        ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/stream">>),
        {Stream, #{3 := AtStream}, Rest3} = await_streams(Sock, [3], Rest1, ?RECV_TIMEOUT),
        ?assertEqual(<<"chunk">>, nhttp_test_helpers:h2_response_body(Stream, 3)),
        ?assert(AtStream - TStream < ?DELAY_MS),
        TReply = now_ms(),
        ok = nhttp_test_helpers:h2_send_request(Sock, 5, <<"/hello">>),
        {Reply, #{5 := AtReply}, _Rest5} = await_streams(Sock, [5], Rest3, ?RECV_TIMEOUT),
        ?assertEqual(<<"hello">>, nhttp_test_helpers:h2_response_body(Reply, 5)),
        ?assert(AtReply - TReply >= ?DELAY_MS),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

response_delay_reset_drops_held_reply(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{h2_response_delay => ?DELAY_MS}),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, <<"cancelled">>),
        timer:sleep(?RESET_AFTER_MS),
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, ?CANCEL),
        Frames = nhttp_test_helpers:h2_recv(Sock, 2 * ?DELAY_MS),
        ?assertEqual([], [F || {headers, 1, _, _} = F <- Frames]),
        ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/hello">>),
        Later = nhttp_test_helpers:h2_recv_stream(Sock, 3, ?RECV_TIMEOUT),
        ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Later)),
        ?assertEqual([], [F || {headers, 1, _, _} = F <- Later]),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

response_delay_uniform_draws_per_response(_Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        h2_response_delay => {uniform, ?UNIFORM_MIN_MS, ?UNIFORM_MAX_MS}
    }),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        StreamIds = [2 * N - 1 || N <- lists:seq(1, ?UNIFORM_DRAWS)],
        {Sock, _Rest, Delays} = lists:foldl(fun measure_reply/2, {Sock, <<>>, []}, StreamIds),
        ?assertEqual([], [D || D <- Delays, D < ?UNIFORM_MIN_MS]),
        ?assertEqual([], [D || D <- Delays, D > ?UNIFORM_MAX_MS + ?MEASURE_SLACK_MS]),
        ?assert(length(lists:usort(Delays)) >= 2),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

stream_and_connection_policies_are_independent(_Config) ->
    Body = binary:copy(<<$i>>, ?CHUNK),
    StreamIds = stream_ids(?POLICY_POSTS),
    StreamNever = all_frames(run_posts(#{h2_stream_window_policy => never}, StreamIds, Body)),
    ?assertEqual([], [F || {window_update, SId, _} = F <- StreamNever, SId > 0]),
    ?assertEqual(lists:duplicate(?POLICY_POSTS, ?CHUNK), window_updates(StreamNever, 0)),
    ConnNever = all_frames(
        run_posts(
            #{h2_connection_window_policy => never, h2_stream_window_policy => eager},
            StreamIds,
            Body
        )
    ),
    ?assertEqual([], window_updates(ConnNever, 0)),
    ?assertEqual([], [Id || Id <- StreamIds, window_updates(ConnNever, Id) =/= [?CHUNK]]).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

all_frames({PerStream, Tail}) ->
    lists:append([Frames || {_, Frames} <- PerStream]) ++ Tail.

assert_credit(Shape, Consumed, Sum) ->
    ?assert(Sum =< Consumed),
    ?assertEqual(expected_credit(Shape, Consumed), Sum).

%% Open ?SUM_STREAMS streams, send ?SUM_CHUNKS chunks on each, let the
%% credit settle while the streams are still open, then close them and
%% check the WINDOW_UPDATE sums per window against the shapes.
assert_credit_sums(Opts, ConnShape, StreamShape) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, Opts),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        StreamIds = stream_ids(?SUM_STREAMS),
        PerStream = ?SUM_CHUNKS * ?SUM_CHUNK,
        Chunk = binary:copy(<<$s>>, ?SUM_CHUNK),
        lists:foreach(
            fun(Id) ->
                ok = nhttp_test_helpers:h2_send_headers(Sock, Id, <<"/echo">>, PerStream, false)
            end,
            StreamIds
        ),
        lists:foreach(
            fun(_Round) ->
                lists:foreach(
                    fun(Id) -> ok = nhttp_test_helpers:h2_send_data(Sock, Id, Chunk, false) end,
                    StreamIds
                )
            end,
            lists:seq(1, ?SUM_CHUNKS)
        ),
        {Credits, Rest} = collect(Sock, <<>>, ?SETTLE_MS),
        lists:foreach(
            fun(Id) -> ok = nhttp_test_helpers:h2_send_data(Sock, Id, <<>>, true) end,
            StreamIds
        ),
        {Replies, _, Rest1} = await_streams(Sock, StreamIds, Rest, ?RECV_TIMEOUT),
        {Tail, _} = collect(Sock, Rest1, ?SETTLE_MS),
        All = Credits ++ Replies ++ Tail,
        Echoed = binary:copy(Chunk, ?SUM_CHUNKS),
        ?assertEqual([], [Id || Id <- StreamIds, response_body(All, Id) =/= Echoed]),
        assert_credit(ConnShape, ?SUM_STREAMS * PerStream, lists:sum(window_updates(All, 0))),
        lists:foreach(
            fun(Id) ->
                assert_credit(StreamShape, PerStream, lists:sum(window_updates(All, Id)))
            end,
            StreamIds
        ),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

%% Receive until every stream in StreamIds is done. Returns the frames, a
%% map from stream id to the time its first HEADERS frame was decoded, and
%% the undecoded tail of the socket buffer.
await_streams(Sock, StreamIds, Buf, Timeout) ->
    await_streams(Sock, StreamIds, Buf, [], #{}, Timeout).

await_streams(Sock, StreamIds, Buf, Acc, Seen, Timeout) ->
    case lists:all(fun(Id) -> nhttp_test_helpers:h2_stream_done(Acc, Id) end, StreamIds) of
        true ->
            {Acc, Seen, Buf};
        false ->
            T0 = now_ms(),
            {ok, Data} = ssl:recv(Sock, 0, Timeout),
            {Frames, Rest} = nhttp_test_helpers:decode_h2_frames(<<Buf/binary, Data/binary>>),
            Seen1 = note_headers_seen(Frames, StreamIds, Seen, now_ms()),
            await_streams(Sock, StreamIds, Rest, Acc ++ Frames, Seen1, Timeout - (now_ms() - T0))
    end.

%% Receive whatever arrives within Timeout. Returns the frames and the
%% undecoded tail of the socket buffer.
collect(Sock, Buf, Timeout) ->
    collect(Sock, Buf, Timeout, []).

collect(_Sock, Buf, Timeout, Acc) when Timeout =< 0 ->
    {Acc, Buf};
collect(Sock, Buf, Timeout, Acc) ->
    T0 = now_ms(),
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            {Frames, Rest} = nhttp_test_helpers:decode_h2_frames(<<Buf/binary, Data/binary>>),
            collect(Sock, Rest, Timeout - (now_ms() - T0), Acc ++ Frames);
        {error, timeout} ->
            {Acc, Buf}
    end.

%% Receive until every stream in StreamIds has N WINDOW_UPDATE frames.
%% Returns each frame with the time its socket read completed.
collect_window_updates(Sock, Buf, StreamIds, N, Timeout) ->
    collect_window_updates(Sock, Buf, StreamIds, N, Timeout, []).

collect_window_updates(Sock, Buf, StreamIds, N, Timeout, Acc) ->
    Frames = [F || {_, F} <- Acc],
    case lists:all(fun(Id) -> length(window_updates(Frames, Id)) >= N end, StreamIds) of
        true ->
            {Acc, Buf};
        false ->
            T0 = now_ms(),
            {ok, Data} = ssl:recv(Sock, 0, Timeout),
            {New, Rest} = nhttp_test_helpers:decode_h2_frames(<<Buf/binary, Data/binary>>),
            At = now_ms(),
            Timed = [{At, F} || F <- New],
            collect_window_updates(Sock, Rest, StreamIds, N, Timeout - (At - T0), Acc ++ Timed)
    end.

early_credits(CreditedAt, SentAt) ->
    [At - T || {At, T} <- lists:zip(CreditedAt, SentAt), At - T < ?CREDIT_DELAY_MS].

error_scope(Frames, StreamId, Code) ->
    Goaway = [C || {goaway, 0, <<_Last:32, C:32, _/binary>>} <- Frames, C =:= Code],
    Rst = [C || {rst_stream, SId, C} <- Frames, SId =:= StreamId, C =:= Code],
    case {Goaway, Rst} of
        {[_ | _], _} -> connection_error;
        {[], [_ | _]} -> stream_error;
        {[], []} -> {no_error_seen, Frames}
    end.

expected_credit(never, _Consumed) -> 0;
expected_credit(_Shape, Consumed) -> Consumed.

frames_before_headers(Frames, StreamId) ->
    lists:takewhile(fun(F) -> not is_headers(F, StreamId) end, Frames).

is_headers({headers, SId, _, _}, StreamId) -> SId =:= StreamId;
is_headers(_, _) -> false.

measure_reply(StreamId, {Sock, Buf, Delays}) ->
    T0 = now_ms(),
    ok = nhttp_test_helpers:h2_send_request(Sock, StreamId, <<"/hello">>),
    {_Frames, #{StreamId := At}, Rest} = await_streams(Sock, [StreamId], Buf, ?RECV_TIMEOUT),
    {Sock, Rest, [At - T0 | Delays]}.

note_headers_seen(Frames, StreamIds, Seen, At) ->
    lists:foldl(
        fun(Id, Acc) ->
            case
                maps:is_key(Id, Acc) orelse not lists:any(fun(F) -> is_headers(F, Id) end, Frames)
            of
                true -> Acc;
                false -> Acc#{Id => At}
            end
        end,
        Seen,
        StreamIds
    ).

now_ms() ->
    erlang:monotonic_time(millisecond).

%% POST Body on each stream in turn and await its response. Returns the
%% frames per stream and the undecoded tail of the socket buffer.
post_each(Sock, StreamIds, Body) ->
    post_each(Sock, StreamIds, Body, <<>>, []).

post_each(_Sock, [], _Body, Buf, Acc) ->
    {lists:reverse(Acc), Buf};
post_each(Sock, [Id | Ids], Body, Buf, Acc) ->
    ok = nhttp_test_helpers:h2_send_post(Sock, Id, <<"/echo">>, Body),
    {Frames, _Seen, Rest} = await_streams(Sock, [Id], Buf, ?RECV_TIMEOUT),
    post_each(Sock, Ids, Body, Rest, [{Id, Frames} | Acc]).

response_body(Frames, StreamId) ->
    nhttp_test_helpers:h2_response_body(Frames, StreamId).

rst_streams(Frames) ->
    [F || {rst_stream, _, _} = F <- Frames].

%% Start a server with Opts, POST Body on every stream in turn, then drain
%% the socket for ?SETTLE_MS. Returns the frames per stream and the tail.
run_posts(Opts, StreamIds, Body) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, Opts),
    try
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        {PerStream, Rest} = post_each(Sock, StreamIds, Body),
        {Tail, _} = collect(Sock, Rest, ?SETTLE_MS),
        ssl:close(Sock),
        {PerStream, Tail}
    after
        nhttp:stop(Pid)
    end.

send_spaced_chunks(_Sock, _StreamId, _Chunk, 0) ->
    [];
send_spaced_chunks(Sock, StreamId, Chunk, N) ->
    T = now_ms(),
    ok = nhttp_test_helpers:h2_send_data(Sock, StreamId, Chunk, false),
    timer:sleep(?CHUNK_SPACING_MS),
    [T | send_spaced_chunks(Sock, StreamId, Chunk, N - 1)].

shapes() ->
    [eager, {threshold, ?SUM_THRESHOLD}, {delay, ?SUM_DELAY_MS}, never].

stream_id(N) ->
    2 * N - 1.

stream_ids(Count) ->
    [stream_id(N) || N <- lists:seq(1, Count)].

window_updates(Frames, StreamId) ->
    [Inc || {window_update, SId, Inc} <- Frames, SId =:= StreamId].
