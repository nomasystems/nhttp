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
    initial_window_size_alias_reaches_codec/1,
    max_frame_size_alias_reaches_codec/1,
    response_delay_holds_concurrent_streams/1,
    response_delay_holds_reply/1,
    response_delay_reply_only/1,
    response_delay_reset_drops_held_reply/1,
    response_delay_uniform_draws_per_response/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

-define(CANCEL, 8).
-define(DELAY_MS, 300).
-define(FRAME_SIZE_ERROR, 6).
-define(MEASURE_SLACK_MS, 50).
-define(RECV_TIMEOUT, 3000).
-define(RESET_AFTER_MS, 100).
-define(SETTINGS_INITIAL_WINDOW_SIZE, 4).
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
        initial_window_size_alias_reaches_codec,
        max_frame_size_alias_reaches_codec,
        response_delay_holds_concurrent_streams,
        response_delay_holds_reply,
        response_delay_reply_only,
        response_delay_reset_drops_held_reply,
        response_delay_uniform_draws_per_response
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

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

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

frames_before_headers(Frames, StreamId) ->
    lists:takewhile(fun(F) -> not is_headers(F, StreamId) end, Frames).

error_scope(Frames, StreamId, Code) ->
    Goaway = [C || {goaway, 0, <<_Last:32, C:32, _/binary>>} <- Frames, C =:= Code],
    Rst = [C || {rst_stream, SId, C} <- Frames, SId =:= StreamId, C =:= Code],
    case {Goaway, Rst} of
        {[_ | _], _} -> connection_error;
        {[], [_ | _]} -> stream_error;
        {[], []} -> {no_error_seen, Frames}
    end.

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

rst_streams(Frames) ->
    [F || {rst_stream, _, _} = F <- Frames].
