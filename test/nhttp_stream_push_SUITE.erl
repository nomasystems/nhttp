-module(nhttp_stream_push_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    h1_basic/1,
    h1_single_chunk/1,
    h1_empty_producer/1,
    h1_head_request/1,
    h1_no_content_rejected/1,
    h1_http1_0_rejected/1,
    h1_client_closes_midstream/1,
    h1_client_closes_during_sleep/1,
    h1_producer_crash/1,
    h1_pipelined_after_push/1,
    h1_pipelined_during_push/1,
    h1_push_trailers/1,
    h1_stuck_producer_reaped/1,
    h1_ssl_basic/1,
    h1_ssl_client_closes_midstream/1
]).

-export([
    h2_basic/1,
    h2_backpressure/1,
    h2_rst_stream_midstream/1,
    h2_producer_crash/1,
    h2_head_request/1,
    h2_concurrent_streams/1,
    h2_no_content_rejected/1,
    h2_not_modified_rejected/1,
    h2_stuck_producer_reaped/1,
    h2_stuck_handler_reaped/1
]).

-export([
    h3_basic/1,
    h3_single_chunk/1,
    h3_empty_producer/1,
    h3_backpressure/1,
    h3_reset_midstream/1,
    h3_producer_crash/1,
    h3_head_request/1,
    h3_no_content_rejected/1,
    h3_not_modified_rejected/1,
    h3_concurrent_streams/1,
    h3_with_trailers/1,
    h3_client_resets_stream/1,
    h3_stuck_producer_reaped/1
]).

-define(HANDLER, nhttp_stream_push_handler).
-define(TRY_AFTER_TABLE, nhttp_stream_push_try_after).

all() ->
    [
        {group, http1},
        {group, http1_ssl},
        {group, http2},
        {group, http3}
    ].

groups() ->
    [
        {http1, [], [
            h1_basic,
            h1_single_chunk,
            h1_empty_producer,
            h1_head_request,
            h1_no_content_rejected,
            h1_http1_0_rejected,
            h1_client_closes_midstream,
            h1_client_closes_during_sleep,
            h1_producer_crash,
            h1_pipelined_after_push,
            h1_pipelined_during_push,
            h1_push_trailers,
            h1_stuck_producer_reaped
        ]},
        {http1_ssl, [], [
            h1_ssl_basic,
            h1_ssl_client_closes_midstream
        ]},
        {http2, [], [
            h2_basic,
            h2_backpressure,
            h2_rst_stream_midstream,
            h2_producer_crash,
            h2_head_request,
            h2_concurrent_streams,
            h2_no_content_rejected,
            h2_not_modified_rejected,
            h2_stuck_producer_reaped,
            h2_stuck_handler_reaped
        ]},
        {http3, [sequence], [
            h3_basic,
            h3_single_chunk,
            h3_empty_producer,
            h3_backpressure,
            h3_reset_midstream,
            h3_producer_crash,
            h3_head_request,
            h3_no_content_rejected,
            h3_not_modified_rejected,
            h3_concurrent_streams,
            h3_with_trailers,
            h3_client_resets_stream,
            h3_stuck_producer_reaped
        ]}
    ].

init_per_suite(Config) ->
    _ = application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) when Group =:= http2; Group =:= http1_ssl; Group =:= http3 ->
    ConfDir = find_test_conf_dir(),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_regular(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found"}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    case ets:info(?TRY_AFTER_TABLE, name) of
        undefined -> ets:new(?TRY_AFTER_TABLE, [named_table, public, set]);
        _ -> ets:delete_all_objects(?TRY_AFTER_TABLE)
    end,
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 CASES
%%%-----------------------------------------------------------------------------

h1_basic(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/basic HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assert(binary:match(Resp, <<"transfer-encoding: chunked">>) =/= nomatch),
        Body = extract_h1_body(Resp),
        ?assertEqual(<<"alphabravocharlie">>, Body),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_single_chunk(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/single HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assertEqual(<<"just-one">>, extract_h1_body(Resp)),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_empty_producer(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/empty HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assertEqual(<<>>, extract_h1_body(Resp)),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_head_request(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"HEAD /push/head HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assertEqual(nomatch, binary:match(Resp, <<"should-not-reach-wire-for-HEAD">>)),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_no_content_rejected(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/no-content HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 500", _/binary>>, Resp),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_http1_0_rejected(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/basic HTTP/1.0\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 500", _/binary>>, Resp),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_client_closes_midstream(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        register(test_parent_marker, self()),
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(
            Sock, <<"GET /push/closed-probe HTTP/1.1\r\nHost: x\r\n\r\n">>
        ),
        {ok, _SomeData} = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock),
        wait_for_ets_row(observed_error, 5000),
        wait_for_ets_row(cleanup_ran, 5000),
        [{_, ObservedReason, _}] = ets:lookup(?TRY_AFTER_TABLE, observed_error),
        ?assert(ObservedReason =:= closed orelse ObservedReason =:= timeout),
        ?assertMatch([{cleanup_ran, true, _}], ets:lookup(?TRY_AFTER_TABLE, cleanup_ran))
    after
        try
            unregister(test_parent_marker)
        catch
            _:_ -> ok
        end,
        nhttp:stop(Pid)
    end.

h1_client_closes_during_sleep(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/slow HTTP/1.1\r\nHost: x\r\n\r\n">>),
        {ok, _First} = gen_tcp:recv(Sock, 0, 2000),
        gen_tcp:close(Sock),
        wait_for_ets_row(slow_result, 5000),
        wait_for_ets_row(slow_cleanup, 5000),
        [{_, Reason, _}] = ets:lookup(?TRY_AFTER_TABLE, slow_result),
        ?assert(Reason =:= closed orelse Reason =:= timeout)
    after
        nhttp:stop(Pid)
    end.

h1_producer_crash(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/crash HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assert(binary:match(Resp, <<"before-crash">>) =/= nomatch),
        gen_tcp:close(Sock),
        ?assert(erlang:is_process_alive(Pid))
    after
        nhttp:stop(Pid)
    end.

h1_pipelined_during_push(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/slow HTTP/1.1\r\nHost: x\r\n\r\n">>),
        {ok, _} = gen_tcp:recv(Sock, 0, 2000),
        ok = gen_tcp:send(Sock, <<"GET /reply/ok HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_until(Sock, <<>>, <<"reply-ok">>, 10000),
        ?assert(binary:match(Resp, <<"reply-ok">>) =/= nomatch),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_push_trailers(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/trailers HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assertEqual(<<"alphatrailing">>, extract_h1_body(Resp)),
        ?assert(binary:match(Resp, <<"\r\n0\r\n">>) =/= nomatch),
        ?assertEqual(nomatch, binary:match(Resp, <<"grpc-status">>)),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_stuck_producer_reaped(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"GET /push/stuck HTTP/1.1\r\nHost: x\r\n\r\n">>),
        WorkerPid = stuck_worker_pid(),
        ?assert(erlang:is_process_alive(WorkerPid)),
        gen_tcp:close(Sock),
        ok = await_worker_death(WorkerPid, 8000),
        ?assert(erlang:is_process_alive(Pid))
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 OVER SSL CASES
%%%-----------------------------------------------------------------------------

h1_ssl_basic(Config) ->
    {Pid, Port} = start_h1_ssl_server(Config),
    try
        {ok, Sock} = ssl_connect_h1(Port),
        ok = ssl:send(Sock, <<"GET /push/basic HTTP/1.1\r\nHost: x\r\n\r\n">>),
        Resp = ssl_recv_all(Sock, <<>>, 5000),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),
        ?assertEqual(<<"alphabravocharlie">>, extract_h1_body(Resp)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h1_ssl_client_closes_midstream(Config) ->
    {Pid, Port} = start_h1_ssl_server(Config),
    try
        {ok, Sock} = ssl_connect_h1(Port),
        ok = ssl:send(Sock, <<"GET /push/slow HTTP/1.1\r\nHost: x\r\n\r\n">>),
        {ok, _} = ssl:recv(Sock, 0, 5000),
        ssl:close(Sock),
        wait_for_ets_row(slow_result, 5000),
        wait_for_ets_row(slow_cleanup, 5000),
        [{_, Reason, _}] = ets:lookup(?TRY_AFTER_TABLE, slow_result),
        ?assert(Reason =:= closed orelse Reason =:= timeout)
    after
        nhttp:stop(Pid)
    end.

h1_pipelined_after_push(_Config) ->
    {Pid, Port} = start_h1_server(#{}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:send(
            Sock,
            <<
                "GET /push/single HTTP/1.1\r\nHost: x\r\n\r\n"
                "GET /reply/ok HTTP/1.1\r\nHost: x\r\n\r\n"
            >>
        ),
        Resp = recv_all(Sock, <<>>, 5000),
        ?assert(binary:match(Resp, <<"just-one">>) =/= nomatch),
        ?assert(binary:match(Resp, <<"reply-ok">>) =/= nomatch),
        gen_tcp:close(Sock)
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 CASES
%%%-----------------------------------------------------------------------------

h2_basic(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/basic">>),
        Frames = h2_recv_frames_until_end(Sock, 1, 5000),
        Body = h2_collect_body(Frames),
        ?assertEqual(<<"alphabravocharlie">>, Body),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_backpressure(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/large">>),
        Frames = h2_recv_frames_until_end(Sock, 1, 10000),
        Body = h2_collect_body(Frames),
        ?assertEqual(8 * 1024 * 16, byte_size(Body)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_rst_stream_midstream(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        register(test_parent_marker, self()),
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/closed-probe">>),
        _ = ssl:recv(Sock, 0, 2000),
        RstFrame = <<0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8>>,
        ok = ssl:send(Sock, RstFrame),
        wait_for_ets_row(observed_error, 5000),
        [{_, Reason, _}] = ets:lookup(?TRY_AFTER_TABLE, observed_error),
        ?assert(Reason =:= closed orelse Reason =:= timeout),
        ssl:close(Sock)
    after
        try
            unregister(test_parent_marker)
        catch
            _:_ -> ok
        end,
        nhttp:stop(Pid)
    end.

h2_producer_crash(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/crash">>),
        Frames = h2_recv_all(Sock, 2000),
        ?assert(lists:any(fun(F) -> element(1, F) =:= rst_stream end, Frames)),
        ?assert(erlang:is_process_alive(Pid)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_head_request(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_head_request(Sock, 1, <<"/push/head">>),
        Frames = h2_recv_frames_until_end(Sock, 1, 3000),
        ?assertNot(lists:any(fun(F) -> element(1, F) =:= data end, Frames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_concurrent_streams(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/basic">>),
        h2_send_request(Sock, 3, <<"/push/single">>),
        Frames = h2_recv_all(Sock, 3000),
        Stream1 = [F || F <- Frames, element(2, F) =:= 1],
        Stream3 = [F || F <- Frames, element(2, F) =:= 3],
        ?assertEqual(<<"alphabravocharlie">>, h2_collect_body(Stream1)),
        ?assertEqual(<<"just-one">>, h2_collect_body(Stream3)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_no_content_rejected(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/no-content">>),
        Frames = h2_recv_all(Sock, 2000),
        Status = h2_response_status(Frames),
        ?assertEqual(<<"500">>, Status),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_not_modified_rejected(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/not-modified">>),
        Frames = h2_recv_all(Sock, 2000),
        Status = h2_response_status(Frames),
        ?assertEqual(<<"500">>, Status),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

h2_stuck_producer_reaped(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/push/stuck">>),
        WorkerPid = stuck_worker_pid(),
        ?assert(erlang:is_process_alive(WorkerPid)),
        ssl:close(Sock),
        ok = await_worker_death(WorkerPid, 8000),
        ?assert(erlang:is_process_alive(Pid))
    after
        nhttp:stop(Pid)
    end.

h2_stuck_handler_reaped(Config) ->
    {Pid, Port} = start_h2_server(Config, #{}),
    try
        {ok, Sock} = h2_connect(Port),
        h2_send_request(Sock, 1, <<"/block/forever">>),
        WorkerPid = stuck_worker_pid(),
        ?assert(erlang:is_process_alive(WorkerPid)),
        ssl:close(Sock),
        ok = await_worker_death(WorkerPid, 8000),
        ?assert(erlang:is_process_alive(Pid))
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% INTERNAL
%%%-----------------------------------------------------------------------------

start_h1_server(ExtraOpts) ->
    Opts = maps:merge(
        #{port => 0, handler => ?HANDLER, versions => [http1_1]},
        ExtraOpts
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

start_h2_server(Config, ExtraOpts) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = maps:merge(
        #{
            port => 0,
            tls => #{certfile => CertFile, keyfile => KeyFile},
            handler => ?HANDLER,
            versions => [http2]
        },
        ExtraOpts
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

start_h1_ssl_server(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = #{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?HANDLER,
        versions => [http1_1]
    },
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

ssl_connect_h1(Port) ->
    ssl:connect(
        "127.0.0.1",
        Port,
        [
            binary,
            {active, false},
            {verify, verify_none},
            {alpn_advertised_protocols, [<<"http/1.1">>]}
        ],
        5000
    ).

ssl_recv_all(Sock, Acc, Timeout) ->
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} -> ssl_recv_all(Sock, <<Acc/binary, Data/binary>>, 200);
        {error, _} -> Acc
    end.

recv_all(Sock, Acc, Timeout) ->
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} -> recv_all(Sock, <<Acc/binary, Data/binary>>, 200);
        {error, _} -> Acc
    end.

recv_until(Sock, Acc, Needle, Timeout) ->
    case binary:match(Acc, Needle) of
        nomatch ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    recv_until(Sock, <<Acc/binary, Data/binary>>, Needle, Timeout);
                {error, _} ->
                    Acc
            end;
        _ ->
            Acc
    end.

extract_h1_body(Response) ->
    case binary:split(Response, <<"\r\n\r\n">>) of
        [_Head, Body] -> decode_chunked(Body, <<>>);
        _ -> <<>>
    end.

decode_chunked(Data, Acc) ->
    case binary:split(Data, <<"\r\n">>) of
        [SizeHex, Rest] ->
            case binary_to_integer(SizeHex, 16) of
                0 ->
                    Acc;
                Size ->
                    <<Chunk:Size/binary, "\r\n", Rest2/binary>> = Rest,
                    decode_chunked(Rest2, <<Acc/binary, Chunk/binary>>)
            end;
        _ ->
            Acc
    end.

wait_for_ets_row(Key, Timeout) ->
    nhttp_test_helpers:wait_until(
        fun() -> ets:lookup(?TRY_AFTER_TABLE, Key) =/= [] end, Timeout
    ).

stuck_worker_pid() ->
    ok = wait_for_ets_row(stuck_worker, 5000),
    [{stuck_worker, WorkerPid, _}] = ets:lookup(?TRY_AFTER_TABLE, stuck_worker),
    WorkerPid.

await_worker_death(WorkerPid, Timeout) ->
    MRef = erlang:monitor(process, WorkerPid),
    receive
        {'DOWN', MRef, process, WorkerPid, _Reason} -> ok
    after Timeout ->
        true = erlang:demonitor(MRef, [flush]),
        ct:fail({worker_not_reaped, WorkerPid})
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 CLIENT HELPERS
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
    _ = ssl:recv(Sock, 0, 2000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    {ok, Sock}.

h2_send_request(Sock, StreamId, Path) ->
    PathLen = byte_size(Path),
    Header = <<16#82, 16#87, 16#44, PathLen, Path/binary>>,
    Frame = <<(byte_size(Header)):24, 1, 5, 0:1, StreamId:31, Header/binary>>,
    ssl:send(Sock, Frame).

h2_send_head_request(Sock, StreamId, Path) ->
    PathLen = byte_size(Path),
    MethodLit = <<16#02, 4, "HEAD">>,
    SchemeHttps = <<16#87>>,
    PathLit = <<16#44, PathLen, Path/binary>>,
    Header = <<MethodLit/binary, SchemeHttps/binary, PathLit/binary>>,
    Frame = <<(byte_size(Header)):24, 1, 5, 0:1, StreamId:31, Header/binary>>,
    ssl:send(Sock, Frame).

h2_recv_frames_until_end(Sock, StreamId, Timeout) ->
    h2_recv_frames_until_end(Sock, StreamId, Timeout, <<>>, []).

h2_recv_frames_until_end(Sock, StreamId, Timeout, Buf, Acc) ->
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            {Frames, Rest} = decode_frames(NewBuf, []),
            _ = maybe_window_update(Sock, StreamId, Frames),
            All = Acc ++ Frames,
            case is_stream_ended(All, StreamId) of
                true -> All;
                false -> h2_recv_frames_until_end(Sock, StreamId, Timeout, Rest, All)
            end;
        {error, _} ->
            Acc
    end.

h2_recv_all(Sock, Timeout) ->
    h2_recv_all(Sock, Timeout, <<>>, []).

h2_recv_all(Sock, Timeout, Buf, Acc) ->
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            {Frames, Rest} = decode_frames(NewBuf, []),
            _ = maybe_window_update(Sock, all, Frames),
            h2_recv_all(Sock, 500, Rest, Acc ++ Frames);
        {error, _} ->
            Acc
    end.

maybe_window_update(Sock, Target, Frames) ->
    lists:foreach(
        fun
            ({data, SId, Payload, _}) when Target =:= SId orelse Target =:= all ->
                send_window_update(Sock, 0, byte_size(Payload)),
                send_window_update(Sock, SId, byte_size(Payload));
            (_) ->
                ok
        end,
        Frames
    ).

send_window_update(_Sock, _SId, 0) ->
    ok;
send_window_update(Sock, SId, Inc) ->
    Frame = <<0, 0, 4, 8, 0, 0:1, SId:31, 0:1, Inc:31>>,
    ssl:send(Sock, Frame).

is_stream_ended(Frames, StreamId) ->
    lists:any(
        fun
            ({data, SId, _, fin}) when SId =:= StreamId -> true;
            ({headers, SId, _, fin}) when SId =:= StreamId -> true;
            ({rst_stream, SId, _}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

decode_frames(
    <<Len:24, Type:8, Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>> = _Bin, Acc
) ->
    Frame = decode_frame(Type, Flags, StreamId, Payload),
    decode_frames(Rest, [Frame | Acc]);
decode_frames(Other, Acc) ->
    {lists:reverse(Acc), Other}.

decode_frame(0, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            _ -> nofin
        end,
    {data, StreamId, Payload, Fin};
decode_frame(1, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            _ -> nofin
        end,
    {headers, StreamId, Payload, Fin};
decode_frame(3, _Flags, StreamId, <<Code:32>>) ->
    {rst_stream, StreamId, Code};
decode_frame(4, _Flags, StreamId, Payload) ->
    {settings, StreamId, Payload};
decode_frame(7, _Flags, StreamId, Payload) ->
    {goaway, StreamId, Payload};
decode_frame(8, _Flags, StreamId, <<Inc:32>>) ->
    {window_update, StreamId, Inc};
decode_frame(Type, _Flags, StreamId, Payload) ->
    {other, StreamId, Type, Payload}.

h2_collect_body(Frames) ->
    iolist_to_binary([P || {data, _, P, _} <- Frames]).

h2_response_status(Frames) ->
    HeaderBlocks = [P || {headers, _, P, _} <- Frames],
    case HeaderBlocks of
        [Block | _] ->
            {ok, Dec0} = nhttp_hpack:new(),
            case nhttp_hpack:decode(Block, Dec0) of
                {ok, Headers, _Dec1} ->
                    proplists:get_value(<<":status">>, Headers);
                _ ->
                    undefined
            end;
        [] ->
            undefined
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/3 CASES
%%%-----------------------------------------------------------------------------

h3_basic(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 200, _Hs, Body, _} = h3_do_request(QConn, H3, <<"GET">>, <<"/push/basic">>),
        ?assertEqual(<<"alphabravocharlie">>, Body),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_single_chunk(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 200, _Hs, Body, _} = h3_do_request(QConn, H3, <<"GET">>, <<"/push/single">>),
        ?assertEqual(<<"just-one">>, Body),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_empty_producer(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 200, _Hs, Body, _} = h3_do_request(QConn, H3, <<"GET">>, <<"/push/empty">>),
        ?assertEqual(<<>>, Body),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_backpressure(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 200, _Hs, Body, _} =
            h3_do_request_with_timeout(QConn, H3, <<"GET">>, <<"/push/xlarge">>, 30000),
        ?assertEqual(640 * 4 * 1024, byte_size(Body)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_reset_midstream(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        register(test_parent_marker, self()),
        {QConn, H3} = h3_connect(Port),
        {ok, StreamId, H3_1} =
            h3_send_request(QConn, H3, <<"GET">>, <<"/push/closed-probe">>),
        _H3_2 = h3_drain_some_data(H3_1, QConn, StreamId, 3000),
        nhttp_h3_test_client:close(QConn),
        wait_for_ets_row(observed_error, 5000),
        [{_, Reason, _}] = ets:lookup(?TRY_AFTER_TABLE, observed_error),
        ?assert(Reason =:= closed orelse Reason =:= timeout)
    after
        try
            unregister(test_parent_marker)
        catch
            _:_ -> ok
        end,
        nhttp:stop(Pid)
    end.

h3_producer_crash(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        Res = h3_do_request(QConn, H3, <<"GET">>, <<"/push/crash">>),
        ?assertMatch({error, {stream_reset, _}}, Res),
        ?assert(erlang:is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_head_request(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 200, _Hs, Body, _} = h3_do_request(QConn, H3, <<"HEAD">>, <<"/push/head">>),
        ?assertEqual(<<>>, Body),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_no_content_rejected(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 500, _Hs, _B, _} = h3_do_request(QConn, H3, <<"GET">>, <<"/push/no-content">>),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_not_modified_rejected(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, 500, _Hs, _B, _} = h3_do_request(QConn, H3, <<"GET">>, <<"/push/not-modified">>),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_concurrent_streams(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, S1, H3_1} = h3_send_request(QConn, H3, <<"GET">>, <<"/push/basic">>),
        {ok, S2, H3_2} = h3_send_request(QConn, H3_1, <<"GET">>, <<"/push/single">>),
        {Map, _} = h3_collect_responses(H3_2, QConn, [S1, S2], #{}, 5000),
        {200, _, B1} = maps:get(S1, Map),
        {200, _, B2} = maps:get(S2, Map),
        ?assertEqual(<<"alphabravocharlie">>, B1),
        ?assertEqual(<<"just-one">>, B2),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_with_trailers(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, StreamId, H3_1} = h3_send_request(QConn, H3, <<"GET">>, <<"/push/trailers">>),
        {ok, 200, _Hs, Body, Trailers, _H3_2} =
            h3_recv_response_with_trailers(H3_1, QConn, StreamId, 5000),
        ?assertEqual(<<"alphatrailing">>, Body),
        ?assertEqual(<<"0">>, proplists:get_value(<<"grpc-status">>, Trailers)),
        ?assertEqual(<<"done">>, proplists:get_value(<<"x-end">>, Trailers)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_client_resets_stream(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        register(test_parent_marker, self()),
        {QConn, H3} = h3_connect(Port),
        {ok, StreamId, H3_1} =
            h3_send_request_nofin(QConn, H3, <<"GET">>, <<"/push/closed-probe">>),
        _H3_2 = h3_drain_some_data(H3_1, QConn, StreamId, 3000),
        ok = nhttp_h3_test_client:reset_stream(QConn, StreamId, 16#100),
        wait_for_ets_row(observed_error, 5000),
        [{_, Reason, _}] = ets:lookup(?TRY_AFTER_TABLE, observed_error),
        ?assert(Reason =:= closed orelse Reason =:= timeout),
        ?assert(erlang:is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        try
            unregister(test_parent_marker)
        catch
            _:_ -> ok
        end,
        nhttp:stop(Pid)
    end.

h3_stuck_producer_reaped(Config) ->
    {Pid, Port} = start_h3_server(Config),
    try
        {QConn, H3} = h3_connect(Port),
        {ok, _StreamId, _H3_1} = h3_send_request(QConn, H3, <<"GET">>, <<"/push/stuck">>),
        WorkerPid = stuck_worker_pid(),
        ?assert(erlang:is_process_alive(WorkerPid)),
        nhttp_h3_test_client:close(QConn),
        ok = await_worker_death(WorkerPid, 8000),
        ?assert(erlang:is_process_alive(Pid))
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/3 HELPERS
%%%-----------------------------------------------------------------------------

start_h3_server(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => ?HANDLER,
        acceptor_count => 1
    }),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

h3_connect(Port) ->
    nhttp_h3_test_client:connect(Port).

h3_do_request(QConn, H3, Method, Path) ->
    nhttp_h3_test_client:request(QConn, H3, Method, Path, <<>>).

h3_do_request_with_timeout(QConn, H3, Method, Path, Timeout) ->
    case nhttp_h3_test_client:open_request(QConn, H3, Method, Path, [], <<>>, fin) of
        {ok, StreamId, H3_1} ->
            nhttp_h3_test_client:recv_response(QConn, H3_1, StreamId, Timeout);
        {error, _} = Err ->
            Err
    end.

h3_send_request(QConn, H3, Method, Path) ->
    nhttp_h3_test_client:open_request(QConn, H3, Method, Path, [], <<>>, fin).

h3_send_request_nofin(QConn, H3, Method, Path) ->
    nhttp_h3_test_client:open_request(QConn, H3, Method, Path, [], <<>>, nofin).

h3_drain_some_data(H3, QConn, StreamId, Timeout) ->
    nhttp_h3_test_client:drain_some_data(QConn, H3, StreamId, Timeout).

h3_recv_response_with_trailers(H3, QConn, StreamId, Timeout) ->
    nhttp_h3_test_client:recv_response_with_trailers(QConn, H3, StreamId, Timeout).

h3_collect_responses(H3, QConn, Pending, _Acc, Timeout) ->
    nhttp_h3_test_client:collect_responses(QConn, H3, Pending, Timeout).
