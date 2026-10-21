-module(nhttp_h2_inline_SUITE).

-moduledoc """
HTTP/2 dispatch: by default `handle_request/2` runs in the connection
process, in stream order, and every result shape works there. With
`h2_dispatch => worker` it runs in a worker per stream.
""".

%%%-----------------------------------------------------------------------------
%%% INCLUDES
%%%-----------------------------------------------------------------------------
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% COMMON TEST CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    end_per_testcase/2,
    init_per_testcase/2
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    accept_body_on_empty_body/1,
    conn_spawn_opts_apply_to_workers/1,
    handler_crash_sends_500_and_conn_survives/1,
    handler_state_is_sequential/1,
    invalid_dispatch_option_rejected/1,
    post_body_runs_through_worker/1,
    reply_runs_in_connection_process/1,
    stream_producer_result/1,
    worker_option_runs_outside_connection_process/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).

-export([
    handle_request/2,
    handle_request_body/3,
    init/1
]).

-define(RECV_TIMEOUT, 5000).

init(TestPid) ->
    {ok, #{test => TestPid, count => 0}}.

handle_request(#{path := <<"/reply">>}, #{test := TestPid} = State) ->
    TestPid ! {handled_in, self()},
    {reply, nhttp_resp:ok(<<"OK">>), State};
handle_request(#{path := <<"/count">>}, #{count := N} = State) ->
    {reply, nhttp_resp:ok(integer_to_binary(N + 1)), State#{count => N + 1}};
handle_request(#{path := <<"/stream">>}, State) ->
    Producer = fun(Send) ->
        ok = Send(<<"a">>),
        ok = Send(<<"b">>),
        ok
    end,
    {stream, {producer, 200, [], Producer}, State};
handle_request(#{path := <<"/accept">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/gc_body">>}, #{test := TestPid} = State) ->
    TestPid ! {handled_in, self()},
    TestPid ! {fullsweep_after, request_worker, fullsweep_after()},
    {accept_body, [], State};
handle_request(#{path := <<"/gc_stream">>}, #{test := TestPid} = State) ->
    Producer = fun(Send) ->
        TestPid ! {fullsweep_after, producer, fullsweep_after()},
        Send(<<"a">>)
    end,
    {stream, {producer, 200, [], Producer}, State};
handle_request(#{path := <<"/crash">>}, _State) ->
    error(boom).

handle_request_body({data, Data}, Acc, State) ->
    {accept_body, [Data | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    {reply, nhttp_resp:ok(iolist_to_binary([<<"FIN:">> | lists:reverse(Acc)])), State}.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------
all() ->
    [
        reply_runs_in_connection_process,
        handler_state_is_sequential,
        stream_producer_result,
        accept_body_on_empty_body,
        post_body_runs_through_worker,
        handler_crash_sends_500_and_conn_survives,
        invalid_dispatch_option_rejected,
        worker_option_runs_outside_connection_process,
        conn_spawn_opts_apply_to_workers
    ].

init_per_testcase(invalid_dispatch_option_rejected, Config) ->
    Config;
init_per_testcase(worker_option_runs_outside_connection_process, Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        handler_args => self(), h2_dispatch => worker
    }),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    [{server, Pid}, {sock, Sock} | Config];
init_per_testcase(conn_spawn_opts_apply_to_workers, Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{
        handler_args => self(), conn_spawn_opts => [{fullsweep_after, 7}]
    }),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    [{server, Pid}, {sock, Sock} | Config];
init_per_testcase(_TestCase, Config) ->
    {Pid, Port} = nhttp_test_helpers:h2_start_server(?MODULE, #{handler_args => self()}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    [{server, Pid}, {sock, Sock} | Config].

end_per_testcase(invalid_dispatch_option_rejected, _Config) ->
    ok;
end_per_testcase(_TestCase, Config) ->
    ok = ssl:close(?config(sock, Config)),
    ok = nhttp:stop(?config(server, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
reply_runs_in_connection_process(Config) ->
    Sock = ?config(sock, Config),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(?config(server, Config), 1, ?RECV_TIMEOUT),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/reply">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    receive
        {handled_in, Pid} -> ?assertEqual(ConnPid, Pid)
    after ?RECV_TIMEOUT -> ct:fail(handler_not_called)
    end,
    ok.

handler_state_is_sequential(Config) ->
    Sock = ?config(sock, Config),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/count">>),
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/count">>),
    Frames1 = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    Frames3 = nhttp_test_helpers:h2_recv_stream(Sock, 3, ?RECV_TIMEOUT),
    Bodies = [
        nhttp_test_helpers:h2_response_body(Frames1 ++ Frames3, 1),
        nhttp_test_helpers:h2_response_body(Frames1 ++ Frames3, 3)
    ],
    ?assertEqual([<<"1">>, <<"2">>], lists:sort(Bodies)),
    ok.

stream_producer_result(Config) ->
    Sock = ?config(sock, Config),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/stream">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"ab">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    ?assert(nhttp_test_helpers:h2_stream_done(Frames, 1)),
    ok.

accept_body_on_empty_body(Config) ->
    Sock = ?config(sock, Config),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/accept">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"FIN:">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    ok.

post_body_runs_through_worker(Config) ->
    Sock = ?config(sock, Config),
    ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, <<"hello">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"FIN:hello">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    ok.

handler_crash_sends_500_and_conn_survives(Config) ->
    Sock = ?config(sock, Config),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/crash">>),
    Frames1 = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"500">>, nhttp_test_helpers:h2_response_status(Frames1)),
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/reply">>),
    Frames3 = nhttp_test_helpers:h2_recv_stream(Sock, 3, ?RECV_TIMEOUT),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames3, 3)),
    ok.

worker_option_runs_outside_connection_process(Config) ->
    Sock = ?config(sock, Config),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(?config(server, Config), 1, ?RECV_TIMEOUT),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/reply">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    receive
        {handled_in, Pid} -> ?assertNotEqual(ConnPid, Pid)
    after ?RECV_TIMEOUT -> ct:fail(handler_not_called)
    end,
    ok.

conn_spawn_opts_apply_to_workers(Config) ->
    Sock = ?config(sock, Config),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(?config(server, Config), 1, ?RECV_TIMEOUT),
    ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/gc_body">>, <<"hello">>),
    Frames1 = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"FIN:hello">>, nhttp_test_helpers:h2_response_body(Frames1, 1)),
    receive
        {handled_in, Pid} -> ?assertNotEqual(ConnPid, Pid)
    after ?RECV_TIMEOUT -> ct:fail(handler_not_called)
    end,
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/gc_stream">>),
    Frames3 = nhttp_test_helpers:h2_recv_stream(Sock, 3, ?RECV_TIMEOUT),
    ?assertEqual(<<"a">>, nhttp_test_helpers:h2_response_body(Frames3, 3)),
    ?assertEqual(7, receive_fullsweep_after(request_worker)),
    ?assertEqual(7, receive_fullsweep_after(producer)),
    ok.

invalid_dispatch_option_rejected(_Config) ->
    {CertFile, KeyFile} = nhttp_test_helpers:certs(),
    ?assertMatch(
        {error, {invalid_opts, _}},
        nhttp:start_link(#{
            port => 0,
            handler => ?MODULE,
            handler_args => self(),
            tls => #{certfile => CertFile, keyfile => KeyFile},
            versions => [http2],
            h2_dispatch => bogus
        })
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------
fullsweep_after() ->
    {garbage_collection, GC} = process_info(self(), garbage_collection),
    proplists:get_value(fullsweep_after, GC).

receive_fullsweep_after(Kind) ->
    receive
        {fullsweep_after, Kind, N} -> N
    after ?RECV_TIMEOUT -> ct:fail({no_fullsweep_after, Kind})
    end.
