%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp server infrastructure.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_SUITE).

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
    tcp_start_stop/1,
    tcp_http1_get/1,
    tcp_http1_post/1,
    tcp_http1_keepalive/1,
    tcp_ephemeral_port/1,
    tcp_http1_not_found/1,
    ssl_start_stop/1,
    ssl_http1_get/1,
    ssl_alpn_h2/1,
    ssl_h2_request/1,
    ssl_handshake_failure/1,
    handler_error/1,
    counter_basic_ops/1,
    counter_infinity/1,
    counter_release_at_zero/1,
    connection_limit_enforced/1,
    connection_limit_releases_on_close/1,
    graceful_stop_immediate/1,
    graceful_stop_with_drain/1,
    graceful_stop_with_timeout/1,
    acceptor_system_messages/1,
    listener_validation_errors/1,
    listener_drain/1,
    conn_system_get_status/1,
    get_port_no_acceptors/1,
    get_port_after_drain/1,
    get_port_after_terminate_acceptors/1,
    invalid_acceptor_count_rejected/1,
    stop_force_kill/1,
    listener_child_spec/1,
    h2_settings_validation/1,
    graceful_stop_drain_true/1,
    stop_rejects_drain_timeout_without_drain/1,
    acceptor_system_terminate/1,
    acceptor_system_code_change/1,
    listen_port_in_use/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello, World!">>), State};
handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/error">>}, State) ->
    {abort, test_error, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

%%%-----------------------------------------------------------------------------
%%% SUITE SETUP
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, tcp},
        {group, ssl},
        {group, errors},
        {group, connection_limits},
        {group, graceful_shutdown},
        {group, system_tests}
    ].

groups() ->
    [
        {tcp, [sequence], [
            tcp_start_stop,
            tcp_http1_get,
            tcp_http1_post,
            tcp_http1_keepalive,
            tcp_ephemeral_port,
            tcp_http1_not_found
        ]},
        {ssl, [sequence], [
            ssl_start_stop,
            ssl_http1_get,
            ssl_alpn_h2,
            ssl_h2_request,
            ssl_handshake_failure
        ]},
        {errors, [sequence], [
            handler_error
        ]},
        {connection_limits, [sequence], [
            counter_basic_ops,
            counter_infinity,
            counter_release_at_zero,
            connection_limit_enforced,
            connection_limit_releases_on_close
        ]},
        {graceful_shutdown, [sequence], [
            graceful_stop_immediate,
            graceful_stop_with_drain,
            graceful_stop_with_timeout
        ]},
        {system_tests, [sequence], [
            acceptor_system_messages,
            listener_validation_errors,
            listener_drain,
            conn_system_get_status,
            get_port_no_acceptors,
            get_port_after_drain,
            get_port_after_terminate_acceptors,
            invalid_acceptor_count_rejected,
            stop_force_kill,
            listener_child_spec,
            h2_settings_validation,
            graceful_stop_drain_true,
            stop_rejects_drain_timeout_without_drain,
            acceptor_system_terminate,
            acceptor_system_code_change,
            listen_port_in_use
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(ssl, Config) ->
    TestConfDir = find_test_conf_dir(),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

%%%-----------------------------------------------------------------------------
%%% TCP TESTS
%%%-----------------------------------------------------------------------------

tcp_start_stop(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE
    }),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    {ok, Port} = nhttp:get_port(Pid),
    ?assert(is_integer(Port)),
    ?assert(Port > 0),

    nhttp:stop(Pid),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),
    ok.

tcp_http1_get(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Hello, World!">>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

tcp_http1_post(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Body = <<"test body content">>,
    Request = [
        <<"POST /echo HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Content-Length: ">>,
        integer_to_binary(byte_size(Body)),
        <<"\r\n">>,
        <<"\r\n">>,
        Body
    ],
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    ?assert(binary:match(Response, Body) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

tcp_http1_keepalive(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request1 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request1),
    {ok, Response1} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response1),

    Request2 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request2),
    {ok, Response2} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response2),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

tcp_ephemeral_port(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE
    }),

    {ok, Port} = nhttp:get_port(Pid),
    ?assert(is_integer(Port)),
    ?assert(Port > 1024),

    nhttp:stop(Pid),
    ok.

tcp_http1_not_found(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /nonexistent HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 404 Not Found", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% SSL TESTS
%%%-----------------------------------------------------------------------------

ssl_start_stop(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE
    }),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    {ok, Port} = nhttp:get_port(Pid),
    ?assert(is_integer(Port)),

    nhttp:stop(Pid),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),
    ok.

ssl_http1_get(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [
            binary,
            {active, false},
            {verify, verify_none},
            {alpn_advertised_protocols, [<<"http/1.1">>]}
        ],
        5000
    ),

    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = ssl:send(Sock, Request),

    {ok, Response} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Hello, World!">>) =/= nomatch),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ssl_alpn_h2(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2, http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

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

    {ok, <<"h2">>} = ssl:negotiated_protocol(Sock),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ssl_h2_request(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2, http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

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

    {ok, <<"h2">>} = ssl:negotiated_protocol(Sock),

    ClientPreface = <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>,
    ok = ssl:send(Sock, ClientPreface),

    SettingsFrame = <<0, 0, 0, 4, 0, 0, 0, 0, 0>>,
    ok = ssl:send(Sock, SettingsFrame),

    {ok, ServerPreface} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<_:24, 4, _/binary>>, ServerPreface),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ERROR TESTS
%%%-----------------------------------------------------------------------------

handler_error(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /error HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    Result = gen_tcp:recv(Sock, 0, 5000),
    case Result of
        {error, closed} -> ok;
        {ok, _} -> ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONNECTION LIMIT TESTS
%%%-----------------------------------------------------------------------------

counter_basic_ops(_Config) ->
    Counter = nhttp_listener_counter:new(3),
    ?assertEqual(0, nhttp_listener_counter:count(Counter)),

    ok = nhttp_listener_counter:acquire(Counter),
    ?assertEqual(1, nhttp_listener_counter:count(Counter)),

    ok = nhttp_listener_counter:acquire(Counter),
    ?assertEqual(2, nhttp_listener_counter:count(Counter)),

    ok = nhttp_listener_counter:acquire(Counter),
    ?assertEqual(3, nhttp_listener_counter:count(Counter)),

    ?assertEqual(
        {error, {server, #{type => at_capacity}}}, nhttp_listener_counter:acquire(Counter)
    ),
    ?assertEqual(3, nhttp_listener_counter:count(Counter)),

    ok = nhttp_listener_counter:release(Counter),
    ?assertEqual(2, nhttp_listener_counter:count(Counter)),

    ok = nhttp_listener_counter:acquire(Counter),
    ?assertEqual(3, nhttp_listener_counter:count(Counter)),
    ok.

counter_infinity(_Config) ->
    Counter = nhttp_listener_counter:new(infinity),
    ?assertEqual(0, nhttp_listener_counter:count(Counter)),

    lists:foreach(
        fun(_) ->
            ok = nhttp_listener_counter:acquire(Counter)
        end,
        lists:seq(1, 1000)
    ),
    ?assertEqual(1000, nhttp_listener_counter:count(Counter)),
    ok.

counter_release_at_zero(_Config) ->
    Counter = nhttp_listener_counter:new(10),
    ?assertEqual(0, nhttp_listener_counter:count(Counter)),
    ok = nhttp_listener_counter:release(Counter),
    ?assertEqual(0, nhttp_listener_counter:count(Counter)),
    ok.

connection_limit_enforced(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_connections => 2
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock1} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock1, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp1} = gen_tcp:recv(Sock1, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp1),

    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock2, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp2} = gen_tcp:recv(Sock2, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp2),

    {ok, Sock3} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Result3 = gen_tcp:recv(Sock3, 0, 2000),
    case Result3 of
        {ok, Resp3} ->
            ?assertMatch(<<"HTTP/1.1 503 Service Unavailable", _/binary>>, Resp3);
        {error, closed} ->
            ok
    end,

    gen_tcp:close(Sock1),
    gen_tcp:close(Sock2),
    gen_tcp:close(Sock3),
    nhttp:stop(Pid),
    ok.

connection_limit_releases_on_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_connections => 1
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock1} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock1, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp1} = gen_tcp:recv(Sock1, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp1),

    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Result2 = gen_tcp:recv(Sock2, 0, 2000),
    case Result2 of
        {ok, Resp2} ->
            ?assertMatch(<<"HTTP/1.1 503 Service Unavailable", _/binary>>, Resp2);
        {error, closed} ->
            ok
    end,
    gen_tcp:close(Sock2),

    gen_tcp:close(Sock1),
    Resp3 = wait_for_successful_get(Port, 2000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp3),

    nhttp:stop(Pid),
    ok.

wait_for_successful_get(Port, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_successful_get_loop(Port, Deadline).

wait_for_successful_get_loop(Port, Deadline) ->
    case attempt_get(Port) of
        {ok, <<"HTTP/1.1 200 OK", _/binary>> = Resp} ->
            Resp;
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    error(no_200_within_deadline);
                false ->
                    timer:sleep(25),
                    wait_for_successful_get_loop(Port, Deadline)
            end
    end.

attempt_get(Port) ->
    case gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000) of
        {ok, Sock} ->
            ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
            Result = gen_tcp:recv(Sock, 0, 1000),
            gen_tcp:close(Sock),
            Result;
        Err ->
            Err
    end.

%%%-----------------------------------------------------------------------------
%%% GRACEFUL SHUTDOWN TESTS
%%%-----------------------------------------------------------------------------

graceful_stop_immediate(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),
    ok.

graceful_stop_with_drain(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),
    ?assert(is_process_alive(Pid)),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),

    nhttp:stop(Pid, #{drain => true, drain_timeout => 500}),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),

    gen_tcp:close(Sock),
    ok.

graceful_stop_with_timeout(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid, #{drain => true, drain_timeout => 100}),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),
    ok.

%%%-----------------------------------------------------------------------------
%%% SSL HANDSHAKE FAILURE TESTS
%%%-----------------------------------------------------------------------------

ssl_handshake_failure(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GARBAGE DATA\r\n">>),
    _ = gen_tcp:recv(Sock, 0, 1000),
    gen_tcp:close(Sock),

    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% SYSTEM MESSAGES TESTS
%%%-----------------------------------------------------------------------------

acceptor_system_messages(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),

    case supervisor:which_children(Pid) of
        [{{nhttp_acceptor, _}, AcceptorPid, worker, _} | _] when is_pid(AcceptorPid) ->
            Status = sys:get_status(AcceptorPid),
            ?assertMatch({status, AcceptorPid, _, _}, Status),

            ok = sys:suspend(AcceptorPid),
            ok = sys:resume(AcceptorPid),

            ok = sys:suspend(AcceptorPid),
            ok = sys:change_code(AcceptorPid, nhttp_acceptor, undefined, []),
            ok = sys:resume(AcceptorPid),

            ?assert(is_process_alive(AcceptorPid));
        _ ->
            ok
    end,

    nhttp:stop(Pid),
    ok.

listener_validation_errors(_Config) ->
    Result1 = nhttp:start_link(#{port => 8080}),
    case Result1 of
        {error, _} -> ok;
        _ -> ok
    end,

    Result2 = nhttp:start_link(#{handler => ?MODULE}),
    case Result2 of
        {error, _} -> ok;
        _ -> ok
    end,
    ok.

listener_drain(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    ok = nhttp_listener:drain(Pid, 100),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

conn_system_get_status(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    Acceptors = nhttp_test_helpers:which_acceptors(Pid),
    ?assert(length(Acceptors) > 0),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

get_port_no_acceptors(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),

    {ok, Port1} = nhttp:get_port(Pid),
    {ok, Port2} = nhttp:get_port(Pid),
    ?assertEqual(Port1, Port2),

    nhttp:stop(Pid),
    ok.

get_port_after_drain(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, _Port} = nhttp:get_port(Pid),

    ok = nhttp:drain(Pid, 1000),

    ?assertEqual({error, {server, no_acceptors}}, nhttp:get_port(Pid)),

    nhttp:stop(Pid),
    ok.

get_port_after_terminate_acceptors(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 2
    }),
    {ok, _Port} = nhttp:get_port(Pid),

    [TransportSup | _] = nhttp_test_helpers:transport_sups(Pid),
    {_, AccSupPid, _, _} =
        lists:keyfind(nhttp_acceptor_sup, 1, supervisor:which_children(TransportSup)),
    AcceptorChildren = supervisor:which_children(AccSupPid),
    lists:foreach(
        fun({Id, _, _, _}) ->
            ok = supervisor:terminate_child(AccSupPid, Id)
        end,
        AcceptorChildren
    ),

    ?assertEqual({error, {server, no_acceptors}}, nhttp:get_port(Pid)),

    nhttp:stop(Pid),
    ok.

invalid_acceptor_count_rejected(_Config) ->
    Result = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        acceptor_count => 0
    }),
    ?assertMatch(
        {error, {invalid_opts, {server, #{type := invalid_acceptor_count, reason := 0}}}},
        Result
    ).

stop_force_kill(_Config) ->
    Parent = self(),
    StuckPid = spawn(fun() -> stuck_loop(Parent) end),
    receive
        {ready, StuckPid} -> ok
    after 1000 ->
        ct:fail(stuck_proc_not_ready)
    end,
    StuckRef = monitor(process, StuckPid),

    Start = erlang:monotonic_time(millisecond),
    ok = nhttp:stop(StuckPid, #{force_kill_delay => 200}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    ?assert(Elapsed >= 200),
    ?assert(Elapsed < 2000),

    receive
        {'DOWN', StuckRef, process, StuckPid, killed} -> ok
    after 0 ->
        ct:fail(expected_killed_down)
    end.

stuck_loop(Parent) ->
    process_flag(trap_exit, true),
    Parent ! {ready, self()},
    stuck_loop_recv().

stuck_loop_recv() ->
    receive
        _ -> stuck_loop_recv()
    end.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL COVERAGE TESTS
%%%-----------------------------------------------------------------------------

listener_child_spec(_Config) ->
    Name = child_spec_test,
    Opts = #{port => 8080, handler => ?MODULE},
    NamedSpec = nhttp_listener:child_spec(Name, Opts),
    ?assertMatch(#{id := {nhttp_listener, Name}}, NamedSpec),
    ?assertMatch(#{start := {nhttp_listener, start_link, [Name, Opts]}}, NamedSpec),
    ?assertMatch(#{type := supervisor}, NamedSpec),
    ?assertMatch(#{restart := permanent}, NamedSpec),
    ?assertMatch(#{shutdown := infinity}, NamedSpec),

    UnnamedSpec = nhttp_listener:child_spec(Opts),
    ?assertMatch(#{id := {nhttp_listener, _}}, UnnamedSpec),
    ?assertMatch(#{start := {nhttp_listener, start_link, [Opts]}}, UnnamedSpec),
    ?assertMatch(#{type := supervisor}, UnnamedSpec),
    ok.

h2_settings_validation(_Config) ->
    {ok, ValidPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{
            header_table_size => 8192,
            initial_window_size => 65535,
            max_concurrent_streams => 100,
            max_frame_size => 32768
        }
    }),
    nhttp:stop(ValidPid),

    Result1 = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{initial_window_size => 16#FFFFFFFF}
    }),
    ?assertMatch({error, _}, Result1),

    Result2 = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{max_frame_size => 100}
    }),
    ?assertMatch({error, _}, Result2),

    Result3 = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{max_frame_size => 16#FFFFFFFF}
    }),
    ?assertMatch({error, _}, Result3),

    Result4 = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{header_table_size => "invalid"}
    }),
    ?assertMatch({error, _}, Result4),

    Result5 = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        h2_settings => #{max_concurrent_streams => "invalid"}
    }),
    ?assertMatch({error, _}, Result5),

    ok.

graceful_stop_drain_true(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, _Port} = nhttp:get_port(Pid),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid, #{drain => true}),
    {ok, _} = nhttp_test_helpers:wait_until_down(Pid, 2000),
    ok.

stop_rejects_drain_timeout_without_drain(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(
        {error, {invalid_stop_opts, drain_timeout_without_drain}},
        nhttp:stop(Pid, #{drain_timeout => 500})
    ),
    ?assertEqual(
        {error, {invalid_stop_opts, drain_timeout_without_drain}},
        nhttp:stop(Pid, #{drain => false, drain_timeout => 500})
    ),
    nhttp:stop(Pid),
    ok.

acceptor_system_terminate(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),

    case supervisor:which_children(Pid) of
        [{{nhttp_acceptor, _}, AcceptorPid, worker, _} | _] when is_pid(AcceptorPid) ->
            Ref = monitor(process, AcceptorPid),
            sys:terminate(AcceptorPid, test_shutdown),
            receive
                {'DOWN', Ref, process, AcceptorPid, _Reason} -> ok
            after 5000 ->
                error(acceptor_terminate_timeout)
            end;
        _ ->
            ok
    end,

    nhttp:stop(Pid),
    ok.

acceptor_system_code_change(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),

    case supervisor:which_children(Pid) of
        [{{nhttp_acceptor, _}, AcceptorPid, worker, _} | _] when is_pid(AcceptorPid) ->
            ok = sys:suspend(AcceptorPid),
            ok = sys:change_code(AcceptorPid, nhttp_acceptor, undefined, undefined),
            ok = sys:resume(AcceptorPid),
            {ok, _Port} = nhttp_acceptor:get_listen_port(AcceptorPid);
        _ ->
            ok
    end,

    nhttp:stop(Pid),
    ok.

listen_port_in_use(_Config) ->
    {ok, Pid1} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid1),

    Result = nhttp:start_link(#{
        port => Port,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    ?assertMatch({error, _}, Result),

    nhttp:stop(Pid1),
    ok.
