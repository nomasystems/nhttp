-module(nhttp_supervision_SUITE).

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
    accept_does_not_wait_for_conn_init/1,
    accept_does_not_wait_for_tracker/1,
    conn_crash_does_not_kill_acceptor/1,
    conn_crash_releases_counter_slot/1,
    drain_completes_in_flight_and_rejects_new/1,
    handler_init_failure_exits_shutdown/1,
    handler_init_failure_releases_slot/1,
    track_during_drain_gets_shutdown/1,
    track_of_dead_pid_releases_slot/1,
    wait_for_socket_runs_handler_terminate/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER (used as the test handler)
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).

-export([
    handle_request/2,
    init/1,
    terminate/2
]).

init({notify, Pid}) ->
    {ok, {notify, Pid}};
init({block_init, Pid}) ->
    Pid ! {init_blocked, self()},
    receive
        release_init -> {ok, undefined}
    after 5000 -> {error, init_not_released}
    end;
init(_) ->
    {ok, undefined}.

handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"OK">>), State}.

terminate(_Reason, {notify, Pid}) ->
    Pid ! {handler_terminated, self()},
    ok;
terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        accept_does_not_wait_for_conn_init,
        accept_does_not_wait_for_tracker,
        conn_crash_does_not_kill_acceptor,
        conn_crash_releases_counter_slot,
        drain_completes_in_flight_and_rejects_new,
        handler_init_failure_exits_shutdown,
        handler_init_failure_releases_slot,
        track_during_drain_gets_shutdown,
        track_of_dead_pid_releases_slot,
        wait_for_socket_runs_handler_terminate
    ].

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    case erase(listener_pid) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> nhttp:stop(Pid);
                false -> ok
            end
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

accept_does_not_wait_for_conn_init(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => {block_init, self()},
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    Socks = [nhttp_test_helpers:tcp_connect(Port) || _ <- [1, 2]],
    Blocked = [
        receive
            {init_blocked, ConnPid} -> ConnPid
        after 2000 -> ct:fail(init_not_started)
        end
     || _ <- Socks
    ],
    lists:foreach(fun(ConnPid) -> ConnPid ! release_init end, Blocked),
    lists:foreach(fun(Sock) -> ok = gen_tcp:close(Sock) end, Socks),
    ok.

accept_does_not_wait_for_tracker(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => {block_init, self()},
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    Tracker = tracker_of(ListenerPid),
    ok = sys:suspend(Tracker),
    Socks = [nhttp_test_helpers:tcp_connect(Port) || _ <- [1, 2]],
    Blocked = [
        receive
            {init_blocked, ConnPid} -> ConnPid
        after 2000 -> ct:fail(accept_waited_for_tracker)
        end
     || _ <- Socks
    ],
    ok = sys:resume(Tracker),
    lists:foreach(fun(ConnPid) -> ConnPid ! release_init end, Blocked),
    lists:foreach(fun(Sock) -> ok = gen_tcp:close(Sock) end, Socks),
    ok.

conn_crash_does_not_kill_acceptor(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    [AcceptorPid] = nhttp_test_helpers:which_acceptors(ListenerPid),
    AccMonRef = monitor(process, AcceptorPid),
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, get_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    [ConnPid] = wait_for_conn_count(ListenerPid, 1, 2000),
    ConnMonRef = monitor(process, ConnPid),
    exit(ConnPid, kill),
    receive
        {'DOWN', ConnMonRef, process, ConnPid, killed} -> ok
    after 2000 ->
        error(conn_did_not_die)
    end,
    receive
        {'DOWN', AccMonRef, process, AcceptorPid, _} ->
            error(acceptor_died_with_conn)
    after 200 ->
        ok
    end,
    true = is_process_alive(AcceptorPid),
    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock2, get_request()),
    {ok, _} = gen_tcp:recv(Sock2, 0, 5000),
    gen_tcp:close(Sock2),
    gen_tcp:close(Sock),
    ok.

conn_crash_releases_counter_slot(_Config) ->
    MaxConns = 2,
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1,
        max_connections => MaxConns
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    Socks = [open_idle_conn(Port) || _ <- lists:seq(1, MaxConns)],
    Conns = wait_for_conn_count(ListenerPid, MaxConns, 2000),
    MaxConns = length(Conns),
    lists:foreach(fun(P) -> exit(P, kill) end, Conns),
    ok = wait_for_conn_count_zero(ListenerPid, 2000),
    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock2, get_request()),
    {ok, _} = gen_tcp:recv(Sock2, 0, 5000),
    gen_tcp:close(Sock2),
    lists:foreach(fun(S) -> gen_tcp:close(S) end, Socks),
    ok.

drain_completes_in_flight_and_rejects_new(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 2
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    N = 20,
    Socks = [open_idle_conn(Port) || _ <- lists:seq(1, N)],
    _ = wait_for_conn_count(ListenerPid, N, 5000),
    Self = self(),
    spawn(fun() ->
        ok = nhttp_listener:drain(ListenerPid, 5000),
        Self ! drain_done
    end),
    receive
        drain_done -> ok
    after 6000 ->
        error(drain_timeout)
    end,
    case gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000) of
        {error, _} ->
            ok;
        {ok, ProbeSock} ->
            gen_tcp:close(ProbeSock),
            ok
    end,
    lists:foreach(fun(S) -> gen_tcp:close(S) end, Socks),
    ok.

handler_init_failure_exits_shutdown(_Config) ->
    Opts = #{handler => nhttp_supervision_failing_handler, handler_args => []},
    Ctx = #{ssl_opts => [], versions => [http1_1], alt_svc_advertise => disabled},
    {ok, ConnPid} = nhttp_conn:start_link_supervised(my_ref, {tcp, fake_socket}, Opts, Ctx),
    receive
        {'EXIT', ConnPid, Reason} ->
            ?assertEqual({shutdown, {handler_init, intentional_failure}}, Reason)
    after 2000 -> ct:fail(conn_did_not_exit)
    end,
    ok.

handler_init_failure_releases_slot(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_supervision_failing_handler,
        versions => [http1_1],
        acceptor_count => 1,
        max_connections => 4
    }),
    put(listener_pid, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),
    Self = self(),
    lists:foreach(
        fun(_) ->
            spawn(fun() ->
                case gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000) of
                    {ok, S} ->
                        ok = gen_tcp:send(S, get_request()),
                        ok = expect_closed_or_503(gen_tcp:recv(S, 0, 2000)),
                        gen_tcp:close(S);
                    _ ->
                        ok
                end,
                Self ! attempt_done
            end)
        end,
        lists:seq(1, 8)
    ),
    [
        receive
            attempt_done -> ok
        after 2000 -> error(attempt_timeout)
        end
     || _ <- lists:seq(1, 8)
    ],
    ok = wait_for_conn_count_zero(ListenerPid, 2000),
    ok.

track_during_drain_gets_shutdown(_Config) ->
    Tracker = start_tracker(infinity),
    ok = nhttp_conn_tracker:drain_all(Tracker),
    Self = self(),
    Conn = spawn(fun() ->
        receive
            shutdown -> Self ! {got_shutdown, self()}
        end
    end),
    ok = nhttp_conn_tracker:track(Tracker, Conn),
    receive
        {got_shutdown, Conn} -> ok
    after 2000 -> ct:fail(late_track_not_drained)
    end,
    ?assertEqual(ok, nhttp_conn_tracker:wait_for_drained(Tracker, 2000)),
    ok = gen_server:stop(Tracker),
    ok.

track_of_dead_pid_releases_slot(_Config) ->
    Tracker = start_tracker(1),
    Counter = get(counter),
    ok = nhttp_listener_counter:acquire(Counter),
    Dead = spawn(fun() -> ok end),
    {ok, _} = nhttp_test_helpers:wait_until_down(Dead),
    ok = nhttp_conn_tracker:track(Tracker, Dead),
    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp_listener_counter:count(Counter) =:= 0 end, 2000
    ),
    ok = gen_server:stop(Tracker),
    ok.

wait_for_socket_runs_handler_terminate(_Config) ->
    Self = self(),
    Opts = #{handler => ?MODULE, handler_args => {notify, Self}},
    Ctx = #{ssl_opts => [], versions => [http1_1, http2], alt_svc_advertise => disabled},
    {ok, ConnPid} = nhttp_conn:start_link_supervised(my_ref, {tcp, fake_socket}, Opts, Ctx),
    {status, ConnPid, _, _} = sys:get_status(ConnPid),
    ConnMon = monitor(process, ConnPid),
    exit(ConnPid, shutdown),
    receive
        {handler_terminated, ConnPid} -> ok
    after 1000 ->
        error(handler_terminate_not_called)
    end,
    receive
        {'DOWN', ConnMon, process, ConnPid, _} -> ok
    after 1000 ->
        error(conn_did_not_die)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

get_request() ->
    <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>.

expect_closed_or_503({error, closed}) -> ok;
expect_closed_or_503({ok, <<"HTTP/1.1 503 ", _/binary>>}) -> ok.

open_idle_conn(Port) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    Sock.

conn_pids(ListenerPid) ->
    nhttp_test_helpers:conn_pids(ListenerPid).

tracker_of(ListenerPid) ->
    [TransportSup] = nhttp_test_helpers:transport_sups(ListenerPid),
    {nhttp_conn_tracker, Tracker, _, _} =
        lists:keyfind(nhttp_conn_tracker, 1, supervisor:which_children(TransportSup)),
    Tracker.

start_tracker(MaxConnections) ->
    Tab = nhttp_registry:new(),
    Counter = nhttp_listener_counter:new(MaxConnections),
    ok = nhttp_registry:register_counter(Tab, Counter),
    put(counter, Counter),
    {ok, Tracker} = nhttp_conn_tracker:start_link(Tab, undefined),
    Tracker.

wait_for_conn_count(ListenerPid, N, Timeout) ->
    ok = nhttp_test_helpers:wait_until(
        fun() -> length(conn_pids(ListenerPid)) =:= N end, Timeout
    ),
    conn_pids(ListenerPid).

wait_for_conn_count_zero(ListenerPid, Timeout) ->
    nhttp_test_helpers:wait_until(
        fun() -> conn_pids(ListenerPid) =:= [] end, Timeout
    ).
