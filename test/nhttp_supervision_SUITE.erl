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
    conn_crash_does_not_kill_acceptor/1,
    conn_crash_releases_counter_slot/1,
    drain_completes_in_flight_and_rejects_new/1,
    handler_init_failure_releases_slot/1,
    track_is_synchronous_when_tracker_down/1,
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
        conn_crash_does_not_kill_acceptor,
        conn_crash_releases_counter_slot,
        drain_completes_in_flight_and_rejects_new,
        handler_init_failure_releases_slot,
        track_is_synchronous_when_tracker_down,
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
                true -> catch nhttp:stop(Pid);
                false -> ok
            end
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

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
    catch gen_tcp:close(Sock),
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
    lists:foreach(fun(S) -> catch gen_tcp:close(S) end, Socks),
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
    lists:foreach(fun(S) -> catch gen_tcp:close(S) end, Socks),
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
                        gen_tcp:send(S, get_request()),
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

track_is_synchronous_when_tracker_down(_Config) ->
    DeadTracker = spawn(fun() -> ok end),
    MRef = erlang:monitor(process, DeadTracker),
    receive
        {'DOWN', MRef, process, DeadTracker, _} -> ok
    after 1000 ->
        ct:fail(dead_tracker_didnt_die)
    end,
    DummyPid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ?assertExit({noproc, _}, nhttp_conn_tracker:track(DeadTracker, DummyPid)),
    DummyPid ! stop,
    ok.

wait_for_socket_runs_handler_terminate(_Config) ->
    Self = self(),
    Opts = #{
        handler => ?MODULE,
        handler_args => {notify, Self},
        transport => tcp
    },
    {ok, ConnPid} = proc_lib:start_link(
        nhttp_conn, init, [{my_ref, {tcp, fake_socket}, Opts, Self}]
    ),
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

open_idle_conn(Port) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    Sock.

conn_pids(ListenerPid) ->
    nhttp_test_helpers:conn_pids(ListenerPid).

wait_for_conn_count(ListenerPid, N, Timeout) ->
    ok = nhttp_test_helpers:wait_until(
        fun() -> length(conn_pids(ListenerPid)) =:= N end, Timeout
    ),
    conn_pids(ListenerPid).

wait_for_conn_count_zero(ListenerPid, Timeout) ->
    nhttp_test_helpers:wait_until(
        fun() -> conn_pids(ListenerPid) =:= [] end, Timeout
    ).
