%%%-----------------------------------------------------------------------------
%%% @doc Connection failure and lifecycle paths shared across protocols.
%%%
%%% Drives a misbehaving peer (abrupt close, half-open stream, silent
%%% idle) and the OTP system-message surface against live connections so
%%% the `nhttp_conn:terminate/2' fan-out, idle-timeout, drain and
%%% `sys'-message clauses get exercised. The suite doubles as its own
%%% `nhttp_handler' for the HTTP/1.1 and HTTP/2 cases.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_failure_SUITE).

-behaviour(nhttp_handler).

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
    conn_sys_lifecycle/1,
    h1_abrupt_close_mid_request/1,
    h1_drain/1,
    h1_idle_timeout/1,
    h1_socket_error_on_response/1,
    h1_sys_messages/1,
    h2_abrupt_close_mid_stream/1,
    h1_close_before_init/1,
    h2_close_before_init/1,
    h2_idle_timeout/1,
    h2_socket_error_on_response/1,
    h3_abrupt_close_mid_request/1
]).

-export([
    init/1,
    handle_request/2,
    terminate/2
]).

-define(IDLE_MS, 150).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        conn_sys_lifecycle,
        h1_abrupt_close_mid_request,
        h1_drain,
        h1_idle_timeout,
        h1_socket_error_on_response,
        h1_sys_messages,
        h1_close_before_init,
        h2_close_before_init,
        h2_abrupt_close_mid_stream,
        h2_idle_timeout,
        h2_socket_error_on_response,
        h3_abrupt_close_mid_request
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    {CertFile, KeyFile} = nhttp_test_helpers:certs(),
    case filelib:is_file(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> [{certfile, missing} | Config]
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - SYSTEM MESSAGES
%%%-----------------------------------------------------------------------------

conn_sys_lifecycle(_Config) ->
    Opts = #{handler => ?MODULE, handler_args => [], transport => tcp},
    {ok, ConnPid} = proc_lib:start_link(
        nhttp_conn, init, [{my_ref, {tcp, fake_socket}, Opts, self()}]
    ),
    ?assertMatch({status, ConnPid, _, _}, sys:get_status(ConnPid)),
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttp_conn, undefined, []),
    ok = sys:resume(ConnPid),
    Ref = monitor(process, ConnPid),
    ok = sys:terminate(ConnPid, shutdown),
    receive
        {'DOWN', Ref, process, ConnPid, shutdown} -> ok
    after 2000 ->
        error(conn_did_not_terminate)
    end,
    ok.

h1_sys_messages(_Config) ->
    {ok, Pid, Port} = start_h1(),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, get_request(<<"/">>)),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ?assertMatch({status, ConnPid, _, _}, sys:get_status(ConnPid)),
    ok = sys:suspend(ConnPid),
    ok = sys:resume(ConnPid),
    Ref = monitor(process, ConnPid),
    ok = sys:terminate(ConnPid, shutdown),
    receive
        {'DOWN', Ref, process, ConnPid, _} -> ok
    after 2000 ->
        error(conn_did_not_terminate)
    end,
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - HTTP/1.1
%%%-----------------------------------------------------------------------------

h1_idle_timeout(_Config) ->
    {ok, Pid, Port} = start_h1(#{timeouts => #{idle => ?IDLE_MS}}),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_abrupt_close_mid_request(_Config) ->
    {ok, Pid, Port} = start_h1(),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n">>),
    _ = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ok = gen_tcp:close(Sock),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    nhttp:stop(Pid),
    ok.

h1_socket_error_on_response(_Config) ->
    {ok, Pid, Port} = start_h1(),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, get_request(<<"/stream-forever">>)),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    ok = gen_tcp:close(Sock),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 5000),
    nhttp:stop(Pid),
    ok.

h1_drain(_Config) ->
    {ok, Pid, Port} = start_h1(),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, get_request(<<"/">>)),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ok = nhttp_conn:drain(ConnPid),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - CLOSE BEFORE PROTOCOL INIT
%%%-----------------------------------------------------------------------------

h1_close_before_init(_Config) ->
    R = run_close_before_init([http1_1]),
    ?assertEqual(normal, R).

h2_close_before_init(_Config) ->
    R = run_close_before_init([http2]),
    ?assertEqual(normal, R).

%%%-----------------------------------------------------------------------------
%%% TEST CASES - HTTP/2
%%%-----------------------------------------------------------------------------

h2_idle_timeout(Config) ->
    with_tls(Config, fun(Tls) ->
        {ok, Pid, Port} = nhttp_test_helpers:start(Tls#{
            handler => ?MODULE,
            versions => [http2],
            timeouts => #{idle => ?IDLE_MS}
        }),
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
        ssl:close(Sock),
        nhttp:stop(Pid)
    end).

h2_abrupt_close_mid_stream(Config) ->
    with_tls(Config, fun(Tls) ->
        {ok, Pid, Port} = start_h2(Tls),
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_open_stream(Sock, 1, <<"/">>),
        _ = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
        ok = ssl:close(Sock),
        ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
        nhttp:stop(Pid)
    end).

h2_socket_error_on_response(Config) ->
    with_tls(Config, fun(Tls) ->
        {ok, Pid, Port} = start_h2(Tls),
        {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
        ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/stream-forever">>),
        _ = nhttp_test_helpers:h2_recv(Sock, 500),
        ok = ssl:close(Sock),
        ok = nhttp_test_helpers:wait_for_no_conns(Pid, 5000),
        nhttp:stop(Pid)
    end).

%%%-----------------------------------------------------------------------------
%%% TEST CASES - HTTP/3
%%%-----------------------------------------------------------------------------

h3_abrupt_close_mid_request(Config) ->
    with_tls(Config, fun(Tls) ->
        {ok, Pid, Port} = nhttp_test_helpers:start(Tls#{
            handler => nhttp_conn_h3_handler,
            versions => [http3]
        }),
        catch unregister(h3_conn_pid_receiver),
        register(h3_conn_pid_receiver, self()),
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        {ok, 200, _, _, H3_1} =
            nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),
        ConnPid =
            receive
                {h3_conn_pid, P} -> P
            after 5000 ->
                error(conn_pid_timeout)
            end,
        {ok, _StreamId, _H3_2} =
            nhttp_h3_test_client:open_request(
                QConn, H3_1, <<"GET">>, <<"/hello">>, [], <<>>, nofin
            ),
        Ref = monitor(process, ConnPid),
        ok = nhttp_h3_test_client:close(QConn),
        receive
            {'DOWN', Ref, process, ConnPid, _} -> ok
        after 5000 ->
            error(conn_did_not_terminate)
        end,
        catch unregister(h3_conn_pid_receiver),
        nhttp:stop(Pid)
    end).

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/stream-forever">>}, State) ->
    Producer = fun stream_forever/1,
    {stream, nhttp_stream:producer(200, [], Producer), State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start_h1() ->
    start_h1(#{}).

start_h1(Extra) ->
    nhttp_test_helpers:start(
        maps:merge(
            #{handler => ?MODULE, versions => [http1_1]}, Extra
        )
    ).

start_h2(Tls) ->
    nhttp_test_helpers:start(Tls#{handler => ?MODULE, versions => [http2]}).

with_tls(Config, Fun) ->
    case ?config(certfile, Config) of
        missing ->
            {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"};
        CertFile ->
            KeyFile = ?config(keyfile, Config),
            Fun(#{tls => #{certfile => CertFile, keyfile => KeyFile}})
    end.

get_request(Path) ->
    <<"GET ", Path/binary, " HTTP/1.1\r\nHost: localhost\r\n\r\n">>.

run_close_before_init(Versions) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    {ok, CSock} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, SSock} = gen_tcp:accept(LSock),
    ok = gen_tcp:close(CSock),
    ok = gen_tcp:close(SSock),
    ok = gen_tcp:close(LSock),
    Opts = #{
        handler => ?MODULE,
        handler_args => [],
        transport => tcp,
        versions => Versions
    },
    {ok, ConnPid} = proc_lib:start_link(
        nhttp_conn, init, [{my_ref, {tcp, SSock}, Opts, self()}]
    ),
    Ref = monitor(process, ConnPid),
    ConnPid ! {socket_ready, {tcp, SSock}},
    receive
        {'DOWN', Ref, process, ConnPid, Reason} -> Reason
    after 2000 ->
        error(conn_did_not_terminate)
    end.

stream_forever(SendChunk) ->
    Chunk = binary:copy(<<0>>, 65536),
    case SendChunk(Chunk) of
        ok -> stream_forever(SendChunk);
        {error, _} -> ok
    end.
