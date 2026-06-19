%%%-----------------------------------------------------------------------------
%%% @doc Mixed h1/h2 + h3 listener integration.
%%%
%%% A single `nhttp:start_link/2' with `versions => [http1_1, http2,
%%% http3]' binds two transports: a TCP/TLS listener (ALPN h2 /
%%% http/1.1) and a UDP/QUIC listener (ALPN h3). These tests assert both
%%% transports bind, serve their protocols, enforce per-transport
%%% connection caps, drain together, and stay isolated under crash.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_mixed_listener_SUITE).

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
    mixed_binds_both_transports/1,
    mixed_alpn_prefers_h2/1,
    mixed_h1_over_tcp/1,
    mixed_h2_over_tcp/1,
    mixed_h3_over_quic/1,
    mixed_max_connections_per_transport/1,
    mixed_drain_waits_both/1,
    mixed_quic_crash_isolates_tcp/1,
    mixed_each_transport_crash_within_period/1,
    mixed_transport_crash_loop_kills_listener/1,
    quic_listen_opts_reject_zero_rtt/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        mixed_binds_both_transports,
        mixed_alpn_prefers_h2,
        mixed_h1_over_tcp,
        mixed_h2_over_tcp,
        mixed_h3_over_quic,
        mixed_max_connections_per_transport,
        mixed_drain_waits_both,
        mixed_quic_crash_isolates_tcp,
        mixed_each_transport_crash_within_period,
        mixed_transport_crash_loop_kills_listener,
        quic_listen_opts_reject_zero_rtt
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    TestConfDir = filename:join(filename:dirname(code:which(?MODULE)), "conf"),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, Config) ->
    case erlang:get(listener_pid) of
        Pid when is_pid(Pid) ->
            catch nhttp:stop(Pid),
            erlang:erase(listener_pid);
        _ ->
            ok
    end,
    Config.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

mixed_binds_both_transports(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, TcpPort} = nhttp:get_port(Pid),
    {ok, TcpPort} = nhttp:get_port(Pid, tcp),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    ?assert(is_integer(TcpPort) andalso TcpPort > 0),
    ?assert(is_integer(QuicPort) andalso QuicPort > 0),
    ?assertNotEqual(TcpPort, QuicPort),
    ?assertEqual(#{tcp => TcpPort, quic => QuicPort}, nhttp:get_ports(Pid)),
    ok.

mixed_alpn_prefers_h2(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, Port} = nhttp:get_port(Pid, tcp),
    ?assertEqual({ok, <<"h2">>}, alpn_negotiated(Port, [<<"h2">>, <<"http/1.1">>])),
    ?assertEqual({ok, <<"h2">>}, alpn_negotiated(Port, [<<"http/1.1">>, <<"h2">>])),
    ?assertEqual({ok, <<"http/1.1">>}, alpn_negotiated(Port, [<<"http/1.1">>])),
    ok.

mixed_h1_over_tcp(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, Port} = nhttp:get_port(Pid, tcp),
    {Sock, Resp} = tls_h1_get(Port, <<"/hello">>),
    ?assertMatch({_, _}, binary:match(Resp, <<"200">>)),
    ?assertMatch({_, _}, binary:match(Resp, <<"Hello!">>)),
    ssl:close(Sock),
    ok.

mixed_h2_over_tcp(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, Port} = nhttp:get_port(Pid, tcp),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/hello">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, 2000),
    ?assert(lists:any(fun(F) -> element(1, F) =:= headers end, Frames)),
    ?assert(
        lists:any(
            fun
                ({data, 1, Body, _}) -> binary:match(Body, <<"Hello!">>) =/= nomatch;
                (_) -> false
            end,
            Frames
        )
    ),
    ssl:close(Sock),
    ok.

mixed_h3_over_quic(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, Port} = nhttp:get_port(Pid, quic),
    {QConn, H3} = nhttp_h3_test_client:connect(Port),
    {ok, 200, _Headers, Body, _H3_1} =
        nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/hello">>, [], <<>>),
    ?assertEqual(<<"Hello!">>, Body),
    nhttp_h3_test_client:close(QConn),
    ok.

mixed_max_connections_per_transport(Config) ->
    Pid = start_mixed(Config, #{max_connections => 1}),
    {ok, TcpPort} = nhttp:get_port(Pid, tcp),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),

    {Sock1, Resp1} = tls_h1_get(TcpPort, <<"/hello">>),
    ?assertMatch({_, _}, binary:match(Resp1, <<"Hello!">>)),
    ok = nhttp_test_helpers:wait_until(
        fun() -> length(nhttp_test_helpers:conn_pids(Pid)) =:= 1 end, 2000
    ),

    ?assertMatch({error, _}, tls_connect(TcpPort)),
    ?assertEqual(1, length(nhttp_test_helpers:conn_pids(Pid))),

    {QConn, H3} = nhttp_h3_test_client:connect(QuicPort),
    {ok, 200, _Headers, Body, _H3_1} =
        nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/hello">>, [], <<>>),
    ?assertEqual(<<"Hello!">>, Body),

    nhttp_h3_test_client:close(QConn),
    catch ssl:close(Sock1),
    ok.

mixed_drain_waits_both(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, TcpPort} = nhttp:get_port(Pid, tcp),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),

    {Sock, _} = tls_h1_get(TcpPort, <<"/hello">>),
    {QConn, H3} = nhttp_h3_test_client:connect(QuicPort),
    {ok, 200, _, _, _} =
        nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/hello">>, [], <<>>),

    ok = nhttp_listener:drain(Pid, 5000),

    case gen_tcp:connect("127.0.0.1", TcpPort, [binary, {active, false}], 500) of
        {error, _} -> ok;
        {ok, Probe} -> gen_tcp:close(Probe)
    end,

    nhttp_h3_test_client:close(QConn),
    catch ssl:close(Sock),

    MRef = monitor(process, Pid),
    nhttp:stop(Pid),
    erlang:erase(listener_pid),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        error(listener_did_not_stop)
    end,
    ok.

mixed_quic_crash_isolates_tcp(Config) ->
    Pid = start_mixed(Config, #{}),
    {ok, TcpPort} = nhttp:get_port(Pid, tcp),
    TcpSup = transport_sup(Pid, tcp),
    QuicSup0 = transport_sup(Pid, quic),
    TcpMon = monitor(process, TcpSup),

    exit(QuicSup0, kill),

    receive
        {'DOWN', TcpMon, process, TcpSup, _} -> error(tcp_transport_died_with_quic)
    after 300 ->
        ok
    end,
    ?assert(is_process_alive(TcpSup)),

    {Sock, Resp} = tls_h1_get(TcpPort, <<"/hello">>),
    ?assertMatch({_, _}, binary:match(Resp, <<"Hello!">>)),
    ssl:close(Sock),

    ok = nhttp_test_helpers:wait_until(
        fun() ->
            case transport_sup(Pid, quic) of
                undefined -> false;
                QuicSup1 -> is_pid(QuicSup1) andalso QuicSup1 =/= QuicSup0
            end
        end,
        5000
    ),
    {ok, _} = nhttp:get_port(Pid, quic),
    ok.

mixed_each_transport_crash_within_period(Config) ->
    Pid = start_mixed(Config, #{}),
    QuicSup0 = transport_sup(Pid, quic),
    exit(QuicSup0, kill),
    ok = wait_for_new_transport_sup(Pid, quic, QuicSup0),
    TcpSup0 = transport_sup(Pid, tcp),
    exit(TcpSup0, kill),
    ok = wait_for_new_transport_sup(Pid, tcp, TcpSup0),
    ?assert(is_process_alive(Pid)),
    {ok, _} = nhttp:get_port(Pid, tcp),
    {ok, _} = nhttp:get_port(Pid, quic),
    ok.

mixed_transport_crash_loop_kills_listener(Config) ->
    Pid = start_mixed(Config, #{}),
    Mon = monitor(process, Pid),
    QuicSup0 = transport_sup(Pid, quic),
    exit(QuicSup0, kill),
    ok = wait_for_new_transport_sup(Pid, quic, QuicSup0),
    QuicSup1 = transport_sup(Pid, quic),
    exit(QuicSup1, kill),
    ok = wait_for_new_transport_sup(Pid, quic, QuicSup1),
    QuicSup2 = transport_sup(Pid, quic),
    exit(QuicSup2, kill),
    receive
        {'DOWN', Mon, process, Pid, Reason} ->
            ?assertEqual(shutdown, Reason)
    after 5000 ->
        error(listener_survived_crash_loop)
    end,
    erlang:erase(listener_pid),
    ok.

quic_listen_opts_reject_zero_rtt(_Config) ->
    Opts = nhttp_transport_sup:quic_listen_opts(
        [<<"h3">>],
        #{certfile => "cert.pem", keyfile => "key.pem"},
        #{},
        #{}
    ),
    ?assertNot(maps:is_key(replay_protection, Opts)),
    ?assertEqual(nhttp_conn_h3, maps:get(conn_handler, Opts)),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

-spec start_mixed(ct_suite:ct_config(), map()) -> pid().
start_mixed(Config, Extra) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = maps:merge(
        #{
            port => 0,
            handler => nhttp_conn_h3_handler,
            versions => [http1_1, http2, http3],
            tls => #{certfile => CertFile, keyfile => KeyFile},
            acceptor_count => 2
        },
        Extra
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    erlang:put(listener_pid, Pid),
    Pid.

-spec alpn_negotiated(inet:port_number(), [binary()]) -> {ok, binary()} | {error, term()}.
alpn_negotiated(Port, Advertised) ->
    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [
            binary,
            {active, false},
            {verify, verify_none},
            {alpn_advertised_protocols, Advertised}
        ],
        5000
    ),
    Result = ssl:negotiated_protocol(Sock),
    ssl:close(Sock),
    Result.

-spec tls_connect(inet:port_number()) -> {ok, ssl:sslsocket()} | {error, term()}.
tls_connect(Port) ->
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

-spec tls_h1_get(inet:port_number(), binary()) -> {ssl:sslsocket(), binary()}.
tls_h1_get(Port, Path) ->
    {ok, Sock} = tls_connect(Port),
    Req = [<<"GET ">>, Path, <<" HTTP/1.1\r\nHost: localhost\r\n\r\n">>],
    ok = ssl:send(Sock, Req),
    {ok, Resp} = ssl:recv(Sock, 0, 5000),
    {Sock, Resp}.

-spec wait_for_new_transport_sup(pid(), tcp | quic, pid()) -> ok | {error, timeout}.
wait_for_new_transport_sup(ListenerPid, Kind, OldSup) ->
    nhttp_test_helpers:wait_until(
        fun() ->
            case transport_sup(ListenerPid, Kind) of
                undefined -> false;
                NewSup -> NewSup =/= OldSup
            end
        end,
        5000
    ).

-spec transport_sup(pid(), tcp | quic) -> pid() | undefined.
transport_sup(ListenerPid, Kind) ->
    case lists:keyfind({nhttp_transport_sup, Kind}, 1, supervisor:which_children(ListenerPid)) of
        {_, Sup, _, _} when is_pid(Sup) -> Sup;
        _ -> undefined
    end.
