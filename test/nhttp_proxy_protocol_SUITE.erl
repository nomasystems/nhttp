%%%-----------------------------------------------------------------------------
%%% @doc PROXY protocol v1/v2 integration tests.
%%%
%%% Covers:
%%%   * v1 ASCII header over plain TCP
%%%   * v2 binary header over plain TCP
%%%   * v2 binary header over TLS (AWS NLB pattern: the PROXY header
%%%     arrives before the TLS handshake on the raw TCP byte stream)
%%%   * Restricted accepted versions (`v1` only / `v2` only)
%%%   * Malformed headers close the connection without a 200 response
%%%   * The original L4 peer is preserved as `proxy_peer` for diagnostics
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_proxy_protocol_SUITE).

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
    v1_tcp_http1_get/1,
    v2_tcp_http1_get/1,
    v2_tls_http1_get/1,
    v2_local_keeps_l4_peer/1,
    v2_proxy_unspec_falls_back/1,
    version_v1_only_rejects_v2/1,
    version_v2_only_rejects_v1/1,
    proxy_closed_before_header/1,
    malformed_header_closes/1,
    invalid_proxy_option_rejected/1,
    invalid_proxy_option_not_boolean_or_map/1,
    invalid_proxy_option_bad_timeout/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

-define(PEER_REGISTRY, nhttp_proxy_protocol_SUITE_peer).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{peer := Peer} = _Req, State) ->
    case whereis(?PEER_REGISTRY) of
        undefined -> ok;
        Pid -> Pid ! {observed_peer, Peer}
    end,
    Body = format_peer(Peer),
    {reply, nhttp_resp:ok(Body), State}.

format_peer({{A, B, C, D}, Port}) ->
    iolist_to_binary(
        io_lib:format("~b.~b.~b.~b:~b", [A, B, C, D, Port])
    );
format_peer({Addr, Port}) ->
    iolist_to_binary(io_lib:format("~p:~b", [Addr, Port])).

%%%-----------------------------------------------------------------------------
%%% SUITE
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, plain_tcp},
        {group, tls},
        {group, accepted_versions},
        {group, errors}
    ].

groups() ->
    [
        {plain_tcp, [sequence], [
            v1_tcp_http1_get,
            v2_tcp_http1_get,
            v2_local_keeps_l4_peer,
            v2_proxy_unspec_falls_back
        ]},
        {tls, [sequence], [
            v2_tls_http1_get
        ]},
        {accepted_versions, [sequence], [
            version_v1_only_rejects_v2,
            version_v2_only_rejects_v1
        ]},
        {errors, [sequence], [
            proxy_closed_before_header,
            malformed_header_closes,
            invalid_proxy_option_rejected,
            invalid_proxy_option_not_boolean_or_map,
            invalid_proxy_option_bad_timeout
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(tls, Config) ->
    ConfDir = find_test_conf_dir(),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
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
    register_peer_collector(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    deregister_peer_collector(),
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS - PLAIN TCP
%%%-----------------------------------------------------------------------------

v1_tcp_http1_get(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        ProxyHdr = <<"PROXY TCP4 198.51.100.7 198.51.100.1 51234 80\r\n">>,
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        Response = roundtrip_plain(Port, <<ProxyHdr/binary, Request/binary>>),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
        assert_observed_peer({{198, 51, 100, 7}, 51234})
    after
        nhttp:stop(Pid)
    end.

v2_tcp_http1_get(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        ProxyHdr = build_v2_inet({203, 0, 113, 42}, {203, 0, 113, 1}, 51235, 80),
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        Response = roundtrip_plain(Port, <<ProxyHdr/binary, Request/binary>>),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
        assert_observed_peer({{203, 0, 113, 42}, 51235})
    after
        nhttp:stop(Pid)
    end.

v2_local_keeps_l4_peer(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        ProxyHdr = build_v2_local(),
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        Response = roundtrip_plain(Port, <<ProxyHdr/binary, Request/binary>>),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
        assert_observed_peer_ip({127, 0, 0, 1})
    after
        nhttp:stop(Pid)
    end.

v2_proxy_unspec_falls_back(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        ProxyHdr = build_v2_unspec(),
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        Response = roundtrip_plain(Port, <<ProxyHdr/binary, Request/binary>>),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
        assert_observed_peer_ip({127, 0, 0, 1})
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% TESTS - TLS (PROXY HEADER ARRIVES BEFORE THE TLS HANDSHAKE)
%%%-----------------------------------------------------------------------------

v2_tls_http1_get(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid, Port} = start_listener(#{
        proxy_protocol => true,
        versions => [http1_1],
        tls => #{certfile => CertFile, keyfile => KeyFile}
    }),
    try
        ProxyHdr = build_v2_inet({192, 0, 2, 99}, {192, 0, 2, 1}, 60000, 443),
        Response = roundtrip_tls_with_proxy(Port, ProxyHdr),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
        assert_observed_peer({{192, 0, 2, 99}, 60000})
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% TESTS - ACCEPTED VERSIONS
%%%-----------------------------------------------------------------------------

version_v1_only_rejects_v2(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => #{version => v1}}),
    try
        Hdr = build_v2_inet({1, 2, 3, 4}, {5, 6, 7, 8}, 1234, 5678),
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        ?assertEqual(closed, attempt_request(Port, <<Hdr/binary, Request/binary>>))
    after
        nhttp:stop(Pid)
    end.

version_v2_only_rejects_v1(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => #{version => v2}}),
    try
        Hdr = <<"PROXY TCP4 1.2.3.4 5.6.7.8 1234 5678\r\n">>,
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        ?assertEqual(closed, attempt_request(Port, <<Hdr/binary, Request/binary>>))
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% TESTS - ERRORS
%%%-----------------------------------------------------------------------------

malformed_header_closes(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        Garbage = <<"\r\n\r\nNOT A PROXY HEADER">>,
        ?assertEqual(closed, attempt_request(Port, Garbage))
    after
        nhttp:stop(Pid)
    end.

proxy_closed_before_header(_Config) ->
    {ok, Pid, Port} = start_listener(#{proxy_protocol => true}),
    try
        {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 5000),
        ok = gen_tcp:close(Sock),
        ProxyHdr = build_v2_inet({203, 0, 113, 9}, {203, 0, 113, 1}, 4321, 80),
        Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
        Response = roundtrip_plain(Port, <<ProxyHdr/binary, Request/binary>>),
        ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response)
    after
        nhttp:stop(Pid)
    end.

invalid_proxy_option_rejected(_Config) ->
    Result = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        proxy_protocol => #{version => v3}
    }),
    ?assertMatch(
        {error, {invalid_opts, {server, #{type := invalid_proxy_protocol}}}},
        Result
    ).

invalid_proxy_option_not_boolean_or_map(_Config) ->
    Result = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        proxy_protocol => <<"yes">>
    }),
    ?assertMatch(
        {error,
            {invalid_opts,
                {server, #{
                    type := invalid_proxy_protocol,
                    reason := not_boolean_or_map
                }}}},
        Result
    ).

invalid_proxy_option_bad_timeout(_Config) ->
    Result = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        proxy_protocol => #{timeout => -1}
    }),
    ?assertMatch(
        {error,
            {invalid_opts,
                {server, #{
                    type := invalid_proxy_protocol,
                    reason := {bad_timeout, -1}
                }}}},
        Result
    ).

%%%-----------------------------------------------------------------------------
%%% HELPERS - LISTENER
%%%-----------------------------------------------------------------------------

start_listener(Extra) ->
    Base = #{port => 0, handler => ?MODULE, versions => [http1_1]},
    {ok, Pid} = nhttp:start_link(maps:merge(Base, Extra)),
    {ok, Port} = nhttp:get_port(Pid),
    {ok, Pid, Port}.

%%%-----------------------------------------------------------------------------
%%% HELPERS - CLIENT
%%%-----------------------------------------------------------------------------

roundtrip_plain(Port, Payload) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 5000),
    ok = gen_tcp:send(Sock, Payload),
    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ok = gen_tcp:close(Sock),
    Response.

attempt_request(Port, Payload) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 5000),
    case gen_tcp:send(Sock, Payload) of
        ok ->
            case gen_tcp:recv(Sock, 0, 1000) of
                {error, closed} ->
                    closed;
                {error, timeout} ->
                    gen_tcp:close(Sock),
                    closed;
                {ok, Resp} ->
                    gen_tcp:close(Sock),
                    {response, Resp}
            end;
        {error, _} ->
            gen_tcp:close(Sock),
            closed
    end.

roundtrip_tls_with_proxy(Port, ProxyHdr) ->
    {ok, Tcp} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 5000),
    ok = gen_tcp:send(Tcp, ProxyHdr),
    {ok, Ssl} = ssl:connect(Tcp, [
        binary,
        {active, false},
        {verify, verify_none},
        {server_name_indication, "localhost"}
    ]),
    Request = <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>,
    ok = ssl:send(Ssl, Request),
    {ok, Response} = ssl:recv(Ssl, 0, 5000),
    ssl:close(Ssl),
    Response.

%%%-----------------------------------------------------------------------------
%%% HELPERS - PROXY HEADER BUILDERS
%%%-----------------------------------------------------------------------------

v2_sig() ->
    <<13, 10, 13, 10, 0, 13, 10, "QUIT", 10>>.

build_v2_inet({A, B, C, D}, {E, F, G, H}, SPort, DPort) ->
    Sig = v2_sig(),
    Payload = <<A, B, C, D, E, F, G, H, SPort:16/big, DPort:16/big>>,
    <<Sig/binary, 16#21, 16#11, (byte_size(Payload)):16/big, Payload/binary>>.

build_v2_local() ->
    Sig = v2_sig(),
    <<Sig/binary, 16#20, 16#00, 0:16/big>>.

build_v2_unspec() ->
    Sig = v2_sig(),
    <<Sig/binary, 16#21, 16#00, 0:16/big>>.

%%%-----------------------------------------------------------------------------
%%% HELPERS - PEER OBSERVATION
%%%-----------------------------------------------------------------------------

register_peer_collector() ->
    Pid = spawn(fun peer_collector/0),
    register(?PEER_REGISTRY, Pid),
    ok.

deregister_peer_collector() ->
    case whereis(?PEER_REGISTRY) of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
    ok.

peer_collector() ->
    peer_collector_loop([]).

peer_collector_loop(Acc) ->
    receive
        {observed_peer, Peer} ->
            peer_collector_loop([Peer | Acc]);
        {get, From} ->
            From ! {peers, lists:reverse(Acc)},
            peer_collector_loop([]);
        stop ->
            ok
    end.

observed_peers() ->
    ?PEER_REGISTRY ! {get, self()},
    receive
        {peers, Peers} -> Peers
    after 2000 ->
        ct:fail(no_observed_peer)
    end.

assert_observed_peer(Expected) ->
    ?assertEqual([Expected], observed_peers()).

assert_observed_peer_ip(ExpectedIp) ->
    case observed_peers() of
        [{ExpectedIp, _Port}] -> ok;
        Other -> ct:fail({unexpected_peer, Other, expected_ip, ExpectedIp})
    end.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").
