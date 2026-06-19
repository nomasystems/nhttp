%%%-----------------------------------------------------------------------------
%%% @doc SNI tests: per-vhost certificates on a single listener.
%%% Covers both `tls.sni_hosts` (static lookup table) and `tls.sni_fun`
%%% (dynamic callback) shapes from `nhttp:tls/0`.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_sni_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    sni_hosts_routes_per_vhost/1,
    sni_fun_routes_per_vhost/1,
    sni_fallback_to_default/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

%%%-----------------------------------------------------------------------------
%%% SUITE SETUP
%%%-----------------------------------------------------------------------------

all() ->
    [
        sni_hosts_routes_per_vhost,
        sni_fun_routes_per_vhost,
        sni_fallback_to_default
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    ConfDir = find_test_conf_dir(),
    Server = #{
        certfile => filename:join(ConfDir, "server.pem"),
        keyfile => filename:join(ConfDir, "server.key")
    },
    VhostA = #{
        certfile => filename:join(ConfDir, "vhost_a.pem"),
        keyfile => filename:join(ConfDir, "vhost_a.key")
    },
    VhostB = #{
        certfile => filename:join(ConfDir, "vhost_b.pem"),
        keyfile => filename:join(ConfDir, "vhost_b.key")
    },
    case
        lists:all(fun filelib:is_file/1, [
            maps:get(certfile, Server),
            maps:get(certfile, VhostA),
            maps:get(certfile, VhostB)
        ])
    of
        true ->
            [{server, Server}, {vhost_a, VhostA}, {vhost_b, VhostB} | Config];
        false ->
            {skip, "Test certificates missing. Run test/conf/gen_test_certs.sh"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

sni_hosts_routes_per_vhost(Config) ->
    Server = ?config(server, Config),
    VhostA = ?config(vhost_a, Config),
    VhostB = ?config(vhost_b, Config),
    SniHosts = [
        {"a.test.local", [
            {certfile, maps:get(certfile, VhostA)},
            {keyfile, maps:get(keyfile, VhostA)}
        ]},
        {"b.test.local", [
            {certfile, maps:get(certfile, VhostB)},
            {keyfile, maps:get(keyfile, VhostB)}
        ]}
    ],
    Tls = Server#{sni_hosts => SniHosts},
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        tls => Tls
    }),
    try
        {ok, Port} = nhttp:get_port(Pid),
        ?assertEqual(<<"a.test.local">>, peer_cn(Port, "a.test.local")),
        ?assertEqual(<<"b.test.local">>, peer_cn(Port, "b.test.local"))
    after
        nhttp:stop(Pid)
    end.

sni_fun_routes_per_vhost(Config) ->
    Server = ?config(server, Config),
    VhostA = ?config(vhost_a, Config),
    VhostB = ?config(vhost_b, Config),
    SniFun = fun
        ("a.test.local") ->
            [
                {certfile, maps:get(certfile, VhostA)},
                {keyfile, maps:get(keyfile, VhostA)}
            ];
        ("b.test.local") ->
            [
                {certfile, maps:get(certfile, VhostB)},
                {keyfile, maps:get(keyfile, VhostB)}
            ];
        (_) ->
            undefined
    end,
    Tls = Server#{sni_fun => SniFun},
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        tls => Tls
    }),
    try
        {ok, Port} = nhttp:get_port(Pid),
        ?assertEqual(<<"a.test.local">>, peer_cn(Port, "a.test.local")),
        ?assertEqual(<<"b.test.local">>, peer_cn(Port, "b.test.local"))
    after
        nhttp:stop(Pid)
    end.

sni_fallback_to_default(Config) ->
    Server = ?config(server, Config),
    VhostA = ?config(vhost_a, Config),
    SniHosts = [
        {"a.test.local", [
            {certfile, maps:get(certfile, VhostA)},
            {keyfile, maps:get(keyfile, VhostA)}
        ]}
    ],
    Tls = Server#{sni_hosts => SniHosts},
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        tls => Tls
    }),
    try
        {ok, Port} = nhttp:get_port(Pid),
        ?assertEqual(<<"a.test.local">>, peer_cn(Port, "a.test.local")),
        ?assertEqual(<<"localhost">>, peer_cn(Port, "localhost"))
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

peer_cn(Port, ServerName) ->
    Opts = [
        binary,
        {active, false},
        {verify, verify_none},
        {server_name_indication, ServerName}
    ],
    {ok, Sock} = ssl:connect("127.0.0.1", Port, Opts, 5000),
    try
        {ok, Der} = ssl:peercert(Sock),
        Cert = public_key:pkix_decode_cert(Der, otp),
        subject_cn(Cert)
    after
        ssl:close(Sock)
    end.

subject_cn(#'OTPCertificate'{tbsCertificate = TBS}) ->
    {rdnSequence, RDNs} = TBS#'OTPTBSCertificate'.subject,
    cn_from_rdns(RDNs).

cn_from_rdns([]) ->
    undefined;
cn_from_rdns([RDN | Rest]) ->
    case cn_from_rdn(RDN) of
        undefined -> cn_from_rdns(Rest);
        CN -> CN
    end.

cn_from_rdn([]) ->
    undefined;
cn_from_rdn([#'AttributeTypeAndValue'{type = ?'id-at-commonName', value = Value} | _]) ->
    cn_value(Value);
cn_from_rdn([_ | Rest]) ->
    cn_from_rdn(Rest).

cn_value({utf8String, V}) -> V;
cn_value({printableString, V}) -> list_to_binary(V);
cn_value({teletexString, V}) -> list_to_binary(V);
cn_value({bmpString, V}) -> list_to_binary(V);
cn_value({universalString, V}) -> list_to_binary(V);
cn_value(V) when is_binary(V) -> V;
cn_value(V) when is_list(V) -> list_to_binary(V).
