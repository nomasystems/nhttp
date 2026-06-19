%%%-----------------------------------------------------------------------------
%%% @doc Alt-Svc advertisement on the TCP path of a mixed listener.
%%%
%%% A listener serving `[http1_1, http2, http3]' auto-emits
%%% `Alt-Svc: h3=":<quic_port>"; ma=<seconds>' on its HTTP/1.1 and HTTP/2
%%% responses (RFC 7838 §3, RFC 9114 §3.1) so clients discover and upgrade
%%% to h3. These tests assert emission, the `port => 0' ephemeral case, the
%%% handler override / suppress hooks, the `ma' config, and that a TCP-only
%%% listener emits nothing.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_alt_svc_SUITE).

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
    h1_carries_alt_svc/1,
    h2_carries_alt_svc/1,
    port_zero_matches_quic_ephemeral/1,
    handler_override_replaces/1,
    handler_suppress_drops/1,
    ma_config_respected/1,
    tcp_only_no_alt_svc/1,
    alt_svc_false_disables/1,
    quic_draining_emits_clear_h1/1,
    quic_draining_emits_clear_h2/1,
    handler_override_wins_over_clear/1,
    clear_recovers_after_restart/1,
    inject_clear_unit/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        h1_carries_alt_svc,
        h2_carries_alt_svc,
        port_zero_matches_quic_ephemeral,
        handler_override_replaces,
        handler_suppress_drops,
        ma_config_respected,
        tcp_only_no_alt_svc,
        alt_svc_false_disables,
        quic_draining_emits_clear_h1,
        quic_draining_emits_clear_h2,
        handler_override_wins_over_clear,
        clear_recovers_after_restart,
        inject_clear_unit
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

end_per_testcase(_TestCase, _Config) ->
    case erlang:erase(listener_pid) of
        Pid when is_pid(Pid) -> catch nhttp:stop(Pid);
        _ -> ok
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

h1_carries_alt_svc(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(expected_value(QuicPort, 86400), h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

h2_carries_alt_svc(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    Headers = h2_get_headers(Pid, <<"/hello">>),
    ?assertEqual(expected_value(QuicPort, 86400), proplists:get_value(<<"alt-svc">>, Headers)),
    ok.

port_zero_matches_quic_ephemeral(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {ok, TcpPort} = nhttp:get_port(Pid, tcp),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    ?assertNotEqual(TcpPort, QuicPort),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    AltSvc = h1_header(Resp, <<"alt-svc">>),
    ?assertEqual(expected_value(QuicPort, 86400), AltSvc),
    ?assertEqual(nomatch, binary:match(AltSvc, integer_to_binary(TcpPort))),
    ssl:close(Sock),
    ok.

handler_override_replaces(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {Sock, Resp} = tls_h1_get(Pid, <<"/override">>),
    ?assertEqual(<<"h3=\":9999\"; ma=1">>, h1_header(Resp, <<"alt-svc">>)),
    ?assertEqual(1, count_occurrences(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

handler_suppress_drops(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {Sock, Resp} = tls_h1_get(Pid, <<"/suppress">>),
    ?assertEqual(undefined, h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

ma_config_respected(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{alt_svc => #{ma => 3600}}),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(expected_value(QuicPort, 3600), h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

tcp_only_no_alt_svc(Config) ->
    Pid = start_listener(Config, [http1_1, http2], #{}),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(undefined, h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

alt_svc_false_disables(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{alt_svc => false}),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(undefined, h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

quic_draining_emits_clear_h1(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    ok = nhttp_conn_tracker:stop_advertising(quic_tracker(Pid)),
    {Sock, Resp} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(<<"clear">>, h1_header(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

quic_draining_emits_clear_h2(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    ok = nhttp_conn_tracker:stop_advertising(quic_tracker(Pid)),
    Headers = h2_get_headers(Pid, <<"/hello">>),
    ?assertEqual(<<"clear">>, proplists:get_value(<<"alt-svc">>, Headers)),
    ok.

handler_override_wins_over_clear(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    ok = nhttp_conn_tracker:stop_advertising(quic_tracker(Pid)),
    {Sock, Resp} = tls_h1_get(Pid, <<"/override">>),
    ?assertEqual(<<"h3=\":9999\"; ma=1">>, h1_header(Resp, <<"alt-svc">>)),
    ?assertEqual(1, count_occurrences(Resp, <<"alt-svc">>)),
    ssl:close(Sock),
    ok.

clear_recovers_after_restart(Config) ->
    Pid = start_listener(Config, [http1_1, http2, http3], #{}),
    {ok, QuicPort} = nhttp:get_port(Pid, quic),
    Tracker0 = quic_tracker(Pid),
    ok = nhttp_conn_tracker:stop_advertising(Tracker0),
    {Sock0, Resp0} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(<<"clear">>, h1_header(Resp0, <<"alt-svc">>)),
    ssl:close(Sock0),

    exit(Tracker0, kill),
    ok = nhttp_test_helpers:wait_until(
        fun() ->
            case quic_tracker(Pid) of
                undefined -> false;
                Tracker1 -> is_pid(Tracker1) andalso Tracker1 =/= Tracker0
            end
        end,
        5000
    ),

    {Sock1, Resp1} = tls_h1_get(Pid, <<"/hello">>),
    ?assertEqual(expected_value(QuicPort, 86400), h1_header(Resp1, <<"alt-svc">>)),
    ssl:close(Sock1),
    ok.

inject_clear_unit(_Config) ->
    ?assertEqual(
        [{<<"alt-svc">>, <<"clear">>}],
        nhttp_alt_svc:inject([], clear)
    ),
    ?assertEqual(
        [{<<"alt-svc">>, <<"h3=\":443\"">>}],
        nhttp_alt_svc:inject([{<<"alt-svc">>, <<"h3=\":443\"">>}], clear)
    ),
    ?assertEqual(
        [],
        nhttp_alt_svc:inject([{<<"alt-svc">>, <<>>}], clear)
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

-spec start_listener(ct_suite:ct_config(), [atom()], map()) -> pid().
start_listener(Config, Versions, Extra) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = maps:merge(
        #{
            port => 0,
            handler => nhttp_alt_svc_handler,
            versions => Versions,
            tls => #{certfile => CertFile, keyfile => KeyFile},
            acceptor_count => 2
        },
        Extra
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    erlang:put(listener_pid, Pid),
    Pid.

-spec expected_value(inet:port_number(), non_neg_integer()) -> binary().
expected_value(Port, Ma) ->
    nhttp_alt_svc:header_value(Port, Ma).

-spec tls_h1_get(pid(), binary()) -> {ssl:sslsocket(), binary()}.
tls_h1_get(Pid, Path) ->
    {ok, Port} = nhttp:get_port(Pid, tcp),
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
    Req = [<<"GET ">>, Path, <<" HTTP/1.1\r\nHost: localhost\r\n\r\n">>],
    ok = ssl:send(Sock, Req),
    {ok, Resp} = ssl:recv(Sock, 0, 5000),
    {Sock, Resp}.

-spec h2_get_headers(pid(), binary()) -> [{binary(), binary()}].
h2_get_headers(Pid, Path) ->
    {ok, Port} = nhttp:get_port(Pid, tcp),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, Path),
    Frames = nhttp_test_helpers:h2_recv(Sock, 2000),
    ssl:close(Sock),
    case [Block || {headers, 1, Block, _} <- Frames] of
        [HeaderBlock | _] ->
            {ok, Dec} = nhttp_hpack:new(),
            case nhttp_hpack:decode(HeaderBlock, Dec) of
                {ok, Headers, _Dec1} -> Headers;
                _ -> []
            end;
        [] ->
            []
    end.

-spec h1_header(binary(), binary()) -> binary() | undefined.
h1_header(Resp, Name) ->
    Lower = string:lowercase(Name),
    Lines = binary:split(Resp, <<"\r\n">>, [global]),
    h1_find_header(Lines, Lower).

-spec h1_find_header([binary()], binary()) -> binary() | undefined.
h1_find_header([], _Lower) ->
    undefined;
h1_find_header([Line | Rest], Lower) ->
    case binary:split(Line, <<":">>) of
        [Key, Value] ->
            case string:lowercase(string:trim(Key)) =:= Lower of
                true -> string:trim(Value);
                false -> h1_find_header(Rest, Lower)
            end;
        _ ->
            h1_find_header(Rest, Lower)
    end.

-spec count_occurrences(binary(), binary()) -> non_neg_integer().
count_occurrences(Haystack, Needle) ->
    length(binary:matches(Haystack, Needle)).

-spec quic_tracker(pid()) -> pid() | undefined.
quic_tracker(ListenerPid) ->
    case lists:keyfind({nhttp_transport_sup, quic}, 1, supervisor:which_children(ListenerPid)) of
        {_, QuicSup, _, _} when is_pid(QuicSup) ->
            case lists:keyfind(nhttp_conn_tracker, 1, supervisor:which_children(QuicSup)) of
                {_, Tracker, _, _} when is_pid(Tracker) -> Tracker;
                _ -> undefined
            end;
        _ ->
            undefined
    end.
