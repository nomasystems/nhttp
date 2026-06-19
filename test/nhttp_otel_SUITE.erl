%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_otel module.
%%%
%%% Tests OpenTelemetry integration for configuration, spans, and metrics.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_otel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    config_disabled_by_default/1,
    config_enabled_boolean/1,
    config_enabled_map/1,
    config_traces_only/1,
    config_metrics_only/1,
    traces_disabled_no_span/1,
    traces_connection_span_end/1,
    traces_request_span_disabled/1,
    traces_request_span_error/1,
    traces_connection_rejected/1,
    active_requests_counter/1,
    active_requests_disabled/1,
    metrics_record_duration_disabled/1,
    metrics_record_duration_with_body_size/1,
    format_ipv6/1,
    integration_h1_request/1,
    integration_h1_ssl_request/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, config},
        {group, traces},
        {group, metrics},
        {group, helpers},
        {group, integration},
        {group, integration_ssl}
    ].

groups() ->
    [
        {config, [sequence], [
            config_disabled_by_default,
            config_enabled_boolean,
            config_enabled_map,
            config_traces_only,
            config_metrics_only
        ]},
        {traces, [sequence], [
            traces_disabled_no_span,
            traces_connection_span_end,
            traces_request_span_disabled,
            traces_request_span_error,
            traces_connection_rejected
        ]},
        {metrics, [sequence], [
            active_requests_counter,
            active_requests_disabled,
            metrics_record_duration_disabled,
            metrics_record_duration_with_body_size
        ]},
        {helpers, [sequence], [
            format_ipv6
        ]},
        {integration, [sequence], [
            integration_h1_request
        ]},
        {integration_ssl, [sequence], [
            integration_h1_ssl_request
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(nhttp),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(integration_ssl, Config) ->
    TestConfDir = find_test_conf_dir(),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init([]) -> {ok, #{}}.

handle_request(#{path := <<"/error">>}, State) ->
    {abort, test_error, State};
handle_request(_Req, State) ->
    {reply, #{status => 200, headers => [], body => <<"OK">>}, State}.

%%%-----------------------------------------------------------------------------
%%% CONFIGURATION TESTS
%%%-----------------------------------------------------------------------------

config_disabled_by_default(_Config) ->
    Result = nhttp_otel:config(#{}),
    ?assertEqual(false, Result).

config_enabled_boolean(_Config) ->
    Result = nhttp_otel:config(#{otel => true}),
    ?assertEqual(#{traces => true, metrics => true}, Result).

config_enabled_map(_Config) ->
    Result = nhttp_otel:config(#{otel => #{traces => true, metrics => true}}),
    ?assertEqual(#{traces => true, metrics => true}, Result).

config_traces_only(_Config) ->
    Result = nhttp_otel:config(#{otel => #{traces => true}}),
    ?assertEqual(#{traces => true, metrics => false}, Result),
    ?assert(nhttp_otel:traces_enabled(Result)),
    ?assertNot(nhttp_otel:metrics_enabled(Result)).

config_metrics_only(_Config) ->
    Result = nhttp_otel:config(#{otel => #{metrics => true}}),
    ?assertEqual(#{traces => false, metrics => true}, Result),
    ?assertNot(nhttp_otel:traces_enabled(Result)),
    ?assert(nhttp_otel:metrics_enabled(Result)).

%%%-----------------------------------------------------------------------------
%%% TRACES TESTS
%%%-----------------------------------------------------------------------------

traces_disabled_no_span(_Config) ->
    DisabledConfig = nhttp_otel:config(#{}),
    SpanCtx = nhttp_otel:connection_span_start(DisabledConfig, #{
        remote_ip => {127, 0, 0, 1},
        remote_port => 12345,
        transport => tcp,
        version => http1_1
    }),
    ?assertEqual(undefined, SpanCtx).

%%%-----------------------------------------------------------------------------
%%% METRICS TESTS
%%%-----------------------------------------------------------------------------

active_requests_counter(_Config) ->
    Config = nhttp_otel:config(#{otel => #{metrics => true}}),

    Initial = nhttp_otel:active_requests(),

    ok = nhttp_otel:incr_active_requests(Config),
    ?assertEqual(Initial + 1, nhttp_otel:active_requests()),

    ok = nhttp_otel:incr_active_requests(Config),
    ?assertEqual(Initial + 2, nhttp_otel:active_requests()),

    ok = nhttp_otel:decr_active_requests(Config),
    ?assertEqual(Initial + 1, nhttp_otel:active_requests()),

    ok = nhttp_otel:decr_active_requests(Config),
    ?assertEqual(Initial, nhttp_otel:active_requests()),
    ok.

active_requests_disabled(_Config) ->
    Config = nhttp_otel:config(#{otel => #{metrics => false}}),
    Initial = nhttp_otel:active_requests(),

    ok = nhttp_otel:incr_active_requests(Config),
    ?assertEqual(Initial, nhttp_otel:active_requests()),

    ok = nhttp_otel:decr_active_requests(Config),
    ?assertEqual(Initial, nhttp_otel:active_requests()),

    ok = nhttp_otel:incr_active_requests(false),
    ?assertEqual(Initial, nhttp_otel:active_requests()),

    ok = nhttp_otel:decr_active_requests(false),
    ?assertEqual(Initial, nhttp_otel:active_requests()),

    persistent_term:erase({nhttp_otel, active_requests}),
    ok = nhttp_otel:decr_active_requests(#{metrics => true}),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL TRACES TESTS
%%%-----------------------------------------------------------------------------

traces_connection_span_end(_Config) ->
    ok = nhttp_otel:connection_span_end(undefined, #{
        requests => 5,
        reason => normal
    }),

    ok = nhttp_otel:connection_span_end(undefined, #{
        requests => 3,
        reason => timeout
    }),

    ok = nhttp_otel:connection_span_end(undefined, #{
        requests => 1,
        reason => socket_error
    }),

    EnabledConfig = nhttp_otel:config(#{otel => #{traces => true}}),

    SpanCtx1 = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {127, 0, 0, 1},
        remote_port => 12345,
        transport => tcp,
        version => http1_1
    }),
    ok = nhttp_otel:connection_span_end(SpanCtx1, #{
        requests => 10,
        reason => normal
    }),

    SpanCtx2 = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {127, 0, 0, 1},
        remote_port => 12346,
        transport => tcp,
        version => http1_1
    }),
    ok = nhttp_otel:connection_span_end(SpanCtx2, #{
        requests => 5,
        reason => timeout
    }),

    SpanCtx3 = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {127, 0, 0, 1},
        remote_port => 12347,
        transport => tcp,
        version => http1_1
    }),
    ok = nhttp_otel:connection_span_end(SpanCtx3, #{
        requests => 1,
        reason => socket_error
    }),
    ok.

traces_request_span_disabled(_Config) ->
    DisabledConfig = nhttp_otel:config(#{}),

    {SpanCtx, StartTime} = nhttp_otel:request_span_start(DisabledConfig, undefined, #{
        method => <<"GET">>,
        path => <<"/">>,
        scheme => <<"http">>,
        stream_id => 0
    }),
    ?assertEqual(undefined, SpanCtx),
    ?assert(is_integer(StartTime)),

    ok = nhttp_otel:request_span_end({undefined, StartTime}, #{status => 200}),

    ok = nhttp_otel:request_span_end({undefined, StartTime}, #{
        status => 200,
        response_body_size => 1024
    }),

    EnabledConfig = nhttp_otel:config(#{otel => #{traces => true}}),

    {SpanCtx2, StartTime2} = nhttp_otel:request_span_start(EnabledConfig, undefined, #{
        method => <<"POST">>,
        path => <<"/api/users">>,
        scheme => <<"https">>,
        stream_id => 1
    }),
    ok = nhttp_otel:request_span_end({SpanCtx2, StartTime2}, #{
        status => 201,
        response_body_size => 2048
    }),

    ConnSpanCtx = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {127, 0, 0, 1},
        remote_port => 12345,
        transport => tcp,
        version => http1_1
    }),
    {SpanCtx3, StartTime3} = nhttp_otel:request_span_start(EnabledConfig, ConnSpanCtx, #{
        method => <<"GET">>,
        path => <<"/health">>,
        scheme => <<"http">>,
        stream_id => 0
    }),
    ok = nhttp_otel:request_span_end({SpanCtx3, StartTime3}, #{status => 500}),

    ok = nhttp_otel:connection_span_end(ConnSpanCtx, #{
        requests => 2,
        reason => normal
    }),
    ok.

traces_request_span_error(_Config) ->
    ok = nhttp_otel:request_span_error(undefined, error, some_reason),

    StartTime = erlang:monotonic_time(),
    ok = nhttp_otel:request_span_error({undefined, StartTime}, error, test_error),

    EnabledConfig = nhttp_otel:config(#{otel => #{traces => true}}),
    {SpanCtx, _StartTime2} = nhttp_otel:request_span_start(EnabledConfig, undefined, #{
        method => <<"GET">>,
        path => <<"/crash">>,
        scheme => <<"http">>,
        stream_id => 0
    }),
    ok = nhttp_otel:request_span_error(SpanCtx, error, badarg),
    ok.

traces_connection_rejected(_Config) ->
    DisabledConfig = nhttp_otel:config(#{}),
    ok = nhttp_otel:connection_rejected(DisabledConfig, #{
        ref => test_ref,
        reason => at_capacity
    }),

    EnabledConfig = nhttp_otel:config(#{otel => #{traces => true}}),
    ok = nhttp_otel:connection_rejected(EnabledConfig, #{
        ref => test_ref2,
        reason => at_capacity
    }),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL METRICS TESTS
%%%-----------------------------------------------------------------------------

metrics_record_duration_disabled(_Config) ->
    DisabledConfig = nhttp_otel:config(#{}),
    StartTime = erlang:monotonic_time(),

    ok = nhttp_otel:record_request_duration(DisabledConfig, StartTime, #{status => 200}),
    ok.

metrics_record_duration_with_body_size(_Config) ->
    Config = nhttp_otel:config(#{otel => #{metrics => true}}),
    StartTime = erlang:monotonic_time(),

    ok = nhttp_otel:record_request_duration(Config, StartTime, #{
        status => 200,
        response_body_size => 4096
    }),

    ok = nhttp_otel:record_request_duration(Config, StartTime, #{
        status => 404
    }),

    ok = nhttp_otel:incr_active_requests(Config),
    ok = nhttp_otel:decr_active_requests(Config),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPER FUNCTION TESTS
%%%-----------------------------------------------------------------------------

format_ipv6(_Config) ->
    DisabledConfig = nhttp_otel:config(#{}),

    SpanCtx = nhttp_otel:connection_span_start(DisabledConfig, #{
        remote_ip => {0, 0, 0, 0, 0, 0, 0, 1},
        remote_port => 12345,
        transport => tcp,
        version => http1_1
    }),
    ?assertEqual(undefined, SpanCtx),

    SpanCtx2 = nhttp_otel:connection_span_start(DisabledConfig, #{
        remote_ip => {16#2001, 16#db8, 0, 0, 0, 0, 0, 1},
        remote_port => 443,
        transport => tcp,
        version => http2,
        server_port => 443
    }),
    ?assertEqual(undefined, SpanCtx2),

    EnabledConfig = nhttp_otel:config(#{otel => #{traces => true}}),

    _ = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {192, 168, 1, 100},
        remote_port => 54321,
        transport => tcp,
        version => http1_1
    }),

    _ = nhttp_otel:connection_span_start(EnabledConfig, #{
        remote_ip => {0, 0, 0, 0, 0, 0, 0, 1},
        remote_port => 12345,
        transport => tcp,
        version => http2,
        server_port => 8443
    }),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTEGRATION TESTS
%%%-----------------------------------------------------------------------------

integration_h1_request(_Config) ->
    Opts = #{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        otel => #{traces => true, metrics => true}
    },
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),

    InitialActive = nhttp_otel:active_requests(),

    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    ok = gen_tcp:send(Socket, Request),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),

    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp_otel:active_requests() =:= InitialActive end, 2000
    ),

    nhttp:stop(Pid),
    ok.

integration_h1_ssl_request(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = #{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        tls => #{certfile => CertFile, keyfile => KeyFile},
        otel => #{traces => true, metrics => true}
    },
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),

    InitialActive = nhttp_otel:active_requests(),

    {ok, Socket} = ssl:connect(
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
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    ok = ssl:send(Socket, Request),
    {ok, Response} = ssl:recv(Socket, 0, 5000),
    ssl:close(Socket),

    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp_otel:active_requests() =:= InitialActive end, 2000
    ),

    nhttp:stop(Pid),
    ok.
