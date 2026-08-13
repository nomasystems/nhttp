%%%-----------------------------------------------------------------------------
%%% @doc Integration test suite for nhttp_conn_h3 module.
%%%
%%% Tests HTTP/3 connection handling using real nquic QUIC connections and
%%% the nhttp_h3 client state machine for protocol-level exchanges.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_h3_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------
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

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    h3_basic_get/1,
    h3_post_with_body/1,
    h3_handler_error/1,
    h3_handler_crash_isolates_stream/1,
    h3_multiple_streams/1,
    h3_empty_body_response/1,
    h3_all_methods/1,
    h3_handler_init_error/1,
    h3_stream_iterator/1,
    h3_stream_iterator_fin_only/1,
    h3_limit_uri_too_long/1,
    h3_limit_header_too_large/1,
    h3_limit_body_too_large/1,
    h3_compression_gzip/1,
    h3_compression_disabled/1,
    h3_compression_below_threshold/1,
    h3_compression_no_content_type/1,
    h3_compression_empty_body/1,
    h3_stream_reset/1,
    h3_connection_error/1,
    h3_goaway_event/1,
    h3_idle_timeout/1,
    h3_hibernate/1,
    h3_graceful_shutdown/1,
    h3_parent_exit/1,
    h3_quic_closed/1,
    h3_handler_terminate/1,
    h3_system_get_status/1,
    h3_system_suspend_resume/1,
    h3_system_code_change/1,
    h3_system_terminate/1,
    h3_listener_integration/1,
    h3_listener_integration_otel/1,
    h3_listener_drain/1,
    h3_listener_validate_quic_opts/1,
    h3_websocket_upgrade/1,
    h3_websocket_text_message/1,
    h3_websocket_ping/1,
    h3_websocket_close/1,
    h3_websocket_handler_noreply/1,
    h3_websocket_no_handler/1,
    h3_websocket_upgrade_rejected/1,
    h3_websocket_stream_reset/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, h3_requests},
        {group, h3_streaming},
        {group, h3_limits},
        {group, h3_compression},
        {group, h3_protocol},
        {group, h3_lifecycle},
        {group, h3_system},
        {group, h3_integration},
        {group, h3_websocket}
    ].

groups() ->
    [
        {h3_requests, [sequence], [
            h3_basic_get,
            h3_post_with_body,
            h3_handler_error,
            h3_handler_crash_isolates_stream,
            h3_multiple_streams,
            h3_empty_body_response,
            h3_all_methods,
            h3_handler_init_error
        ]},
        {h3_streaming, [sequence], [
            h3_stream_iterator,
            h3_stream_iterator_fin_only
        ]},
        {h3_limits, [sequence], [
            h3_limit_uri_too_long,
            h3_limit_header_too_large,
            h3_limit_body_too_large
        ]},
        {h3_compression, [sequence], [
            h3_compression_gzip,
            h3_compression_disabled,
            h3_compression_below_threshold,
            h3_compression_no_content_type,
            h3_compression_empty_body
        ]},
        {h3_protocol, [sequence], [
            h3_stream_reset,
            h3_connection_error,
            h3_goaway_event
        ]},
        {h3_lifecycle, [sequence], [
            h3_idle_timeout,
            h3_hibernate,
            h3_graceful_shutdown,
            h3_parent_exit,
            h3_quic_closed,
            h3_handler_terminate
        ]},
        {h3_system, [sequence], [
            h3_system_get_status,
            h3_system_suspend_resume,
            h3_system_code_change,
            h3_system_terminate
        ]},
        {h3_integration, [sequence], [
            h3_listener_integration,
            h3_listener_integration_otel,
            h3_listener_drain,
            h3_listener_validate_quic_opts
        ]},
        {h3_websocket, [sequence], [
            h3_websocket_upgrade,
            h3_websocket_text_message,
            h3_websocket_ping,
            h3_websocket_close,
            h3_websocket_handler_noreply,
            h3_websocket_no_handler,
            h3_websocket_upgrade_rejected,
            h3_websocket_stream_reset
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    TestConfDir = find_test_conf_dir(),
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    erase(h3_acceptor),
    case erase(h3_listener) of
        undefined ->
            ok;
        ListenerPid when is_pid(ListenerPid) ->
            case is_process_alive(ListenerPid) of
                true ->
                    try
                        nhttp:stop(ListenerPid)
                    catch
                        _:_ -> ok
                    end,
                    _ = nhttp_test_helpers:wait_until_down(ListenerPid, 1000),
                    ok;
                false ->
                    ok
            end
    end,
    try
        unregister(h3_conn_pid_receiver)
    catch
        _:_ -> ok
    end,
    flush_mailbox(),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST TESTS
%%%-----------------------------------------------------------------------------

h3_basic_get(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _Headers, Body, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    ?assertEqual(<<"Hello!">>, Body),

    close_h3(QConn),
    ok.

h3_post_with_body(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    ReqBody = <<"test body content">>,
    {ok, 200, _Headers, RespBody, _H3_1} =
        h3_request(QConn, H3, <<"POST">>, <<"/echo">>, ReqBody),
    ?assertEqual(ReqBody, RespBody),

    close_h3(QConn),
    ok.

h3_handler_error(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    Result = h3_request(QConn, H3, <<"GET">>, <<"/error">>, <<>>),
    ?assertMatch({error, {stream_reset, _}}, Result),

    close_h3(QConn),
    ok.

h3_handler_crash_isolates_stream(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    Crash = h3_request(QConn, H3, <<"GET">>, <<"/crash-error">>, <<>>),
    ?assertMatch({error, {stream_reset, _}}, Crash),

    {ok, 200, _Headers, Body, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    ?assertEqual(<<"Hello!">>, Body),

    close_h3(QConn),
    ok.

h3_multiple_streams(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _, Body1, H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    ?assertEqual(<<"Hello!">>, Body1),

    {ok, 200, _, Body2, _H3_2} = h3_request(QConn, H3_1, <<"POST">>, <<"/echo">>, <<"data">>),
    ?assertEqual(<<"data">>, Body2),

    close_h3(QConn),
    ok.

h3_empty_body_response(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _Headers, Body, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/empty">>, <<>>),
    ?assertEqual(<<>>, Body),

    close_h3(QConn),
    ok.

h3_all_methods(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    Methods = [
        <<"GET">>,
        <<"HEAD">>,
        <<"POST">>,
        <<"PUT">>,
        <<"DELETE">>,
        <<"OPTIONS">>,
        <<"PATCH">>,
        <<"TRACE">>,
        <<"CONNECT">>,
        <<"FOOBAR">>
    ],

    lists:foldl(
        fun(Method, H3Acc) ->
            {ok, 200, _, _, H3New} = h3_request(QConn, H3Acc, Method, <<"/hello">>, <<>>),
            H3New
        end,
        H3,
        Methods
    ),

    close_h3(QConn),
    ok.

h3_handler_init_error(Config) ->
    {_, _, Port} = start_h3_server(Config, #{handler_args => fail}),

    {ok, QConn} = nhttp_h3_test_client:connect_raw(Port),

    case nhttp_h3_test_client:expect_connection_close(QConn, 5000) of
        ok -> ok;
        {error, no_close} -> error(expected_connection_close)
    end.

%%%-----------------------------------------------------------------------------
%%% STREAMING TESTS
%%%-----------------------------------------------------------------------------

h3_stream_iterator(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _, Body, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/stream-iterator">>, <<>>),
    ?assert(binary:match(Body, <<"chunk1">>) =/= nomatch),
    ?assert(binary:match(Body, <<"chunk2">>) =/= nomatch),
    ?assert(binary:match(Body, <<"chunk3">>) =/= nomatch),

    close_h3(QConn),
    ok.

h3_stream_iterator_fin_only(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _, _Body, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/stream-iterator-fin">>, <<>>),

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% LIMIT TESTS
%%%-----------------------------------------------------------------------------

h3_limit_uri_too_long(Config) ->
    {_, _, Port} = start_h3_server(Config, #{max_uri_length => 50}),
    {QConn, H3} = connect_h3(Port),

    LongPath = <<"/", (binary:copy(<<"a">>, 100))/binary>>,
    {ok, 414, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, LongPath, <<>>),

    close_h3(QConn),
    ok.

h3_limit_header_too_large(Config) ->
    {_, _, Port} = start_h3_server(Config, #{max_header_value_length => 50}),
    {QConn, H3} = connect_h3(Port),

    LargeValue = binary:copy(<<"x">>, 100),
    {ok, 431, _, _, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/hello">>, [{<<"x-large">>, LargeValue}], <<>>),

    close_h3(QConn),
    ok.

h3_limit_body_too_large(Config) ->
    {_, _, Port} = start_h3_server(Config, #{max_body_size => 64}),
    {QConn, H3} = connect_h3(Port),

    LargeBody = binary:copy(<<"x">>, 1024),
    {ok, 413, _, _, _H3_1} =
        h3_request(QConn, H3, <<"POST">>, <<"/echo">>, LargeBody),

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% COMPRESSION TESTS
%%%-----------------------------------------------------------------------------

h3_compression_gzip(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, RespHeaders, Body, _H3_1} =
        h3_request(
            QConn,
            H3,
            <<"GET">>,
            <<"/large">>,
            [{<<"accept-encoding">>, <<"gzip">>}],
            <<>>
        ),

    ?assert(lists:keymember(<<"content-encoding">>, 1, RespHeaders)),
    ?assert(byte_size(Body) < 3700),

    close_h3(QConn),
    ok.

h3_compression_disabled(Config) ->
    {_, _, Port} = start_h3_server(Config, #{compression => false}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, RespHeaders, _Body, _H3_1} =
        h3_request(
            QConn,
            H3,
            <<"GET">>,
            <<"/large">>,
            [{<<"accept-encoding">>, <<"gzip">>}],
            <<>>
        ),

    ?assertNot(lists:keymember(<<"content-encoding">>, 1, RespHeaders)),

    close_h3(QConn),
    ok.

h3_compression_below_threshold(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, RespHeaders, Body, _H3_1} =
        h3_request(
            QConn,
            H3,
            <<"GET">>,
            <<"/small">>,
            [{<<"accept-encoding">>, <<"gzip">>}],
            <<>>
        ),

    ?assertNot(lists:keymember(<<"content-encoding">>, 1, RespHeaders)),
    ?assertEqual(<<"tiny">>, Body),

    close_h3(QConn),
    ok.

h3_compression_no_content_type(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, RespHeaders, _Body, _H3_1} =
        h3_request(
            QConn,
            H3,
            <<"GET">>,
            <<"/no-ct">>,
            [{<<"accept-encoding">>, <<"gzip">>}],
            <<>>
        ),

    ?assertNot(lists:keymember(<<"content-encoding">>, 1, RespHeaders)),

    close_h3(QConn),
    ok.

h3_compression_empty_body(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, RespHeaders, Body, _H3_1} =
        h3_request(
            QConn,
            H3,
            <<"GET">>,
            <<"/empty">>,
            [{<<"accept-encoding">>, <<"gzip">>}],
            <<>>
        ),

    ?assertNot(lists:keymember(<<"content-encoding">>, 1, RespHeaders)),
    ?assertEqual(<<>>, Body),

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% PROTOCOL EVENT TESTS
%%%-----------------------------------------------------------------------------

h3_stream_reset(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, StreamId} = nhttp_h3_test_client:open_stream(QConn, bidi),
    Headers = [
        {<<":method">>, <<"POST">>},
        {<<":path">>, <<"/echo">>},
        {<<":scheme">>, <<"https">>}
    ],
    {ok, H3_1} = nhttp_h3_test_client:send_h3_headers(QConn, H3, StreamId, Headers, nofin),

    ok = nhttp_h3_test_client:reset_stream(QConn, StreamId, 16#010c),
    timer:sleep(200),

    {ok, 200, _, _, _H3_2} = h3_request(QConn, H3_1, <<"GET">>, <<"/hello">>, <<>>),

    close_h3(QConn),
    ok.

h3_connection_error(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),

    {ok, DupStream} = nhttp_h3_test_client:open_stream(QConn, uni),
    ok = nhttp_h3_test_client:send_raw(QConn, DupStream, <<0>>),

    case nhttp_h3_test_client:expect_connection_close(QConn, 5000) of
        ok -> ok;
        {error, no_close} -> error(expected_connection_close)
    end.

h3_goaway_event(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {ok, 200, _, _, H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),

    {ok, _H3_2, GoActions} = nhttp_h3:send_goaway(H3_1),
    execute_client_actions(QConn, GoActions),

    timer:sleep(200),

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------

h3_idle_timeout(Config) ->
    {_, _, Port} = start_h3_server(Config, #{
        handler => nhttp_h3_compliance,
        timeouts => #{idle => 200}
    }),
    {QConn, _H3} = connect_h3(Port),

    case nhttp_h3_test_client:expect_connection_close(QConn, 5000) of
        ok -> ok;
        {error, no_close} -> error(idle_timeout_not_triggered)
    end.

h3_hibernate(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = nhttp_test_helpers:wait_until(
        fun() ->
            erlang:process_info(ConnPid, current_function) =:=
                {current_function, {erlang, hibernate, 3}}
        end,
        2000
    ),

    {ok, 200, _, _, _H3_2} = h3_request(QConn, H3_1, <<"GET">>, <<"/conn-pid">>, <<>>),
    true = is_process_alive(ConnPid),

    close_h3(QConn),
    ok.

h3_graceful_shutdown(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),

    ConnPid ! shutdown,

    receive
        {'DOWN', Ref, process, ConnPid, normal} -> ok
    after 5000 ->
        error(shutdown_timeout)
    end,

    close_h3(QConn),
    ok.

h3_parent_exit(Config) ->
    {ListenerPid, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,
    Ref = monitor(process, ConnPid),

    [TransportSup | _] = nhttp_test_helpers:transport_sups(ListenerPid),
    {_, TrackerPid, _, _} = lists:keyfind(
        nhttp_conn_tracker, 1, supervisor:which_children(TransportSup)
    ),
    exit(TrackerPid, kill),

    receive
        {'DOWN', Ref, process, ConnPid, Reason} ->
            error({unexpected_conn_exit, Reason})
    after 1000 ->
        ok
    end,
    true = is_process_alive(ConnPid),

    close_h3(QConn),
    ok.

h3_quic_closed(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),

    close_h3(QConn),

    receive
        {'DOWN', Ref, process, ConnPid, _} -> ok
    after 5000 ->
        error(quic_closed_timeout)
    end,
    ok.

h3_handler_terminate(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),
    close_h3(QConn),

    receive
        {'DOWN', Ref, process, ConnPid, normal} -> ok
    after 5000 ->
        error(terminate_timeout)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% SYSTEM MESSAGE TESTS
%%%-----------------------------------------------------------------------------

h3_system_get_status(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    timer:sleep(100),
    Status = sys:get_status(ConnPid),
    ?assertMatch({status, ConnPid, _, _}, Status),

    close_h3(QConn),
    ok.

h3_system_suspend_resume(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    timer:sleep(100),
    ok = sys:suspend(ConnPid),
    ok = sys:resume(ConnPid),

    ?assert(is_process_alive(ConnPid)),

    close_h3(QConn),
    ok.

h3_system_code_change(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    timer:sleep(100),
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttp_conn_h3, undefined, []),
    ok = sys:resume(ConnPid),

    ?assert(is_process_alive(ConnPid)),

    close_h3(QConn),
    ok.

h3_system_terminate(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),

    reregister(h3_conn_pid_receiver),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/conn-pid">>, <<>>),

    ConnPid =
        receive
            {h3_conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),
    timer:sleep(100),

    sys:terminate(ConnPid, test_shutdown),

    receive
        {'DOWN', Ref, process, ConnPid, _} -> ok
    after 5000 ->
        error(sys_terminate_timeout)
    end,

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTEGRATION TESTS
%%%-----------------------------------------------------------------------------

h3_listener_integration(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => nhttp_conn_h3_handler,
        acceptor_count => 2
    }),
    put(h3_listener, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _Headers, Body, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    <<"Hello!">> = Body,
    close_h3(QConn),

    nhttp:stop(ListenerPid),
    ok.

h3_listener_integration_otel(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => nhttp_conn_h3_handler,
        acceptor_count => 1,
        otel => #{traces => true, metrics => true}
    }),
    put(h3_listener, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),

    InitialActive = nhttp_otel:active_requests(),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _Headers, Body, _H3_1} =
        h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    <<"Hello!">> = Body,
    close_h3(QConn),

    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp_otel:active_requests() =:= InitialActive end, 2000
    ),

    nhttp:stop(ListenerPid),
    ok.

h3_listener_drain(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => nhttp_conn_h3_handler,
        acceptor_count => 1
    }),
    put(h3_listener, ListenerPid),
    {ok, Port} = nhttp:get_port(ListenerPid),

    {QConn, H3} = connect_h3(Port),
    {ok, 200, _, _, _H3_1} = h3_request(QConn, H3, <<"GET">>, <<"/hello">>, <<>>),
    close_h3(QConn),

    ok = nhttp_listener:drain(ListenerPid, 5000),
    nhttp:stop(ListenerPid),
    ok.

h3_listener_validate_quic_opts(_Config) ->
    {error, _} = nhttp:start_link(#{
        port => 0,
        versions => [http3],
        handler => nhttp_conn_h3_handler
    }),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/3 WEBSOCKET TESTS (RFC 9220)
%%%-----------------------------------------------------------------------------

h3_ws_upgrade(QConn, H3, Path) ->
    {ok, StreamId} = nhttp_h3_test_client:open_stream(QConn, bidi),
    Headers = [
        {<<":method">>, <<"CONNECT">>},
        {<<":protocol">>, <<"websocket">>},
        {<<":path">>, Path},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>}
    ],
    {ok, H3_1} = nhttp_h3_test_client:send_h3_headers(QConn, H3, StreamId, Headers, nofin),
    {StreamId, H3_1}.

h3_ws_send(QConn, H3, StreamId, WsFrame) ->
    {ok, H3_1} = nhttp_h3_test_client:send_h3_data(QConn, H3, StreamId, WsFrame, nofin),
    H3_1.

h3_ws_recv(H3, QConn, StreamId, Timeout) ->
    case nhttp_h3_test_client:recv_events(QConn, H3, StreamId, Timeout) of
        {ok, Events, H3_1} -> {ok, Events, H3_1};
        {error, _} -> {error, timeout}
    end.

h3_websocket_upgrade(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),

    case h3_ws_recv(H3_1, QConn, StreamId, 2000) of
        {ok, Events, _H3_2} ->
            HasHeaders = lists:any(
                fun
                    ({response, _, _, _}) -> true;
                    (_) -> false
                end,
                Events
            ),
            ?assert(HasHeaders);
        {error, timeout} ->
            ok
    end,

    close_h3(QConn),
    ok.

h3_websocket_text_message(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),

    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"hello">>})),
    H3_3 = h3_ws_send(QConn, H3_2, StreamId, TextFrame),

    case h3_ws_recv(H3_3, QConn, StreamId, 2000) of
        {ok, Events, _H3_4} ->
            HasData = lists:any(
                fun
                    ({data, S, _, _}) when S =:= StreamId -> true;
                    (_) -> false
                end,
                Events
            ),
            ?assert(HasData);
        {error, timeout} ->
            ct:fail(ws_text_timeout)
    end,

    close_h3(QConn),
    ok.

h3_websocket_ping(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),
    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    PingFrame = iolist_to_binary(nhttp_ws:encode_masked(ping)),
    H3_3 = h3_ws_send(QConn, H3_2, StreamId, PingFrame),

    case h3_ws_recv(H3_3, QConn, StreamId, 2000) of
        {ok, Events, _H3_4} ->
            HasData = lists:any(
                fun
                    ({data, S, _, _}) when S =:= StreamId -> true;
                    (_) -> false
                end,
                Events
            ),
            ?assert(HasData);
        {error, timeout} ->
            ct:fail(ws_ping_timeout)
    end,

    close_h3(QConn),
    ok.

h3_websocket_close(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),
    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    CloseFrame = iolist_to_binary(nhttp_ws:encode_masked(close)),
    H3_3 = h3_ws_send(QConn, H3_2, StreamId, CloseFrame),

    case h3_ws_recv(H3_3, QConn, StreamId, 2000) of
        {ok, _Events, _H3_4} -> ok;
        {error, timeout} -> ok
    end,

    close_h3(QConn),
    ok.

h3_websocket_handler_noreply(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),
    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    PongFrame = iolist_to_binary(nhttp_ws:encode_masked(pong)),
    H3_3 = h3_ws_send(QConn, H3_2, StreamId, PongFrame),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"test">>})),
    H3_4 = h3_ws_send(QConn, H3_3, StreamId, TextFrame),

    case h3_ws_recv(H3_4, QConn, StreamId, 2000) of
        {ok, _Events, _H3_5} -> ok;
        {error, timeout} -> ct:fail(ws_noreply_timeout)
    end,

    close_h3(QConn),
    ok.

h3_websocket_no_handler(Config) ->
    {_, _, Port} = start_h3_server(Config, #{handler => nhttp_conn_no_ws_handler}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),
    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"hello">>})),
    H3_3 = h3_ws_send(QConn, H3_2, StreamId, TextFrame),

    PingFrame = iolist_to_binary(nhttp_ws:encode_masked(ping)),
    H3_4 = h3_ws_send(QConn, H3_3, StreamId, PingFrame),

    case h3_ws_recv(H3_4, QConn, StreamId, 2000) of
        {ok, _Events, _H3_5} -> ok;
        {error, timeout} -> ok
    end,

    close_h3(QConn),
    ok.

h3_websocket_upgrade_rejected(Config) ->
    {_, _, Port} = start_h3_server(Config, #{handler => nhttp_conn_no_content_type_handler}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),

    case h3_ws_recv(H3_1, QConn, StreamId, 2000) of
        {ok, Events, _H3_2} ->
            HasHeaders = lists:any(
                fun
                    ({response, S, #{status := 400}, _}) when S =:= StreamId -> true;
                    (_) -> false
                end,
                Events
            ),
            ?assert(HasHeaders);
        {error, timeout} ->
            ok
    end,

    close_h3(QConn),
    ok.

h3_websocket_stream_reset(Config) ->
    {_, _, Port} = start_h3_server(Config, #{}),
    {QConn, H3} = connect_h3(Port),

    {StreamId, H3_1} = h3_ws_upgrade(QConn, H3, <<"/ws">>),
    H3_2 = nhttp_h3_test_client:drain(QConn, H3_1, StreamId, 500),

    ok = nhttp_h3_test_client:reset_stream(QConn, StreamId, 16#010c),
    timer:sleep(200),

    {ok, 200, _, _, _H3_3} = h3_request(QConn, H3_2, <<"GET">>, <<"/hello">>, <<>>),

    close_h3(QConn),
    ok.

%%%-----------------------------------------------------------------------------
%%% SERVER HELPERS
%%%-----------------------------------------------------------------------------

start_h3_server(Config, Opts) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    BaseOpts = #{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => nhttp_conn_h3_handler,
        acceptor_count => 1
    },
    ListenerOpts = maps:merge(BaseOpts, Opts),
    {ok, ListenerPid} = nhttp:start_link(ListenerOpts),
    {ok, Port} = nhttp:get_port(ListenerPid),
    put(h3_listener, ListenerPid),
    {ListenerPid, undefined, Port}.

%%%-----------------------------------------------------------------------------
%%% CLIENT HELPERS
%%%-----------------------------------------------------------------------------

connect_h3(Port) ->
    nhttp_h3_test_client:connect(Port).

close_h3(QConn) ->
    nhttp_h3_test_client:close(QConn).

h3_request(QConn, H3, Method, Path, Body) ->
    h3_request(QConn, H3, Method, Path, [], Body).

h3_request(QConn, H3, Method, Path, ExtraHeaders, Body) ->
    nhttp_h3_test_client:request(QConn, H3, Method, Path, ExtraHeaders, Body).

%%%-----------------------------------------------------------------------------
%%% UTILITY FUNCTIONS
%%%-----------------------------------------------------------------------------

execute_client_actions(QConn, Actions) ->
    nhttp_h3_test_client:execute_actions(QConn, Actions).

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

flush_mailbox() ->
    receive
        _ -> flush_mailbox()
    after 0 ->
        ok
    end.

reregister(Name) ->
    try
        unregister(Name)
    catch
        _:_ -> ok
    end,
    register(Name, self()).
