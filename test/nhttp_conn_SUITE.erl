%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_conn module - connection handler coverage.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

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
    h2_full_request_response/1,
    h2_request_with_body/1,
    h2_multiple_streams/1,
    h2_multiple_streams_with_bodies/1,
    h2_handler_error/1,
    h2_stream_iterator/1,
    h2_all_methods/1,
    h2_limit_uri_too_long/1,
    h2_limit_header_too_large/1,
    h2_limit_body_too_large/1,
    h2_goaway_event/1,
    h2_data_unknown_stream/1,
    h2_settings_ack/1,
    h2_stream_reset/1,
    h2_empty_body_response/1,
    h1_partial_request/1,
    h1_malformed_request/1,
    h1_duplicate_host/1,
    h1_missing_host/1,
    h1_handler_init_error/1,
    h1_handler_error/1,
    h1_handler_crash_variants/1,
    h1_handler_crash_burst/1,
    h1_handler_init_crash/1,
    h1_handler_terminate_crash/1,
    h1_websocket_handler_crash/1,
    h1_websocket_handler_crash_on_ping/1,
    h1_websocket_handler_crash_on_ping_with_data/1,
    h1_websocket_handler_crash_on_close/1,
    h2_handler_crash_isolates_stream/1,
    h2_websocket_connect_crash/1,
    h2_websocket_message_crash/1,
    h1_all_methods/1,
    h1_connect_trace_methods/1,
    h1_pipeline_two_requests/1,
    h1_pipeline_mixed_methods/1,
    h1_pipeline_close_after/1,
    h1_pipeline_depth_limit/1,
    h1_pipeline_shutdown_mid_batch/1,
    h1_stream_iterator/1,
    h1_stream_iterator_fin_only/1,
    h1_limit_uri_too_long/1,
    h1_limit_header_too_large/1,
    h1_limit_body_too_large/1,
    h1_limit_incomplete_headers_capped/1,
    h1_limit_unterminated_header_line_capped/1,
    h1_request_timeout_head_slowloris/1,
    h1_request_timeout_keepalive_not_killed/1,
    h1_body_deadline_slow_body/1,
    h1_body_deadline_default_allows_slow_body/1,
    h1_compression_disabled/1,
    h1_compression_below_threshold/1,
    h1_compression_gzip/1,
    h1_compression_no_accept_encoding/1,
    h1_websocket_upgrade/1,
    h1_websocket_ping/1,
    h1_websocket_ping_with_data/1,
    h1_websocket_pong/1,
    h1_websocket_pong_with_data/1,
    h1_websocket_binary_message/1,
    h1_websocket_close/1,
    h1_websocket_close_with_code/1,
    h1_websocket_idle_timeout/1,
    h1_websocket_shutdown/1,
    h1_websocket_no_handler/1,
    h1_websocket_ping_reply/1,
    h1_websocket_close_noreply/1,
    h1_websocket_ping_no_handler/1,
    h1_websocket_ping_data_no_handler/1,
    h1_websocket_ping_close/1,
    h1_websocket_ping_data_close/1,
    h1_websocket_close_reply/1,
    h1_websocket_ping_data_reply/1,
    h1_websocket_close_code_noreply/1,
    h1_websocket_close_code_no_handler/1,
    conn_tcp_closed/1,
    conn_ssl_closed/1,
    conn_idle_timeout/1,
    conn_graceful_shutdown_h1/1,
    conn_graceful_shutdown_h2/1,
    conn_system_messages/1,
    conn_parent_exit/1,
    alpn_http1_1/1,
    alpn_h2_not_allowed/1,
    alpn_no_alpn_fallback/1,
    alpn_http1_0/1,
    alpn_h2_first_fallback/1,
    h1_http1_0_no_keepalive/1,
    h1_http1_0_explicit_keepalive/1,
    h1_compression_empty_body/1,
    h1_compression_no_content_type/1,
    h1_compression_deflate/1,
    h2_window_update/1,
    h1_websocket_invalid_upgrade/1,
    h1_websocket_protocol_error/1,
    conn_ssl_error/1,
    h1_connection_close_header/1,
    h2_connection_preface_timeout/1,
    h2_ping_frame/1,
    h2_priority_frame/1,
    h1_all_other_methods/1,
    h1_system_code_change/1,
    h1_system_terminate/1,
    h2_stream_iterator_direct/1,
    h1_yield_continue_buffer/1,
    h1_resp_connection_close/1,
    h1_compression_fail/1,
    h2_send_headers_stream_closed/1,
    h1_response_connection_close/1,
    h2_websocket_upgrade/1,
    h2_websocket_text_message/1,
    h2_websocket_binary_message/1,
    h2_websocket_ping/1,
    h2_websocket_pong/1,
    h2_websocket_close/1,
    h2_websocket_close_with_code/1,
    h2_websocket_handler_close/1,
    h2_websocket_handler_noreply/1,
    h2_websocket_no_handler/1,
    h2_websocket_upgrade_rejected/1,
    h2_websocket_stream_reset/1,
    h2_large_body_flow_control/1,
    h1_websocket_ssl_upgrade/1,
    h1_websocket_ssl_text/1,
    h2_iterator_trailers/1,
    h1_iterator_trailers_graceful/1,
    h1_hibernate_keepalive/1,
    h1_hibernate_idle_timeout/1,
    h2_hibernate_ping/1,
    h1_websocket_hibernate_ping/1
]).

-behaviour(nhttp_handler).
-export([
    init/1,
    handle_request/2,
    handle_request_body/3,
    terminate/2,
    handle_ws_open/2,
    handle_ws_frame/3,
    handle_ws_closed/3
]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(crash) ->
    erlang:error(intentional_init_crash);
init(fail) ->
    {error, intentional_failure};
init({terminate_crash, _} = Args) ->
    {ok, Args};
init(ws_crash_all) ->
    {ok, ws_crash_all};
init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/error">>}, State) ->
    {abort, handler_error, State};
handle_request(#{path := <<"/crash-exit">>}, _State) ->
    exit(intentional_handler_exit);
handle_request(#{path := <<"/crash-error">>}, _State) ->
    erlang:error(intentional_handler_error);
handle_request(#{path := <<"/crash-throw">>}, _State) ->
    throw(intentional_handler_throw);
handle_request(#{path := <<"/crash-badmatch">>}, _State) ->
    {ok, _} = crash_opaque_result();
handle_request(#{path := <<"/ws-crash">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/conn-pid">>}, State) ->
    case whereis(conn_pid_receiver) of
        undefined -> ok;
        Pid -> Pid ! {conn_pid, self()}
    end,
    {reply, nhttp_resp:ok(<<"ok">>), State};
handle_request(#{path := <<"/slow-pipeline">>}, State) ->
    timer:sleep(100),
    {reply, nhttp_resp:ok(<<"ok">>), State};
handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/large">>}, State) ->
    Body = binary:copy(<<"Hello World! This is a test message. ">>, 100),
    {reply,
        #{
            status => 200,
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body => Body
        },
        State};
handle_request(#{path := <<"/small">>}, State) ->
    {reply,
        #{
            status => 200,
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body => <<"tiny">>
        },
        State};
handle_request(#{path := <<"/empty">>}, State) ->
    {reply, #{status => 200, headers => [], body => <<>>}, State};
handle_request(#{path := <<"/resp-close">>}, State) ->
    {reply,
        #{
            status => 200,
            headers => [{<<"connection">>, <<"close">>}],
            body => <<"closing">>
        },
        State};
handle_request(#{path := <<"/stream-iterator">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    Producer = stream_chunks_producer([<<"chunk1">>, <<"chunk2">>, <<"chunk3">>]),
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/stream-iterator-fin">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    Producer = stream_chunks_producer([]),
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/stream-iterator-trailers">>}, State) ->
    Headers = [{<<"content-type">>, <<"application/grpc">>}],
    Trailers = [{<<"grpc-status">>, <<"0">>}, {<<"grpc-message">>, <<"OK">>}],
    Producer = stream_chunks_with_trailers_producer(
        [<<"data1">>, <<"data2">>], Trailers
    ),
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{method := Method}, State) ->
    MethodBin = method_to_binary(Method),
    {reply, nhttp_resp:ok(<<"Method: ", MethodBin/binary>>), State}.

handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

method_to_binary(Method) -> nhttp_lib:encode_method(Method).

terminate(_Reason, {terminate_crash, _}) ->
    erlang:error(intentional_terminate_crash);
terminate(_Reason, _State) ->
    ok.

crash_opaque_result() ->
    case erlang:unique_integer() of
        _ -> {error, boom}
    end.

stream_chunks_producer(Chunks) ->
    fun(SendChunk) ->
        lists:foreach(fun(Chunk) -> _ = SendChunk(Chunk) end, Chunks),
        ok
    end.

stream_chunks_with_trailers_producer(Chunks, Trailers) ->
    fun(SendChunk) ->
        lists:foreach(fun(Chunk) -> _ = SendChunk(Chunk) end, Chunks),
        {trailers, Trailers}
    end.

handle_ws_open(_Session, ws_crash_all) ->
    erlang:error(intentional_ws_crash_all);
handle_ws_open(_Session, State) ->
    {ok, State, #{deliver_ping => true}}.

handle_ws_frame(_Frame, _Session, ws_crash_all) ->
    erlang:error(intentional_ws_crash_all);
handle_ws_frame({text, <<"crash">>}, _Session, _State) ->
    erlang:error(intentional_ws_crash);
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, <<"echo: ", Data/binary>>}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

handle_ws_closed(_Reason, _Session, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% SUITE SETUP
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, h2_requests},
        {group, h1_edge_cases},
        {group, h1_pipelining},
        {group, h1_streaming},
        {group, h1_limits},
        {group, h1_timeouts},
        {group, h1_compression},
        {group, h1_websocket},
        {group, conn_events},
        {group, protocol_detection},
        {group, h1_http1_0},
        {group, h1_compression_extra},
        {group, h2_flow_control},
        {group, ws_errors},
        {group, coverage_extra},
        {group, h2_websocket},
        {group, h2_trailers},
        {group, h1_websocket_ssl},
        {group, handler_crashes},
        {group, hibernation}
    ].

groups() ->
    [
        {h2_requests, [sequence], [
            h2_full_request_response,
            h2_request_with_body,
            h2_multiple_streams,
            h2_multiple_streams_with_bodies,
            h2_handler_error,
            h2_stream_iterator,
            h2_all_methods,
            h2_limit_uri_too_long,
            h2_limit_header_too_large,
            h2_limit_body_too_large,
            h2_goaway_event,
            h2_data_unknown_stream,
            h2_settings_ack,
            h2_stream_reset,
            h2_empty_body_response
        ]},
        {h1_edge_cases, [sequence], [
            h1_partial_request,
            h1_malformed_request,
            h1_duplicate_host,
            h1_missing_host,
            h1_handler_init_error,
            h1_handler_error,
            h1_all_methods,
            h1_connect_trace_methods
        ]},
        {h1_pipelining, [sequence], [
            h1_pipeline_two_requests,
            h1_pipeline_mixed_methods,
            h1_pipeline_close_after,
            h1_pipeline_depth_limit,
            h1_pipeline_shutdown_mid_batch
        ]},
        {h1_streaming, [sequence], [
            h1_stream_iterator,
            h1_stream_iterator_fin_only
        ]},
        {h1_limits, [sequence], [
            h1_limit_uri_too_long,
            h1_limit_header_too_large,
            h1_limit_body_too_large,
            h1_limit_incomplete_headers_capped,
            h1_limit_unterminated_header_line_capped
        ]},
        {h1_timeouts, [sequence], [
            h1_request_timeout_head_slowloris,
            h1_request_timeout_keepalive_not_killed,
            h1_body_deadline_slow_body,
            h1_body_deadline_default_allows_slow_body
        ]},
        {h1_compression, [sequence], [
            h1_compression_disabled,
            h1_compression_below_threshold,
            h1_compression_gzip,
            h1_compression_no_accept_encoding
        ]},
        {h1_websocket, [sequence], [
            h1_websocket_upgrade,
            h1_websocket_ping,
            h1_websocket_ping_with_data,
            h1_websocket_pong,
            h1_websocket_pong_with_data,
            h1_websocket_binary_message,
            h1_websocket_close,
            h1_websocket_close_with_code,
            h1_websocket_idle_timeout,
            h1_websocket_shutdown,
            h1_websocket_no_handler,
            h1_websocket_ping_reply,
            h1_websocket_close_noreply,
            h1_websocket_ping_no_handler,
            h1_websocket_ping_data_no_handler,
            h1_websocket_ping_close,
            h1_websocket_ping_data_close,
            h1_websocket_close_reply,
            h1_websocket_ping_data_reply,
            h1_websocket_close_code_noreply,
            h1_websocket_close_code_no_handler
        ]},
        {conn_events, [sequence], [
            conn_tcp_closed,
            conn_ssl_closed,
            conn_idle_timeout,
            conn_graceful_shutdown_h1,
            conn_graceful_shutdown_h2,
            conn_system_messages,
            conn_parent_exit
        ]},
        {hibernation, [sequence], [
            h1_hibernate_keepalive,
            h1_hibernate_idle_timeout,
            h2_hibernate_ping,
            h1_websocket_hibernate_ping
        ]},
        {protocol_detection, [sequence], [
            alpn_http1_1,
            alpn_h2_not_allowed,
            alpn_no_alpn_fallback,
            alpn_http1_0,
            alpn_h2_first_fallback
        ]},
        {h1_http1_0, [sequence], [
            h1_http1_0_no_keepalive,
            h1_http1_0_explicit_keepalive,
            h1_connection_close_header
        ]},
        {h1_compression_extra, [sequence], [
            h1_compression_empty_body,
            h1_compression_no_content_type,
            h1_compression_deflate
        ]},
        {h2_flow_control, [sequence], [
            h2_window_update,
            h2_connection_preface_timeout,
            h2_ping_frame,
            h2_priority_frame
        ]},
        {ws_errors, [sequence], [
            h1_websocket_invalid_upgrade,
            h1_websocket_protocol_error,
            conn_ssl_error
        ]},
        {coverage_extra, [sequence], [
            h1_all_other_methods,
            h1_system_code_change,
            h1_system_terminate,
            h2_stream_iterator_direct,
            h1_yield_continue_buffer,
            h1_resp_connection_close,
            h1_compression_fail,
            h2_send_headers_stream_closed,
            h1_response_connection_close
        ]},
        {h2_websocket, [sequence], [
            h2_websocket_upgrade,
            h2_websocket_text_message,
            h2_websocket_binary_message,
            h2_websocket_ping,
            h2_websocket_pong,
            h2_websocket_close,
            h2_websocket_close_with_code,
            h2_websocket_handler_close,
            h2_websocket_handler_noreply,
            h2_websocket_no_handler,
            h2_websocket_upgrade_rejected,
            h2_websocket_stream_reset,
            h2_large_body_flow_control
        ]},
        {h2_trailers, [sequence], [
            h2_iterator_trailers,
            h1_iterator_trailers_graceful
        ]},
        {h1_websocket_ssl, [sequence], [
            h1_websocket_ssl_upgrade,
            h1_websocket_ssl_text
        ]},
        {handler_crashes, [sequence], [
            h1_handler_crash_variants,
            h1_handler_crash_burst,
            h1_handler_init_crash,
            h1_handler_terminate_crash,
            h1_websocket_handler_crash,
            h1_websocket_handler_crash_on_ping,
            h1_websocket_handler_crash_on_ping_with_data,
            h1_websocket_handler_crash_on_close,
            h2_handler_crash_isolates_stream,
            h2_websocket_connect_crash,
            h2_websocket_message_crash
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) when
    Group =:= h2_requests;
    Group =:= conn_events;
    Group =:= protocol_detection;
    Group =:= h2_flow_control;
    Group =:= ws_errors;
    Group =:= coverage_extra;
    Group =:= h2_websocket;
    Group =:= h2_trailers;
    Group =:= h1_websocket_ssl;
    Group =:= handler_crashes
->
    TestConfDir = find_test_conf_dir(),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            {skip, "SSL certificates not found"}
    end;
init_per_group(h1_websocket, Config) ->
    process_flag(trap_exit, true),
    Config;
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
%%% HTTP/2 REQUEST TESTS
%%%-----------------------------------------------------------------------------

h2_full_request_response(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),

    {ok, _} = ssl:recv(Sock, 0, 5000),

    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#84, 16#87>>,
    Len = byte_size(HeaderBlock),
    HeadersFrame = <<Len:24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>,
    ok = ssl:send(Sock, HeadersFrame),

    {ok, RespData} = ssl:recv(Sock, 0, 5000),

    ?assert(byte_size(RespData) >= 9),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_request_with_body(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<
        16#83,
        16#87,
        16#44,
        5,
        "/echo"
    >>,
    Len = byte_size(HeaderBlock),
    HeadersFrame = <<Len:24, 1, 4, 0, 0, 0, 1, HeaderBlock/binary>>,
    ok = ssl:send(Sock, HeadersFrame),

    Body = <<"test body">>,
    BodyLen = byte_size(Body),
    DataFrame = <<BodyLen:24, 0, 1, 0, 0, 0, 1, Body/binary>>,
    ok = ssl:send(Sock, DataFrame),

    {ok, _RespData} = ssl:recv(Sock, 0, 5000),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_multiple_streams(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    H1 = <<16#82, 16#84, 16#87>>,
    ok = ssl:send(Sock, <<(byte_size(H1)):24, 1, 5, 0, 0, 0, 1, H1/binary>>),

    ok = ssl:send(Sock, <<(byte_size(H1)):24, 1, 5, 0, 0, 0, 3, H1/binary>>),

    {ok, _} = ssl:recv(Sock, 0, 5000),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_multiple_streams_with_bodies(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    H1 = <<16#83, 16#87, 16#44, 5, "/echo">>,
    ok = ssl:send(Sock, <<(byte_size(H1)):24, 1, 4, 0, 0, 0, 1, H1/binary>>),
    ok = ssl:send(Sock, <<3:24, 0, 1, 0, 0, 0, 1, "aaa">>),

    ok = ssl:send(Sock, <<(byte_size(H1)):24, 1, 4, 0, 0, 0, 3, H1/binary>>),
    ok = ssl:send(Sock, <<3:24, 0, 1, 0, 0, 0, 3, "bbb">>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),

    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_handler_error(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<
        16#82,
        16#87,
        16#44,
        6,
        "/error"
    >>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    {ok, RespData} = ssl:recv(Sock, 0, 5000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_handler_crash_isolates_stream(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    CrashHeaders = <<
        16#82,
        16#87,
        16#44,
        12,
        "/crash-error"
    >>,
    CrashLen = byte_size(CrashHeaders),
    ok = ssl:send(Sock, <<CrashLen:24, 1, 5, 0, 0, 0, 1, CrashHeaders/binary>>),

    CrashFrames = recv_h2_frames_until(
        Sock,
        2000,
        fun(F) -> match_headers_end_stream(F, 1) end
    ),
    ?assert(
        lists:any(fun(F) -> match_headers_end_stream(F, 1) end, CrashFrames),
        {no_headers_end_stream_for_stream_1, CrashFrames}
    ),

    OkHeaders = <<16#82, 16#84, 16#87>>,
    OkLen = byte_size(OkHeaders),
    ok = ssl:send(Sock, <<OkLen:24, 1, 5, 0, 0, 0, 3, OkHeaders/binary>>),

    {ok, OkResp} = ssl:recv(Sock, 0, 5000),
    ?assert(byte_size(OkResp) >= 9),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_handler_crash(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Key = base64:encode(crypto:strong_rand_bytes(16)),
    Upgrade = iolist_to_binary([
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: ">>,
        Key,
        <<"\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n\r\n">>
    ]),
    ok = gen_tcp:send(Sock, Upgrade),
    {ok, HandshakeResp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 101", _/binary>>, HandshakeResp),

    Payload = <<"crash">>,
    MaskKey = <<1, 2, 3, 4>>,
    Masked = mask(Payload, MaskKey),
    Frame = <<
        1:1, 0:3, 1:4, 1:1, (byte_size(Payload)):7, MaskKey/binary, Masked/binary
    >>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<1:1, _:3, 8:4, _/binary>>, CloseFrame),
    <<_:16, StatusCode:16, _/binary>> = CloseFrame,
    ?assertEqual(1011, StatusCode),

    gen_tcp:close(Sock),
    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h1_websocket_handler_crash_on_ping(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => ws_crash_all,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, Handshake} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    ok = gen_tcp:send(Sock, <<16#89, 16#80, MaskKey/binary>>),

    AllBytes = <<Handshake/binary, (drain_tcp(Sock))/binary>>,
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<16#88>>)),
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<1011:16>>)),
    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h1_websocket_handler_crash_on_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => ws_crash_all,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, Handshake} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Payload = mask(<<?WS_CLOSE_NORMAL:16>>, MaskKey),
    CloseFrame = <<16#88, 16#82, MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Sock, CloseFrame),

    AllBytes = <<Handshake/binary, (drain_tcp(Sock))/binary>>,
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<16#88>>)),
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<1011:16>>)),
    ?assert(is_process_alive(Pid)),

    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock2, ws_upgrade_request()),
    {ok, Handshake2} = gen_tcp:recv(Sock2, 0, 5000),
    EmptyClose = <<16#88, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock2, EmptyClose),
    AllBytes2 = <<Handshake2/binary, (drain_tcp(Sock2))/binary>>,
    ?assertNotEqual(nomatch, binary:match(AllBytes2, <<16#88>>)),
    ?assertNotEqual(nomatch, binary:match(AllBytes2, <<1011:16>>)),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    ok.

h1_websocket_handler_crash_on_ping_with_data(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => ws_crash_all,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, Handshake} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PingData = mask(<<"probe">>, MaskKey),
    PingFrame = <<16#89, 16#85, MaskKey/binary, PingData/binary>>,
    ok = gen_tcp:send(Sock, PingFrame),

    AllBytes = <<Handshake/binary, (drain_tcp(Sock))/binary>>,
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<16#88>>)),
    ?assertNotEqual(nomatch, binary:match(AllBytes, <<1011:16>>)),
    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h2_websocket_connect_crash(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<
        16#42,
        7,
        "CONNECT",
        16#40,
        9,
        ":protocol",
        9,
        "websocket",
        16#44,
        12,
        "/crash-error",
        16#87,
        16#41,
        9,
        "localhost"
    >>,
    Len = byte_size(HeaderBlock),
    ok = ssl:send(Sock, <<Len:24, 1, 4, 0, 0, 0, 1, HeaderBlock/binary>>),

    {ok, Resp} = ssl:recv(Sock, 0, 5000),
    ?assert(byte_size(Resp) > 0),

    ssl:close(Sock),
    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h2_websocket_message_crash(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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
    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    WsHeaders = <<
        16#42,
        7,
        "CONNECT",
        16#40,
        9,
        ":protocol",
        9,
        "websocket",
        16#44,
        3,
        "/ws",
        16#87,
        16#41,
        9,
        "localhost"
    >>,
    WsLen = byte_size(WsHeaders),
    ok = ssl:send(Sock, <<WsLen:24, 1, 4, 0, 0, 0, 1, WsHeaders/binary>>),
    _ = ssl:recv(Sock, 0, 500),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"crash">>})),
    TLen = byte_size(TextFrame),
    ok = ssl:send(Sock, <<TLen:24, 0, 0, 0, 0, 0, 1, TextFrame/binary>>),

    {ok, Resp} = ssl:recv(Sock, 0, 5000),
    ?assert(byte_size(Resp) > 0),

    ssl:close(Sock),
    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h1_partial_request(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\n">>),
    timer:sleep(50),
    ok = gen_tcp:send(Sock, <<"Host: localhost\r\n\r\n">>),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_malformed_request(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"INVALID REQUEST LINE\r\n\r\n">>),

    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Response} ->
            ?assertMatch(<<"HTTP/1.1 400", _/binary>>, Response);
        {error, closed} ->
            ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_duplicate_host(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n">>),
    ?assertMatch({ok, <<"HTTP/1.1 400", _/binary>>}, gen_tcp:recv(Sock, 0, 5000)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_missing_host(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\n\r\n">>),
    ?assertMatch({ok, <<"HTTP/1.1 400", _/binary>>}, gen_tcp:recv(Sock, 0, 5000)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_handler_init_error(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => fail,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),

    Result = gen_tcp:recv(Sock, 0, 2000),
    case Result of
        {error, closed} -> ok;
        {error, timeout} -> ok;
        {ok, _} -> ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_handler_error(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"GET /error HTTP/1.1\r\nHost: localhost\r\n\r\n">>),

    Result = gen_tcp:recv(Sock, 0, 2000),
    case Result of
        {error, closed} -> ok;
        {error, timeout} -> ok;
        {ok, _} -> ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_handler_crash_variants(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    Paths = [
        <<"/crash-exit">>,
        <<"/crash-error">>,
        <<"/crash-throw">>,
        <<"/crash-badmatch">>
    ],
    lists:foreach(
        fun(Path) ->
            {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            Req = <<"GET ", Path/binary, " HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
            ok = gen_tcp:send(Sock, Req),
            {ok, Response} = gen_tcp:recv(Sock, 0, 2000),
            ?assertMatch(<<"HTTP/1.1 500 ", _/binary>>, Response),
            ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 2000)),
            gen_tcp:close(Sock)
        end,
        Paths
    ),

    {ok, HealthSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(HealthSock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response} = gen_tcp:recv(HealthSock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    gen_tcp:close(HealthSock),

    nhttp:stop(Pid),
    ok.

h1_handler_crash_burst(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    Parent = self(),
    N = 50,
    Workers = [
        spawn_link(fun() ->
            {ok, S} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            ok = gen_tcp:send(
                S, <<"GET /crash-error HTTP/1.1\r\nHost: localhost\r\n\r\n">>
            ),
            _ = gen_tcp:recv(S, 0, 5000),
            gen_tcp:close(S),
            Parent ! {done, self()}
        end)
     || _ <- lists:seq(1, N)
    ],
    lists:foreach(
        fun(W) ->
            receive
                {done, W} -> ok
            after 10000 ->
                ct:fail({worker_stuck, W})
            end
        end,
        Workers
    ),

    ?assert(is_process_alive(Pid)),

    {ok, HealthSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(HealthSock, <<"GET /echo HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response} = gen_tcp:recv(HealthSock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    gen_tcp:close(HealthSock),

    nhttp:stop(Pid),
    ok.

h1_handler_init_crash(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => crash,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    lists:foreach(
        fun(_) ->
            {ok, S} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            _ = gen_tcp:send(S, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
            _ = gen_tcp:recv(S, 0, 1000),
            gen_tcp:close(S)
        end,
        lists:seq(1, 5)
    ),

    ?assert(is_process_alive(Pid)),
    nhttp:stop(Pid),
    ok.

h1_handler_terminate_crash(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        handler_args => {terminate_crash, undefined},
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(
        Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>
    ),
    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    gen_tcp:close(Sock),

    {error, timeout} = nhttp_test_helpers:wait_until_down(Pid, 100),
    ?assert(is_process_alive(Pid)),

    {ok, HealthSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(HealthSock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, HealthResp} = gen_tcp:recv(HealthSock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, HealthResp),
    gen_tcp:close(HealthSock),

    nhttp:stop(Pid),
    ok.

h1_all_methods(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    Methods = [<<"HEAD">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>, <<"PATCH">>],

    lists:foreach(
        fun(Method) ->
            {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            Request =
                <<Method/binary, " / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
            ok = gen_tcp:send(Sock, Request),
            {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
            ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
            gen_tcp:close(Sock)
        end,
        Methods
    ),

    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 PIPELINING TESTS
%%%-----------------------------------------------------------------------------

h1_pipeline_two_requests(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request1 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    Request2 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, <<Request1/binary, Request2/binary>>),

    Response = recv_all(Sock, <<>>, 5000),

    Count = count_responses(Response, <<"HTTP/1.1 200 OK">>),
    ?assertEqual(2, Count),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_pipeline_mixed_methods(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request1 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    Request2 = <<"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\ntest">>,
    ok = gen_tcp:send(Sock, <<Request1/binary, Request2/binary>>),

    Response = recv_all(Sock, <<>>, 5000),

    ?assertMatch({match, _}, re:run(Response, <<"Method: GET">>)),
    ?assertMatch({match, _}, re:run(Response, <<"test">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_pipeline_close_after(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request1 = <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    Request2 = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, <<Request1/binary, Request2/binary>>),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    Count = count_responses(Response, <<"HTTP/1.1 200 OK">>),
    ?assertEqual(1, Count),

    Result = gen_tcp:recv(Sock, 0, 500),
    ?assertMatch({error, _}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

recv_all(Sock, Acc, Timeout) ->
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            recv_all(Sock, <<Acc/binary, Data/binary>>, 100);
        {error, timeout} ->
            Acc;
        {error, _} ->
            Acc
    end.

recv_ssl_all(Sock, Acc, Timeout) ->
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            recv_ssl_all(Sock, <<Acc/binary, Data/binary>>, 100);
        {error, timeout} ->
            Acc;
        {error, _} ->
            Acc
    end.

count_responses(Binary, Pattern) ->
    count_responses(Binary, Pattern, 0).

count_responses(Binary, Pattern, Count) ->
    case binary:match(Binary, Pattern) of
        nomatch ->
            Count;
        {Pos, Len} ->
            Rest = binary:part(Binary, Pos + Len, byte_size(Binary) - Pos - Len),
            count_responses(Rest, Pattern, Count + 1)
    end.

reregister(Name) ->
    try
        unregister(Name)
    catch
        _:_ -> ok
    end,
    register(Name, self()).

%%%-----------------------------------------------------------------------------
%%% CONNECTION EVENT TESTS
%%%-----------------------------------------------------------------------------

conn_tcp_closed(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    gen_tcp:close(Sock),

    {error, timeout} = nhttp_test_helpers:wait_until_down(Pid, 100),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    ok.

conn_ssl_closed(Config) ->
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

    ssl:close(Sock),

    {error, timeout} = nhttp_test_helpers:wait_until_down(Pid, 100),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    ok.

conn_idle_timeout(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 100}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\n">>),

    Result = gen_tcp:recv(Sock, 0, 100),
    ?assertMatch({error, _}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

conn_graceful_shutdown_h1(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),

    ConnPid ! shutdown,

    receive
        {'DOWN', Ref, process, ConnPid, _Reason} -> ok
    after 5000 ->
        error(shutdown_timeout)
    end,

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

conn_graceful_shutdown_h2(Config) ->
    {CertFile, KeyFile} =
        case ?config(certfile, Config) of
            undefined ->
                TestConfDir = find_test_conf_dir(),
                {
                    filename:join(TestConfDir, "server.pem"),
                    filename:join(TestConfDir, "server.key")
                };
            Cert ->
                {Cert, ?config(keyfile, Config)}
        end,

    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2],
        timeouts => #{idle => 30000}
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#44, 9, "/conn-pid", 16#87>>,
    HeaderLen = byte_size(HeaderBlock),
    ok = ssl:send(Sock, <<HeaderLen:24, 1, 5, 0:1, 1:31, HeaderBlock/binary>>),
    {ok, _ResponseData} = ssl:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),

    ConnPid ! shutdown,

    receive
        {'DOWN', Ref, process, ConnPid, _Reason} -> ok
    after 5000 ->
        error(shutdown_timeout)
    end,

    unregister(conn_pid_receiver),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% PROTOCOL DETECTION TESTS
%%%-----------------------------------------------------------------------------

alpn_http1_1(Config) ->
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
            {alpn_advertised_protocols, [<<"http/1.1">>]}
        ],
        5000
    ),

    {ok, <<"http/1.1">>} = ssl:negotiated_protocol(Sock),

    ok = ssl:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

alpn_h2_not_allowed(Config) ->
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

    ok = ssl:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

alpn_no_alpn_fallback(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

alpn_http1_0(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.0\r\n\r\n">>),
    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 STREAMING TESTS
%%%-----------------------------------------------------------------------------

h1_stream_iterator(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /stream-iterator HTTP/1.1\r\nHost: localhost\r\n\r\n">>),

    Response = recv_all(Sock, <<>>, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response),
    ?assert(binary:match(Response, <<"transfer-encoding: chunked">>) =/= nomatch),
    ?assert(binary:match(Response, <<"chunk1">>) =/= nomatch),
    ?assert(binary:match(Response, <<"chunk2">>) =/= nomatch),
    ?assert(binary:match(Response, <<"chunk3">>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 LIMIT TESTS
%%%-----------------------------------------------------------------------------

h1_limit_uri_too_long(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_uri_length => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    LongPath = binary:copy(<<"a">>, 200),
    Request = <<"GET /", LongPath/binary, " HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 414", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_limit_header_too_large(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_header_value_length => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    LongValue = binary:copy(<<"x">>, 200),
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: ", LongValue/binary, "\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 431", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_limit_incomplete_headers_capped(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_header_size => 1024
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\nX-Endless: ">>),
    Chunk = binary:copy(<<"x">>, 512),
    ok = send_chunks(Sock, Chunk, 19),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 431", _/binary>>, Response),
    ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 5000)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_limit_unterminated_header_line_capped(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_header_size => 1024
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Line = <<"X-Huge: ", (binary:copy(<<"x">>, 16384))/binary>>,
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n", Line/binary>>),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 431", _/binary>>, Response),
    ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 5000)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_limit_body_too_large(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_body_size => 64
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request =
        <<"POST /echo HTTP/1.1\r\nHost: localhost\r\n", "Content-Length: 1024\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 413", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 REQUEST-DEADLINE TESTS
%%%-----------------------------------------------------------------------------

h1_request_timeout_head_slowloris(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{request => 300, idle => 60000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n">>),

    {ok, Response} = gen_tcp:recv(Sock, 0, 2000),
    ?assertMatch(<<"HTTP/1.1 408", _/binary>>, Response),
    ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 2000)),

    ok = nhttp_test_helpers:wait_until(fun() -> nhttp:active_connections() =:= 0 end, 2000),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_request_timeout_keepalive_not_killed(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{request => 200, idle => 3000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /small HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp1} = gen_tcp:recv(Sock, 0, 2000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp1),

    timer:sleep(600),

    ok = gen_tcp:send(Sock, <<"GET /small HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp2} = gen_tcp:recv(Sock, 0, 2000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp2),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_body_deadline_slow_body(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{body_deadline => 300}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(
        Sock,
        <<"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n">>
    ),
    ok = gen_tcp:send(Sock, <<"partial">>),

    ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 2000)),

    ok = nhttp_test_helpers:wait_until(fun() -> nhttp:active_connections() =:= 0 end, 2000),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_body_deadline_default_allows_slow_body(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(
        Sock,
        <<"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n">>
    ),
    ok = gen_tcp:send(Sock, <<"abcde">>),
    timer:sleep(400),
    ok = gen_tcp:send(Sock, <<"fghij">>),

    {ok, Response} = gen_tcp:recv(Sock, 0, 2000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assert(binary:match(Response, <<"abcdefghij">>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 WEBSOCKET TESTS
%%%-----------------------------------------------------------------------------

h1_websocket_upgrade(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = ws_upgrade_request(),
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 101 ", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Upgrade: websocket">>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, PongFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#8A, 0>>, PongFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_with_data(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PingData = mask(<<"test">>, MaskKey),
    Frame = <<16#89, 16#84, MaskKey/binary, PingData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, PongFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#8A, 4, "test">>, PongFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_pong(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PongData = mask(<<"pong">>, MaskKey),
    Frame = <<16#8A, 16#84, MaskKey/binary, PongData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    TextData = mask(<<"hello">>, MaskKey),
    TextFrame = <<16#81, 16#85, MaskKey/binary, TextData/binary>>,
    ok = gen_tcp:send(Sock, TextFrame),

    {ok, EchoFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#81, 11, "echo: hello">>, EchoFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#88, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, 0>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close_with_code(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    ClosePayload = <<1001:16, "going away">>,
    Payload = mask(ClosePayload, MaskKey),
    Frame = <<16#88, (16#80 bor byte_size(ClosePayload)), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, _Len, 1001:16, _/binary>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_idle_timeout(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 100},
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    case gen_tcp:recv(Sock, 0, 500) of
        {ok, <<16#88, _/binary>>} -> ok;
        {error, closed} -> ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_shutdown(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    nhttp:stop(Pid),

    case gen_tcp:recv(Sock, 0, 1000) of
        {ok, <<16#88, _/binary>>} -> ok;
        {error, closed} -> ok
    end,

    gen_tcp:close(Sock),
    ok.

h1_websocket_binary_message(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    BinaryData = <<1, 2, 3, 4, 5>>,
    MaskedData = mask(BinaryData, MaskKey),
    Frame = <<16#82, (16#80 bor byte_size(BinaryData)), MaskKey/binary, MaskedData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, RespFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#82, _Len, 1, 2, 3, 4, 5>>, RespFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_no_handler(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_no_ws_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Msg = <<"hello">>,
    MaskedMsg = mask(Msg, MaskKey),
    Frame = <<16#81, (16#80 bor byte_size(Msg)), MaskKey/binary, MaskedMsg/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    _ = gen_tcp:recv(Sock, 0, 100),

    CloseFrame = <<16#88, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONNECTION EVENT TESTS (ADDITIONAL)
%%%-----------------------------------------------------------------------------

conn_system_messages(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 5000),

    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _Response} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = sys:suspend(ConnPid),
    ok = sys:resume(ConnPid),

    {status, ConnPid, _Mod, _} = sys:get_status(ConnPid),

    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response2} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response2),

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 STREAMING TESTS
%%%-----------------------------------------------------------------------------

h2_stream_iterator(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#87, 16#44, 16, "/stream-iterator">>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_all_methods(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    Methods = [
        {<<"PUT">>, 3},
        {<<"DELETE">>, 6},
        {<<"HEAD">>, 4},
        {<<"OPTIONS">>, 7},
        {<<"PATCH">>, 5},
        {<<"CONNECT">>, 7},
        {<<"TRACE">>, 5},
        {<<"CUSTOM">>, 6}
    ],

    lists:foreach(
        fun({Method, Len}) ->
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

            ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
            ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
            {ok, _} = ssl:recv(Sock, 0, 5000),
            ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

            HeaderBlock = <<16#42, Len, Method/binary, 16#84, 16#87>>,
            FrameLen = byte_size(HeaderBlock),
            ok = ssl:send(Sock, <<FrameLen:24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

            RespData = recv_ssl_all(Sock, <<>>, 2000),
            ?assert(byte_size(RespData) > 0),

            ssl:close(Sock)
        end,
        Methods
    ),

    nhttp:stop(Pid),
    ok.

h2_limit_uri_too_long(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2],
        max_uri_length => 50
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    LongPath = binary:copy(<<"a">>, 100),
    HeaderBlock = <<16#82, 16#87, 16#44, 100, LongPath/binary>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_limit_header_too_large(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2],
        max_header_value_length => 50
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    LongValue = binary:copy(<<"x">>, 100),
    HeaderBlock = <<16#82, 16#87, 16#84, 16#40, 7, "x-large", 100, LongValue/binary>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_limit_body_too_large(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2],
        max_body_size => 64
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#83, 16#87, 16#44, 5, "/echo">>,
    HLen = byte_size(HeaderBlock),
    HeadersFrame = <<HLen:24, 1, 4, 0, 0, 0, 1, HeaderBlock/binary>>,
    ok = ssl:send(Sock, HeadersFrame),

    Body = binary:copy(<<"x">>, 1024),
    BodyLen = byte_size(Body),
    DataFrame = <<BodyLen:24, 0, 1, 0, 0, 0, 1, Body/binary>>,
    ok = ssl:send(Sock, DataFrame),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_goaway_event(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    GoawayFrame = <<0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
    ok = ssl:send(Sock, GoawayFrame),

    _ = ssl:recv(Sock, 0, 100),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_data_unknown_stream(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    DataPayload = <<"test data">>,
    DataFrame = <<(byte_size(DataPayload)):24, 0, 1, 0, 0, 0, 99, DataPayload/binary>>,
    ok = ssl:send(Sock, DataFrame),

    _ = ssl:recv(Sock, 0, 100),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% NEW HTTP/2 TESTS
%%%-----------------------------------------------------------------------------

h2_settings_ack(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),

    SettingsPayload = <<0, 3, 0, 0, 0, 100>>,
    SettingsFrame = <<(byte_size(SettingsPayload)):24, 4, 0, 0, 0, 0, 0, SettingsPayload/binary>>,
    ok = ssl:send(Sock, SettingsFrame),

    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    timer:sleep(100),

    HeaderBlock = <<16#82, 16#84, 16#87>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_stream_reset(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#83, 16#87, 16#44, 5, "/echo">>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 4, 0, 0, 0, 1, HeaderBlock/binary>>),

    RstFrame = <<0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8>>,
    ok = ssl:send(Sock, RstFrame),

    _ = ssl:recv(Sock, 0, 100),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_empty_body_response(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#87, 16#44, 6, "/empty">>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL HTTP/1.1 TESTS
%%%-----------------------------------------------------------------------------

h1_connect_trace_methods(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock1} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(
        Sock1, <<"CONNECT localhost:443 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>
    ),
    {ok, Response1} = gen_tcp:recv(Sock1, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response1),
    gen_tcp:close(Sock1),

    {ok, Sock2} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(
        Sock2, <<"TRACE / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>
    ),
    {ok, Response2} = gen_tcp:recv(Sock2, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Response2),
    gen_tcp:close(Sock2),

    nhttp:stop(Pid),
    ok.

h1_pipeline_depth_limit(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_pipeline_depth => 2
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    Requests = binary:copy(Request, 5),
    ok = gen_tcp:send(Sock, Requests),

    Response = recv_all(Sock, <<>>, 5000),
    Count = count_responses(Response, <<"HTTP/1.1 200">>),
    ?assert(Count >= 2),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_pipeline_shutdown_mid_batch(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_pipeline_depth => 50
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    PidReq = <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    SlowReq = <<"GET /slow-pipeline HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    Pipeline = <<PidReq/binary, (binary:copy(SlowReq, 20))/binary>>,
    ok = gen_tcp:send(Sock, Pipeline),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,
    MRef = monitor(process, ConnPid),

    ConnPid ! shutdown,
    Start = erlang:monotonic_time(millisecond),
    receive
        {'DOWN', MRef, process, ConnPid, _Reason} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            ?assert(Elapsed < 500)
    after 5000 ->
        error(shutdown_timeout)
    end,

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_stream_iterator_fin_only(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /stream-iterator-fin HTTP/1.1\r\nHost: localhost\r\n\r\n">>),

    Response = recv_all(Sock, <<>>, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assert(binary:match(Response, <<"transfer-encoding: chunked">>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 COMPRESSION TESTS
%%%-----------------------------------------------------------------------------

h1_compression_disabled(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /large HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assertEqual(nomatch, binary:match(Response, <<"content-encoding: gzip">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_below_threshold(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true,
        compression_threshold => 1024
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /small HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assertEqual(nomatch, binary:match(Response, <<"content-encoding: gzip">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_gzip(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true,
        compression_threshold => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request =
        <<"GET /large HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, deflate\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    Response = recv_all(Sock, <<>>, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_no_accept_encoding(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true,
        compression_threshold => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    Response = recv_all(Sock, <<>>, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assertEqual(nomatch, binary:match(Response, <<"content-encoding: gzip">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL WEBSOCKET TESTS
%%%-----------------------------------------------------------------------------

h1_websocket_pong_with_data(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PongData = mask(<<"pong-data">>, MaskKey),
    Frame = <<16#8A, (16#80 bor 9), MaskKey/binary, PongData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    TextData = mask(<<"test">>, MaskKey),
    TextFrame = <<16#81, 16#84, MaskKey/binary, TextData/binary>>,
    ok = gen_tcp:send(Sock, TextFrame),

    {ok, EchoFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#81, 10, "echo: test">>, EchoFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_reply(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_reply_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    Response = recv_all(Sock, <<>>, 1000),
    ?assert(byte_size(Response) >= 2),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close_noreply(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_noreply_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#88, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, 0>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_no_handler(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_no_ws_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, PongFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#8A, 0>>, PongFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_data_no_handler(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_no_ws_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PingData = mask(<<"test">>, MaskKey),
    Frame = <<16#89, 16#84, MaskKey/binary, PingData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, PongFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#8A, 4, "test">>, PongFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_close_on_ping_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    Response = recv_all(Sock, <<>>, 1000),
    ?assert(binary:match(Response, <<16#88>>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_data_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_close_on_ping_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PingData = mask(<<"test">>, MaskKey),
    Frame = <<16#89, 16#84, MaskKey/binary, PingData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    Response = recv_all(Sock, <<>>, 1000),
    ?assert(binary:match(Response, <<16#88>>) =/= nomatch),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close_reply(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_reply_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    UpgradeReq = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, UpgradeReq),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#88, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, 0>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ping_data_reply(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_reply_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    PingData = mask(<<"test">>, MaskKey),
    Frame = <<16#89, 16#84, MaskKey/binary, PingData/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    Response = recv_all(Sock, <<>>, 1000),
    ?assert(byte_size(Response) >= 6),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close_code_noreply(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_ws_noreply_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    ClosePayload = <<?WS_CLOSE_NORMAL:16, "bye">>,
    Payload = mask(ClosePayload, MaskKey),
    Frame = <<16#88, (16#80 bor 5), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, _Len, ?WS_CLOSE_NORMAL:16, _/binary>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_close_code_no_handler(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_no_ws_handler,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    ClosePayload = <<1001:16, "going away">>,
    Payload = mask(ClosePayload, MaskKey),
    Frame = <<16#88, (16#80 bor 12), MaskKey/binary, Payload/binary>>,
    ok = gen_tcp:send(Sock, Frame),

    {ok, CloseFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#88, _Len, 1001:16, _/binary>>, CloseFrame),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL CONNECTION TESTS
%%%-----------------------------------------------------------------------------

conn_parent_exit(_Config) ->
    Self = self(),
    Parent = spawn(fun() ->
        {ok, Pid} = nhttp:start_link(#{
            port => 0,
            handler => ?MODULE,
            versions => [http1_1]
        }),
        {ok, Port} = nhttp:get_port(Pid),
        Self ! {started, Port, Pid},
        receive
            stop -> nhttp:stop(Pid)
        end
    end),

    receive
        {started, Port, _ServerPid} ->
            {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
            {ok, _} = gen_tcp:recv(Sock, 0, 5000),
            gen_tcp:close(Sock),
            Parent ! stop
    after 5000 ->
        ct:fail(timeout)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL PROTOCOL DETECTION TESTS
%%%-----------------------------------------------------------------------------

alpn_h2_first_fallback(_Config) ->
    case
        nhttp:start_link(#{
            port => 0,
            handler => ?MODULE,
            versions => [http2, http1_1]
        })
    of
        {error, _} -> ok;
        {ok, Pid} -> ct:fail({unexpected_start, Pid})
    end.

%%%-----------------------------------------------------------------------------
%%% HIBERNATION TESTS
%%%-----------------------------------------------------------------------------

h1_hibernate_keepalive(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = wait_until_hibernating(ConnPid),

    ok = gen_tcp:send(Sock, <<"GET /small HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch({_, _}, binary:match(Resp, <<"HTTP/1.1 200">>)),
    ?assert(is_process_alive(ConnPid)),

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_hibernate_idle_timeout(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 300}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = wait_until_hibernating(ConnPid),
    Ref = monitor(process, ConnPid),

    receive
        {'DOWN', Ref, process, ConnPid, normal} -> ok
    after 2000 ->
        error(idle_timeout_not_fired_while_hibernated)
    end,
    {error, closed} = gen_tcp:recv(Sock, 0, 1000),

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_hibernate_ping(Config) ->
    {CertFile, KeyFile} =
        case ?config(certfile, Config) of
            undefined ->
                TestConfDir = find_test_conf_dir(),
                {
                    filename:join(TestConfDir, "server.pem"),
                    filename:join(TestConfDir, "server.key")
                };
            Cert ->
                {Cert, ?config(keyfile, Config)}
        end,

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2],
        timeouts => #{idle => 30000}
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#44, 6, "/small", 16#87>>,
    HeaderLen = byte_size(HeaderBlock),
    ok = ssl:send(Sock, <<HeaderLen:24, 1, 5, 0:1, 1:31, HeaderBlock/binary>>),
    {ok, _ResponseData} = ssl:recv(Sock, 0, 5000),

    [ConnPid] = nhttp_test_helpers:conn_pids(Pid),
    ok = wait_until_hibernating(ConnPid),

    Opaque = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    ok = ssl:send(Sock, <<0, 0, 8, 6, 0, 0:1, 0:31, Opaque/binary>>),
    ok = ssl_recv_until(Sock, <<0, 0, 8, 6, 1, 0, 0, 0, 0, Opaque/binary>>, <<>>, 10),
    ?assert(is_process_alive(ConnPid)),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_hibernate_ping(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000},
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, <<"HTTP/1.1 101", _/binary>>} = gen_tcp:recv(Sock, 0, 5000),

    ok = wait_until_hibernating(ConnPid),

    MaskKey = <<1, 2, 3, 4>>,
    Frame = <<16#89, 16#80, MaskKey/binary>>,
    ok = gen_tcp:send(Sock, Frame),
    {ok, PongFrame} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<16#8A, 0>>, PongFrame),
    ?assert(is_process_alive(ConnPid)),

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

wait_until_hibernating(Pid) ->
    nhttp_test_helpers:wait_until(
        fun() ->
            erlang:process_info(Pid, current_function) =:=
                {current_function, {erlang, hibernate, 3}}
        end,
        2000
    ).

ssl_recv_until(_Sock, Pattern, Acc, 0) ->
    error({pattern_not_received, Pattern, Acc});
ssl_recv_until(Sock, Pattern, Acc, Attempts) ->
    {ok, Data} = ssl:recv(Sock, 0, 5000),
    Acc1 = <<Acc/binary, Data/binary>>,
    case binary:match(Acc1, Pattern) of
        {_, _} -> ok;
        nomatch -> ssl_recv_until(Sock, Pattern, Acc1, Attempts - 1)
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

ws_upgrade_request() ->
    [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ].

drain_tcp(Sock) ->
    drain_tcp(Sock, <<>>).

drain_tcp(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 1000) of
        {ok, Bin} ->
            drain_tcp(Sock, <<Acc/binary, Bin/binary>>);
        {error, _} ->
            gen_tcp:close(Sock),
            Acc
    end.

mask(Data, <<K1, K2, K3, K4>>) ->
    mask_loop(Data, <<K1, K2, K3, K4>>, 0, <<>>).

mask_loop(<<>>, _Key, _Idx, Acc) ->
    Acc;
mask_loop(<<B, Rest/binary>>, <<K1, K2, K3, K4>> = Key, Idx, Acc) ->
    KeyByte =
        case Idx rem 4 of
            0 -> K1;
            1 -> K2;
            2 -> K3;
            3 -> K4
        end,
    mask_loop(Rest, Key, Idx + 1, <<Acc/binary, (B bxor KeyByte)>>).

%%%-----------------------------------------------------------------------------
%%% HTTP/1.0 KEEP-ALIVE TESTS
%%%-----------------------------------------------------------------------------

h1_http1_0_no_keepalive(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    Result = gen_tcp:recv(Sock, 0, 500),
    ?assertMatch({error, _}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_http1_0_explicit_keepalive(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    ok = gen_tcp:send(Sock, Request),
    {ok, Response2} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response2),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL COMPRESSION TESTS
%%%-----------------------------------------------------------------------------

h1_compression_empty_body(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /empty HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assertEqual(nomatch, binary:match(Response, <<"content-encoding">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_no_content_type(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => nhttp_conn_no_content_type_handler,
        versions => [http1_1],
        compression => true,
        compression_threshold => 10
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /no-ct HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),
    ?assertEqual(nomatch, binary:match(Response, <<"content-encoding">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_deflate(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true,
        compression_threshold => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /large HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: deflate\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    Response = recv_all(Sock, <<>>, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 FLOW CONTROL TESTS
%%%-----------------------------------------------------------------------------

h2_window_update(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#84, 16#87>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    timer:sleep(100),

    WindowUpdateConn = <<0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 16#FF, 16#FF>>,
    ok = ssl:send(Sock, WindowUpdateConn),

    WindowUpdateStream = <<0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 16#FF, 16#FF>>,
    ok = ssl:send(Sock, WindowUpdateStream),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% WEBSOCKET ERROR TESTS
%%%-----------------------------------------------------------------------------

h1_websocket_invalid_upgrade(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    InvalidRequest = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"\r\n">>
    ],
    ok = gen_tcp:send(Sock, InvalidRequest),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 400", _/binary>>, Response),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_protocol_error(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _} = gen_tcp:recv(Sock, 0, 5000),

    InvalidFrame = <<16#FF, 16#00>>,
    ok = gen_tcp:send(Sock, InvalidFrame),

    case gen_tcp:recv(Sock, 0, 2000) of
        {ok, <<16#88, _/binary>>} -> ok;
        {error, closed} -> ok
    end,

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

conn_ssl_error(Config) ->
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
            {verify, verify_none}
        ],
        5000
    ),

    ok = ssl:send(Sock, <<"GET / HTTP/1.1">>),
    ssl:close(Sock),

    {error, timeout} = nhttp_test_helpers:wait_until_down(Pid, 100),
    ?assert(is_process_alive(Pid)),

    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL EDGE CASE TESTS
%%%-----------------------------------------------------------------------------

h1_connection_close_header(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Response} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response),

    Result = gen_tcp:recv(Sock, 0, 500),
    ?assertMatch({error, _}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_connection_preface_timeout(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    timer:sleep(100),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_ping_frame(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    PingData = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    PingFrame = <<0, 0, 8, 6, 0, 0, 0, 0, 0, PingData/binary>>,
    ok = ssl:send(Sock, PingFrame),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_priority_frame(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    PriorityPayload = <<0, 0, 0, 0, 16>>,
    PriorityFrame = <<0, 0, 5, 2, 0, 0, 0, 0, 1, PriorityPayload/binary>>,
    ok = ssl:send(Sock, PriorityFrame),

    timer:sleep(100),

    HeaderBlock = <<16#82, 16#84, 16#87>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 3, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 2000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL COVERAGE TESTS
%%%-----------------------------------------------------------------------------

h1_all_other_methods(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    Methods = [
        {<<"HEAD">>, <<"HEAD">>},
        {<<"PUT">>, <<"PUT">>},
        {<<"DELETE">>, <<"DELETE">>},
        {<<"OPTIONS">>, <<"OPTIONS">>},
        {<<"PATCH">>, <<"PATCH">>},
        {<<"CONNECT">>, <<"CONNECT">>},
        {<<"TRACE">>, <<"TRACE">>},
        {<<"CUSTOM">>, <<"CUSTOM">>}
    ],
    lists:foreach(
        fun({Method, ExpectedMethod}) ->
            {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            Request = <<Method/binary, " /method-test HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
            ok = gen_tcp:send(Sock, Request),
            {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
            ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
            ?assert(binary:match(Resp, ExpectedMethod) =/= nomatch),
            gen_tcp:close(Sock)
        end,
        Methods
    ),

    nhttp:stop(Pid),
    ok.

h1_system_code_change(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _Response} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    ok = sys:suspend(ConnPid),

    ok = sys:change_code(ConnPid, nhttp_conn, undefined, []),

    ok = sys:resume(ConnPid),

    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, Response2} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response2),

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_system_terminate(_Config) ->
    reregister(conn_pid_receiver),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        timeouts => #{idle => 30000}
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    ok = gen_tcp:send(Sock, <<"GET /conn-pid HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {ok, _Response} = gen_tcp:recv(Sock, 0, 5000),

    ConnPid =
        receive
            {conn_pid, P} -> P
        after 5000 ->
            error(conn_pid_timeout)
        end,

    Ref = monitor(process, ConnPid),

    sys:terminate(ConnPid, test_reason),

    receive
        {'DOWN', Ref, process, ConnPid, _Reason} -> ok
    after 5000 ->
        error(terminate_timeout)
    end,

    unregister(conn_pid_receiver),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_stream_iterator_direct(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [binary, {active, false}, {verify, verify_none}, {alpn_advertised_protocols, [<<"h2">>]}],
        5000
    ),

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock2 = <<16#82, 16#44, 16#10, "/stream-iterator", 16#87>>,
    HeaderFrame = <<(byte_size(HeaderBlock2)):24, 1, 5, 0, 0, 0, 1, HeaderBlock2/binary>>,
    ok = ssl:send(Sock, HeaderFrame),

    RespData2 = recv_ssl_all(Sock, <<>>, 3000),
    ?assert(byte_size(RespData2) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_yield_continue_buffer(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        max_pipeline_depth => 2
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    Requests = [
        <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
        <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
        <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
        <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
        <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, iolist_to_binary(Requests)),

    Resp = recv_tcp_all(Sock, <<>>, 5),
    Count = count_occurrences(Resp, <<"HTTP/1.1 200 OK">>),
    ?assertEqual(5, Count),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

recv_tcp_all(Sock, Acc, ExpectedCount) ->
    CurrentCount = count_occurrences(Acc, <<"HTTP/1.1 200 OK">>),
    case CurrentCount >= ExpectedCount of
        true ->
            Acc;
        false ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} ->
                    recv_tcp_all(Sock, <<Acc/binary, Data/binary>>, ExpectedCount);
                {error, _} ->
                    Acc
            end
    end.

h1_resp_connection_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200 OK", _/binary>>, Resp),

    Result = gen_tcp:recv(Sock, 0, 1000),
    ?assertMatch({error, closed}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_compression_fail(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        compression => true,
        compression_threshold => 10
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /large HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertEqual(nomatch, binary:match(Resp, <<"content-encoding">>)),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_send_headers_stream_closed(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [binary, {active, false}, {verify, verify_none}, {alpn_advertised_protocols, [<<"h2">>]}],
        5000
    ),

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock4 = <<16#82, 16#84, 16#87>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock4)):24, 1, 5, 0, 0, 0, 1, HeaderBlock4/binary>>),

    RstStream = <<0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8>>,
    ok = ssl:send(Sock, RstStream),

    timer:sleep(200),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_response_connection_close(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Request = <<"GET /resp-close HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ok = gen_tcp:send(Sock, Request),

    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assert(binary:match(Resp, <<"connection: close">>) =/= nomatch),

    Result = gen_tcp:recv(Sock, 0, 1000),
    ?assertMatch({error, closed}, Result),

    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 WEBSOCKET TESTS (RFC 8441)
%%%-----------------------------------------------------------------------------

h2_connect(Config) ->
    h2_connect_with_handler(Config, ?MODULE).

h2_connect_with_handler(Config, Handler) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => Handler,
        versions => [http2]
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
    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    H2 = nhttp_h2:new(client, #{}),
    {Sock, H2, Pid}.

h2_ws_upgrade(Sock, _H2, StreamId) ->
    HeaderBlock = <<
        16#42,
        7,
        "CONNECT",
        16#40,
        9,
        ":protocol",
        9,
        "websocket",
        16#44,
        3,
        "/ws",
        16#87,
        16#41,
        9,
        "localhost"
    >>,
    Len = byte_size(HeaderBlock),
    ok = ssl:send(Sock, <<Len:24, 1, 4, 0:1, StreamId:31, HeaderBlock/binary>>),
    _H2.

h2_ws_send(Sock, H2, StreamId, WsFrame) ->
    Len = byte_size(WsFrame),
    ok = ssl:send(Sock, <<Len:24, 0, 0, 0:1, StreamId:31, WsFrame/binary>>),
    H2.

h2_websocket_upgrade(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    _H2_1 = h2_ws_upgrade(Sock, H2, 1),
    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_text_message(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"hello">>})),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, TextFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_binary_message(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    BinFrame = iolist_to_binary(nhttp_ws:encode_masked({binary, <<1, 2, 3, 4, 5>>})),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, BinFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_ping(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    PingFrame = iolist_to_binary(nhttp_ws:encode_masked(ping)),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, PingFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_pong(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    PongFrame = iolist_to_binary(nhttp_ws:encode_masked(pong)),
    H2_2 = h2_ws_send(Sock, H2_1, 1, PongFrame),

    PongDataFrame = iolist_to_binary(nhttp_ws:encode_masked({pong, <<"data">>})),
    H2_3 = h2_ws_send(Sock, H2_2, 1, PongDataFrame),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"after-pong">>})),
    _H2_4 = h2_ws_send(Sock, H2_3, 1, TextFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_close(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    CloseFrame = iolist_to_binary(nhttp_ws:encode_masked(close)),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, CloseFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_close_with_code(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    CloseFrame = iolist_to_binary(nhttp_ws:encode_masked({close, 1001, <<"going away">>})),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, CloseFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_handler_close(Config) ->
    {Sock, H2, Pid} = h2_connect_with_handler(Config, nhttp_conn_ws_close_on_ping_handler),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    PingFrame = iolist_to_binary(nhttp_ws:encode_masked(ping)),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, PingFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_handler_noreply(Config) ->
    {Sock, H2, Pid} = h2_connect_with_handler(Config, nhttp_conn_ws_noreply_handler),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    TextFrame = iolist_to_binary(nhttp_ws:encode_masked({text, <<"test">>})),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, TextFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_no_handler(Config) ->
    {Sock, H2, Pid} = h2_connect_with_handler(Config, nhttp_conn_no_ws_handler),
    H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    PingFrame = iolist_to_binary(nhttp_ws:encode_masked(ping)),
    _H2_2 = h2_ws_send(Sock, H2_1, 1, PingFrame),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_upgrade_rejected(Config) ->
    {Sock, H2, Pid} = h2_connect_with_handler(Config, nhttp_conn_no_content_type_handler),
    _H2_1 = h2_ws_upgrade(Sock, H2, 1),

    RespData = recv_ssl_all(Sock, <<>>, 1000),
    ?assert(byte_size(RespData) > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_websocket_stream_reset(Config) ->
    {Sock, H2, Pid} = h2_connect(Config),
    _H2_1 = h2_ws_upgrade(Sock, H2, 1),
    _SetupData = recv_ssl_all(Sock, <<>>, 500),

    RstFrame = <<0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8>>,
    ok = ssl:send(Sock, RstFrame),
    timer:sleep(200),

    HeaderBlock = <<16#82, 16#84, 16#87>>,
    case ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 3, HeaderBlock/binary>>) of
        ok ->
            RespData = recv_ssl_all(Sock, <<>>, 1000),
            ?assert(byte_size(RespData) > 0);
        {error, closed} ->
            ok
    end,

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 FLOW CONTROL BUFFERING TEST
%%%-----------------------------------------------------------------------------

h2_large_body_flow_control(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    SettingsPayload = <<0, 4, 0, 0, 4, 0>>,
    ok = ssl:send(Sock, <<
        (byte_size(SettingsPayload)):24, 4, 0, 0, 0, 0, 0, SettingsPayload/binary
    >>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    HeaderBlock = <<16#82, 16#87, 16#44, 6, "/large">>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),
    timer:sleep(200),

    Data1 = recv_ssl_all(Sock, <<>>, 500),

    ok = ssl:send(Sock, <<0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 1, 0, 0>>),
    ok = ssl:send(Sock, <<0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 1, 0, 0>>),
    timer:sleep(200),

    Data2 = recv_ssl_all(Sock, <<>>, 1000),

    TotalSize = byte_size(Data1) + byte_size(Data2),
    ?assert(TotalSize > 0),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% WEBSOCKET OVER SSL TESTS
%%%-----------------------------------------------------------------------------

h1_websocket_ssl_upgrade(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
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

    Request = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = ssl:send(Sock, Request),
    {ok, Response} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 101 ", _/binary>>, Response),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_websocket_ssl_text(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
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

    Request = [
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"\r\n">>
    ],
    ok = ssl:send(Sock, Request),
    {ok, _} = ssl:recv(Sock, 0, 5000),

    MaskKey = <<1, 2, 3, 4>>,
    TextData = mask(<<"hello">>, MaskKey),
    Frame = <<16#81, 16#85, MaskKey/binary, TextData/binary>>,
    ok = ssl:send(Sock, Frame),

    {ok, EchoFrame} = ssl:recv(Sock, 0, 5000),
    ?assertMatch(<<16#81, 11, "echo: hello">>, EchoFrame),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 TRAILER TESTS
%%%-----------------------------------------------------------------------------

h2_iterator_trailers(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
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

    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, _} = ssl:recv(Sock, 0, 5000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),

    Path = "/stream-iterator-trailers",
    HeaderBlock =
        <<16#82, 16#87, 16#44, (byte_size(list_to_binary(Path))), (list_to_binary(Path))/binary>>,
    ok = ssl:send(Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0, 0, 0, 1, HeaderBlock/binary>>),

    RespData = recv_ssl_all(Sock, <<>>, 3000),
    ?assert(byte_size(RespData) > 0),

    Frames = parse_h2_frames(RespData),

    Stream1Frames = [F || F = {_, _, StreamId, _} <- Frames, StreamId =:= 1],
    ?assert(length(Stream1Frames) >= 3),

    {1, InitFlags, 1, _} = hd(Stream1Frames),
    ?assertEqual(0, InitFlags band 1),

    {LastType, LastFlags, 1, _} = lists:last(Stream1Frames),
    ?assertEqual(1, LastType),
    ?assertNotEqual(0, LastFlags band 1),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_iterator_trailers_graceful(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http1_1],
        telemetry => false
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

    Request = [
        <<"GET /stream-iterator-trailers HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"\r\n">>
    ],
    ok = ssl:send(Sock, Request),

    RespData = recv_ssl_all(Sock, <<>>, 3000),
    ?assert(byte_size(RespData) > 0),
    ?assertNotEqual(nomatch, binary:match(RespData, <<"200">>)),
    ?assertNotEqual(nomatch, binary:match(RespData, <<"0\r\n\r\n">>)),

    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

parse_h2_frames(Bin) ->
    parse_h2_frames(Bin, []).

parse_h2_frames(
    <<Len:24, Type:8, Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>>, Acc
) ->
    parse_h2_frames(Rest, [{Type, Flags, StreamId, Payload} | Acc]);
parse_h2_frames(_, Acc) ->
    lists:reverse(Acc).

recv_h2_frames_until(Sock, TimeoutMs, Pred) ->
    recv_h2_frames_until(Sock, TimeoutMs, Pred, <<>>, []).

recv_h2_frames_until(_Sock, TimeoutMs, _Pred, _Buf, Acc) when TimeoutMs =< 0 ->
    Acc;
recv_h2_frames_until(Sock, TimeoutMs, Pred, Buf, Acc) ->
    case ssl:recv(Sock, 0, 200) of
        {ok, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            Frames = parse_h2_frames(NewBuf),
            NewAcc = Acc ++ Frames,
            case lists:any(Pred, NewAcc) of
                true -> NewAcc;
                false -> recv_h2_frames_until(Sock, TimeoutMs - 200, Pred, <<>>, NewAcc)
            end;
        {error, timeout} ->
            recv_h2_frames_until(Sock, TimeoutMs - 200, Pred, Buf, Acc);
        {error, _} ->
            Acc
    end.

match_headers_end_stream({1, Flags, StreamId, _Payload}, StreamId) ->
    (Flags band 16#01) =/= 0;
match_headers_end_stream(_, _) ->
    false.

send_chunks(_Sock, _Chunk, 0) ->
    ok;
send_chunks(Sock, Chunk, N) ->
    case gen_tcp:send(Sock, Chunk) of
        ok -> send_chunks(Sock, Chunk, N - 1);
        {error, _} -> ok
    end.

count_occurrences(Bin, Pattern) ->
    count_occurrences(Bin, Pattern, 0).

count_occurrences(Bin, Pattern, Count) ->
    case binary:match(Bin, Pattern) of
        nomatch ->
            Count;
        {Pos, Len} ->
            Rest = binary:part(Bin, Pos + Len, byte_size(Bin) - Pos - Len),
            count_occurrences(Rest, Pattern, Count + 1)
    end.
