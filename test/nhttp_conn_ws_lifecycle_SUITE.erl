%%%-----------------------------------------------------------------------------
%%% @doc WebSocket lifecycle failure paths for HTTP/1.1 and HTTP/2.
%%%
%%% Drives the `nhttp_conn_ws_h1' / `nhttp_conn_ws_h2' branches that
%%% need OTP system messages, unhandled gen_server envelopes, server
%%% drain (GOING_AWAY), a protocol-error frame, and a peer RST_STREAM
%%% on the WebSocket stream. The suite doubles as its own
%%% `nhttp_handler'.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_ws_lifecycle_SUITE).

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
    ws_h1_server_drain/1,
    ws_h1_sys_and_info/1,
    ws_h2_bad_gencall/1,
    ws_h2_client_rst/1,
    ws_h2_client_rst_close_callback/1,
    ws_h2_client_rst_closed_crash/1,
    ws_h2_protocol_error/1,
    ws_h2_server_drain/1
]).

-export([
    init/1,
    handle_request/2,
    handle_ws_open/2,
    handle_ws_frame/3,
    handle_ws_closed/3,
    terminate/2
]).

-export([log/2]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        ws_h1_server_drain,
        ws_h1_sys_and_info,
        ws_h2_bad_gencall,
        ws_h2_client_rst,
        ws_h2_client_rst_close_callback,
        ws_h2_client_rst_closed_crash,
        ws_h2_protocol_error,
        ws_h2_server_drain
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    {CertFile, KeyFile} = nhttp_test_helpers:certs(),
    case filelib:is_file(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - WEBSOCKET OVER HTTP/1.1
%%%-----------------------------------------------------------------------------

ws_h1_sys_and_info(_Config) ->
    {ok, Pid, Port} = nhttp_test_helpers:start(#{
        handler => ?MODULE, versions => [http1_1]
    }),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 101", _/binary>>, Resp),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ?assertMatch({status, ConnPid, _, _}, sys:get_status(ConnPid)),
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttp_conn_ws_h1, undefined, []),
    ok = sys:resume(ConnPid),
    ?assertEqual({error, badarg}, gen_server:call(ConnPid, bogus_request, 2000)),
    ConnPid ! arbitrary_info_message,
    ?assert(is_process_alive(ConnPid)),
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

ws_h1_server_drain(_Config) ->
    {ok, Pid, Port} = nhttp_test_helpers:start(#{
        handler => ?MODULE, versions => [http1_1]
    }),
    Sock = nhttp_test_helpers:tcp_connect(Port),
    ok = gen_tcp:send(Sock, ws_upgrade_request()),
    {ok, _Resp} = gen_tcp:recv(Sock, 0, 5000),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ok = nhttp_conn:drain(ConnPid),
    Bytes = nhttp_test_helpers:drain_tcp(Sock),
    ?assertNotEqual(nomatch, binary:match(Bytes, <<16#88>>)),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - WEBSOCKET OVER HTTP/2
%%%-----------------------------------------------------------------------------

ws_h2_protocol_error(Config) ->
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1),
    UnmaskedFrame = <<1:1, 0:3, 1:4, 0:1, 5:7, "hello">>,
    ok = h2_data(Sock, 1, UnmaskedFrame),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assert(lists:any(fun(F) -> element(2, F) =:= 1 end, Frames)),
    ?assert(is_process_alive(Pid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ws_h2_server_drain(Config) ->
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ok = nhttp_conn:drain(ConnPid),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assert(lists:any(fun(F) -> element(2, F) =:= 1 end, Frames)),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ws_h2_client_rst(Config) ->
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1),
    ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, 8),
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assert(lists:any(fun(F) -> element(2, F) =:= 3 end, Frames)),
    ?assert(is_process_alive(Pid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ws_h2_client_rst_close_callback(Config) ->
    Sessions0 = nhttp_stats:active_ws_sessions(),
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1, #{notify => self()}),
    ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, 8),
    receive
        {ws_closed, Reason} -> ?assertEqual({h2_reset, cancel}, Reason)
    after 2000 ->
        error(ws_closed_not_fired)
    end,
    receive
        {ws_closed, Dup} -> error({duplicate_ws_closed, Dup})
    after 300 -> ok
    end,
    Frames = nhttp_test_helpers:h2_recv(Sock, 500),
    ?assertEqual([], [F || {data, 1, _, _} = F <- Frames]),
    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp_stats:active_ws_sessions() =:= Sessions0 end, 2000
    ),
    ?assert(is_process_alive(Pid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ws_h2_client_rst_closed_crash(Config) ->
    Sessions0 = nhttp_stats:active_ws_sessions(),
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1, #{
        notify => self(), crash_on_closed => true
    }),
    ok = logger:add_handler(?FUNCTION_NAME, ?MODULE, #{config => self()}),
    try
        ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, 8),
        receive
            {ws_closed, Reason} -> ?assertEqual({h2_reset, cancel}, Reason)
        after 2000 ->
            error(ws_closed_not_fired)
        end,
        receive
            handler_crash_logged -> ok
        after 2000 ->
            error(handler_crash_not_logged)
        end,
        ok = nhttp_test_helpers:wait_until(
            fun() -> nhttp_stats:active_ws_sessions() =:= Sessions0 end, 2000
        ),
        ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/">>),
        Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
        ?assert(lists:any(fun(F) -> element(2, F) =:= 3 end, Frames))
    after
        _ = logger:remove_handler(?FUNCTION_NAME)
    end,
    ?assert(is_process_alive(Pid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

ws_h2_bad_gencall(Config) ->
    {ok, Pid, _Port, Sock} = start_ws_h2(Config, 1),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ?assertEqual({error, badarg}, gen_server:call(ConnPid, bogus_request, 2000)),
    ?assert(is_process_alive(ConnPid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

handle_ws_open(_Session, State) ->
    {ok, State}.

handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

handle_ws_closed(Reason, _Session, #{notify := Pid, crash_on_closed := true}) ->
    Pid ! {ws_closed, Reason},
    error(deliberate_crash);
handle_ws_closed(Reason, _Session, #{notify := Pid}) ->
    Pid ! {ws_closed, Reason},
    ok;
handle_ws_closed(_Reason, _Session, _State) ->
    ok.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% LOGGER HANDLER CALLBACK
%%%-----------------------------------------------------------------------------

log(#{msg := {report, #{event := handler_crashed, callback := handle_ws_closed}}}, #{
    config := Pid
}) ->
    Pid ! handler_crash_logged,
    ok;
log(_Event, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start_ws_h2(Config, StreamId) ->
    start_ws_h2(Config, StreamId, []).

start_ws_h2(Config, StreamId, HandlerArgs) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid, Port} = nhttp_test_helpers:start(#{
        handler => ?MODULE,
        handler_args => HandlerArgs,
        versions => [http2],
        tls => #{certfile => CertFile, keyfile => KeyFile}
    }),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_raw(Sock, h2_ws_upgrade_frame(StreamId)),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assert(lists:any(fun(F) -> element(1, F) =:= headers end, Frames)),
    {ok, Pid, Port, Sock}.

h2_ws_upgrade_frame(StreamId) ->
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
    <<Len:24, 1, 4, 0:1, StreamId:31, HeaderBlock/binary>>.

h2_data(Sock, StreamId, Payload) ->
    Len = byte_size(Payload),
    nhttp_test_helpers:h2_send_raw(Sock, <<Len:24, 0, 0, 0:1, StreamId:31, Payload/binary>>).

ws_upgrade_request() ->
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    iolist_to_binary([
        <<"GET /ws HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: ">>,
        Key,
        <<"\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n\r\n">>
    ]).
