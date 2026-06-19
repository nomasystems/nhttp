%%%-----------------------------------------------------------------------------
%%% @doc HTTP/2 connection lifecycle and flow-control failure paths.
%%%
%%% Exercises the `nhttp_conn_h2' branches that need a misbehaving or
%%% slow peer: connection-error GOAWAY, drain killing an in-flight
%%% worker, client RST_STREAM during a streamed response, send-window
%%% exhaustion + WINDOW_UPDATE drain, worker crash mid-stream, and the
%%% request-limit error responses. The suite doubles as its own
%%% `nhttp_handler'.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_h2_lifecycle_SUITE).

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
    h2_client_rst_during_stream/1,
    h2_connection_error_goaway/1,
    h2_drain_kills_active_worker/1,
    h2_streaming_body_too_large/1,
    h2_sys_messages/1,
    h2_uri_too_long/1,
    h2_window_exhaustion_then_update/1,
    h2_worker_crash_mid_stream/1
]).

-export([
    init/1,
    handle_request/2,
    handle_request_body/3,
    terminate/2
]).

-define(BIG_BODY, 200000).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        h2_client_rst_during_stream,
        h2_connection_error_goaway,
        h2_drain_kills_active_worker,
        h2_streaming_body_too_large,
        h2_sys_messages,
        h2_uri_too_long,
        h2_window_exhaustion_then_update,
        h2_worker_crash_mid_stream
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
%%% TEST CASES
%%%-----------------------------------------------------------------------------

h2_sys_messages(Config) ->
    {ok, Pid, Port} = start(Config, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/">>),
    _ = nhttp_test_helpers:h2_recv(Sock, 1000),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ?assertMatch({status, ConnPid, _, _}, sys:get_status(ConnPid)),
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttp_conn_h2, undefined, []),
    ok = sys:resume(ConnPid),
    Ref = monitor(process, ConnPid),
    ok = sys:terminate(ConnPid, shutdown),
    receive
        {'DOWN', Ref, process, ConnPid, _} -> ok
    after 2000 ->
        error(conn_did_not_terminate)
    end,
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_connection_error_goaway(Config) ->
    {ok, Pid, Port} = start(Config, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    HeaderBlock = <<16#82, 16#87, 16#84>>,
    ok = nhttp_test_helpers:h2_send_raw(
        Sock, <<(byte_size(HeaderBlock)):24, 1, 5, 0:1, 0:31, HeaderBlock/binary>>
    ),
    Frames = nhttp_test_helpers:h2_recv(Sock, 2000),
    ?assert(lists:keymember(goaway, 1, Frames)),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 3000),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_drain_kills_active_worker(Config) ->
    {ok, Pid, Port} = start(Config, #{drain_timeout => 100}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/slow-stream">>),
    _ = nhttp_test_helpers:h2_recv(Sock, 500),
    [ConnPid] = nhttp_test_helpers:wait_for_conns(Pid, 1, 2000),
    ok = nhttp_conn:drain(ConnPid),
    ok = nhttp_test_helpers:wait_for_no_conns(Pid, 5000),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_client_rst_during_stream(Config) ->
    {ok, Pid, Port} = start(Config, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/stream">>),
    _ = nhttp_test_helpers:h2_recv(Sock, 500),
    ok = nhttp_test_helpers:h2_send_rst_stream(Sock, 1, 8),
    ok = nhttp_test_helpers:h2_send_request(Sock, 3, <<"/">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assert(lists:any(fun(F) -> element(2, F) =:= 3 end, Frames)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_window_exhaustion_then_update(Config) ->
    {ok, Pid, Port} = start(Config, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/big">>),
    Got1 = stream_data_size(nhttp_test_helpers:h2_recv(Sock, 1000), 1),
    ?assert(Got1 > 0),
    ?assert(Got1 < ?BIG_BODY),
    Total = drain_with_window_updates(Sock, Got1, 30),
    ?assertEqual(?BIG_BODY, Total),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_worker_crash_mid_stream(Config) ->
    {ok, Pid, Port} = start(Config, #{}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/crash-stream">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, 2000),
    ?assert(lists:any(fun(F) -> element(2, F) =:= 1 end, Frames)),
    ?assert(is_process_alive(Pid)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_uri_too_long(Config) ->
    {ok, Pid, Port} = start(Config, #{max_uri_length => 10}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/this-path-is-far-too-long">>),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assertEqual(<<"414">>, status_of(Frames, 1)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

h2_streaming_body_too_large(Config) ->
    {ok, Pid, Port} = start(Config, #{max_body_size => 10}),
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    ok = nhttp_test_helpers:h2_open_stream(Sock, 1, <<"/accept-body">>),
    timer:sleep(300),
    Payload = binary:copy(<<"x">>, 1000),
    ok = nhttp_test_helpers:h2_send_raw(
        Sock, <<(byte_size(Payload)):24, 0, 1, 0:1, 1:31, Payload/binary>>
    ),
    Frames = nhttp_test_helpers:h2_recv(Sock, 1500),
    ?assertEqual(<<"413">>, status_of(Frames, 1)),
    ssl:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/big">>}, State) ->
    {reply, nhttp_resp:ok(binary:copy(<<"x">>, ?BIG_BODY)), State};
handle_request(#{path := <<"/stream">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun stream_until_closed/1), State};
handle_request(#{path := <<"/slow-stream">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun slow_stream/1), State};
handle_request(#{path := <<"/crash-stream">>}, State) ->
    {stream, nhttp_stream:producer(200, [], fun crash_stream/1), State};
handle_request(#{path := <<"/accept-body">>}, State) ->
    {accept_body, [], State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    {reply, nhttp_resp:ok(iolist_to_binary(lists:reverse(Acc))), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start(Config, Extra) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    nhttp_test_helpers:start(
        maps:merge(
            #{
                handler => ?MODULE,
                versions => [http2],
                tls => #{certfile => CertFile, keyfile => KeyFile}
            },
            Extra
        )
    ).

stream_until_closed(SendChunk) ->
    case SendChunk(binary:copy(<<0>>, 32768)) of
        ok -> stream_until_closed(SendChunk);
        {error, _} -> ok
    end.

slow_stream(SendChunk) ->
    _ = SendChunk(binary:copy(<<0>>, 1024)),
    timer:sleep(10000),
    ok.

crash_stream(SendChunk) ->
    _ = SendChunk(binary:copy(<<0>>, 1024)),
    exit(intentional_producer_crash).

drain_with_window_updates(_Sock, Acc, 0) ->
    Acc;
drain_with_window_updates(_Sock, Acc, _Rounds) when Acc >= ?BIG_BODY ->
    Acc;
drain_with_window_updates(Sock, Acc, Rounds) ->
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 0, 10 * ?BIG_BODY),
    ok = nhttp_test_helpers:h2_send_window_update(Sock, 1, 10 * ?BIG_BODY),
    Got = stream_data_size(nhttp_test_helpers:h2_recv(Sock, 400), 1),
    drain_with_window_updates(Sock, Acc + Got, Rounds - 1).

stream_data_size(Frames, StreamId) ->
    lists:foldl(
        fun
            ({data, Sid, Payload, _Fin}, Acc) when Sid =:= StreamId ->
                Acc + byte_size(Payload);
            (_, Acc) ->
                Acc
        end,
        0,
        Frames
    ).

status_of(Frames, StreamId) ->
    case [P || {headers, Sid, P, _} <- Frames, Sid =:= StreamId] of
        [Block | _] ->
            {ok, Dec0} = nhttp_hpack:new(),
            case nhttp_hpack:decode(Block, Dec0) of
                {ok, Headers, _Dec1} -> proplists:get_value(<<":status">>, Headers);
                _ -> undefined
            end;
        [] ->
            undefined
    end.
