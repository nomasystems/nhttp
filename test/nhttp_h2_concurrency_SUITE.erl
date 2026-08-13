-module(nhttp_h2_concurrency_SUITE).

-moduledoc """
HTTP/2 worker-per-stream concurrency tests.

These cover the invariants that motivate the per-stream-worker
refactor:

- a slow `handle_request/2` does not block other streams on the same
  connection;
- a handler that crashes before replying produces a 500 response;
- a handler that crashes mid-stream is reset cleanly;
- a worker killed externally does not bring down the connection;
- a peer RST_STREAM unblocks the worker (it observes `{error, closed}`
  on its next chunk send);
- drain waits for in-flight workers' responses before exiting.

The wire encoding helpers are inlined here (small, stable HPACK
literals) so the suite is independent of the push suite's helpers.
""".

%%%-----------------------------------------------------------------------------
%%% INCLUDES
%%%-----------------------------------------------------------------------------
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% COMMON TEST CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    drain_waits_for_in_flight_workers/1,
    handler_crash_after_headers_sends_rst/1,
    handler_crash_in_worker_sends_500/1,
    peer_rst_stream_releases_worker/1,
    slow_handler_does_not_block_fast/1,
    worker_killed_externally_does_not_kill_conn/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER (used as the test handler)
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

-define(SLOW_MS, 200).

init(_Args) ->
    {ok, undefined}.

handle_request(#{path := <<"/fast">>}, State) ->
    {reply, nhttp_resp:ok(<<"FAST">>), State};
handle_request(#{path := <<"/slow">>}, State) ->
    timer:sleep(?SLOW_MS),
    {reply, nhttp_resp:ok(<<"SLOW">>), State};
handle_request(#{path := <<"/crash-pre-reply">>}, _State) ->
    erlang:error(boom_pre_reply);
handle_request(#{path := <<"/crash-mid-stream">>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"hello-">>),
        erlang:error(boom_mid_stream)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/long-stream">>}, State) ->
    Producer = fun(SendChunk) ->
        loop_send(SendChunk, 100)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

loop_send(_SendChunk, 0) ->
    ok;
loop_send(SendChunk, N) ->
    timer:sleep(2),
    case SendChunk(<<"x">>) of
        ok -> loop_send(SendChunk, N - 1);
        {error, _} -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        slow_handler_does_not_block_fast,
        handler_crash_in_worker_sends_500,
        handler_crash_after_headers_sends_rst,
        worker_killed_externally_does_not_kill_conn,
        peer_rst_stream_releases_worker,
        drain_waits_for_in_flight_workers
    ].

init_per_suite(Config) ->
    _ = application:ensure_all_started(ssl),
    ConfDir = find_test_conf_dir(),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_regular(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found"}
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

slow_handler_does_not_block_fast(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/slow">>),
        ok = h2_send_request(Sock, 3, <<"/fast">>),
        T0 = erlang:monotonic_time(millisecond),
        Frames = h2_recv_for(Sock, 100),
        FastFrames = [F || F <- Frames, element(2, F) =:= 3],
        ?assert(stream_ended(FastFrames, 3)),
        ?assertEqual(<<"FAST">>, body(FastFrames)),
        FastElapsed = erlang:monotonic_time(millisecond) - T0,
        ?assert(FastElapsed < ?SLOW_MS),
        Frames2 = h2_recv_until_end(Sock, 1, 2000, drop_buf(Frames)),
        SlowFrames = [F || F <- Frames2, element(2, F) =:= 1],
        ?assertEqual(<<"SLOW">>, body(SlowFrames)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

handler_crash_in_worker_sends_500(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/crash-pre-reply">>),
        Frames = h2_recv_until_end(Sock, 1, 3000),
        Status = h2_response_status([F || F <- Frames, element(2, F) =:= 1]),
        ?assertEqual(<<"500">>, Status),
        ok = h2_send_request(Sock, 3, <<"/fast">>),
        Frames2 = h2_recv_until_end(Sock, 3, 3000, drop_buf(Frames)),
        ?assertEqual(<<"FAST">>, body([F || F <- Frames2, element(2, F) =:= 3])),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

handler_crash_after_headers_sends_rst(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/crash-mid-stream">>),
        Frames = h2_recv_until_end(Sock, 1, 3000),
        Stream1 = [F || F <- Frames, element(2, F) =:= 1],
        ?assert(lists:any(fun(F) -> element(1, F) =:= rst_stream end, Stream1)),
        ?assert(
            lists:any(
                fun
                    ({data, _, Payload, _}) -> binary:match(Payload, <<"hello-">>) =/= nomatch;
                    (_) -> false
                end,
                Stream1
            )
        ),
        ?assert(erlang:is_process_alive(Pid)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

worker_killed_externally_does_not_kill_conn(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/slow">>),
        ok = nhttp_test_helpers:wait_until(
            fun() -> find_worker_pid() =/= undefined end, 2000
        ),
        WorkerPid = find_worker_pid(),
        ?assert(is_pid(WorkerPid)),
        exit(WorkerPid, kill),
        ok = h2_send_request(Sock, 3, <<"/fast">>),
        Frames = h2_recv_until_end(Sock, 3, 3000),
        ?assertEqual(<<"FAST">>, body([F || F <- Frames, element(2, F) =:= 3])),
        ?assert(erlang:is_process_alive(Pid)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

peer_rst_stream_releases_worker(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/long-stream">>),
        _ = ssl:recv(Sock, 0, 2000),
        ok = nhttp_test_helpers:wait_until(
            fun() -> [P || P <- processes(), proc_is_worker(P)] =/= [] end, 2000
        ),
        WorkersBefore = lists:filter(
            fun(P) -> proc_is_worker(P) end, processes()
        ),
        ?assert(WorkersBefore =/= []),
        RstFrame = <<0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8>>,
        ok = ssl:send(Sock, RstFrame),
        ok = nhttp_test_helpers:wait_until(
            fun() ->
                lists:all(
                    fun(P) -> not erlang:is_process_alive(P) end,
                    WorkersBefore
                )
            end,
            2000
        ),
        ?assert(erlang:is_process_alive(Pid)),
        ssl:close(Sock)
    after
        nhttp:stop(Pid)
    end.

drain_waits_for_in_flight_workers(Config) ->
    {Pid, Port} = start_server(Config),
    try
        {ok, Sock} = h2_connect(Port),
        ok = h2_send_request(Sock, 1, <<"/slow">>),
        ok = nhttp_test_helpers:wait_until(
            fun() -> find_worker_pid() =/= undefined end, 2000
        ),
        Self = self(),
        spawn(fun() ->
            ok = nhttp_listener:drain(Pid, 5000),
            Self ! drain_done
        end),
        Frames = h2_recv_until_end(Sock, 1, 3000),
        ?assertEqual(<<"SLOW">>, body([F || F <- Frames, element(2, F) =:= 1])),
        receive
            drain_done -> ok
        after 6000 ->
            error(drain_timeout)
        end,
        ssl:close(Sock)
    after
        try
            nhttp:stop(Pid)
        catch
            _:_ -> ok
        end
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start_server(Config) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    Opts = #{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => ?MODULE,
        versions => [http2]
    },
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

find_test_conf_dir() ->
    ModPath = code:which(?MODULE),
    TestDir = filename:dirname(ModPath),
    filename:join(TestDir, "conf").

find_worker_pid() ->
    AllPids = processes(),
    lists:foldl(
        fun
            (P, undefined) ->
                case proc_is_worker(P) of
                    true -> P;
                    false -> undefined
                end;
            (_, Found) ->
                Found
        end,
        undefined,
        AllPids
    ).

proc_is_worker(P) ->
    case process_info(P, dictionary) of
        {dictionary, Dict} ->
            case lists:keyfind('$initial_call', 1, Dict) of
                {_, {nhttp_stream_worker, _, _}} -> true;
                _ -> false
            end;
        _ ->
            false
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 CLIENT HELPERS (inline)
%%%-----------------------------------------------------------------------------

h2_connect(Port) ->
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
    _ = ssl:recv(Sock, 0, 2000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    {ok, Sock}.

h2_send_request(Sock, StreamId, Path) ->
    PathLen = byte_size(Path),
    Header = <<16#82, 16#87, 16#44, PathLen, Path/binary>>,
    Frame = <<(byte_size(Header)):24, 1, 5, 0:1, StreamId:31, Header/binary>>,
    ssl:send(Sock, Frame).

h2_recv_for(Sock, Timeout) ->
    h2_recv_for(Sock, Timeout, <<>>, []).

h2_recv_for(_Sock, Timeout, Buf, Acc) when Timeout =< 0 ->
    {Frames, _Rest} = decode_frames(Buf, []),
    Acc ++ Frames;
h2_recv_for(Sock, Timeout, Buf, Acc) ->
    T0 = erlang:monotonic_time(millisecond),
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            {Frames, Rest} = decode_frames(NewBuf, []),
            _ = maybe_window_update(Sock, all, Frames),
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            h2_recv_for(Sock, Timeout - Elapsed, Rest, Acc ++ Frames);
        {error, _} ->
            {Frames, _Rest} = decode_frames(Buf, []),
            Acc ++ Frames
    end.

h2_recv_until_end(Sock, StreamId, Timeout) ->
    h2_recv_until_end(Sock, StreamId, Timeout, []).

h2_recv_until_end(Sock, StreamId, Timeout, InitialFrames) ->
    h2_recv_until_end_loop(Sock, StreamId, Timeout, <<>>, InitialFrames).

h2_recv_until_end_loop(Sock, StreamId, Timeout, Buf, Acc) ->
    case stream_ended([F || F <- Acc, element(2, F) =:= StreamId], StreamId) of
        true ->
            Acc;
        false ->
            case ssl:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    NewBuf = <<Buf/binary, Data/binary>>,
                    {Frames, Rest} = decode_frames(NewBuf, []),
                    _ = maybe_window_update(Sock, all, Frames),
                    h2_recv_until_end_loop(
                        Sock, StreamId, Timeout, Rest, Acc ++ Frames
                    );
                {error, _} ->
                    Acc
            end
    end.

drop_buf(Frames) ->
    Frames.

stream_ended(Frames, StreamId) ->
    lists:any(
        fun
            ({data, SId, _, fin}) when SId =:= StreamId -> true;
            ({headers, SId, _, fin}) when SId =:= StreamId -> true;
            ({rst_stream, SId, _}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

body(Frames) ->
    iolist_to_binary([P || {data, _, P, _} <- Frames]).

h2_response_status(Frames) ->
    HeaderBlocks = [P || {headers, _, P, _} <- Frames],
    case HeaderBlocks of
        [Block | _] ->
            {ok, Dec0} = nhttp_hpack:new(),
            case nhttp_hpack:decode(Block, Dec0) of
                {ok, Headers, _Dec1} ->
                    proplists:get_value(<<":status">>, Headers);
                _ ->
                    undefined
            end;
        [] ->
            undefined
    end.

maybe_window_update(Sock, Target, Frames) ->
    lists:foreach(
        fun
            ({data, SId, Payload, _}) when Target =:= SId orelse Target =:= all ->
                send_window_update(Sock, 0, byte_size(Payload)),
                send_window_update(Sock, SId, byte_size(Payload));
            (_) ->
                ok
        end,
        Frames
    ).

send_window_update(_Sock, _SId, 0) ->
    ok;
send_window_update(Sock, SId, Inc) ->
    Frame = <<0, 0, 4, 8, 0, 0:1, SId:31, 0:1, Inc:31>>,
    ssl:send(Sock, Frame).

decode_frames(
    <<Len:24, Type:8, Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>>, Acc
) ->
    Frame = decode_frame(Type, Flags, StreamId, Payload),
    decode_frames(Rest, [Frame | Acc]);
decode_frames(Other, Acc) ->
    {lists:reverse(Acc), Other}.

decode_frame(0, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            _ -> nofin
        end,
    {data, StreamId, Payload, Fin};
decode_frame(1, Flags, StreamId, Payload) ->
    Fin =
        case Flags band 1 of
            1 -> fin;
            _ -> nofin
        end,
    {headers, StreamId, Payload, Fin};
decode_frame(3, _Flags, StreamId, <<Code:32>>) ->
    {rst_stream, StreamId, Code};
decode_frame(4, _Flags, StreamId, Payload) ->
    {settings, StreamId, Payload};
decode_frame(7, _Flags, StreamId, Payload) ->
    {goaway, StreamId, Payload};
decode_frame(8, _Flags, StreamId, <<Inc:32>>) ->
    {window_update, StreamId, Inc};
decode_frame(Type, _Flags, StreamId, Payload) ->
    {other, StreamId, Type, Payload}.
