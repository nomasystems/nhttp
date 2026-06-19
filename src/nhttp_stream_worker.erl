-module(nhttp_stream_worker).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    reap/1,
    start/3,
    start_request/6,
    stop/1
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB ENTRY
%%%-----------------------------------------------------------------------------
-export([
    reaper/1,
    run/3,
    run_request/6
]).

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(TERMINAL_KEY, '$nhttp_stream_terminal').
-define(REAP_GRACE, 5000).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Reap outstanding workers on connection teardown without blocking the
caller.

Spawns a detached reaper that monitors each worker and waits up to the
grace window. A worker that exits on its own (a graceful producer
reacting to the conn's `DOWN`, or one already finished) is dropped; any
survivor past the grace is `kill`ed. This preserves a producer's chance
to clean up on disconnect while guaranteeing a stuck worker cannot
orphan.
""".
-spec reap([pid()]) -> ok.
reap([]) ->
    ok;
reap(Pids) ->
    _Reaper = spawn(?MODULE, reaper, [Pids]),
    ok.

-doc """
Spawn a worker to run `Producer(SendChunk)`.

The worker process owns the producer's execution. The connection process
(`ConnPid`) owns the socket and applies wire-level flow control. Messages
between worker and conn are tagged with `Ref`.

The worker is spawned with `proc_lib:spawn/3` (no link): a producer crash
does not propagate to the connection. The connection should monitor the
returned pid to learn when the worker exits.

Monitoring alone only reaps a worker that is parked at a `receive` (it
sees the conn's `DOWN` and exits). A producer stuck in handler code (a
blocked external call, an infinite loop) never reaches a `receive`, so
it would outlive conn death, drain, and application shutdown. The
connection therefore `reap/1`s every outstanding worker on termination:
graceful workers exit on the conn's `DOWN` within the grace window and
are dropped, while stuck survivors are `kill`ed. See
`nhttp_conn:terminate/2`, `nhttp_conn_h3:terminate/2`, and the h1 push
teardown.
""".
-spec start(pid(), reference(), nhttp_stream:producer()) -> pid().
start(ConnPid, Ref, Producer) ->
    proc_lib:spawn(?MODULE, run, [ConnPid, Ref, Producer]).

-doc """
Spawn a worker to run `Handler:handle_request(Request, HState)` and post
the result to the conn as `{request_result, StreamId, Ref, Result}`.
For `{reply, _, _}`, `{abort, _, _}`, and exception results the worker
posts the result and exits. For `{stream, Spec, _}` streaming results
the worker posts the result then enters the iterator/producer loop,
sending chunks via `{send_chunk, ...}` messages and blocking on
`chunk_ack` for backpressure. For `{accept_body, _, _}` results the
worker posts the result and waits for `{body_chunk, Ref, Event}`
messages from the conn, dispatching each into
`Handler:handle_request_body/3`.
Exceptions raised by streaming callbacks are surfaced via the worker's
exit reason. The conn monitors the pid and emits the appropriate
response.
""".
-spec start_request(
    pid(), nhttp_lib:stream_id(), reference(), module(), nhttp_lib:request(), term()
) ->
    pid().
start_request(ConnPid, StreamId, Ref, Handler, Request, HState) ->
    proc_lib:spawn(?MODULE, run_request, [ConnPid, StreamId, Ref, Handler, Request, HState]).

-doc """
Unconditionally `kill` a single worker.

Used where the connection has already given the worker its chance to
finish (the h1 push pipeline hand-off) and only needs to reap a stuck
survivor. For teardown of a worker map use `reap/1`, which preserves
graceful cleanup.
""".
-spec stop(pid()) -> ok.
stop(WorkerPid) ->
    true = exit(WorkerPid, kill),
    ok.

%%%-----------------------------------------------------------------------------
%% PROC_LIB ENTRY
%%%-----------------------------------------------------------------------------
-spec reaper([pid()]) -> ok.
reaper(Pids) ->
    MRefs = maps:from_list([{erlang:monitor(process, P), P} || P <- Pids]),
    reaper_wait(MRefs).

-spec run(pid(), reference(), nhttp_stream:producer()) -> ok.
run(ConnPid, Ref, Producer) ->
    MRef = erlang:monitor(process, ConnPid),
    SendChunk = fun(Data) -> send_chunk(ConnPid, Ref, MRef, Data) end,
    finalize_producer(ConnPid, Ref, MRef, Producer(SendChunk)).

-spec run_request(
    pid(), nhttp_lib:stream_id(), reference(), module(), nhttp_lib:request(), term()
) -> ok.
run_request(ConnPid, StreamId, Ref, Handler, Request, HState) ->
    MRef = erlang:monitor(process, ConnPid),
    Result = nhttp_handler:safe_handle_request(Handler, Request, HState),
    dispatch_result(ConnPid, StreamId, Ref, MRef, Handler, Result, HState).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec ack_body_chunk(pid(), reference()) -> ok.
ack_body_chunk(ConnPid, Ref) ->
    ConnPid ! {body_chunk_ack, self(), Ref},
    ok.

-spec dispatch_result(
    pid(),
    nhttp_lib:stream_id(),
    reference(),
    reference(),
    module(),
    term(),
    term()
) -> ok.
dispatch_result(ConnPid, StreamId, Ref, _MRef, _Handler, {reply, _, _} = Result, _HState) ->
    post_result(ConnPid, StreamId, Ref, Result),
    ok;
dispatch_result(
    ConnPid, StreamId, Ref, MRef, _Handler, {stream, Spec, _NewHState} = Result, _HState
) ->
    post_result(ConnPid, StreamId, Ref, Result),
    run_stream_spec(ConnPid, Ref, MRef, Spec);
dispatch_result(
    ConnPid, StreamId, Ref, MRef, Handler, {accept_body, _, _} = Result, _HState
) ->
    post_result(ConnPid, StreamId, Ref, Result),
    run_body(ConnPid, StreamId, Ref, MRef, Handler, Result);
dispatch_result(ConnPid, StreamId, Ref, _MRef, _Handler, {upgrade, websocket, _} = Result, _HState) ->
    post_result(ConnPid, StreamId, Ref, Result),
    ok;
dispatch_result(
    ConnPid, StreamId, Ref, _MRef, _Handler, {upgrade, websocket, _, _} = Result, _HState
) ->
    post_result(ConnPid, StreamId, Ref, Result),
    ok;
dispatch_result(ConnPid, StreamId, Ref, _MRef, _Handler, {abort, _, _} = Result, _HState) ->
    post_result(ConnPid, StreamId, Ref, Result),
    ok;
dispatch_result(
    ConnPid, StreamId, Ref, _MRef, _Handler, {nhttp_handler_exception, _, _} = Exc, _HState
) ->
    post_result(ConnPid, StreamId, Ref, Exc),
    ok;
dispatch_result(ConnPid, StreamId, Ref, _MRef, _Handler, Other, HState) ->
    post_result(ConnPid, StreamId, Ref, {abort, {bad_handler_return, Other}, HState}),
    ok.

-spec finalize_producer(
    pid(),
    reference(),
    reference(),
    ok | {trailers, nhttp_lib:headers()} | {error, term()} | term()
) -> ok.
finalize_producer(ConnPid, Ref, MRef, {trailers, Trailers}) when is_list(Trailers) ->
    send_trailers(ConnPid, Ref, MRef, Trailers);
finalize_producer(ConnPid, Ref, MRef, _Other) ->
    finish(ConnPid, Ref, MRef).

-spec finish(pid(), reference(), reference()) -> ok.
finish(ConnPid, Ref, MRef) ->
    case get(?TERMINAL_KEY) of
        closed ->
            ok;
        _ ->
            ConnPid ! {stream_done, self(), Ref},
            receive
                {chunk_ack, Ref, _Result} ->
                    ok;
                {'DOWN', MRef, process, ConnPid, _Reason} ->
                    ok
            end
    end.

-spec post_result(pid(), nhttp_lib:stream_id(), reference(), term()) -> ok.
post_result(ConnPid, StreamId, Ref, Result) ->
    ConnPid ! {request_result, StreamId, Ref, Result},
    ok.

-spec reaper_wait(#{reference() => pid()}) -> ok.
reaper_wait(MRefs) when map_size(MRefs) =:= 0 ->
    ok;
reaper_wait(MRefs) ->
    receive
        {'DOWN', MRef, process, _Pid, _Reason} ->
            reaper_wait(maps:remove(MRef, MRefs))
    after ?REAP_GRACE ->
        ok = lists:foreach(fun(Pid) -> exit(Pid, kill) end, maps:values(MRefs))
    end.

-spec run_body(
    pid(),
    nhttp_lib:stream_id(),
    reference(),
    reference(),
    module(),
    {accept_body, term(), term()}
) -> ok.
run_body(ConnPid, StreamId, Ref, MRef, Handler, {accept_body, BodyState, HState}) ->
    receive
        {body_chunk, Ref, Event} ->
            ack_body_chunk(ConnPid, Ref),
            Result = nhttp_handler:safe_handle_request_body(
                Handler, Event, BodyState, HState
            ),
            case Result of
                {accept_body, _NewBodyState, _NewHState} = Next ->
                    case Event of
                        {fin, _} ->
                            post_result(
                                ConnPid, StreamId, Ref, {abort, body_unconsumed, element(3, Next)}
                            ),
                            ok;
                        {abort, _} ->
                            post_result(
                                ConnPid, StreamId, Ref, {abort, peer_aborted, element(3, Next)}
                            ),
                            ok;
                        _ ->
                            run_body(ConnPid, StreamId, Ref, MRef, Handler, Next)
                    end;
                {nhttp_handler_exception, _, _} = Exc ->
                    post_result(ConnPid, StreamId, Ref, Exc),
                    ok;
                Final ->
                    dispatch_result(ConnPid, StreamId, Ref, MRef, Handler, Final, HState)
            end;
        {'DOWN', MRef, process, ConnPid, _Reason} ->
            ok
    end.

-spec run_stream_spec(
    pid(), reference(), reference(), nhttp_stream:spec()
) -> ok.
run_stream_spec(ConnPid, Ref, MRef, {producer, _Status, _Headers, Producer}) ->
    SendChunk = fun(Data) -> send_chunk(ConnPid, Ref, MRef, Data) end,
    finalize_producer(ConnPid, Ref, MRef, Producer(SendChunk)).

-spec send_chunk(pid(), reference(), reference(), iodata()) ->
    ok | {error, closed | timeout}.
send_chunk(ConnPid, Ref, MRef, Data) ->
    case get(?TERMINAL_KEY) of
        closed ->
            {error, closed};
        _ ->
            ConnPid ! {send_chunk, self(), Ref, Data},
            receive
                {chunk_ack, Ref, ok} ->
                    ok;
                {chunk_ack, Ref, {error, _Reason} = Err} ->
                    put(?TERMINAL_KEY, closed),
                    Err;
                {'DOWN', MRef, process, ConnPid, _Reason} ->
                    put(?TERMINAL_KEY, closed),
                    {error, closed}
            end
    end.

-spec send_trailers(pid(), reference(), reference(), nhttp_lib:headers()) -> ok.
send_trailers(ConnPid, Ref, MRef, TrailerHeaders) ->
    case get(?TERMINAL_KEY) of
        closed ->
            ok;
        _ ->
            ConnPid ! {send_trailers, self(), Ref, TrailerHeaders},
            receive
                {chunk_ack, Ref, _Result} ->
                    ok;
                {'DOWN', MRef, process, ConnPid, _Reason} ->
                    ok
            end
    end.
