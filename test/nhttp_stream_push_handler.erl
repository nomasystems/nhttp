-module(nhttp_stream_push_handler).

-behaviour(nhttp_handler).

-export([
    handle_request/2,
    init/1,
    terminate/2
]).

-define(TRY_AFTER_TABLE, nhttp_stream_push_try_after).

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/push/basic">>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"alpha">>),
        ok = SendChunk(<<"bravo">>),
        ok = SendChunk(<<"charlie">>)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/single">>}, State) ->
    Producer = fun(SendChunk) ->
        SendChunk(<<"just-one">>)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/empty">>}, State) ->
    Producer = fun(_SendChunk) -> ok end,
    {stream, nhttp_stream:producer(200, [], Producer), State};
handle_request(#{path := <<"/push/large">>}, State) ->
    Producer = fun(SendChunk) ->
        Chunk = binary:copy(<<"A">>, 4 * 1024),
        lists:foreach(
            fun(_) -> ok = SendChunk(Chunk) end,
            lists:seq(1, 32)
        )
    end,
    Headers = [{<<"content-type">>, <<"application/octet-stream">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/xlarge">>}, State) ->
    Producer = fun(SendChunk) ->
        Chunk = binary:copy(<<"B">>, 4 * 1024),
        lists:foreach(
            fun(_) -> ok = SendChunk(Chunk) end,
            lists:seq(1, 640)
        )
    end,
    Headers = [{<<"content-type">>, <<"application/octet-stream">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/crash">>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"before-crash">>),
        erlang:error(intentional_producer_crash)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/closed-probe">>}, State) ->
    Parent = self(),
    Producer = fun(SendChunk) ->
        Table = ?TRY_AFTER_TABLE,
        try
            SendFirst = SendChunk(binary:copy(<<"x">>, 4096)),
            ets:insert(Table, {first_send, SendFirst, Parent}),
            loop_until_closed(SendChunk, Table, Parent)
        after
            ets:insert(Table, {cleanup_ran, true, Parent})
        end
    end,
    Headers = [
        {<<"content-type">>, <<"application/octet-stream">>},
        {<<"cache-control">>, <<"no-store">>}
    ],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/head">>}, State) ->
    Producer = fun(SendChunk) ->
        SendChunk(<<"should-not-reach-wire-for-HEAD">>)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/slow">>}, State) ->
    Table = ?TRY_AFTER_TABLE,
    Parent = self(),
    Producer = fun(SendChunk) ->
        try
            _ = SendChunk(<<"first">>),
            timer:sleep(1500),
            case SendChunk(<<"after-sleep">>) of
                ok -> ets:insert(Table, {slow_result, ok, Parent});
                {error, Reason} -> ets:insert(Table, {slow_result, Reason, Parent})
            end
        after
            ets:insert(Table, {slow_cleanup, true, Parent})
        end
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/no-content">>}, State) ->
    Producer = fun(SendChunk) -> SendChunk(<<"noop">>) end,
    {stream, nhttp_stream:producer(204, [], Producer), State};
handle_request(#{path := <<"/push/not-modified">>}, State) ->
    Producer = fun(SendChunk) -> SendChunk(<<"noop">>) end,
    {stream, nhttp_stream:producer(304, [], Producer), State};
handle_request(#{path := <<"/push/trailers">>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"alpha">>),
        ok = SendChunk(<<"trailing">>),
        {trailers, [{<<"grpc-status">>, <<"0">>}, {<<"x-end">>, <<"done">>}]}
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/push/stuck">>}, State) ->
    Parent = self(),
    Producer = fun(_SendChunk) ->
        ets:insert(?TRY_AFTER_TABLE, {stuck_worker, self(), Parent}),
        receive
        after infinity -> ok
        end
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/block/forever">>}, _State) ->
    ets:insert(?TRY_AFTER_TABLE, {stuck_worker, self(), undefined}),
    receive
    after infinity -> ok
    end;
handle_request(#{path := <<"/reply/ok">>}, State) ->
    {reply, nhttp_resp:ok(<<"reply-ok">>), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL
%%%-----------------------------------------------------------------------------

loop_until_closed(SendChunk, Table, Parent) ->
    Chunk = binary:copy(<<"y">>, 4096),
    case SendChunk(Chunk) of
        ok ->
            loop_until_closed(SendChunk, Table, Parent);
        {error, Reason} ->
            ets:insert(Table, {observed_error, Reason, Parent}),
            ok
    end.
