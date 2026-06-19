-module(sse_handler).
-behaviour(nhttp_handler).

-moduledoc """
Server-Sent Events example built on `nhttp_sse` + `nhttp_stream:producer/3`.

The producer fun runs in a dedicated worker and ticks every 500 ms,
encoding each event with `nhttp_sse:event/2`. `SendChunk/1` returns
`{error, closed}` when the client disconnects, which terminates the
loop cleanly.

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => sse_handler}).
```

Then `curl -N http://localhost:8080/events`.
""".

-export([init/1, handle_request/2]).

-define(TICK_INTERVAL_MS, 500).
-define(TICK_COUNT, 10).

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {stream, nhttp_stream:spec(), map()} | {reply, nhttp:response(), map()}.
handle_request(#{method := get, path := <<"/events">>}, State) ->
    Producer = fun(SendChunk) -> tick(SendChunk, 0) end,
    Spec = nhttp_stream:producer(200, nhttp_sse:headers(), Producer),
    {stream, Spec, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec tick(nhttp_stream:send_chunk_fun(), non_neg_integer()) -> ok.
tick(_SendChunk, ?TICK_COUNT) ->
    ok;
tick(SendChunk, N) ->
    Event = nhttp_sse:event(<<"tick">>, integer_to_binary(N)),
    case SendChunk(Event) of
        ok ->
            timer:sleep(?TICK_INTERVAL_MS),
            tick(SendChunk, N + 1);
        {error, _} ->
            ok
    end.
