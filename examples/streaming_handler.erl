-module(streaming_handler).
-behaviour(nhttp_handler).

-moduledoc """
Streaming response example using `nhttp_stream:producer/3`.

The producer fun runs in a dedicated worker process; each `SendChunk/1`
blocks until the connection has accepted the chunk, so the producer
sees end-to-end backpressure on every protocol (HTTP/1.1, HTTP/2, HTTP/3).
`SendChunk/1` returns `{error, closed}` once the peer goes away, which
the producer should use to exit early.

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => streaming_handler}).
```

Then `curl -N http://localhost:8080/stream`.
""".

-export([init/1, handle_request/2]).

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {stream, nhttp_stream:spec(), map()} | {reply, nhttp:response(), map()}.
handle_request(#{method := get, path := <<"/stream">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    Producer = fun(SendChunk) -> send_chunks(SendChunk, 0) end,
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec send_chunks(nhttp_stream:send_chunk_fun(), non_neg_integer()) -> ok.
send_chunks(_SendChunk, 10) ->
    ok;
send_chunks(SendChunk, N) ->
    Chunk = <<"Chunk ", (integer_to_binary(N))/binary, "\n">>,
    case SendChunk(Chunk) of
        ok -> send_chunks(SendChunk, N + 1);
        {error, _} -> ok
    end.
