-module(nhttp_stream).

-moduledoc """
Streaming response constructors for nhttp.

Handlers return `{stream, nhttp_stream:spec(), State}` from
`c:nhttp_handler:handle_request/2` to start a chunked response. Build the
`spec()` with `producer/3`.

A `fun((SendChunk) -> _)` runs in a dedicated worker process. `SendChunk/1`
blocks until the connection has accepted the chunk, applying end-to-end
backpressure (TCP for HTTP/1.1, stream flow control for HTTP/2 and HTTP/3).
On peer close `SendChunk/1` returns `{error, closed}` so the producer can
clean up via `try/after`.

```erlang
handle_request(#{method := get, path := <<"/export.csv">>}, State) ->
    Producer = fun(SendChunk) ->
        SendChunk(<<"id,name\\n">>),
        Cursor = db:open_cursor(users),
        try
            stream_rows(Cursor, SendChunk)
        after
            db:close_cursor(Cursor)
        end
    end,
    Headers = [{<<"content-type">>, <<"text/csv">>}],
    Spec = nhttp_stream:producer(200, Headers, Producer),
    {stream, Spec, State}.
```

## Trailers

Return `{trailers, Headers}` from the producer fun to send HTTP trailers
after the body (RFC 9110 §6.5). Trailers terminate the response stream.

```erlang
Producer = fun(SendChunk) ->
    _ = SendChunk(ResponseBody),
    {trailers, [{<<"grpc-status">>, <<"0">>}]}
end.
```

Trailers are wired natively on HTTP/2. On HTTP/1.1 the response is
terminated with the chunked-encoding terminator (without the trailer
fields, since most clients do not implement RFC 9112 §7.1.2 trailer
parsing). On HTTP/3 the stream is closed with FIN.
""".

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    producer/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    producer/0,
    send_chunk_fun/0,
    spec/0
]).

-type send_chunk_fun() :: fun((iodata()) -> ok | {error, closed | timeout}).

-type producer() ::
    fun(
        (send_chunk_fun()) ->
            ok
            | {trailers, nhttp_lib:headers()}
            | {error, term()}
    ).

-type spec() ::
    {producer, nhttp_lib:status(), nhttp_lib:headers(), producer()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Build a producer-form streaming spec.

`Producer` is a fun of arity 1 that receives a `SendChunk` fun and runs
in a dedicated worker process. `SendChunk(IoData)` blocks until the chunk
has been accepted by the connection (end-to-end backpressure). On peer
close it returns `{error, closed}`.

The producer terminates the body by returning. Returning `ok` (or
`{error, _}`) closes the stream cleanly. Returning
`{trailers, Headers}` sends those trailers as the terminator.
""".
-spec producer(nhttp_lib:status(), nhttp_lib:headers(), producer()) -> spec().
producer(Status, Headers, Producer) ->
    {producer, Status, Headers, Producer}.
