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

## Request body on HTTP/2

A producer of arity 2 reads the request body of its stream while it sends
the response. Its second argument is `RecvBody`. `RecvBody(Timeout)`
returns the next body event: `{data, Binary}`, `{fin, Trailers}` or
`{abort, Reason}`. It returns `timeout` when no event arrives in
`Timeout` milliseconds.

```erlang
handle_request(#{path := <<"/echo">>}, State) ->
    Producer = fun(SendChunk, RecvBody) -> echo(SendChunk, RecvBody) end,
    {stream, nhttp_stream:producer(200, [], Producer), State}.

echo(SendChunk, RecvBody) ->
    case RecvBody(infinity) of
        {data, Chunk} ->
            case SendChunk(Chunk) of
                ok -> echo(SendChunk, RecvBody);
                {error, _Reason} -> ok
            end;
        {fin, _Trailers} ->
            {trailers, [{<<"x-echo">>, <<"done">>}]};
        {abort, _Reason} ->
            ok
    end.
```

The events arrive in the order of the request. The body that the
connection received before the `{stream, _, _}` result comes first. An
empty DATA frame gives `{data, <<>>}`.

`{fin, _}` and `{abort, _}` are the last event. Each later call returns
the same event. `Reason` has one of these values:

- `{peer_reset, ErrorCode}`: the peer reset the stream. `SendChunk`
  returns `{error, closed}`.
- `body_too_large`: the body exceeded `max_body_size`. The connection
  reset the stream.
- `closed`: the stream or the connection ended and no other event says
  why.

The connection credits the HTTP/2 receive windows for a chunk when
`RecvBody` returns it. A producer that does not read stops the client
after one window, and the worker holds one window of request data at
most.

The producer is one process. It waits in `SendChunk` or in `RecvBody`,
not in the two. If the producer waits in `SendChunk` for the send window
while the client waits for receive credit, the stream stops. Make sure
that the producer reads the request while the client sends it.

A producer that returns before the end of the request ends the response.
The connection then resets the stream with NO_ERROR, which tells the
client to stop the request.

A producer of arity 1 reads nothing. The connection drops the request
body that arrives on its stream and credits the windows.

HTTP/1.1 and HTTP/3 do not support a producer of arity 2. The result
gets a 500 response and a `stream_push_rejected` log line.
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
    duplex_producer/0,
    producer/0,
    producer_result/0,
    recv_body_fun/0,
    send_chunk_fun/0,
    spec/0
]).

-type send_chunk_fun() :: fun((iodata()) -> ok | {error, closed | timeout}).

-type recv_body_fun() :: fun((timeout()) -> nhttp_handler:body_event() | timeout).

-type producer_result() :: ok | {trailers, nhttp_lib:headers()} | {error, term()}.

-type producer() :: fun((send_chunk_fun()) -> producer_result()).

-type duplex_producer() :: fun((send_chunk_fun(), recv_body_fun()) -> producer_result()).

-type spec() ::
    {producer, nhttp_lib:status(), nhttp_lib:headers(), producer() | duplex_producer()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Build a producer-form streaming spec.

`Producer` is a fun of arity 1 that receives a `SendChunk` fun and runs
in a dedicated worker process. `SendChunk(IoData)` blocks until the chunk
has been accepted by the connection (end-to-end backpressure). On peer
close it returns `{error, closed}`.

A fun of arity 2 also receives a `RecvBody` fun that reads the request
body on HTTP/2. See "Request body on HTTP/2" in the module documentation.

The producer terminates the body by returning. Returning `ok` (or
`{error, _}`) closes the stream cleanly. Returning
`{trailers, Headers}` sends those trailers as the terminator.
""".
-spec producer(nhttp_lib:status(), nhttp_lib:headers(), producer() | duplex_producer()) -> spec().
producer(Status, Headers, Producer) when is_function(Producer, 1); is_function(Producer, 2) ->
    {producer, Status, Headers, Producer}.
