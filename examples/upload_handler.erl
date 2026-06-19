-module(upload_handler).
-behaviour(nhttp_handler).

-moduledoc """
Streaming file upload example using `accept_body` + `handle_request_body/3`.

Returning `{accept_body, BodyState, State}` from `handle_request/2`
opts into chunked delivery. Each `{data, Chunk}` event is acknowledged
by returning `{accept_body, _, _}` again, which applies end-to-end
backpressure (TCP recv on HTTP/1.1, WINDOW_UPDATE on HTTP/2, QUIC stream
flow control on HTTP/3). The destination file is opened lazily on the
first data event so empty bodies don't create a zero-byte file, and is
closed on both `{fin, _}` and `{abort, _}` because `terminate/2` is
connection-scoped, not request-scoped.

`max_body_size` on the listener remains a hard upper bound regardless
of handler willingness.

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => upload_handler}).
```

Then:

```bash
curl --data-binary @somefile.bin http://localhost:8080/upload
```

Uploads land under `/tmp/nhttp-upload-<unique>.bin`.
""".

-export([init/1, handle_request/2, handle_request_body/3]).

-type body_state() :: pending | {file:io_device(), file:filename(), non_neg_integer()}.

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {accept_body, body_state(), map()} | {reply, nhttp:response(), map()}.
handle_request(#{method := post, path := <<"/upload">>}, State) ->
    {accept_body, pending, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec handle_request_body(nhttp_handler:body_event(), body_state(), map()) ->
    {accept_body, body_state(), map()}
    | {reply, nhttp:response(), map()}
    | {abort, term(), map()}.
handle_request_body({data, Chunk}, pending, State) ->
    {ok, Fd, Path} = open_upload(),
    ok = file:write(Fd, Chunk),
    {accept_body, {Fd, Path, byte_size(Chunk)}, State};
handle_request_body({data, Chunk}, {Fd, Path, Bytes}, State) ->
    ok = file:write(Fd, Chunk),
    {accept_body, {Fd, Path, Bytes + byte_size(Chunk)}, State};
handle_request_body({fin, _Trailers}, pending, State) ->
    {reply, nhttp_resp:ok(<<"0 bytes uploaded\n">>), State};
handle_request_body({fin, _Trailers}, {Fd, Path, Bytes}, State) ->
    ok = file:close(Fd),
    Body = iolist_to_binary([
        integer_to_binary(Bytes), <<" bytes uploaded to ">>, Path, <<"\n">>
    ]),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, pending, State) ->
    {abort, Reason, State};
handle_request_body({abort, Reason}, {Fd, Path, _Bytes}, State) ->
    ok = file:close(Fd),
    ok = file:delete(Path),
    {abort, Reason, State}.

-spec open_upload() -> {ok, file:io_device(), file:filename()}.
open_upload() ->
    Unique = integer_to_list(erlang:unique_integer([positive, monotonic])),
    Path = filename:join("/tmp", "nhttp-upload-" ++ Unique ++ ".bin"),
    {ok, Fd} = file:open(Path, [write, raw, binary]),
    {ok, Fd, Path}.
