-module(cors_handler).
-behaviour(nhttp_handler).

-moduledoc """
CORS example using `nhttp_cors`.

Handles preflight `OPTIONS` requests with `nhttp_cors:preflight/2` and
adds CORS headers to actual responses with `nhttp_cors:headers/2`. The
allowed origin is echoed back from the request `Origin` header so the
example also works against `null` origins (e.g. `file://`).

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => cors_handler}).
```

Then from a different origin:

```js
fetch("http://localhost:8080/data", {method: "GET"})
  .then(r => r.json())
  .then(console.log);
```
""".

-export([init/1, handle_request/2]).

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {reply, nhttp:response(), map()}.
handle_request(#{method := options} = Req, State) ->
    Origin = nhttp_req:header(<<"origin">>, Req, <<"*">>),
    {reply, nhttp_cors:preflight(Origin, cors_opts()), State};
handle_request(#{method := get, path := <<"/data">>} = Req, State) ->
    Origin = nhttp_req:header(<<"origin">>, Req, <<"*">>),
    Headers =
        [
            {<<"content-type">>, <<"application/json">>}
            | nhttp_cors:headers(Origin, cors_opts())
        ],
    Body = <<"{\"hello\":\"world\"}">>,
    {reply, nhttp_resp:ok(Headers, Body), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec cors_opts() -> nhttp_cors:opts().
cors_opts() ->
    #{
        methods => [<<"GET">>, <<"POST">>, <<"OPTIONS">>],
        headers => [<<"content-type">>, <<"authorization">>],
        max_age => 86400
    }.
