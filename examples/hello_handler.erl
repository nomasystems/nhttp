-module(hello_handler).
-behaviour(nhttp_handler).

-moduledoc """
Minimal HTTP handler. Plain text on `/`, JSON on `/json`, 404 otherwise.

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => hello_handler}).
```

Then:
- `curl http://localhost:8080/`
- `curl http://localhost:8080/json`
""".

-export([init/1, handle_request/2]).

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {reply, nhttp:response(), map()}.
handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello, World!">>), State};
handle_request(#{method := get, path := <<"/json">>}, State) ->
    Headers = [{<<"content-type">>, <<"application/json">>}],
    {reply, nhttp_resp:ok(Headers, <<"{\"message\":\"Hello, JSON!\"}">>), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.
