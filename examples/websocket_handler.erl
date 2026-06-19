-module(websocket_handler).
-behaviour(nhttp_handler).

-moduledoc """
WebSocket echo handler using the async `nhttp_handler` WS surface.

`handle_ws_open/2` fires after the upgrade response is on the wire,
`handle_ws_frame/3` receives data frames (PING/PONG are auto-handled),
`handle_ws_closed/3` runs at session termination with an RFC-grounded
reason. CLOSE is not delivered to `handle_ws_frame/3`. See `nhttp_handler`.

## Usage

```erlang
{ok, _} = nhttp:start_link(#{port => 8080, handler => websocket_handler}).
```

Then connect to `ws://localhost:8080/ws`; text/binary frames are echoed
with an `Echo: ` prefix on text.
""".

-export([
    init/1,
    handle_request/2,
    handle_ws_open/2,
    handle_ws_frame/3,
    handle_ws_closed/3
]).

-spec init(term()) -> {ok, map()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp:request(), map()) ->
    {upgrade, websocket, map()} | {reply, nhttp:response(), map()}.
handle_request(#{path := <<"/ws">>} = Req, State) ->
    case nhttp_req:is_websocket(Req) of
        true -> {upgrade, websocket, State};
        false -> {reply, nhttp_resp:not_found(), State}
    end;
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec handle_ws_open(nhttp_ws:session(), map()) -> {ok, map()}.
handle_ws_open(_Session, State) ->
    {ok, State}.

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), map()) ->
    nhttp_handler:ws_result(map()).
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, <<"Echo: ", Data/binary>>}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

-spec handle_ws_closed(nhttp_handler:ws_close_reason(), nhttp_ws:session(), map()) ->
    ok.
handle_ws_closed(_Reason, _Session, _State) ->
    ok.
