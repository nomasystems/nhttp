-module(nhttp_ws_compliance).

-moduledoc """
WebSocket compliance test server.

Starts a plaintext HTTP/1.1 listener that upgrades `/ws` to WebSocket and
echoes every data frame back unchanged, which is what the Autobahn
fuzzing client asserts on. The port is printed in a parseable format for
the Makefile to read.

Section 9 of the suite sends messages up to 16 MiB, so the session raises
`max_message_size` to hold one. Nothing else is configured: the defaults
are what the suite must find.

Usage from the Makefile:

```
rebar3 as test shell --eval 'nhttp_ws_compliance:start().'
```
""".

-behaviour(nhttp_handler).

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    start/0,
    start/1
]).

%%%-----------------------------------------------------------------------------
%% HANDLER EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    handle_request/2,
    handle_ws_closed/3,
    handle_ws_frame/3,
    handle_ws_open/2,
    init/1
]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(MAX_MESSAGE_SIZE, 16777216).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec start() -> no_return().
start() ->
    start(#{}).

-spec start(map()) -> no_return().
start(_Opts) ->
    {ok, _} = application:ensure_all_started(nhttp_lib),
    {ok, _} = application:ensure_all_started(nhttp),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        versions => [http1_1],
        handler => ?MODULE
    }),

    {ok, Port} = nhttp:get_port(Pid),

    io:format("PORT:~B~n", [Port]),

    receive
        stop ->
            nhttp:stop(Pid),
            halt(0)
    end.

%%%-----------------------------------------------------------------------------
%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), #{}) -> nhttp_handler:request_result(#{}).
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:not_found(), State}.

-spec handle_ws_open(nhttp_ws:session(), #{}) ->
    {ok, #{}, nhttp_handler:ws_runtime_opts()}.
handle_ws_open(_Session, State) ->
    {ok, State, #{max_message_size => ?MAX_MESSAGE_SIZE}}.

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), #{}) ->
    nhttp_handler:ws_result(#{}).
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State}.

-spec handle_ws_closed(nhttp_handler:ws_close_reason(), nhttp_ws:session(), #{}) -> ok.
handle_ws_closed(_Reason, _Session, _State) ->
    ok.
