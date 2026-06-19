%%%-----------------------------------------------------------------------------
%%% @doc Test handler without handle_websocket/2 for coverage testing.
%%%
%%% This handler supports WebSocket upgrade but deliberately does NOT export
%%% handle_websocket/2, which allows testing the "no handler" code path in
%%% nhttp_conn.erl where WebSocket messages are silently ignored.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_no_ws_handler).

-behaviour(nhttp_handler).

-export([init/1, handle_request/2]).

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), term()) ->
    {reply, nhttp_lib:response(), term()} | {upgrade, websocket, term()}.
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, #{status => 200, headers => [], body => <<"OK">>}, State}.
