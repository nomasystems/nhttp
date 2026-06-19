%%%-----------------------------------------------------------------------------
%%% @doc Test handler that returns {reply, _} for ping messages.
%%%
%%% Migrated to the async API: the handler asks for `deliver_ping => true`
%%% in `handle_ws_open/2` so that PING/PONG observability still goes through
%%% `handle_ws_frame/3`. CLOSE handling moves to `handle_ws_closed/3` since
%%% the library auto-reciprocates per RFC 6455 §5.5.1.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_ws_reply_handler).

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2,
    handle_ws_open/2,
    handle_ws_frame/3,
    handle_ws_closed/3
]).

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), term()) ->
    {reply, nhttp_lib:response(), term()} | {upgrade, websocket, term()}.
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, #{status => 200, headers => [], body => <<"OK">>}, State}.

-spec handle_ws_open(nhttp_ws:session(), term()) ->
    {ok, term(), nhttp_ws:ws_runtime_opts()}.
handle_ws_open(_Session, State) ->
    {ok, State, #{deliver_ping => true}}.

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), term()) ->
    nhttp_handler:ws_result(term()).
handle_ws_frame({ping, <<>>}, _Session, State) ->
    {reply, {text, <<"ping received">>}, State};
handle_ws_frame({ping, _Data}, _Session, State) ->
    {reply, {text, <<"ping with data received">>}, State};
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, <<"echo: ", Data/binary>>}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

-spec handle_ws_closed(nhttp_handler:ws_close_reason(), nhttp_ws:session(), term()) -> ok.
handle_ws_closed(_Reason, _Session, _State) ->
    ok.
