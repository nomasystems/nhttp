%%%-----------------------------------------------------------------------------
%%% @doc Test handler that does not reply on close (library auto-reciprocates).
%%%
%%% Migrated to the async API: text frames echo back; CLOSE frames are
%%% handled exclusively by the library (RFC 6455 §5.5.1 reciprocal close)
%%% and surfaced to the handler via `handle_ws_closed/3`. The handler does
%%% not need to do anything special on close.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_ws_noreply_handler).

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2,
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

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), term()) ->
    nhttp_handler:ws_result(term()).
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, <<"echo: ", Data/binary>>}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

-spec handle_ws_closed(nhttp_handler:ws_close_reason(), nhttp_ws:session(), term()) -> ok.
handle_ws_closed(_Reason, _Session, _State) ->
    ok.
