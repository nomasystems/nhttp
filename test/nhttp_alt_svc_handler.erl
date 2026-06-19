%%%-----------------------------------------------------------------------------
%%% @doc Test handler for nhttp_alt_svc_SUITE.
%%%
%%% `/hello' returns a plain response (framework adds Alt-Svc), `/override'
%%% sets its own `alt-svc' header (framework defers), `/suppress' sets an
%%% empty `alt-svc' header (framework drops it).
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_alt_svc_handler).

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2
]).

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/hello">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello!">>), State};
handle_request(#{path := <<"/override">>}, State) ->
    Headers = [{<<"alt-svc">>, <<"h3=\":9999\"; ma=1">>}],
    {reply, nhttp_resp:ok(Headers, <<"Hello!">>), State};
handle_request(#{path := <<"/suppress">>}, State) ->
    Headers = [{<<"alt-svc">>, <<>>}],
    {reply, nhttp_resp:ok(Headers, <<"Hello!">>), State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:not_found(), State}.
