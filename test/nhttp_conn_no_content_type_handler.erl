%%%-----------------------------------------------------------------------------
%%% @doc Test handler that returns responses without content-type.
%%%
%%% Used for testing compression behavior when content-type is not set.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_no_content_type_handler).

-behaviour(nhttp_handler).

-export([init/1, handle_request/2]).

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), term()) -> {reply, nhttp_lib:response(), term()}.
handle_request(#{path := <<"/no-ct">>}, State) ->
    {reply,
        #{
            status => 200,
            headers => [],
            body => <<"This is a response body without content-type header for testing">>
        },
        State};
handle_request(_Req, State) ->
    {reply, #{status => 200, headers => [], body => <<"OK">>}, State}.
