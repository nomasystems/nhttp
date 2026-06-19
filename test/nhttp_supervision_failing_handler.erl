-module(nhttp_supervision_failing_handler).

-behaviour(nhttp_handler).

-export([
    handle_request/2,
    init/1
]).

init(_Args) ->
    {error, intentional_failure}.

handle_request(_Req, State) ->
    {reply, #{status => 500, headers => [], body => <<>>}, State}.
