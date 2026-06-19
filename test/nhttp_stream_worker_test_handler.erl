%%%-----------------------------------------------------------------------------
%%% @doc Minimal handler driving nhttp_stream_worker body-phase branches.
%%% The connection role is played by the test process; `HState` selects the
%%% body-callback behaviour.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_stream_worker_test_handler).

-behaviour(nhttp_handler).

-export([init/1, handle_request/2, handle_request_body/3]).

init(Args) ->
    {ok, Args}.

handle_request(_Request, HState) ->
    {accept_body, body0, HState}.

handle_request_body(_Event, _BodyState, crash) ->
    error(boom);
handle_request_body(_Event, _BodyState, always_accept = HState) ->
    {accept_body, body1, HState}.
