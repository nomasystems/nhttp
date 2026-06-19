%%%-----------------------------------------------------------------------------
%%% @doc HTTP/1.1 compliance test server.
%%%
%%% This module provides a minimal HTTP/1.1 server for running h1spec compliance
%%% tests. It starts a plaintext HTTP/1.1 server, echoes back the request body
%%% (h1spec requires the server to echo whatever body it receives, for all
%%% methods) and prints the port in a parseable format for use by the Makefile.
%%%
%%% Usage from Makefile:
%%%   erl -noinput -pa ebin -eval 'nhttp_h1_compliance:start().'
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_h1_compliance).

-behaviour(nhttp_handler).

-export([
    start/0,
    start/1
]).

-export([
    init/1,
    handle_request/2,
    handle_request_body/3
]).

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

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), #{}) -> nhttp_handler:request_result(#{}).
handle_request(#{headers := Headers}, State) ->
    case has_body(Headers) of
        true ->
            {accept_body, [], State};
        false ->
            {reply, nhttp_resp:ok(<<>>), State}
    end.

-spec handle_request_body(nhttp_handler:body_event(), [binary()], #{}) ->
    nhttp_handler:request_result(#{}).
handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

-spec has_body(nhttp_lib:headers()) -> boolean().
has_body(Headers) ->
    nhttp_headers:get(<<"content-length">>, Headers) =/= undefined orelse
        nhttp_headers:get(<<"transfer-encoding">>, Headers) =/= undefined.
