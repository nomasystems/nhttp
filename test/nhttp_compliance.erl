%%%-----------------------------------------------------------------------------
%%% @doc HTTP/2 RFC 9113 compliance test server.
%%%
%%% This module provides a minimal HTTP/2 server for running h2spec compliance
%%% tests. It starts an HTTP/2 over TLS server and prints the port in a
%%% parseable format for use by the Makefile.
%%%
%%% Usage from Makefile:
%%%   erl -noinput -pa ebin -eval 'nhttp_compliance:start().'
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_compliance).

-behaviour(nhttp_handler).

-export([
    start/0,
    start/1
]).

-export([
    init/1,
    handle_request/2
]).

-define(DEFAULT_CERT_DIR, "test/conf").

-spec start() -> no_return().
start() ->
    start(#{}).

-spec start(map()) -> no_return().
start(Opts) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(asn1),
    {ok, _} = application:ensure_all_started(public_key),
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(nhttp_lib),
    {ok, _} = application:ensure_all_started(nhttp),

    CertDir = maps:get(cert_dir, Opts, ?DEFAULT_CERT_DIR),
    CertFile = maps:get(certfile, Opts, filename:join(CertDir, "server.pem")),
    KeyFile = maps:get(keyfile, Opts, filename:join(CertDir, "server.key")),

    case filelib:is_file(CertFile) of
        false ->
            io:format(standard_error, "Error: Certificate not found: ~s~n", [CertFile]),
            halt(1);
        true ->
            ok
    end,

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http2],
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

-spec handle_request(nhttp_lib:request(), #{}) -> {reply, nhttp_lib:response(), #{}}.
handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.
