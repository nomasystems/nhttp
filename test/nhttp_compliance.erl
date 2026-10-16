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
    handle_request/2,
    handle_request_body/3
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

-doc """
Consume a streaming request body before the reply. An early reply makes the
connection reset the stream with NO_ERROR (RFC 9113 Section 8.1), and the
codec then discards the frames that h2spec sends to provoke a stream error.
""".
-spec handle_request(nhttp_lib:request(), #{}) -> nhttp_handler:request_result(#{}).
handle_request(#{body := streaming}, State) ->
    {accept_body, unread, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.

-spec handle_request_body(nhttp_handler:body_event(), unread, #{}) ->
    nhttp_handler:request_result(#{}).
handle_request_body({data, _Chunk}, unread, State) ->
    {accept_body, unread, State};
handle_request_body({fin, _Trailers}, unread, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State};
handle_request_body({abort, Reason}, unread, State) ->
    {abort, Reason, State}.
