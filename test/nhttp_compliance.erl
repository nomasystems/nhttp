%%%-----------------------------------------------------------------------------
%%% @doc HTTP/2 RFC 9113 compliance test server.
%%%
%%% This module provides a minimal HTTP/2 server for running h2spec compliance
%%% tests. It starts an HTTP/2 over TLS server, or a cleartext server with
%%% HTTP/1.1 and h2c on one port, and prints the port in a parseable format
%%% for use by the Makefile.
%%%
%%% Usage from Makefile:
%%%   erl -noinput -pa ebin -eval 'nhttp_compliance:start().'
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_compliance).

-behaviour(nhttp_handler).

-export([
    start/0,
    start/1,
    start_h2c/0,
    start_mixed/0
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

-doc "Start the server in cleartext, HTTP/2 with prior knowledge (h2c).".
-spec start_h2c() -> no_return().
start_h2c() ->
    start(#{tls => false}).

-doc """
Start the server in cleartext with HTTP/1.1 and h2c on one port.
HTTP/1.x requests go to the `nhttp_h1_compliance` handler, so h2spec and
h1spec run against the same port.
""".
-spec start_mixed() -> no_return().
start_mixed() ->
    start(#{tls => false, versions => [http1_1, http2]}).

-spec start(map()) -> no_return().
start(Opts) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(asn1),
    {ok, _} = application:ensure_all_started(public_key),
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(nhttp_lib),
    {ok, _} = application:ensure_all_started(nhttp),

    {ok, Pid} = nhttp:start_link(
        maps:merge(
            #{port => 0, versions => maps:get(versions, Opts, [http2]), handler => ?MODULE},
            tls_opts(Opts)
        )
    ),

    {ok, Port} = nhttp:get_port(Pid),

    io:format("PORT:~B~n", [Port]),

    receive
        stop ->
            nhttp:stop(Pid),
            halt(0)
    end.

-spec tls_opts(map()) -> #{tls => nhttp:tls()}.
tls_opts(#{tls := false}) ->
    #{};
tls_opts(Opts) ->
    CertDir = maps:get(cert_dir, Opts, ?DEFAULT_CERT_DIR),
    CertFile = maps:get(certfile, Opts, filename:join(CertDir, "server.pem")),
    KeyFile = maps:get(keyfile, Opts, filename:join(CertDir, "server.key")),
    case filelib:is_file(CertFile) of
        false ->
            io:format(standard_error, "Error: Certificate not found: ~s~n", [CertFile]),
            halt(1);
        true ->
            #{tls => #{certfile => CertFile, keyfile => KeyFile}}
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
handle_request(#{version := Version} = Req, State) when
    Version =:= http1_0; Version =:= http1_1
->
    nhttp_h1_compliance:handle_request(Req, State);
handle_request(#{body := streaming}, State) ->
    {accept_body, unread, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.

-spec handle_request_body(nhttp_handler:body_event(), unread | [binary()], #{}) ->
    nhttp_handler:request_result(#{}).
handle_request_body(Event, Acc, State) when is_list(Acc) ->
    nhttp_h1_compliance:handle_request_body(Event, Acc, State);
handle_request_body({data, _Chunk}, unread, State) ->
    {accept_body, unread, State};
handle_request_body({fin, _Trailers}, unread, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State};
handle_request_body({abort, Reason}, unread, State) ->
    {abort, Reason, State}.
