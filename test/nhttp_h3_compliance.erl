%%%-----------------------------------------------------------------------------
%%% @doc HTTP/3 compliance test server.
%%%
%%% Minimal HTTP/3 server for running h3spec compliance tests. Uses nhttp's
%%% standard listener infrastructure for proper connection handling.
%%%
%%% Usage from Makefile:
%%%   erl -noinput -pa ebin -eval 'nhttp_h3_compliance:start().'
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_h3_compliance).

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

start() ->
    start(#{}).

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

    {ok, Listener} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => ?MODULE,
        timeouts => #{idle => 30000},
        connected_socket => false
    }),

    {ok, Port} = nhttp:get_port(Listener),
    io:format("PORT:~B~n", [Port]),

    receive
        stop -> ok
    end.

init(_Args) ->
    {ok, #{}}.

handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.
