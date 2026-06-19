-module(nhttp_acceptor).

-moduledoc false.

-behaviour(nhttp_acceptor_core).

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    get_listen_port/1,
    start_link/2,
    stop_accepting/1
]).

%%%-----------------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    do_accept/1,
    init_sub/1,
    reject/1,
    spawn_conn/2
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL RECORDS
%%%-----------------------------------------------------------------------------
-record(sub, {
    listen_socket :: nhttp_sock:t()
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec get_listen_port(pid()) -> {ok, inet:port_number()} | nhttp_error:t().
get_listen_port(AcceptorPid) ->
    nhttp_acceptor_core:get_listen_port(AcceptorPid).

-spec start_link(nhttp_registry:tab(), nhttp:opts()) -> {ok, pid()}.
start_link(Tab, Opts) ->
    nhttp_acceptor_core:start_link(?MODULE, Tab, Opts).

-spec stop_accepting(pid()) -> ok.
stop_accepting(AcceptorPid) ->
    nhttp_acceptor_core:stop_accepting(AcceptorPid).

%%%-----------------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec do_accept(#sub{}) -> {ok, nhttp_sock:t()} | {error, term()}.
do_accept(#sub{listen_socket = ListenSocket}) ->
    nhttp_sock:accept(ListenSocket, 1000).

-spec init_sub(nhttp:opts()) -> #sub{}.
init_sub(Opts) ->
    #sub{listen_socket = maps:get(listen_socket, Opts)}.

-spec reject(nhttp_sock:t()) -> ok.
reject(Socket) ->
    send_service_unavailable(Socket),
    nhttp_sock:close(Socket),
    ok.

-spec spawn_conn(nhttp_sock:t(), nhttp_acceptor_core:spawn_ctx()) -> ok.
spawn_conn(Socket, #{
    name := Name, counter := Counter, opts := Opts, conn_sup := ConnSup, tracker := Tracker
}) ->
    case nhttp_conn_sup:start_conn(ConnSup, Tracker, [Name, Socket, Opts]) of
        {ok, Pid} ->
            case nhttp_sock:controlling_process(Socket, Pid) of
                ok ->
                    Pid ! {socket_ready, Socket},
                    ok;
                {error, _Reason} ->
                    nhttp_conn:drain(Pid),
                    nhttp_sock:close(Socket),
                    ok
            end;
        {error, _Reason} ->
            nhttp_listener_counter:release(Counter),
            nhttp_sock:close(Socket),
            ok
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec send_service_unavailable(nhttp_sock:t()) -> ok.
send_service_unavailable(Socket) ->
    Response =
        <<"HTTP/1.1 503 Service Unavailable\r\n", "Content-Length: 19\r\n", "Connection: close\r\n",
            "\r\n", "Service Unavailable">>,
    _ = nhttp_sock:send(Socket, Response),
    ok.
