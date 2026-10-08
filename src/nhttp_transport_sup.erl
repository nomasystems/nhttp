-module(nhttp_transport_sup).

-moduledoc """
Per-transport supervisor for nhttp.

Owns one transport's registry table, connection counter, listen socket
(or QUIC listener) and the subtree that accepts and tracks connections.
A `nhttp_listener` parents one `nhttp_transport_sup` per transport it
serves. A single-transport listener has exactly one.

The subtree restarts `rest_for_one` so a tracker crash cascades through
the connection and acceptor supervisors that depend on it, while a crash
of the whole transport stays contained below the `one_for_one` listener.
""".

-behaviour(supervisor).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_defaults.hrl").

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([start_link/5]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS (TESTING)
%%%-----------------------------------------------------------------------------
-export([quic_listen_opts/4]).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([alt_svc_advertise/0]).

-type alt_svc_advertise() ::
    disabled
    | #{registry := nhttp_registry:tab(), ma := non_neg_integer()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_MAX_CONNECTIONS, 1024).
-define(H2_DEFAULT_MAX_FRAME_SIZE, 16384).
-define(SUP_RESTART_PERIOD_SECONDS, 5).
-define(SUPERVISOR_SHUTDOWN_TIMEOUT, 10000).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Start a transport supervisor linked to the calling process.

`LogicalName` is the listener's logical name, copied into this
transport's registry table for logs / otel. `Transport` selects the
listening machinery. `Versions` are the already-validated protocol
versions this transport serves. `Advertise` carries the registry and
`ma` of the HTTP/3 Alt-Svc advertisement, or `disabled`.
""".
-spec start_link(
    term(), tcp | ssl | quic, [nhttp:version()], nhttp:opts(), alt_svc_advertise()
) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(LogicalName, Transport, Versions, Opts, Advertise) ->
    supervisor:start_link(?MODULE, {LogicalName, Transport, Versions, Opts, Advertise}).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({term(), tcp | ssl | quic, [nhttp:version()], nhttp:opts(), alt_svc_advertise()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | {stop, nhttp:start_error()}.
init({LogicalName, Transport, Versions, Opts, Advertise}) ->
    Tab = nhttp_registry:new(),
    ok = nhttp_registry:register_name(Tab, LogicalName),
    case Transport of
        quic -> init_quic_listener(Tab, Opts, Versions, Advertise);
        ssl -> init_tcp_listener(Tab, Opts, Versions, ssl, Advertise);
        tcp -> init_tcp_listener(Tab, Opts, Versions, tcp, Advertise)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec advertise_registry(alt_svc_advertise()) -> nhttp_registry:tab() | undefined.
advertise_registry(disabled) -> undefined;
advertise_registry(#{registry := PrimaryTab}) -> PrimaryTab.

-spec build_listen_opts(nhttp:opts(), tcp | ssl) -> nhttp_sock:listen_opts().
build_listen_opts(Opts, Transport) ->
    Timeouts = maps:get(timeouts, Opts, #{}),
    #{
        port => maps:get(port, Opts),
        transport => Transport,
        backlog => maps:get(backlog, Opts, 1024),
        nodelay => maps:get(nodelay, Opts, true),
        send_timeout => maps:get(send, Timeouts, ?DEFAULT_LISTEN_SEND_TIMEOUT),
        buffer => maps:get(buffer, Opts, ?H2_DEFAULT_MAX_FRAME_SIZE)
    }.

-spec build_listener_children(
    nhttp_registry:tab(), module(), module(), nhttp_acceptor_core:ctx(), pos_integer()
) -> [supervisor:child_spec()].
build_listener_children(Tab, ConnModule, AcceptorModule, AcceptorCtx, AcceptorCount) ->
    [
        #{
            id => nhttp_conn_tracker,
            start => {nhttp_conn_tracker, start_link, [Tab, undefined]},
            restart => permanent,
            shutdown => ?WORKER_SHUTDOWN_TIMEOUT,
            type => worker,
            modules => [nhttp_conn_tracker]
        },
        #{
            id => nhttp_conn_sup,
            start => {nhttp_conn_sup, start_link, [Tab, ConnModule]},
            restart => permanent,
            shutdown => ?SUPERVISOR_SHUTDOWN_TIMEOUT,
            type => supervisor,
            modules => [nhttp_conn_sup]
        },
        #{
            id => nhttp_acceptor_sup,
            start =>
                {nhttp_acceptor_sup, start_link, [
                    Tab, AcceptorModule, AcceptorCtx, AcceptorCount
                ]},
            restart => permanent,
            shutdown => ?SUPERVISOR_SHUTDOWN_TIMEOUT,
            type => supervisor,
            modules => [nhttp_acceptor_sup]
        }
    ].

-spec build_tls_options(nhttp:tls(), [binary()]) -> [ssl:tls_server_option()].
build_tls_options(Tls, Alpn) ->
    Flat = #{
        alpn_preferred_protocols => Alpn,
        certfile => maps:get(certfile, Tls),
        keyfile => maps:get(keyfile, Tls)
    },
    Flat1 = maybe_put(cacertfile, maps:find(cacertfile, Tls), Flat),
    Flat2 = maybe_put(verify, maps:find(verify, Tls), Flat1),
    Base = nhttp_sock:build_ssl_opts(Flat2),
    WithSniFun = maybe_prepend({sni_fun, maps:find(sni_fun, Tls)}, Base),
    WithSniHosts = maybe_prepend({sni_hosts, maps:find(sni_hosts, Tls)}, WithSniFun),
    Extra = maps:get(extra, Tls, []),
    WithSniHosts ++ Extra.

-spec default_acceptor_count() -> pos_integer().
default_acceptor_count() ->
    erlang:system_info(schedulers) * 2.

-spec init_quic_listener(
    nhttp_registry:tab(), nhttp:opts(), [nhttp:version()], alt_svc_advertise()
) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | {stop, nhttp:start_error()}.
init_quic_listener(Tab, Opts, Versions, Advertise) ->
    Port = maps:get(port, Opts),
    Alpn = versions_to_alpn(Versions),
    HandlerOpts = #{opts => Opts, name => nhttp_registry:lookup_name(Tab), registry => Tab},
    QuicListenOpts = quic_listen_opts(Alpn, maps:get(tls, Opts), Opts, HandlerOpts),
    case nquic:listen(Port, QuicListenOpts) of
        {ok, Listener} ->
            {ok, ActualPort} = nquic:get_port(Listener),
            ok = nhttp_registry:register_port(Tab, ActualPort),
            ok = maybe_register_advertise_port(Advertise, ActualPort),
            MaxConns = maps:get(max_connections, Opts, ?DEFAULT_MAX_CONNECTIONS),
            Counter = nhttp_listener_counter:new(MaxConns),
            ok = nhttp_registry:register_counter(Tab, Counter),
            {ok, {
                transport_sup_flags(), quic_listener_children(Tab, advertise_registry(Advertise))
            }};
        {error, Reason} ->
            {stop, {listen_failed, Reason}}
    end.

-spec init_tcp_listener(
    nhttp_registry:tab(), nhttp:opts(), [nhttp:version()], tcp | ssl, alt_svc_advertise()
) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | {stop, nhttp:start_error()}.
init_tcp_listener(Tab, Opts, Versions, Transport, Advertise) ->
    ListenOpts = build_listen_opts(Opts, Transport),
    case nhttp_sock:listen(ListenOpts) of
        {ok, ListenSocket} ->
            {ok, {_, Port}} = nhttp_sock:sockname(ListenSocket),
            ok = nhttp_registry:register_port(Tab, Port),
            MaxConns = maps:get(max_connections, Opts, ?DEFAULT_MAX_CONNECTIONS),
            Counter = nhttp_listener_counter:new(MaxConns),
            ok = nhttp_registry:register_counter(Tab, Counter),
            Alpn = versions_to_alpn(Versions),
            SslOpts =
                case Transport of
                    ssl ->
                        build_tls_options(maps:get(tls, Opts), Alpn);
                    tcp ->
                        []
                end,
            AcceptorCount = maps:get(acceptor_count, Opts, default_acceptor_count()),
            AcceptorCtx = #{
                opts => Opts,
                port => Port,
                sub => ListenSocket,
                conn => #{ssl_opts => SslOpts, versions => Versions, alt_svc_advertise => Advertise}
            },
            Children = build_listener_children(
                Tab, nhttp_conn, nhttp_acceptor, AcceptorCtx, AcceptorCount
            ),
            {ok, {transport_sup_flags(), Children}};
        {error, Reason} ->
            {stop, {listen_failed, Reason}}
    end.

-spec maybe_prepend({atom(), {ok, term()} | error}, [tuple()]) -> [tuple()].
maybe_prepend({_Key, error}, Opts) -> Opts;
maybe_prepend({Key, {ok, Value}}, Opts) -> [{Key, Value} | Opts].

-spec maybe_put(atom(), {ok, term()} | error, map()) -> map().
maybe_put(_Key, error, Map) -> Map;
maybe_put(Key, {ok, Value}, Map) -> Map#{Key => Value}.

-spec maybe_register_advertise_port(alt_svc_advertise(), inet:port_number()) -> ok.
maybe_register_advertise_port(disabled, _Port) ->
    ok;
maybe_register_advertise_port(#{registry := PrimaryTab}, Port) ->
    nhttp_registry:register_advertise_port(PrimaryTab, Port).

-doc """
Build the nquic listen options for this transport.
0-RTT (QUIC early data) is left off: nquic refuses early data unless the
listen options carry `{replay_protection, Module}`, and this map never
sets it. No application bytes are processed before the handshake
completes, so a replayed non-idempotent method cannot reach the handler
(RFC 8470, RFC 9001 §9.2). Enabling 0-RTT here would require pairing it
with a Too Early (425) / defer policy.
""".
-spec quic_listen_opts([binary()], nhttp:tls(), nhttp:opts(), nhttp_conn_h3:handler_opts()) ->
    map().
quic_listen_opts(Alpn, Tls, Opts, HandlerOpts) ->
    Timeouts = maps:get(timeouts, Opts, #{}),
    #{
        tls => #{
            certfile => maps:get(certfile, Tls),
            keyfile => maps:get(keyfile, Tls),
            alpn => Alpn
        },
        idle_timeout => maps:get(idle, Timeouts, ?DEFAULT_QUIC_IDLE_TIMEOUT),
        conn_handler => nhttp_conn_h3,
        conn_handler_opts => HandlerOpts
    }.

-spec quic_listener_children(nhttp_registry:tab(), nhttp_registry:tab() | undefined) ->
    [supervisor:child_spec()].
quic_listener_children(Tab, AdvertiseTab) ->
    [
        #{
            id => nhttp_conn_tracker,
            start => {nhttp_conn_tracker, start_link, [Tab, AdvertiseTab]},
            restart => permanent,
            shutdown => ?WORKER_SHUTDOWN_TIMEOUT,
            type => worker,
            modules => [nhttp_conn_tracker]
        }
    ].

-spec transport_sup_flags() -> supervisor:sup_flags().
transport_sup_flags() ->
    #{strategy => rest_for_one, intensity => 1, period => ?SUP_RESTART_PERIOD_SECONDS}.

-spec version_to_alpn(nhttp:version()) -> binary().
version_to_alpn(http1_1) -> <<"http/1.1">>;
version_to_alpn(http2) -> <<"h2">>;
version_to_alpn(http3) -> <<"h3">>.

-spec versions_to_alpn([nhttp:version()]) -> [binary()].
versions_to_alpn(Versions) ->
    Preference = [http3, http2, http1_1],
    [version_to_alpn(V) || V <- Preference, lists:member(V, Versions)].
