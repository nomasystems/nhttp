-module(nhttp_listener).

-moduledoc """
Listener supervisor for nhttp.

A listener is the public supervisor users embed in their own supervision
trees. It owns one `nhttp_transport_sup` per transport it serves (one for
a single-transport server) and fans `get_port` / `drain` out to them.
Connection caps, registry tables and listen sockets live inside each
transport supervisor. The listener stays a thin `one_for_one` parent so a
crash in one transport never disturbs another.

## Usage

```erlang
ChildSpec = nhttp_listener:child_spec(my_http_listener, #{
    port => 8080,
    handler => my_handler
}),
{ok, {SupFlags, [ChildSpec | OtherChildren]}}.
```
""".

-behaviour(supervisor).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_defaults.hrl").

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    child_spec/1,
    child_spec/2,
    drain/2,
    get_port/1,
    get_port/2,
    get_ports/1,
    start_link/1,
    start_link/2
]).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([name/0]).

-type name() ::
    atom()
    | {local, atom()}
    | {global, term()}
    | {via, module(), term()}.

-type alt_svc_advertise() ::
    disabled
    | #{registry := nhttp_registry:tab(), ma := non_neg_integer()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(H2_DEFAULT_MAX_FRAME_SIZE, 16384).
-define(H2_MAX_FRAME_SIZE_UPPER_LIMIT, 16777215).
-define(H2_MAX_WINDOW_SIZE, 2147483647).
-define(SUP_RESTART_PERIOD_SECONDS, 5).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Generate a child spec for embedding in your supervisor with an
auto-generated id.

Use this form when you do not need to reference the listener later by
name. The supervisor child id is internally generated and not exposed.
""".
-spec child_spec(nhttp:opts()) -> supervisor:child_spec().
child_spec(Opts) ->
    #{
        id => {?MODULE, make_ref()},
        start => {?MODULE, start_link, [Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

-doc """
Generate a child spec for embedding in your supervisor under a
caller-supplied name.
The supervisor child id is `{nhttp_listener, Name}`. When the listener
starts it also registers itself under `Name` so it can be looked up
later. Accepts the standard `gen_server` name forms (`atom()`,
`{local, atom()}`, `{global, term()}`, `{via, module(), term()}`).
""".
-spec child_spec(name(), nhttp:opts()) -> supervisor:child_spec().
child_spec(Name, Opts) ->
    #{
        id => {?MODULE, normalise_name(Name)},
        start => {?MODULE, start_link, [Name, Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

-doc """
Drain connections from this listener.
0. Stop advertising h3 over `Alt-Svc` so the TCP path emits
   `Alt-Svc: clear` before any GOAWAY (RFC 7838 §2.1: stop advertising
   before you stop accepting).
1. Stop the acceptor sub-supervisor so no new connections are accepted.
2. Signal every live connection to wind down (via the tracker).
3. Block until the tracker reports every monitored connection has
   exited, or the timeout elapses (reactive, no polling).
4. Brutally terminate the conn sub-supervisor to kill any stragglers.
""".
-spec drain(pid(), timeout()) -> ok.
drain(ListenerPid, Timeout) ->
    TransportSups = transport_sups(ListenerPid),
    ok = lists:foreach(fun stop_advertising/1, TransportSups),
    Monitors = [
        element(2, spawn_monitor(fun() -> drain_transport(TransportSup, Timeout) end))
     || TransportSup <- TransportSups
    ],
    await_drain_monitors(Monitors).

-doc """
Get the TCP port the listener is bound to.
Returns the TCP/TLS transport's port (the historical meaning). For a
QUIC-only listener it returns the sole transport's UDP port. Useful when
port 0 was specified to get an ephemeral port. Use `get_port/2` or
`get_ports/1` to read a specific transport's port in a mixed listener.
With `port => 0` the TCP and UDP transports bind *different* ephemeral
ports, never assume they are equal.
""".
-spec get_port(pid()) -> {ok, inet:port_number()} | {error, term()}.
get_port(ListenerPid) ->
    Transports = transport_sups_by_kind(ListenerPid),
    case lists:keyfind(tcp, 1, Transports) of
        {tcp, Sup} ->
            port_of_transport_sup(Sup, tcp);
        false ->
            case lists:keyfind(quic, 1, Transports) of
                {quic, Sup} -> port_of_transport_sup(Sup, quic);
                false -> {error, {server, no_acceptors}}
            end
    end.

-doc """
Get the bound port for a specific transport (`tcp` or `quic`).
Returns `{error, {server, {no_transport, Kind}}}` when the listener does
not serve that transport.
""".
-spec get_port(pid(), tcp | quic) -> {ok, inet:port_number()} | {error, term()}.
get_port(ListenerPid, Kind) ->
    case lists:keyfind(Kind, 1, transport_sups_by_kind(ListenerPid)) of
        {Kind, Sup} -> port_of_transport_sup(Sup, Kind);
        false -> {error, {server, {no_transport, Kind}}}
    end.

-doc """
Get every bound transport port as a map keyed by transport.
A single-transport listener yields a one-entry map. A mixed listener
yields `#{tcp => P1, quic => P2}`. With `port => 0` the two ports
differ.
""".
-spec get_ports(pid()) -> #{tcp | quic => inet:port_number()}.
get_ports(ListenerPid) ->
    lists:foldl(
        fun({Kind, Sup}, Acc) ->
            case port_of_transport_sup(Sup, Kind) of
                {ok, Port} -> Acc#{Kind => Port};
                {error, _} -> Acc
            end
        end,
        #{},
        transport_sups_by_kind(ListenerPid)
    ).

-doc """
Start an unnamed listener supervisor linked to the calling process.
The returned pid is the only handle to the listener.
""".
-spec start_link(nhttp:opts()) -> {ok, pid()} | ignore | {error, nhttp:start_error()}.
start_link(Opts) ->
    case validate_opts(Opts) of
        ok -> supervisor:start_link(?MODULE, {undefined, Opts});
        {error, Reason} -> {error, {invalid_opts, Reason}}
    end.

-doc """
Start a named listener supervisor linked to the calling process.
`Name` is one of `atom()`, `{local, atom()}`, `{global, term()}`, or
`{via, module(), term()}`. The supervisor is registered under that name
so it can be looked up later.
""".
-spec start_link(name(), nhttp:opts()) -> {ok, pid()} | ignore | {error, nhttp:start_error()}.
start_link(Name, Opts) ->
    case validate_opts(Opts) of
        ok -> supervisor:start_link(reg_name(Name), ?MODULE, {Name, Opts});
        {error, Reason} -> {error, {invalid_opts, Reason}}
    end.

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({name() | undefined, nhttp:opts()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Name, Opts}) ->
    PrimaryTab = nhttp_registry:new(),
    LogicalName = listener_name(Name),
    ok = nhttp_registry:register_name(PrimaryTab, LogicalName),
    Versions = effective_versions(Opts),
    {TcpVersions, QuicVersions} = partition_versions(Versions),
    Children = transport_sup_children(
        LogicalName, TcpVersions, QuicVersions, PrimaryTab, Opts
    ),
    {ok, {listener_sup_flags(), Children}}.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec alt_svc_advertise(nhttp:opts(), [nhttp:version()], nhttp_registry:tab()) ->
    alt_svc_advertise().
alt_svc_advertise(_Opts, [], _PrimaryTab) ->
    disabled;
alt_svc_advertise(Opts, _QuicVersions, PrimaryTab) ->
    case maps:get(alt_svc, Opts, #{}) of
        false ->
            disabled;
        Map when is_map(Map) ->
            #{registry => PrimaryTab, ma => maps:get(ma, Map, ?DEFAULT_ALT_SVC_MA)}
    end.

-spec await_drain_monitors([reference()]) -> ok.
await_drain_monitors([]) ->
    ok;
await_drain_monitors([Ref | Rest]) ->
    receive
        {'DOWN', Ref, process, _Pid, _Reason} -> await_drain_monitors(Rest)
    end.

-spec derive_tcp_transport(nhttp:opts()) -> tcp | ssl.
derive_tcp_transport(Opts) ->
    case has_tls(Opts) of
        true -> ssl;
        false -> tcp
    end.

-spec drain_transport(pid(), timeout()) -> ok.
drain_transport(TransportSup, Timeout) ->
    Children = supervisor:which_children(TransportSup),
    _ = supervisor:terminate_child(TransportSup, nhttp_acceptor_sup),
    ok =
        case lists:keyfind(nhttp_conn_tracker, 1, Children) of
            {_, TrackerPid, _, _} when is_pid(TrackerPid) ->
                ok = nhttp_conn_tracker:drain_all(TrackerPid),
                case nhttp_conn_tracker:wait_for_drained(TrackerPid, Timeout) of
                    ok -> ok;
                    {error, timeout} -> ok
                end;
            _ ->
                ok
        end,
    _ = supervisor:terminate_child(TransportSup, nhttp_conn_sup),
    ok.

-spec effective_versions(nhttp:opts()) -> [nhttp:version()].
effective_versions(Opts) ->
    case maps:find(versions, Opts) of
        {ok, Versions} ->
            Versions;
        error ->
            case has_tls(Opts) of
                true -> [http1_1, http2];
                false -> [http1_1]
            end
    end.

-spec find_acceptor_in_sup([{term(), pid() | restarting | undefined, term(), term()}]) ->
    {nhttp_acceptor, pid()} | none.
find_acceptor_in_sup([]) ->
    none;
find_acceptor_in_sup([{{nhttp_acceptor, _}, Pid, worker, _} | _]) when is_pid(Pid) ->
    {nhttp_acceptor, Pid};
find_acceptor_in_sup([_ | Rest]) ->
    find_acceptor_in_sup(Rest).

-spec find_acceptor_in_transports([pid()]) ->
    {nhttp_acceptor, pid()} | none.
find_acceptor_in_transports([]) ->
    none;
find_acceptor_in_transports([TransportSup | Rest]) ->
    case lists:keyfind(nhttp_acceptor_sup, 1, supervisor:which_children(TransportSup)) of
        {_, AccSupPid, _, _} when is_pid(AccSupPid) ->
            case find_acceptor_in_sup(supervisor:which_children(AccSupPid)) of
                {nhttp_acceptor, _} = Found -> Found;
                none -> find_acceptor_in_transports(Rest)
            end;
        _ ->
            find_acceptor_in_transports(Rest)
    end.

-spec has_tls(map()) -> boolean().
has_tls(Opts) ->
    maps:is_key(tls, Opts).

-spec invalid_alt_svc_error(term()) -> nhttp_error:t().
invalid_alt_svc_error(Reason) ->
    {error, {server, #{type => invalid_alt_svc, reason => Reason}}}.

-spec invalid_proxy_protocol_error(term()) -> nhttp_error:t().
invalid_proxy_protocol_error(Reason) ->
    {error, {server, #{type => invalid_proxy_protocol, reason => Reason}}}.

-spec invalid_versions_error(term()) -> nhttp_error:t().
invalid_versions_error(Reason) ->
    {error, {server, #{type => invalid_versions, reason => Reason}}}.

-spec listener_name(name() | undefined) -> term().
listener_name(undefined) -> undefined;
listener_name({local, Name}) -> Name;
listener_name({global, Name}) -> Name;
listener_name({via, _Module, Name}) -> Name;
listener_name(Name) when is_atom(Name) -> Name.

-spec listener_sup_flags() -> supervisor:sup_flags().
%% intensity 2: one crash per transport child (tcp + quic) per period; grow with transports
listener_sup_flags() ->
    #{strategy => one_for_one, intensity => 2, period => ?SUP_RESTART_PERIOD_SECONDS}.

-spec normalise_name(name()) -> term().
normalise_name({local, Name}) -> Name;
normalise_name({global, Name}) -> {global, Name};
normalise_name({via, Module, Name}) -> {via, Module, Name};
normalise_name(Name) when is_atom(Name) -> Name.

-spec partition_versions([nhttp:version()]) -> {[nhttp:version()], [nhttp:version()]}.
partition_versions(Versions) ->
    lists:partition(fun(V) -> V =/= http3 end, Versions).

-spec port_of_transport_sup(pid(), tcp | quic) -> {ok, inet:port_number()} | {error, term()}.
port_of_transport_sup(TransportSup, tcp) ->
    case find_acceptor_in_transports([TransportSup]) of
        {nhttp_acceptor, Pid} -> nhttp_acceptor:get_listen_port(Pid);
        none -> tracker_port_in_transports([TransportSup])
    end;
port_of_transport_sup(TransportSup, quic) ->
    tracker_port_in_transports([TransportSup]).

-spec quic_opts_with_registry(nhttp:opts(), alt_svc_advertise()) -> nhttp:opts().
quic_opts_with_registry(Opts, disabled) ->
    Opts;
quic_opts_with_registry(Opts, #{registry := PrimaryTab}) ->
    Opts#{alt_svc_registry => PrimaryTab}.

-spec reg_name(name()) ->
    {local, atom()} | {global, term()} | {via, module(), term()}.
reg_name(Name) when is_atom(Name) -> {local, Name};
reg_name({local, _} = N) -> N;
reg_name({global, _} = N) -> N;
reg_name({via, _, _} = N) -> N.

-spec stop_advertising(pid()) -> ok.
stop_advertising(TransportSup) ->
    case lists:keyfind(nhttp_conn_tracker, 1, supervisor:which_children(TransportSup)) of
        {_, TrackerPid, _, _} when is_pid(TrackerPid) ->
            nhttp_conn_tracker:stop_advertising(TrackerPid);
        _ ->
            ok
    end.

-spec tracker_port_in_transports([pid()]) -> {ok, inet:port_number()} | {error, term()}.
tracker_port_in_transports([]) ->
    {error, {server, no_acceptors}};
tracker_port_in_transports([TransportSup | Rest]) ->
    case lists:keyfind(nhttp_conn_tracker, 1, supervisor:which_children(TransportSup)) of
        {_, TrackerPid, _, _} when is_pid(TrackerPid) ->
            case tracker_port_or_no_acceptors(TrackerPid) of
                {ok, _} = Found -> Found;
                {error, _} -> tracker_port_in_transports(Rest)
            end;
        _ ->
            tracker_port_in_transports(Rest)
    end.

-spec tracker_port_or_no_acceptors(pid()) -> {ok, inet:port_number()} | {error, term()}.
tracker_port_or_no_acceptors(TrackerPid) ->
    case nhttp_conn_tracker:listener_port(TrackerPid) of
        {ok, Port} -> {ok, Port};
        {error, {server, no_port}} -> {error, {server, no_acceptors}}
    end.

-spec transport_kind(tcp | ssl | quic) -> tcp | quic.
transport_kind(quic) -> quic;
transport_kind(_) -> tcp.

-spec transport_sup_child(term(), tcp | ssl | quic, [nhttp:version()], map()) ->
    supervisor:child_spec().
transport_sup_child(LogicalName, Transport, Versions, Opts) ->
    #{
        id => {nhttp_transport_sup, transport_kind(Transport)},
        start => {nhttp_transport_sup, start_link, [LogicalName, Transport, Versions, Opts]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [nhttp_transport_sup]
    }.

-spec transport_sup_children(
    term(), [nhttp:version()], [nhttp:version()], nhttp_registry:tab(), nhttp:opts()
) -> [supervisor:child_spec()].
transport_sup_children(LogicalName, TcpVersions, QuicVersions, PrimaryTab, Opts) ->
    Advertise = alt_svc_advertise(Opts, QuicVersions, PrimaryTab),
    TcpChild =
        case TcpVersions of
            [] ->
                [];
            _ ->
                TcpOpts = Opts#{alt_svc_advertise => Advertise},
                [transport_sup_child(LogicalName, derive_tcp_transport(Opts), TcpVersions, TcpOpts)]
        end,
    QuicChild =
        case QuicVersions of
            [] ->
                [];
            _ ->
                QuicOpts = quic_opts_with_registry(Opts, Advertise),
                [transport_sup_child(LogicalName, quic, QuicVersions, QuicOpts)]
        end,
    TcpChild ++ QuicChild.

-spec transport_sups(pid()) -> [pid()].
transport_sups(ListenerPid) ->
    [Pid || {_Kind, Pid} <- transport_sups_by_kind(ListenerPid)].

-spec transport_sups_by_kind(pid()) -> [{tcp | quic, pid()}].
transport_sups_by_kind(ListenerPid) ->
    [
        {Kind, Pid}
     || {{nhttp_transport_sup, Kind}, Pid, supervisor, [nhttp_transport_sup]} <-
            supervisor:which_children(ListenerPid),
        is_pid(Pid)
    ].

-spec validate_acceptor_count(pos_integer() | undefined) -> ok | nhttp_error:t().
validate_acceptor_count(undefined) ->
    ok;
validate_acceptor_count(N) when is_integer(N), N >= 1 ->
    ok;
validate_acceptor_count(N) ->
    {error, {server, #{type => invalid_acceptor_count, reason => N}}}.

-spec validate_alt_svc(term()) -> ok | nhttp_error:t().
validate_alt_svc(false) ->
    ok;
validate_alt_svc(Map) when is_map(Map) ->
    case maps:get(ma, Map, ?DEFAULT_ALT_SVC_MA) of
        Ma when is_integer(Ma), Ma >= 0 -> ok;
        Ma -> invalid_alt_svc_error({bad_ma, Ma})
    end;
validate_alt_svc(_Other) ->
    invalid_alt_svc_error(not_false_or_map).

-spec validate_h2_settings(nhttp_h2:settings()) -> ok | {error, term()}.
validate_h2_settings(Settings) ->
    maybe
        ok ?= validate_header_table_size(maps:get(header_table_size, Settings, undefined)),
        ok ?= validate_initial_window_size(maps:get(initial_window_size, Settings, undefined)),
        ok ?=
            validate_max_concurrent_streams(maps:get(max_concurrent_streams, Settings, undefined)),
        ok ?= validate_max_frame_size(maps:get(max_frame_size, Settings, undefined)),
        ok
    end.

-spec validate_header_table_size(non_neg_integer() | undefined) -> ok | {error, term()}.
validate_header_table_size(undefined) ->
    ok;
validate_header_table_size(V) when is_integer(V), V >= 0 -> ok;
validate_header_table_size(V) ->
    {error, {invalid_h2_setting, header_table_size, V, "must be non-negative integer"}}.

-spec validate_initial_window_size(pos_integer() | undefined) -> ok | {error, term()}.
validate_initial_window_size(undefined) ->
    ok;
validate_initial_window_size(V) when is_integer(V), V >= 1, V =< ?H2_MAX_WINDOW_SIZE -> ok;
validate_initial_window_size(V) ->
    {error, {invalid_h2_setting, initial_window_size, V, "must be 1..2147483647"}}.

-spec validate_max_concurrent_streams(non_neg_integer() | undefined) -> ok | {error, term()}.
validate_max_concurrent_streams(undefined) ->
    ok;
validate_max_concurrent_streams(V) when is_integer(V), V >= 0 -> ok;
validate_max_concurrent_streams(V) ->
    {error, {invalid_h2_setting, max_concurrent_streams, V, "must be non-negative integer"}}.

-spec validate_max_frame_size(pos_integer() | undefined) -> ok | {error, term()}.
validate_max_frame_size(undefined) ->
    ok;
validate_max_frame_size(V) when
    is_integer(V), V >= ?H2_DEFAULT_MAX_FRAME_SIZE, V =< ?H2_MAX_FRAME_SIZE_UPPER_LIMIT
->
    ok;
validate_max_frame_size(V) ->
    {error, {invalid_h2_setting, max_frame_size, V, "must be 16384..16777215"}}.

-spec validate_opts(nhttp:opts()) -> ok | nhttp_error:t().
validate_opts(Opts) ->
    maybe
        ok ?= validate_required_opts(Opts),
        ok ?= validate_versions(Opts),
        ok ?= validate_h2_settings(maps:get(h2_settings, Opts, #{})),
        ok ?= validate_proxy_protocol(maps:get(proxy_protocol, Opts, false)),
        ok ?= validate_acceptor_count(maps:get(acceptor_count, Opts, undefined)),
        ok ?= validate_alt_svc(maps:get(alt_svc, Opts, #{})),
        ok
    end.

-spec validate_proxy_protocol(term()) -> ok | nhttp_error:t().
validate_proxy_protocol(false) ->
    ok;
validate_proxy_protocol(true) ->
    ok;
validate_proxy_protocol(Map) when is_map(Map) ->
    maybe
        ok ?= validate_proxy_version(maps:get(version, Map, both)),
        ok ?= validate_proxy_timeout(maps:get(timeout, Map, ?DEFAULT_PROXY_TIMEOUT)),
        ok
    end;
validate_proxy_protocol(_) ->
    invalid_proxy_protocol_error(not_boolean_or_map).

-spec validate_proxy_timeout(term()) -> ok | nhttp_error:t().
validate_proxy_timeout(infinity) ->
    ok;
validate_proxy_timeout(T) when is_integer(T), T >= 0 ->
    ok;
validate_proxy_timeout(T) ->
    invalid_proxy_protocol_error({bad_timeout, T}).

-spec validate_proxy_version(term()) -> ok | nhttp_error:t().
validate_proxy_version(V) when V =:= v1; V =:= v2; V =:= both ->
    ok;
validate_proxy_version(V) ->
    invalid_proxy_protocol_error({unknown_version, V}).

-spec validate_required_opts(nhttp:opts()) -> ok | nhttp_error:t().
validate_required_opts(Opts) ->
    case maps:is_key(handler, Opts) of
        true ->
            case maps:is_key(port, Opts) of
                true -> ok;
                false -> nhttp_error:missing_config(port)
            end;
        false ->
            nhttp_error:missing_config(handler)
    end.

-spec validate_tls_map(term()) -> ok | nhttp_error:t().
validate_tls_map(Tls) when is_map(Tls) ->
    case maps:is_key(certfile, Tls) of
        false ->
            nhttp_error:missing_config('tls.certfile');
        true ->
            case maps:is_key(keyfile, Tls) of
                false -> nhttp_error:missing_config('tls.keyfile');
                true -> ok
            end
    end;
validate_tls_map(_) ->
    {error, {server, #{type => invalid_tls, reason => not_a_map}}}.

-spec validate_version_atoms([term()]) -> ok | nhttp_error:t().
validate_version_atoms([]) ->
    ok;
validate_version_atoms([V | Rest]) when V =:= http1_1; V =:= http2; V =:= http3 ->
    validate_version_atoms(Rest);
validate_version_atoms([V | _]) ->
    invalid_versions_error({unknown_version, V}).

-spec validate_versions(nhttp:opts()) -> ok | nhttp_error:t().
validate_versions(Opts) ->
    case effective_versions(Opts) of
        [] ->
            invalid_versions_error(empty);
        Versions ->
            maybe
                ok ?= validate_version_atoms(Versions),
                ok ?= validate_versions_tls(Versions, Opts),
                ok
            end
    end.

-spec validate_versions_tls([nhttp:version()], nhttp:opts()) -> ok | nhttp_error:t().
validate_versions_tls(Versions, Opts) ->
    NeedsTls = lists:member(http2, Versions) orelse lists:member(http3, Versions),
    case {NeedsTls, maps:find(tls, Opts)} of
        {false, error} ->
            ok;
        {true, error} ->
            nhttp_error:missing_config(tls);
        {_, {ok, Tls}} ->
            validate_tls_map(Tls)
    end.
