-module(nhttp_conn).

-moduledoc false.

-behaviour(nhttp_protocol).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_conn.hrl").
-include("nhttp_defaults.hrl").
-include("nhttp_proc_lib.hrl").

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    drain/1,
    start_link_supervised/3
]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB / SYS CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    init/1,
    loop/3,
    system_code_change/4,
    system_continue/3,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS (CROSS-MODULE HELPERS USED BY NHTTP_CONN_H1, NHTTP_CONN_H2)
%%%-----------------------------------------------------------------------------
-export([
    activate/1,
    activate/2,
    alt_svc_headers/2,
    cancel_hibernate_timer/1,
    compress_config/1,
    emit_handler_exception/3,
    emit_request_start/3,
    emit_request_stop/3,
    emit_request_stop_with_size/4,
    emit_stream_start/2,
    exit_reason/1,
    family_to_version/1,
    graceful_shutdown/1,
    init_exit_reason/1,
    log_ctx/1,
    mailbox_empty/0,
    select_family/2,
    sock_send/2,
    sock_stop_reason/1,
    start_hibernate_timer/1,
    stop/2,
    stop_parent/2,
    terminate/2,
    transport_to_scheme/1,
    version_to_family/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([hibernate_timer/0]).

-type hibernate_timer() :: none | {reference(), reference()}.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(SOCKET_TRANSFER_TIMEOUT, 5000).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Signal the connection to drain.
For HTTP/1.1 the conn closes after the current request completes. For
HTTP/2 a GOAWAY is sent and the conn shuts down. For WebSocket a close
frame is sent. Non-blocking, the conn handles the message in its loop.
""".
-spec drain(pid()) -> ok.
drain(Pid) ->
    Pid ! shutdown,
    ok.

-doc """
Start a connection under `nhttp_conn_sup`.
Uses `proc_lib:start_link/3` semantics: the caller (the conn supervisor)
blocks until init_ack is received, so `supervisor:start_child/2` correctly
reports init failures.
""".
-spec start_link_supervised(term(), nhttp_sock:t(), nhttp:opts()) ->
    {ok, pid()} | {error, term()}.
start_link_supervised(Name, Socket, Opts) ->
    proc_lib:start_link(?MODULE, init, [{Name, Socket, Opts, self()}]).

%%%-----------------------------------------------------------------------------
%% PROC_LIB CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({term(), nhttp_sock:t(), nhttp:opts(), pid()}) ->
    no_return().
init({Name, Socket, Opts, Parent}) ->
    process_flag(trap_exit, true),
    Handler = maps:get(handler, Opts),
    HandlerArgs = maps:get(handler_args, Opts, []),
    Transport = nhttp_sock:transport(Socket),
    OtelConfig = nhttp_otel:config(Opts),
    Timeouts = maps:get(timeouts, Opts, #{}),
    IdleTimeout = maps:get(idle, Timeouts, ?DEFAULT_IDLE_TIMEOUT),
    CompressionEnabled = maps:get(compression, Opts, true),
    CompressionLevel = maps:get(compression_level, Opts, ?DEFAULT_COMPRESSION_LEVEL),
    CompressionThreshold = maps:get(compression_threshold, Opts, ?DEFAULT_COMPRESSION_THRESHOLD),
    CompressMimeTypes = maps:get(compress_mime_types, Opts, nhttp_compress:default_mime_types()),
    case nhttp_handler:safe_init(Handler, HandlerArgs) of
        {ok, HandlerState} ->
            proc_lib:init_ack(Parent, {ok, self()}),
            ok = nhttp_stats:incr_connection(),
            Debug = sys:debug_options([]),
            State = #state{
                name = Name,
                socket = undefined,
                transport = Transport,
                handler = Handler,
                handler_state = HandlerState,
                opts = Opts,
                limits = nhttp_limits:from_opts(Opts),
                idle_timeout = IdleTimeout,
                otel_config = OtelConfig,
                conn_span = undefined,
                compression_enabled = CompressionEnabled,
                compression_level = CompressionLevel,
                compression_threshold = CompressionThreshold,
                compress_mime_types = CompressMimeTypes
            },
            wait_for_socket(Parent, Debug, State);
        {error, Reason} ->
            proc_lib:init_ack(Parent, {error, {handler_init, Reason}});
        {nhttp_handler_exception, Class, Reason} ->
            InitCtx = #{listener_name => Name, transport => Transport, handler => Handler},
            nhttp_log:handler_crashed(InitCtx, init, Class, Reason),
            proc_lib:init_ack(Parent, {error, {handler_init_exception, Class, Reason}})
    end.

-doc """
`nhttp_protocol` entry point for the pre-classification phase: blocks on
the acceptor's socket handoff, classifies the protocol, then tail-calls
the selected protocol module's `loop/3`.
""".
-spec loop(pid(), [sys:debug_option()], #state{}) -> no_return().
loop(Parent, Debug, #state{family = undefined} = State) ->
    wait_for_socket(Parent, Debug, State).

-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
?NHTTP_SYSTEM_CODE_CHANGE_NOOP.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(Parent, Debug, State) ->
    loop(Parent, Debug, State).

-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    stop_parent(Reason, State).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ALT-SVC (RFC 7838 §3)
%%%-----------------------------------------------------------------------------
-spec alt_svc_directive(nhttp:opts()) -> nhttp_alt_svc:directive().
alt_svc_directive(Opts) ->
    case maps:get(alt_svc_advertise, Opts, disabled) of
        disabled ->
            disabled;
        #{registry := Tab, ma := Ma} ->
            case quic_advertise_ready(Tab) of
                {ready, Port} -> #{port => Port, ma => Ma};
                cleared -> clear
            end
    end.

-doc """
Inject the auto-emitted `Alt-Svc` header into a TCP response's headers
when this listener also serves HTTP/3, deferring to any handler-supplied
`alt-svc` header (RFC 7838 §3).
Advertisement is gated on live QUIC readiness (RFC 7838 §2.1): while the
QUIC transport is accepting, h3 is advertised with the QUIC port read
from the shared registry (so `port => 0` resolves to the ephemeral port).
Once it drains or goes down, `Alt-Svc: clear` is emitted instead (RFC
7838 §4).
""".
-spec alt_svc_headers(#state{}, nhttp_lib:headers()) -> nhttp_lib:headers().
alt_svc_headers(#state{opts = Opts}, Headers) ->
    nhttp_alt_svc:inject(Headers, alt_svc_directive(Opts)).

-spec quic_advertise_ready(nhttp_registry:tab()) ->
    {ready, inet:port_number()} | cleared.
quic_advertise_ready(Tab) ->
    Port = nhttp_registry:lookup_advertise_port(Tab),
    Tracker = nhttp_registry:lookup_advertise_tracker(Tab),
    Ready =
        is_integer(Port) andalso
            is_pid(Tracker) andalso
            is_process_alive(Tracker) andalso
            not nhttp_registry:lookup_advertise_draining(Tab),
    case Ready of
        true -> {ready, Port};
        false -> cleared
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - COMPRESSION
%%%-----------------------------------------------------------------------------
-spec compress_config(#state{}) -> nhttp_conn_compress:config().
compress_config(#state{
    compression_enabled = Enabled,
    compression_level = Level,
    compression_threshold = Threshold,
    compress_mime_types = MimeTypes
}) ->
    nhttp_conn_compress:config(Enabled, Level, Threshold, MimeTypes).

-doc """
Project the connection state into the structured-logging context map
consumed by `nhttp_log`. Cheap (record field reads) so call sites can
build it on demand without caching.
""".
-spec log_ctx(#state{}) -> nhttp_log:ctx().
log_ctx(#state{
    name = Name, transport = Transport, family = Family, peer = Peer, handler = Handler
}) ->
    nhttp_log:conn_ctx(Name, Transport, family_to_version(Family), Peer, Handler).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - CONNECTION LOOP
%%%-----------------------------------------------------------------------------
-doc """
Re-arm `{active, once}` on the connection socket for the next read.
A peer that closed between the last read and this call makes the call
give an error: `einval` on TCP, because the port is already gone, and
`closed` on TLS. Both are a normal end of the connection, so the caller
receives a classified stop reason and never a raised error.
""".
-spec activate(#state{}) -> ok | {stop, term()}.
activate(State) ->
    activate(State, []).

-doc """
Same as `activate/1`, with socket options applied together with
`{active, once}`.
""".
-spec activate(#state{}, [gen_tcp:option() | ssl:tls_option()]) -> ok | {stop, term()}.
activate(#state{socket = Socket}, ExtraOpts) ->
    case nhttp_sock:setopts(Socket, ExtraOpts ++ [{active, once}]) of
        ok -> ok;
        {error, Reason} -> {stop, sock_stop_reason(Reason)}
    end.

-spec enter_protocol_loop(pid(), [sys:debug_option()], #state{}) -> no_return().
enter_protocol_loop(Parent, Debug, #state{family = http1} = State) ->
    nhttp_conn_h1:loop(Parent, Debug, State);
enter_protocol_loop(Parent, Debug, #state{family = http2} = State) ->
    nhttp_conn_h2:loop(Parent, Debug, State).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HIBERNATION
%%%-----------------------------------------------------------------------------
-doc """
Cancel the idle timer armed by `start_hibernate_timer/1` after a
hibernation wake-up. Returns `expired` when the timer already fired (its
message, guaranteed delivered to the caller's mailbox by then, is
flushed), `ok` otherwise. On `expired` the caller must treat the wake-up
as an idle timeout unless other traffic is pending.
""".
-spec cancel_hibernate_timer(hibernate_timer()) -> ok | expired.
cancel_hibernate_timer(none) ->
    ok;
cancel_hibernate_timer({TimerRef, Ref}) ->
    case erlang:cancel_timer(TimerRef) of
        false ->
            receive
                {conn_hibernate_timeout, Ref} -> expired
            after 0 -> expired
            end;
        Remaining when is_integer(Remaining) ->
            ok
    end.

-doc """
Whether the process mailbox is empty. Protocol loops hibernate only on
an empty mailbox: with a message already queued the wake-up would be
immediate and the full-sweep GC pure overhead.
""".
-spec mailbox_empty() -> boolean().
mailbox_empty() ->
    {message_queue_len, Len} = erlang:process_info(self(), message_queue_len),
    Len =:= 0.

-doc """
Arm the idle timer that bounds a hibernation wait. `receive ... after`
deadlines do not survive `proc_lib:hibernate/3`, so the idle timeout is
carried by a timer message instead; any message (including this one)
wakes the process, which must then call `cancel_hibernate_timer/1`.
""".
-spec start_hibernate_timer(timeout()) -> hibernate_timer().
start_hibernate_timer(infinity) ->
    none;
start_hibernate_timer(IdleTimeout) ->
    Ref = make_ref(),
    {erlang:send_after(IdleTimeout, self(), {conn_hibernate_timeout, Ref}), Ref}.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - HANDLER SAFETY
%%%-----------------------------------------------------------------------------
-spec emit_handler_exception(
    {nhttp_otel:span_ctx(), integer()}, error | exit | throw, term()
) -> ok.
emit_handler_exception(ReqSpan, Class, Reason) ->
    nhttp_otel:request_span_error(ReqSpan, Class, Reason).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - OPENTELEMETRY
%%%-----------------------------------------------------------------------------
-spec conn_attrs(#state{}) -> nhttp_conn_otel:conn_attrs().
conn_attrs(#state{transport = Transport, family = Family}) ->
    #{
        transport => Transport,
        version => family_to_version(Family),
        scheme => nhttp_lib:encode_scheme(transport_to_scheme(Transport))
    }.

-spec emit_connection_start(#state{}) -> #state{}.
emit_connection_start(#state{otel_config = OtelConfig, peer = Peer} = State) ->
    State#state{
        conn_span = nhttp_conn_otel:connection_start(OtelConfig, Peer, conn_attrs(State))
    }.

-spec emit_connection_stop(#state{}, term()) -> ok.
emit_connection_stop(#state{conn_span = ConnSpan, requests_count = RequestsCount}, Reason) ->
    nhttp_conn_otel:connection_stop(ConnSpan, RequestsCount, Reason).

-spec emit_request_start(#state{}, nhttp_lib:stream_id(), nhttp_lib:request()) ->
    {nhttp_otel:span_ctx(), integer()}.
emit_request_start(
    #state{otel_config = OtelConfig, conn_span = ConnSpan} = State, StreamId, Request
) ->
    nhttp_conn_otel:request_start(OtelConfig, ConnSpan, StreamId, Request, conn_attrs(State)).

-spec emit_request_stop(#state{}, nhttp_lib:status(), {nhttp_otel:span_ctx(), integer()}) ->
    ok.
emit_request_stop(#state{otel_config = OtelConfig}, Status, ReqSpanCtx) ->
    nhttp_conn_otel:request_stop(OtelConfig, Status, ReqSpanCtx).

-spec emit_request_stop_with_size(
    #state{},
    nhttp_lib:status(),
    {nhttp_otel:span_ctx(), integer()},
    non_neg_integer()
) -> ok.
emit_request_stop_with_size(#state{otel_config = OtelConfig}, Status, ReqSpanCtx, Size) ->
    nhttp_conn_otel:request_stop_with_size(OtelConfig, Status, ReqSpanCtx, Size).

-spec emit_stream_start(#state{}, {nhttp_otel:span_ctx(), integer()}) -> ok.
emit_stream_start(#state{otel_config = OtelConfig}, ReqSpanCtx) ->
    nhttp_conn_otel:stream_start(OtelConfig, ReqSpanCtx).

-spec family_to_version(http1 | http2 | websocket | undefined) ->
    nhttp_lib:version() | undefined.
family_to_version(http1) -> http1_1;
family_to_version(http2) -> http2;
family_to_version(websocket) -> http1_1;
family_to_version(undefined) -> undefined.

-spec get_remote_addr(nhttp_sock:t()) -> {inet:ip_address(), inet:port_number()}.
get_remote_addr(Socket) ->
    case nhttp_sock:peername(Socket) of
        {ok, {IP, Port}} -> {IP, Port};
        {error, _} -> {{0, 0, 0, 0}, 0}
    end.

-spec transport_to_scheme(tcp | ssl) -> nhttp_lib:scheme().
transport_to_scheme(tcp) -> http;
transport_to_scheme(ssl) -> https.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - PROTOCOL DETECTION
%%%-----------------------------------------------------------------------------
-spec detect_protocol(nhttp_sock:t(), nhttp:opts()) ->
    {ok, http1 | http2} | {error, {unsupported_alpn, binary()}}.
detect_protocol(Socket, Opts) ->
    AllowedFamilies = [version_to_family(V) || V <- maps:get(versions, Opts, [http1_1, http2])],
    select_family(nhttp_sock:negotiated_protocol(Socket), AllowedFamilies).

-spec select_family({ok, binary()} | {error, no_alpn}, [http1 | http2 | http3]) ->
    {ok, http1 | http2} | {error, {unsupported_alpn, binary()}}.
select_family({ok, <<"h2">> = Protocol}, AllowedFamilies) ->
    family_if_allowed(http2, Protocol, AllowedFamilies);
select_family({ok, <<"http/1.1">> = Protocol}, AllowedFamilies) ->
    family_if_allowed(http1, Protocol, AllowedFamilies);
select_family({ok, Protocol}, _AllowedFamilies) ->
    {error, {unsupported_alpn, Protocol}};
select_family({error, no_alpn}, [http2 | _]) ->
    {ok, http2};
select_family({error, no_alpn}, _AllowedFamilies) ->
    {ok, http1}.

-spec family_if_allowed(http1 | http2, binary(), [http1 | http2 | http3]) ->
    {ok, http1 | http2} | {error, {unsupported_alpn, binary()}}.
family_if_allowed(Family, Protocol, AllowedFamilies) ->
    case lists:member(Family, AllowedFamilies) of
        true -> {ok, Family};
        false -> {error, {unsupported_alpn, Protocol}}
    end.

-spec h1_state_from_opts(nhttp:opts()) -> #h1_state{}.
h1_state_from_opts(Opts) ->
    Timeouts = maps:get(timeouts, Opts, #{}),
    #h1_state{
        body_timeout = maps:get(body, Timeouts, ?DEFAULT_BODY_TIMEOUT),
        request_timeout = maps:get(request, Timeouts, ?DEFAULT_REQUEST_TIMEOUT),
        body_deadline = maps:get(body_deadline, Timeouts, infinity)
    }.

-spec init_protocol(#state{}) -> {ok, #state{}} | {error, nhttp_sock:socket_error()}.
init_protocol(#state{family = http2, socket = Socket, peer = Peer, opts = Opts} = State) ->
    UserH2Settings = maps:get(h2_settings, Opts, #{}),
    H2Settings = UserH2Settings#{enable_connect_protocol => true},
    H2Conn0 = nhttp_h2:new(server, H2Settings),
    H2Conn = nhttp_h2:set_peer(H2Conn0, Peer),
    Preface = nhttp_h2:preface(H2Conn),
    maybe
        ok ?= nhttp_sock:send(Socket, Preface),
        ok ?= nhttp_sock:setopts(Socket, [{active, once}]),
        {ok, State#state{protocol_state = #h2_state{h2_conn = H2Conn}}}
    end;
init_protocol(#state{family = http1, socket = Socket, opts = Opts} = State) ->
    case nhttp_sock:setopts(Socket, [{active, once}]) of
        ok -> {ok, State#state{protocol_state = h1_state_from_opts(Opts)}};
        {error, _} = Error -> Error
    end.

-spec version_to_family(nhttp_lib:version()) -> http1 | http2 | http3.
version_to_family(http1_0) -> http1;
version_to_family(http1_1) -> http1;
version_to_family(http2) -> http2;
version_to_family(http3) -> http3.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - SOCKET HANDOFF
%%%-----------------------------------------------------------------------------
-spec apply_proxy_header(nhttp_sock:t(), nhttp_proxy_protocol:header(), #state{}) ->
    #state{}.
apply_proxy_header(Socket, #{command := local}, State) ->
    State#state{peer = get_remote_addr(Socket)};
apply_proxy_header(Socket, #{command := proxy, family := Family} = Header, State) when
    Family =:= inet; Family =:= inet6
->
    L4Peer = get_remote_addr(Socket),
    SrcAddr = maps:get(src_addr, Header),
    SrcPort = maps:get(src_port, Header),
    State#state{peer = {SrcAddr, SrcPort}, proxy_peer = L4Peer};
apply_proxy_header(Socket, _Header, State) ->
    State#state{peer = get_remote_addr(Socket)}.

-doc "Complete TLS handshake if needed.".
-spec complete_handshake(nhttp_sock:t(), nhttp:opts()) ->
    {ok, nhttp_sock:t()} | {error, term()}.
complete_handshake(Socket, Opts) ->
    Transport = maps:get(transport, Opts, tcp),
    case Transport of
        tcp ->
            {ok, Socket};
        ssl ->
            SslOpts = maps:get(ssl_opts, Opts, []),
            nhttp_sock:handshake(Socket, ?DEFAULT_TLS_HANDSHAKE_TIMEOUT, SslOpts)
    end.

-spec complete_handshake_result(nhttp_sock:t(), nhttp:opts()) ->
    {ok, nhttp_sock:t()} | {error, {handshake_error, term()}}.
complete_handshake_result(Socket, Opts) ->
    case complete_handshake(Socket, Opts) of
        {ok, ReadySocket} -> {ok, ReadySocket};
        {error, Reason} -> {error, {handshake_error, Reason}}
    end.

-spec finalize_peer(#state{}) -> #state{}.
finalize_peer(#state{proxy_peer = undefined, peer = {{0, 0, 0, 0}, 0}, socket = Socket} = State) ->
    State#state{peer = get_remote_addr(Socket)};
finalize_peer(State) ->
    State.

-spec handshake_after_proxy(nhttp_sock:t(), nhttp:opts(), #state{}) ->
    {ok, nhttp_sock:t(), #state{}} | {error, term()}.
handshake_after_proxy(Socket, Opts, State) ->
    maybe
        {ok, State1} ?= maybe_read_proxy(Socket, Opts, State),
        {ok, ReadySocket} ?= complete_handshake_result(Socket, Opts),
        {ok, ReadySocket, State1}
    end.

-doc """
Map an init-path termination reason to a proc_lib exit reason. A peer that
closes or resets the connection before or during protocol init (raw socket
close, TLS handshake abort, proxy-header read interrupted) is routine, so
those exit `normal` and emit no crash report. Genuine setup failures (TLS
alerts, malformed proxy headers, socket-transfer timeout) keep their reason
and surface as crash reports. A supervisor `shutdown` propagates unchanged.
""".
-spec init_exit_reason(term()) -> normal | shutdown | {shutdown, term()} | term().
init_exit_reason(shutdown) ->
    shutdown;
init_exit_reason({shutdown, _} = Reason) ->
    Reason;
init_exit_reason(Reason) ->
    case is_clean_close(Reason) of
        true -> normal;
        false -> Reason
    end.

-spec is_clean_close(term()) -> boolean().
is_clean_close(closed) -> true;
is_clean_close(einval) -> true;
is_clean_close(enotconn) -> true;
is_clean_close(econnreset) -> true;
is_clean_close(econnaborted) -> true;
is_clean_close(epipe) -> true;
is_clean_close(etimedout) -> true;
is_clean_close({handshake_error, Reason}) -> is_clean_close(Reason);
is_clean_close({proxy_protocol, {recv, Reason}}) -> is_clean_close(Reason);
is_clean_close(_) -> false.

-spec maybe_read_proxy(nhttp_sock:t(), nhttp:opts(), #state{}) ->
    {ok, #state{}} | {error, {proxy_protocol, term()}}.
maybe_read_proxy(Socket, Opts, State) ->
    case proxy_protocol_config(Opts) of
        disabled ->
            {ok, State};
        ProxyOpts ->
            read_proxy_header(Socket, ProxyOpts, State)
    end.

-spec proxy_protocol_config(nhttp:opts()) -> disabled | nhttp:proxy_protocol_opts().
proxy_protocol_config(Opts) ->
    case maps:get(proxy_protocol, Opts, false) of
        false -> disabled;
        true -> #{};
        Map when is_map(Map) -> Map
    end.

-spec read_proxy_header(nhttp_sock:t(), nhttp:proxy_protocol_opts(), #state{}) ->
    {ok, #state{}} | {error, {proxy_protocol, term()}}.
read_proxy_header(Socket, ProxyOpts, State) ->
    Versions = maps:get(version, ProxyOpts, both),
    Timeout = maps:get(timeout, ProxyOpts, ?DEFAULT_PROXY_TIMEOUT),
    case read_proxy_loop(Socket, Versions, Timeout, <<>>) of
        {ok, Header, <<>>} ->
            {ok, apply_proxy_header(Socket, Header, State)};
        {ok, _Header, _Leftover} ->
            {error, {proxy_protocol, leftover_bytes}};
        {error, Reason} ->
            {error, {proxy_protocol, Reason}}
    end.

-spec read_proxy_loop(nhttp_sock:t(), v1 | v2 | both, timeout(), binary()) ->
    {ok, nhttp_proxy_protocol:header(), binary()} | {error, term()}.
read_proxy_loop(Socket, Versions, Timeout, Acc) ->
    case nhttp_proxy_protocol:parse(Acc, #{version => Versions}) of
        {ok, Header, Consumed} ->
            Rest = binary:part(Acc, Consumed, byte_size(Acc) - Consumed),
            {ok, Header, Rest};
        {error, Reason} ->
            {error, Reason};
        {more, N} ->
            case nhttp_sock:recv(Socket, N, Timeout) of
                {ok, Data} ->
                    read_proxy_loop(Socket, Versions, Timeout, <<Acc/binary, Data/binary>>);
                {error, RecvReason} ->
                    {error, {recv, RecvReason}}
            end
    end.

-spec sock_send(#state{}, iodata()) -> ok.
sock_send(#state{socket = Socket} = State, Data) ->
    case nhttp_sock:send(Socket, Data) of
        ok ->
            ok;
        {error, Reason} ->
            nhttp_log:sock_send_failed(log_ctx(State), Reason),
            ok
    end.

-doc """
Map a socket error to a connection stop reason. A peer close gives
`normal`, which keeps the connection out of the crash reports. Any other
error keeps its reason, so `emit_connection_stop/2` reports the fault.
""".
-spec sock_stop_reason(term()) -> normal | {socket_error, term()}.
sock_stop_reason(Reason) ->
    case is_clean_close(Reason) of
        true -> normal;
        false -> {socket_error, Reason}
    end.

-spec start_protocol(pid(), [sys:debug_option()], #state{}) -> no_return().
start_protocol(Parent, Debug, #state{socket = Socket, opts = Opts} = State) ->
    case detect_protocol(Socket, Opts) of
        {ok, Family} ->
            State1 = finalize_peer(State#state{family = Family}),
            case init_protocol(State1) of
                {ok, State2} ->
                    State3 = emit_connection_start(State2),
                    enter_protocol_loop(Parent, Debug, State3);
                {error, Reason} ->
                    wait_for_socket_terminate(Reason, State1)
            end;
        {error, Reason} ->
            wait_for_socket_terminate(Reason, State)
    end.

-spec wait_for_socket(pid(), [sys:debug_option()], #state{}) -> no_return().
wait_for_socket(Parent, Debug, #state{opts = Opts} = State) ->
    receive
        {socket_ready, Socket} ->
            case handshake_after_proxy(Socket, Opts, State) of
                {ok, ReadySocket, State1} ->
                    start_protocol(Parent, Debug, State1#state{socket = ReadySocket});
                {error, Reason} ->
                    wait_for_socket_terminate(
                        Reason, State#state{socket = Socket}
                    )
            end;
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        {'EXIT', Parent, Reason} ->
            wait_for_socket_terminate(Reason, State)
    after ?SOCKET_TRANSFER_TIMEOUT ->
        wait_for_socket_terminate(socket_transfer_timeout, State)
    end.

-spec wait_for_socket_terminate(term(), #state{}) -> no_return().
wait_for_socket_terminate(Reason, #state{socket = undefined} = State) ->
    State1 =
        receive
            {socket_ready, Socket} -> State#state{socket = Socket}
        after 0 -> State
        end,
    ok = terminate(Reason, State1),
    exit(init_exit_reason(Reason));
wait_for_socket_terminate(Reason, State) ->
    ok = terminate(Reason, State),
    exit(init_exit_reason(Reason)).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - TERMINATION
%%%-----------------------------------------------------------------------------
-spec drain_timeout(#state{}) -> non_neg_integer().
drain_timeout(#state{opts = Opts}) ->
    maps:get(drain_timeout, Opts, ?DEFAULT_DRAIN_GOAWAY_DELAY).

-doc """
Map an internal termination reason to a proc_lib exit reason. Routine
endings (socket errors, idle timeouts, protocol errors) exit `normal` so
they do not generate crash reports for a busy server. The precise reason
is preserved for telemetry by `terminate/2` via `emit_connection_stop/2`.
A supervisor-driven `shutdown` is propagated unchanged.
""".
-spec exit_reason(term()) -> normal | shutdown | {shutdown, term()}.
exit_reason(shutdown) -> shutdown;
exit_reason({shutdown, _} = Reason) -> Reason;
exit_reason(_) -> normal.

-doc """
Start the HTTP/2 drain: close active WS sessions and arm the drain
deadline; the h2 loop sends GOAWAY when it expires or the connection
goes idle. H1 loops stop directly on `shutdown` instead of calling this.
""".
-spec graceful_shutdown(#state{}) -> #state{}.
graceful_shutdown(#state{family = http2} = State) ->
    State1 = nhttp_conn_ws_h2:close_all(State),
    Deadline = erlang:monotonic_time(millisecond) + drain_timeout(State1),
    #state{protocol_state = #h2_state{} = H2} = State1,
    State1#state{protocol_state = H2#h2_state{drain_deadline = Deadline}}.

-spec stop(term(), #state{}) -> no_return().
stop(Reason, State) ->
    ok = terminate(Reason, State),
    exit(exit_reason(Reason)).

-spec stop_parent(term(), #state{}) -> no_return().
stop_parent(Reason, State) ->
    ok = terminate(Reason, State),
    exit(Reason).

-spec terminate(term(), #state{}) -> ok.
terminate(
    Reason,
    #state{
        socket = Socket,
        handler = Handler,
        handler_state = HState
    } = State
) ->
    ok = nhttp_stream_worker:reap(worker_pids(State)),
    emit_connection_stop(State, Reason),
    case erlang:function_exported(Handler, terminate, 2) of
        true ->
            case nhttp_handler:safe_terminate(Handler, normal, HState) of
                {nhttp_handler_exception, Class, CrashReason} ->
                    nhttp_log:handler_crashed(
                        log_ctx(State), terminate, Class, CrashReason
                    );
                _ ->
                    ok
            end;
        false ->
            ok
    end,
    case Socket of
        undefined -> ok;
        _ -> nhttp_sock:close(Socket)
    end,
    ok = nhttp_stats:decr_connection(),
    ok.

-spec worker_pids(#state{}) -> [pid()].
worker_pids(#state{protocol_state = #h2_state{h2_workers = Workers}}) ->
    maps:keys(Workers);
worker_pids(#state{}) ->
    [].
