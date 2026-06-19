-module(nhttp_log).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    accept_error/2,
    conn_ctx/5,
    h1_push_idle_timeout/2,
    h1_push_worker_stuck/2,
    handler_crashed/4,
    request_ctx/3,
    request_id/0,
    sock_send_failed/2,
    stream_push_producer_crashed/2,
    stream_push_rejected/4,
    ws_send_failed/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([ctx/0]).

-type ctx() :: #{
    listener_name => term(),
    transport => tcp | ssl | quic | udp,
    version => nhttp_lib:version() | undefined,
    peer => {inet:ip_address(), inet:port_number()},
    handler => module(),
    method => nhttp_lib:method(),
    path => binary(),
    stream_id => nhttp_lib:stream_id(),
    request_id => binary()
}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Accept loop hit a persistent error (resource exhaustion such as
`emfile`/`enfile`/`enobufs`/`enomem`, or an unknown transport error).
Throttled by the caller so a sustained failure cannot flood the log.
""".
-spec accept_error(ctx(), term()) -> ok.
accept_error(Ctx, Reason) ->
    logger:warning(Ctx#{event => accept_error, reason => Reason}).

-doc """
Build the connection-scoped `ctx()` the `nhttp_conn*` modules log with.
Cheap (map literal) so call sites can build it on demand without caching.
""".
-spec conn_ctx(
    term(),
    tcp | ssl | quic,
    nhttp_lib:version() | undefined,
    {inet:ip_address(), inet:port_number()},
    module()
) -> ctx().
conn_ctx(ListenerName, Transport, Version, Peer, Handler) ->
    #{
        listener_name => ListenerName,
        transport => Transport,
        version => Version,
        peer => Peer,
        handler => Handler
    }.

-doc "Stream push receive timed out: producer wedged with no chunk activity for idle_timeout.".
-spec h1_push_idle_timeout(ctx(), pid()) -> ok.
h1_push_idle_timeout(Ctx, WorkerPid) ->
    logger:warning(Ctx#{event => h1_push_idle_timeout, worker_pid => WorkerPid}).

-doc "Worker process did not exit within the bounded wait after acking stream_done.".
-spec h1_push_worker_stuck(ctx(), pid()) -> ok.
h1_push_worker_stuck(Ctx, WorkerPid) ->
    logger:warning(Ctx#{event => h1_push_worker_stuck, worker_pid => WorkerPid}).

-doc """
A handler callback raised. Emitted by the per-callback safe wrapper's
caller (`nhttp_handler:safe_*`), not by the wrapper itself, so the
connection context is preserved.
Stacktraces are intentionally not captured upstream. This event reports
class + reason only.
`Ctx` should carry method, path, stream id, and request id when the
crash happened during a request, use `request_ctx/3` to build it.
""".
-spec handler_crashed(ctx(), atom(), error | exit | throw, term()) -> ok.
handler_crashed(Ctx, Callback, Class, Reason) ->
    logger:error(Ctx#{
        event => handler_crashed,
        callback => Callback,
        class => Class,
        crash_reason => Reason
    }).

-doc """
Enrich a connection ctx with per-request fields for log correlation.
Pulls `method` and `path` from the request map. `stream_id` is supplied
by the caller (`undefined` on H1, the actual stream id on H2/H3). A
freshly generated `request_id` is added so log streams from the same
request can be joined across modules.
""".
-spec request_ctx(ctx(), nhttp_lib:request(), nhttp_lib:stream_id() | undefined) ->
    ctx().
request_ctx(Ctx, Request, StreamId) ->
    Base = Ctx#{request_id => request_id()},
    Base1 =
        case Request of
            #{method := M} -> Base#{method => M};
            _ -> Base
        end,
    Base2 =
        case Request of
            #{path := P} -> Base1#{path => P};
            _ -> Base1
        end,
    case StreamId of
        undefined -> Base2;
        _ -> Base2#{stream_id => StreamId}
    end.

-doc """
Generate a short, sortable request id. Used for log correlation across
the modules that participate in a single request's lifetime. Not
cryptographically random. Operators that need stronger guarantees should
inject their own correlation header at the edge.
""".
-spec request_id() -> binary().
request_id() ->
    Bytes = crypto:strong_rand_bytes(8),
    iolist_to_binary([io_lib:format("~2.16.0b", [B]) || <<B:8>> <= Bytes]).

-doc "Best-effort socket send returned `{error, _}`.".
-spec sock_send_failed(ctx(), term()) -> ok.
sock_send_failed(Ctx, Reason) ->
    logger:debug(Ctx#{event => sock_send_failed, reason => Reason}).

-doc "Stream-push producer worker exited with a non-normal reason.".
-spec stream_push_producer_crashed(ctx(), term()) -> ok.
stream_push_producer_crashed(Ctx, Reason) ->
    logger:warning(Ctx#{event => stream_push_producer_crashed, reason => Reason}).

-doc """
Server rejected a `stream_push` reply (e.g. on a method/version/status
that doesn't support it). Carries the request triple plus the reject
reason atom.
""".
-spec stream_push_rejected(ctx(), atom() | binary(), binary(), term()) -> ok.
stream_push_rejected(Ctx, Method, Path, Reason) ->
    logger:warning(Ctx#{
        event => stream_push_rejected,
        method => Method,
        path => Path,
        reject_reason => Reason
    }).

-doc "WebSocket frame send returned `{error, _}` on a fire-and-forget path.".
-spec ws_send_failed(ctx(), term()) -> ok.
ws_send_failed(Ctx, Reason) ->
    logger:debug(Ctx#{event => ws_send_failed, reason => Reason}).
