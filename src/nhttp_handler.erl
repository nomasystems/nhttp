-module(nhttp_handler).

-moduledoc """
Handler behaviour for nhttp server.

Implement this behaviour to handle HTTP requests. Your handler module
is called synchronously for each request.

> #### HTTP/2 handler concurrency {: .info}
>
> On HTTP/2, `handle_request/2` runs in a per-stream worker process
> (`nhttp_stream_worker`), so a slow handler does not block other
> multiplexed streams on the same connection. This means that, on a
> single HTTP/2 connection, multiple workers may be executing handler
> callbacks concurrently. Each worker receives a snapshot of the
> connection's `handler_state`. The connection updates its own copy
> from each worker's `NewState` in arrival order. Concurrent streams
> therefore observe a last-write-wins view of `handler_state`. Do not
> use `handler_state` as the canonical source of truth for state that
> must be coordinated across requests on the same connection. Use
> ETS, gproc, or `persistent_term` for that.
>
> HTTP/1.1 keeps the synchronous in-process dispatch (no multiplexing),
> so its `handler_state` semantics are unchanged.

## Example Handler (Simple)

```erlang
-module(my_handler).
-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

init(_Args) ->
    {ok, #{}}.

handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello, World!">>), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.
```

## Lifecycle and `terminate/2`

`terminate/2` is **connection-scoped on every protocol**, not
request-scoped. On HTTP/1.1 a connection serves one request at a time,
so the boundaries coincide. On HTTP/2 and HTTP/3 a single connection
multiplexes many requests through per-stream workers, and `terminate/2`
fires once when the whole connection ends, not after each request.

This has practical consequences:

- **Resources opened inside `handle_request/2` or `handle_request_body/3`
  must be released before that callback returns** (or, for streaming
  responses, before the producer fun returns).
  `nhttp_stream:producer/3` has `try/after` semantics for exactly this.
- **Do not rely on `terminate/2` to close per-request files, DB cursors,
  or external sockets** opened during request handling on H2/H3. Those
  resources will leak across the rest of the connection's lifetime.
- `terminate/2` is the right place for connection-scoped cleanup
  (deregistration, connection-wide state shutdown), not per-request
  cleanup.

`init/1` runs once per connection process, symmetric with `terminate/2`.

## Streaming responses

For streaming responses, return `{stream, Spec, State}` where `Spec` is
built with `nhttp_stream:producer/3` (push-based fun in a dedicated
worker). See `nhttp_stream` for examples.

## Streaming request bodies

To consume a request body chunk-by-chunk instead of buffering, return
`{accept_body, BodyState, State}` from `handle_request/2` and implement
`handle_request_body/3`. The connection forwards each `body_event/0`
(`{data, _}` | `{fin, Trailers}` | `{abort, Reason}`) to your callback,
threading `BodyState` between calls. Returning `{accept_body, _, _}`
again continues consuming. Any other tag (`reply`, `stream`, `upgrade`,
`abort`) terminates the body phase and proceeds to the response.

The connection applies end-to-end backpressure: TCP receive on HTTP/1.1,
WINDOW_UPDATE on HTTP/2, and QUIC stream flow control on HTTP/3 are all
gated on handler acknowledgement of the previous chunk. The configured
`max_body_size` remains a hard upper bound regardless of handler
willingness.

## Example Handler (WebSocket, async)

The WebSocket surface is split across four optional callbacks:

- `handle_ws_open/2` fires once after the upgrade response is on the wire.
  Use it to register the session for external pushes (pubsub, gproc, ...).
- `handle_ws_frame/3` receives `text` and `binary` data frames. PING/PONG
  frames are handled automatically. Opt in via `ws_runtime_opts` to also
  observe them. CLOSE frames are NOT delivered here. See `handle_ws_closed/3`.
- `handle_ws_info/3` receives arbitrary Erlang messages, just like a
  `gen_server` `handle_info/2` callback.
- `handle_ws_closed/3` fires once at session termination with an
  RFC-grounded reason (`{peer | local | fail, Code, Reason}`,
  `{transport, _}`, `{h2_reset | h3_reset, ErrorCode}`, `timeout`,
  `shutdown`, `{handler_stop, _}`, `{handler_crash, _}`).

External processes push frames via `nhttp_ws:send/2,3` and
`nhttp_ws:send_async/2`. See `nhttp_ws` for the full session API.

```erlang
-module(ws_handler).
-behaviour(nhttp_handler).
-export([init/1, handle_request/2,
         handle_ws_open/2, handle_ws_frame/3,
         handle_ws_info/3, handle_ws_closed/3]).

init(_Args) -> {ok, #{}}.

handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_ws_open(Session, _State) ->
    chat_room:join(Session),
    {ok, #{session => Session}}.

handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State}.

handle_ws_info({chat, From, Msg}, _Session, State) ->
    {reply, {text, format(From, Msg)}, State}.

handle_ws_closed(_Reason, Session, _State) ->
    chat_room:leave(Session),
    ok.
```

See also: `nhttp_resp`, `nhttp_ws`.
""".

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    safe_handle_request/3,
    safe_handle_request_body/4,
    safe_handle_ws_closed/4,
    safe_handle_ws_frame/4,
    safe_handle_ws_info/4,
    safe_handle_ws_open/3,
    safe_init/2,
    safe_terminate/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    abort_reason/0,
    body_event/0,
    handler_exception/0,
    request_result/1,
    ws_close_reason/0,
    ws_result/1,
    ws_runtime_opts/0,
    ws_session_opts/0
]).

-type abort_reason() :: term().

-type body_event() ::
    {data, binary()}
    | {fin, nhttp_lib:headers()}
    | {abort, abort_reason()}.

-type request_result(State) ::
    {reply, Response :: nhttp_lib:response(), NewState :: State}
    | {stream, Spec :: nhttp_stream:spec(), NewState :: State}
    | {accept_body, BodyState :: term(), NewState :: State}
    | {upgrade, websocket, NewState :: State}
    | {upgrade, websocket, SessionOpts :: ws_session_opts(), NewState :: State}
    | {abort, Reason :: abort_reason(), NewState :: State}.

-type handler_exception() ::
    {nhttp_handler_exception, Class :: error | exit | throw, Reason :: term()}.

-type ws_close_reason() ::
    {peer, Code :: nhttp_ws:close_code(), Reason :: binary()}
    | {local, Code :: nhttp_ws:close_code(), Reason :: binary()}
    | {fail, Code :: nhttp_ws:close_code(), Reason :: binary()}
    | {transport, Cause :: term()}
    | {h2_reset, ErrorCode :: non_neg_integer() | atom()}
    | {h3_reset, ErrorCode :: non_neg_integer() | atom()}
    | timeout
    | shutdown
    | {handler_stop, Reason :: term()}
    | {handler_crash, {Class :: atom(), Reason :: term()}}.

-type ws_result(State) ::
    {ok, NewState :: State}
    | {reply, nhttp_ws:ws_message() | [nhttp_ws:ws_message()], NewState :: State}
    | {close, Code :: nhttp_ws:close_code(), Reason :: binary(), NewState :: State}
    | {stop, Reason :: term(), NewState :: State}.

-type ws_runtime_opts() :: nhttp_ws:ws_runtime_opts().
-type ws_session_opts() :: nhttp_ws:ws_session_opts().

%%%-----------------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%%-----------------------------------------------------------------------------
-callback handle_request(Request :: nhttp_lib:request(), State :: term()) ->
    request_result(term()).

-callback handle_request_body(
    Event :: body_event(),
    BodyState :: term(),
    State :: term()
) -> request_result(term()).

-callback handle_ws_open(Session :: nhttp_ws:session(), State :: term()) ->
    {ok, SessionState :: term()}
    | {ok, SessionState :: term(), Opts :: ws_runtime_opts()}
    | {close, Code :: nhttp_ws:close_code(), Reason :: binary()}.

-callback handle_ws_frame(
    Frame :: nhttp_ws:ws_frame(),
    Session :: nhttp_ws:session(),
    State :: term()
) -> ws_result(term()).

-callback handle_ws_info(
    Info :: term(),
    Session :: nhttp_ws:session(),
    State :: term()
) -> ws_result(term()).

-callback handle_ws_closed(
    Reason :: ws_close_reason(),
    Session :: nhttp_ws:session(),
    State :: term()
) -> ok | {ok, NewState :: term()}.

-callback init(Args :: term()) ->
    {ok, State :: term()}
    | {error, Reason :: term()}.

-callback terminate(Reason :: term(), State :: term()) -> ok.

-optional_callbacks([
    handle_request_body/3,
    handle_ws_closed/3,
    handle_ws_frame/3,
    handle_ws_info/3,
    handle_ws_open/2,
    terminate/2
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc false.
-spec safe_handle_request(module(), nhttp_lib:request(), term()) ->
    request_result(term()) | handler_exception().
safe_handle_request(Handler, Request, State) ->
    apply_safe(fun() -> Handler:handle_request(Request, State) end).

-doc false.
-spec safe_handle_request_body(module(), body_event(), term(), term()) ->
    request_result(term()) | handler_exception().
safe_handle_request_body(Handler, Event, BodyState, State) ->
    apply_safe(fun() -> Handler:handle_request_body(Event, BodyState, State) end).

-doc false.
-spec safe_handle_ws_closed(module(), ws_close_reason(), nhttp_ws:session(), term()) ->
    ok | {ok, term()} | handler_exception().
safe_handle_ws_closed(Handler, Reason, Session, State) ->
    apply_safe(fun() -> Handler:handle_ws_closed(Reason, Session, State) end).

-doc false.
-spec safe_handle_ws_frame(module(), nhttp_ws:ws_frame(), nhttp_ws:session(), term()) ->
    ws_result(term()) | handler_exception().
safe_handle_ws_frame(Handler, Frame, Session, State) ->
    apply_safe(fun() -> Handler:handle_ws_frame(Frame, Session, State) end).

-doc false.
-spec safe_handle_ws_info(module(), term(), nhttp_ws:session(), term()) ->
    ws_result(term()) | handler_exception().
safe_handle_ws_info(Handler, Info, Session, State) ->
    apply_safe(fun() -> Handler:handle_ws_info(Info, Session, State) end).

-doc false.
-spec safe_handle_ws_open(module(), nhttp_ws:session(), term()) ->
    {ok, term()}
    | {ok, term(), ws_runtime_opts()}
    | {close, nhttp_ws:close_code(), binary()}
    | handler_exception().
safe_handle_ws_open(Handler, Session, State) ->
    apply_safe(fun() -> Handler:handle_ws_open(Session, State) end).

-doc false.
-spec safe_init(module(), term()) ->
    {ok, term()} | {error, term()} | handler_exception().
safe_init(Handler, Args) ->
    apply_safe(fun() -> Handler:init(Args) end).

-doc false.
-spec safe_terminate(module(), term(), term()) -> ok | handler_exception().
safe_terminate(Handler, Reason, State) ->
    apply_safe(fun() -> ok = Handler:terminate(Reason, State) end).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec apply_safe(fun(() -> Result)) -> Result | handler_exception().
apply_safe(Fun) ->
    try Fun() of
        Result -> Result
    catch
        Class:Err -> {nhttp_handler_exception, Class, Err}
    end.
