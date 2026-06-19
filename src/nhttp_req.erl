-module(nhttp_req).

-moduledoc """
Accessors for `t:nhttp:request/0`. Read fields by name, never reach into
the map directly.

The dispatch path populates a fixed shape across HTTP/1.1, HTTP/2, and
HTTP/3 (built by `nhttp_lib`'s parsers), so the accessors here return
real values on every protocol, with no defaults and no per-protocol surprises.

`scheme/1`, `authority/1`, `headers/1`, `method/1`, `path/1`, and
`version/1` are total. `body/1`, `trailers/1`, `peer/1`,
`connect_protocol/1`, `header/2,3` return a default when the field is
absent.

Header lookup is case-insensitive on the *name*. The stored name is
already lowercased on ingest.
""".

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    authority/1,
    body/1,
    connect_protocol/1,
    header/2,
    header/3,
    headers/1,
    is_websocket/1,
    is_websocket_upgrade/1,
    method/1,
    path/1,
    peer/1,
    scheme/1,
    trailers/1,
    version/1
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Return the authority (`host[:port]`) component of the request URI.".
-spec authority(nhttp:request()) -> nhttp:authority().
authority(#{authority := A}) -> A.

-doc """
Return the request body as buffered bytes, or `streaming` when the
handler opted into streaming-body delivery.
""".
-spec body(nhttp:request()) -> iodata() | streaming.
body(Req) -> maps:get(body, Req, <<>>).

-doc """
Return the RFC 8441 / 9220 `:protocol` pseudo-header value sent on
Extended CONNECT requests, or `undefined` if absent.
""".
-spec connect_protocol(nhttp:request()) -> binary() | undefined.
connect_protocol(Req) -> maps:get(connect_protocol, Req, undefined).

-doc """
Return the value of the named header, or `undefined` when missing.
Names are matched case-insensitively against the request's header list.
""".
-spec header(nhttp:header_name(), nhttp:request()) ->
    nhttp:header_value() | undefined.
header(Name, Req) -> header(Name, Req, undefined).

-doc """
Return the value of the named header, or `Default` when missing.
Names are matched case-insensitively against the request's header list.
""".
-spec header(nhttp:header_name(), nhttp:request(), Default) ->
    nhttp:header_value() | Default.
header(Name, #{headers := Headers}, Default) ->
    case nhttp_headers:get(Name, Headers) of
        undefined -> Default;
        Value -> Value
    end.

-doc "Return the request headers as a list of `{Name, Value}` pairs.".
-spec headers(nhttp:request()) -> nhttp:headers().
headers(#{headers := H}) -> H.

-doc """
Return `true` when the request is a WebSocket upgrade. On HTTP/1.1 this
is a `GET` with `Upgrade: websocket`. On HTTP/2 and HTTP/3 it is the
Extended CONNECT shape (`method = connect`,
`connect_protocol = <<"websocket">>`).
""".
-spec is_websocket(nhttp:request()) -> boolean().
is_websocket(#{method := connect} = Req) ->
    maps:get(connect_protocol, Req, undefined) =:= <<"websocket">>;
is_websocket(#{method := get, headers := Headers}) ->
    case nhttp_headers:get(<<"upgrade">>, Headers) of
        undefined -> false;
        Value -> nhttp_headers:to_lower(Value) =:= <<"websocket">>
    end;
is_websocket(_) ->
    false.

-doc """
Return `true` only for the RFC 8441 / RFC 9220 Extended CONNECT WebSocket
shape (`method = connect`, `connect_protocol = <<"websocket">>`). This is
the upgrade signal used by HTTP/2 and HTTP/3 dispatch. It deliberately
ignores the HTTP/1.1 `GET + Upgrade: websocket` form, which is detected
upstream by the parser.
""".
-spec is_websocket_upgrade(nhttp:request()) -> boolean().
is_websocket_upgrade(#{method := connect} = Req) ->
    maps:get(connect_protocol, Req, undefined) =:= <<"websocket">>;
is_websocket_upgrade(_) ->
    false.

-doc "Return the request method.".
-spec method(nhttp:request()) -> nhttp:method().
method(#{method := M}) -> M.

-doc "Return the request path (origin-form for non-CONNECT requests).".
-spec path(nhttp:request()) -> binary().
path(#{path := P}) -> P.

-doc """
Return the remote peer (`{IP, Port}`) when the dispatch path supplied
one, or `undefined`.
""".
-spec peer(nhttp:request()) -> nhttp:peer() | undefined.
peer(Req) -> maps:get(peer, Req, undefined).

-doc "Return the request URI scheme (`http | https | ws | wss`).".
-spec scheme(nhttp:request()) -> nhttp:scheme().
scheme(#{scheme := S}) -> S.

-doc """
Return the trailing header fields delivered after the request body, or
`[]` when there were no trailers.
""".
-spec trailers(nhttp:request()) -> nhttp:headers().
trailers(Req) -> maps:get(trailers, Req, []).

-doc """
Return the wire HTTP version (`http1_0 | http1_1 | http2 | http3`). The
dispatch path always populates it via the parser. `undefined` is only
returned for synthetic requests that bypass the parser (e.g. unit tests).
""".
-spec version(nhttp:request()) -> nhttp:version() | undefined.
version(Req) -> maps:get(version, Req, undefined).
