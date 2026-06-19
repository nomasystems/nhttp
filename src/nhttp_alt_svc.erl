-module(nhttp_alt_svc).

-moduledoc """
`Alt-Svc` advertisement helpers (RFC 7838 §3).

A listener that serves HTTP/3 alongside HTTP/1.1 / HTTP/2 auto-emits an
`Alt-Svc` header on its TCP responses so clients discover and upgrade to
the QUIC endpoint. The advertised protocol token is `h3` (RFC 9114 §3.1)
and the value carries the QUIC port plus a `ma` (max-age) freshness
lifetime: `h3=":<port>"; ma=<seconds>`.

Advertisement is gated on live QUIC readiness, not static config: h3 is
advertised only while the QUIC transport is actually accepting. When the
QUIC transport is draining or down the framework emits `Alt-Svc: clear`
instead, telling clients to drop the cached alternative rather than waste
a UDP attempt and fall back (RFC 7838 §2.1, §4).

A handler controls the framework header through the response headers:

- set a different `alt-svc` header to **replace** the framework value;
- set `alt-svc` to an empty value to **suppress** it entirely.
""".

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    header_value/2,
    inject/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([directive/0]).

-type directive() ::
    disabled
    | clear
    | #{port := inet:port_number(), ma := non_neg_integer()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Build the `Alt-Svc` field value advertising h3 on `Port` with a `ma`
freshness lifetime of `Ma` seconds (RFC 7838 §3, RFC 9114 §3.1).
""".
-spec header_value(inet:port_number(), non_neg_integer()) -> binary().
header_value(Port, Ma) ->
    PortBin = integer_to_binary(Port),
    MaBin = integer_to_binary(Ma),
    <<"h3=\":", PortBin/binary, "\"; ma=", MaBin/binary>>.

-doc """
Apply the advertisement directive to a response's header list.
When the handler already set an `alt-svc` header the framework defers to
it: a non-empty value is left untouched (replace), an empty value is
removed so nothing is emitted (suppress). Otherwise the framework header
is appended: an `h3=...` advertisement when the directive carries a port,
or `clear` when the QUIC transport is no longer advertising (RFC 7838
§4).
""".
-spec inject(nhttp_lib:headers(), directive()) -> nhttp_lib:headers().
inject(Headers, Directive) ->
    case nhttp_headers:get(<<"alt-svc">>, Headers) of
        undefined -> append_directive(Headers, Directive);
        <<>> -> nhttp_headers:delete(<<"alt-svc">>, Headers);
        _Value -> Headers
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec append_directive(nhttp_lib:headers(), directive()) -> nhttp_lib:headers().
append_directive(Headers, disabled) ->
    Headers;
append_directive(Headers, clear) ->
    Headers ++ [{<<"alt-svc">>, <<"clear">>}];
append_directive(Headers, #{port := Port, ma := Ma}) ->
    Headers ++ [{<<"alt-svc">>, header_value(Port, Ma)}].
