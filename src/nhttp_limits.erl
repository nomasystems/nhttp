-module(nhttp_limits).

-moduledoc """
Header validation and size limits for nhttp.

Enforces configurable limits on request headers and body to prevent abuse.
All limits are optional - if not configured, defaults are used.

Configuration options:
- `max_header_name_length` - Maximum length of a header name (default: 256)
- `max_header_value_length` - Maximum length of a header value (default: 8192)
- `max_headers` - Maximum number of headers (default: 100)
- `max_header_size` - Total header block size in bytes (default: 65536)
- `max_uri_length` - Maximum URI/path length (default: 8192)
- `max_body_size` - Maximum request body size in bytes (default: 8388608, i.e. 8 MiB)
""".

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    check_body_size/2,
    default_limits/0,
    error_to_status/1,
    from_opts/1,
    h1_parse_opts/1,
    max_body_size/1,
    validate_request/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([limit_error/0, t/0]).

-type limit_error() ::
    body_too_large
    | header_name_too_long
    | header_too_large
    | header_value_too_long
    | too_many_headers
    | uri_too_long.

-type t() :: #{
    max_header_name_length => pos_integer(),
    max_header_value_length => pos_integer(),
    max_header_size => pos_integer(),
    max_headers => pos_integer(),
    max_uri_length => pos_integer(),
    max_body_size => pos_integer()
}.

-doc """
Check accumulated body size against the configured `max_body_size`.
Returns `ok` if `Size` is within the limit, `{error, body_too_large}`
otherwise. Used by H2/H3 incremental body accumulators where the request
body is delivered across multiple DATA frames.
""".
-spec check_body_size(non_neg_integer(), t()) -> ok | {error, body_too_large}.
check_body_size(Size, Limits) ->
    Max = max_body_size(Limits),
    case Size =< Max of
        true -> ok;
        false -> {error, body_too_large}
    end.

-doc "Return default limit values.".
-spec default_limits() -> t().
default_limits() ->
    #{
        max_header_name_length => 256,
        max_header_value_length => 8192,
        max_headers => 100,
        max_header_size => 65536,
        max_uri_length => 8192,
        max_body_size => 8388608
    }.

-doc """
Canonical HTTP status for a limit violation: 413 for the body limit,
414 for the URI limit, 431 for every header limit. Exhaustive on
purpose (no catch-all): a new `limit_error()` variant must add its
clause here.
""".
-spec error_to_status(limit_error()) -> nhttp_lib:status().
error_to_status(body_too_large) -> 413;
error_to_status(header_name_too_long) -> 431;
error_to_status(header_too_large) -> 431;
error_to_status(header_value_too_long) -> 431;
error_to_status(too_many_headers) -> 431;
error_to_status(uri_too_long) -> 414.

-doc """
Extract the request-limit subset from a connection's `nhttp:opts()` map.
The result is a `t()` ready to pass to `validate_request/2` and
`check_body_size/2`. Unknown keys are dropped. Missing keys fall back to
`default_limits/0` at lookup time.
""".
-spec from_opts(nhttp:opts()) -> t().
from_opts(Opts) ->
    maps:with(
        [
            max_header_name_length,
            max_header_value_length,
            max_headers,
            max_header_size,
            max_uri_length,
            max_body_size
        ],
        Opts
    ).

-doc """
Resolve the limits the HTTP/1.1 parser enforces while reading a request
head into the option keys `nhttp_h1:parse_request_headers/2` expects.
The nhttp key `max_headers` maps to the lib key `max_headers_count`.
""".
-spec h1_parse_opts(t()) ->
    #{
        max_header_size := pos_integer(),
        max_headers_count := pos_integer(),
        max_body_size := pos_integer()
    }.
h1_parse_opts(Limits) ->
    Merged = maps:merge(default_limits(), Limits),
    #{
        max_header_size => maps:get(max_header_size, Merged),
        max_headers_count => maps:get(max_headers, Merged),
        max_body_size => maps:get(max_body_size, Merged)
    }.

-doc "Resolved `max_body_size` from a limits map (default applied if missing).".
-spec max_body_size(t()) -> pos_integer().
max_body_size(Limits) ->
    maps:get(max_body_size, Limits, maps:get(max_body_size, default_limits())).

-doc """
Validate a request against configured limits.
Returns ok if all limits pass, or `{error, Reason}` for the first failure.
""".
-spec validate_request(nhttp_lib:request(), t()) -> ok | {error, limit_error()}.
validate_request(#{path := Path, headers := Headers}, Limits) ->
    MergedLimits = maps:merge(default_limits(), Limits),
    maybe
        ok ?= validate_uri_length(Path, MergedLimits),
        ok ?= validate_header_count(Headers, MergedLimits),
        ok ?= validate_header_names(Headers, MergedLimits),
        ok ?= validate_header_values(Headers, MergedLimits),
        ok ?= validate_total_header_size(Headers, MergedLimits)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec validate_header_count([{binary(), binary()}], t()) ->
    ok | {error, too_many_headers}.
validate_header_count(Headers, #{max_headers := MaxCount}) ->
    case length(Headers) =< MaxCount of
        true -> ok;
        false -> {error, too_many_headers}
    end.

-spec validate_header_names([{binary(), binary()}], t()) ->
    ok | {error, header_name_too_long}.
validate_header_names(Headers, #{max_header_name_length := MaxLen}) ->
    validate_header_names_loop(Headers, MaxLen).

-spec validate_header_names_loop([{binary(), binary()}], pos_integer()) ->
    ok | {error, header_name_too_long}.
validate_header_names_loop([], _MaxLen) ->
    ok;
validate_header_names_loop([{Name, _Value} | Rest], MaxLen) ->
    case byte_size(Name) =< MaxLen of
        true -> validate_header_names_loop(Rest, MaxLen);
        false -> {error, header_name_too_long}
    end.

-spec validate_header_values([{binary(), binary()}], t()) ->
    ok | {error, header_value_too_long}.
validate_header_values(Headers, #{max_header_value_length := MaxLen}) ->
    validate_header_values_loop(Headers, MaxLen).

-spec validate_header_values_loop([{binary(), binary()}], pos_integer()) ->
    ok | {error, header_value_too_long}.
validate_header_values_loop([], _MaxLen) ->
    ok;
validate_header_values_loop([{_Name, Value} | Rest], MaxLen) ->
    case byte_size(Value) =< MaxLen of
        true -> validate_header_values_loop(Rest, MaxLen);
        false -> {error, header_value_too_long}
    end.

-spec validate_total_header_size([{binary(), binary()}], t()) ->
    ok | {error, header_too_large}.
validate_total_header_size(Headers, #{max_header_size := MaxSize}) ->
    TotalSize = lists:foldl(
        fun({Name, Value}, Acc) ->
            Acc + byte_size(Name) + byte_size(Value) + 4
        end,
        0,
        Headers
    ),
    case TotalSize =< MaxSize of
        true -> ok;
        false -> {error, header_too_large}
    end.

-spec validate_uri_length(binary(), t()) -> ok | {error, uri_too_long}.
validate_uri_length(Path, #{max_uri_length := MaxLen}) ->
    case byte_size(Path) =< MaxLen of
        true -> ok;
        false -> {error, uri_too_long}
    end.
