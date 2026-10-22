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

-type header_failure() :: header_name_too_long | header_value_too_long.

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
Extract the request limits from a connection's `nhttp:opts()` map.
The result holds every key of `default_limits/0`, with the value of `Opts`
where it sets one. Unknown keys are dropped. The result is ready to pass to
`validate_request/2` and `check_body_size/2`.
""".
-spec from_opts(nhttp:opts()) -> t().
from_opts(Opts) ->
    maps:merge(
        default_limits(),
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
        )
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
A limit missing from `Limits` takes its value from `default_limits/0`.
""".
-spec validate_request(nhttp_lib:request(), t()) -> ok | {error, limit_error()}.
validate_request(
    #{path := Path, headers := Headers},
    #{
        max_uri_length := MaxUri,
        max_headers := MaxCount,
        max_header_name_length := MaxName,
        max_header_value_length := MaxValue,
        max_header_size := MaxSize
    }
) ->
    case byte_size(Path) =< MaxUri of
        true -> validate_headers(Headers, MaxCount, MaxName, MaxValue, MaxSize, 0, 0, ok);
        false -> {error, uri_too_long}
    end;
validate_request(Request, Limits) ->
    validate_request(Request, maps:merge(default_limits(), Limits)).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec header_failure(ok | header_failure(), boolean(), boolean()) -> ok | header_failure().
header_failure(header_name_too_long, _NameTooLong, _ValueTooLong) -> header_name_too_long;
header_failure(_Failure, true, _ValueTooLong) -> header_name_too_long;
header_failure(ok, false, true) -> header_value_too_long;
header_failure(Failure, false, _ValueTooLong) -> Failure.

-doc """
One walk over the headers. The failures keep the order of the checks:
the count, then any name, then any value, then the total size. The count
fails as soon as the walk passes it. A name or value failure waits for the
end of the walk, because a later header can still exceed the count.
""".
-spec validate_headers(
    [{binary(), binary()}],
    pos_integer(),
    pos_integer(),
    pos_integer(),
    pos_integer(),
    non_neg_integer(),
    non_neg_integer(),
    ok | header_failure()
) -> ok | {error, too_many_headers | header_failure() | header_too_large}.
validate_headers([], _MaxCount, _MaxName, _MaxValue, MaxSize, _Count, Size, ok) when
    Size > MaxSize
->
    {error, header_too_large};
validate_headers([], _MaxCount, _MaxName, _MaxValue, _MaxSize, _Count, _Size, ok) ->
    ok;
validate_headers([], _MaxCount, _MaxName, _MaxValue, _MaxSize, _Count, _Size, Failure) ->
    {error, Failure};
validate_headers([_ | _], MaxCount, _MaxName, _MaxValue, _MaxSize, Count, _Size, _Failure) when
    Count >= MaxCount
->
    {error, too_many_headers};
validate_headers(
    [{Name, Value} | Rest], MaxCount, MaxName, MaxValue, MaxSize, Count, Size, Failure
) ->
    NameSize = byte_size(Name),
    ValueSize = byte_size(Value),
    validate_headers(
        Rest,
        MaxCount,
        MaxName,
        MaxValue,
        MaxSize,
        Count + 1,
        Size + NameSize + ValueSize + 4,
        header_failure(Failure, NameSize > MaxName, ValueSize > MaxValue)
    ).
