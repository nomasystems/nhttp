-module(nhttp_cors).

-moduledoc """
Cross-Origin Resource Sharing (CORS) helpers for nhttp.

Provides convenience functions for handling CORS preflight requests
and adding CORS headers to responses.

## Simple CORS

```erlang
handle_request(#{method := options}, State) ->
    {reply, nhttp_cors:preflight(<<"*">>), State};
handle_request(Req, State) ->
    Response = nhttp_resp:ok(Headers, Body),
    CorsHeaders = nhttp_cors:headers(<<"*">>),
    {reply, Response#{headers := CorsHeaders ++ maps:get(headers, Response)}, State}.
```

## Advanced CORS

```erlang
Opts = #{
    methods => [<<"GET">>, <<"POST">>, <<"PUT">>, <<"DELETE">>],
    headers => [<<"content-type">>, <<"authorization">>],
    max_age => 86400,
    credentials => true
},
Response = nhttp_cors:preflight(Origin, Opts).
```
""".

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    headers/1,
    headers/2,
    preflight/1,
    preflight/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([opts/0]).

-type opts() :: #{
    credentials => boolean(),
    expose_headers => [binary()],
    headers => [binary()],
    max_age => non_neg_integer(),
    methods => [binary()]
}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Get CORS headers for the given origin with default options.".
-spec headers(binary()) -> nhttp_lib:headers().
headers(Origin) ->
    headers(Origin, #{}).

-doc """
Get CORS headers with custom options.
Options:
- `methods` - List of allowed methods (default: GET, POST, OPTIONS)
- `headers` - List of allowed request headers (default: content-type)
- `expose_headers` - List of headers to expose to the client
- `max_age` - Preflight cache duration in seconds
- `credentials` - Whether to allow credentials (cookies, auth)
""".
-spec headers(binary(), opts()) -> nhttp_lib:headers().
headers(Origin, Opts) ->
    Methods = maps:get(methods, Opts, [<<"GET">>, <<"POST">>, <<"OPTIONS">>]),
    AllowHeaders = maps:get(headers, Opts, [<<"content-type">>]),
    BaseHeaders = [
        {<<"access-control-allow-origin">>, Origin},
        {<<"access-control-allow-methods">>, join_values(Methods)},
        {<<"access-control-allow-headers">>, join_values(AllowHeaders)}
    ],
    BaseHeaders ++
        maybe_credentials(Opts) ++
        maybe_expose_headers(Opts) ++
        maybe_max_age(Opts).

-doc """
Create a preflight response for the given origin with default options.
Returns a 204 No Content response with appropriate CORS headers.
""".
-spec preflight(binary()) -> nhttp_lib:response().
preflight(Origin) ->
    preflight(Origin, #{}).

-doc "Create a preflight response with custom options.".
-spec preflight(binary(), opts()) -> nhttp_lib:response().
preflight(Origin, Opts) ->
    #{
        status => 204,
        reason => <<"No Content">>,
        headers => headers(Origin, Opts)
    }.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec join_values([binary()]) -> binary().
join_values([]) ->
    <<>>;
join_values(Values) ->
    iolist_to_binary(lists:join(<<", ">>, Values)).

-spec maybe_credentials(opts()) -> nhttp_lib:headers().
maybe_credentials(Opts) ->
    case maps:get(credentials, Opts, false) of
        true ->
            [{<<"access-control-allow-credentials">>, <<"true">>}];
        false ->
            []
    end.

-spec maybe_expose_headers(opts()) -> nhttp_lib:headers().
maybe_expose_headers(Opts) ->
    case maps:get(expose_headers, Opts, undefined) of
        undefined ->
            [];
        [] ->
            [];
        ExposeHeaders ->
            [{<<"access-control-expose-headers">>, join_values(ExposeHeaders)}]
    end.

-spec maybe_max_age(opts()) -> nhttp_lib:headers().
maybe_max_age(Opts) ->
    case maps:get(max_age, Opts, undefined) of
        undefined ->
            [];
        MaxAge when is_integer(MaxAge) ->
            [{<<"access-control-max-age">>, integer_to_binary(MaxAge)}]
    end.
