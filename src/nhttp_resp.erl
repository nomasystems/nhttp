-module(nhttp_resp).

-moduledoc """
HTTP response builders for nhttp.

Provides convenience functions for building HTTP responses with proper
status codes and reason phrases. Responses are `nhttp_lib:response()`
maps.

## Example

```erlang
handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello!">>), State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.
```

For streaming responses, see `nhttp_stream`.
""".

%%%-----------------------------------------------------------------------------
%% 2XX SUCCESS
%%%-----------------------------------------------------------------------------
-export([
    created/1,
    created/2,
    no_content/0,
    ok/0,
    ok/1,
    ok/2
]).

%%%-----------------------------------------------------------------------------
%% 3XX REDIRECTS
%%%-----------------------------------------------------------------------------
-export([
    found/1,
    moved_permanently/1,
    permanent_redirect/1,
    see_other/1,
    temporary_redirect/1
]).

%%%-----------------------------------------------------------------------------
%% 4XX CLIENT ERRORS
%%%-----------------------------------------------------------------------------
-export([
    bad_request/0,
    bad_request/1,
    conflict/0,
    conflict/1,
    forbidden/0,
    forbidden/1,
    gone/0,
    method_not_allowed/1,
    not_found/0,
    not_found/1,
    too_many_requests/0,
    too_many_requests/1,
    unauthorized/0,
    unauthorized/1,
    unprocessable_content/0,
    unprocessable_content/1
]).

%%%-----------------------------------------------------------------------------
%% 5XX SERVER ERRORS
%%%-----------------------------------------------------------------------------
-export([
    internal_error/0,
    internal_error/1,
    not_implemented/0,
    service_unavailable/0,
    service_unavailable/1
]).

%%%-----------------------------------------------------------------------------
%% GENERIC BUILDERS
%%%-----------------------------------------------------------------------------
-export([
    new/1,
    new/2,
    new/3,
    reason_phrase/1
]).

%%%-----------------------------------------------------------------------------
%% 2XX SUCCESS
%%%-----------------------------------------------------------------------------
-doc "Create a 201 Created response with Location header.".
-spec created(binary()) -> nhttp_lib:response().
created(Location) ->
    #{
        status => 201,
        reason => <<"Created">>,
        headers => [{<<"location">>, Location}]
    }.

-doc "Create a 201 Created response with Location header and body.".
-spec created(binary(), iodata()) -> nhttp_lib:response().
created(Location, Body) ->
    #{
        status => 201,
        reason => <<"Created">>,
        headers => [{<<"location">>, Location}],
        body => Body
    }.

-doc "Create a 204 No Content response.".
-spec no_content() -> nhttp_lib:response().
no_content() ->
    #{status => 204, reason => <<"No Content">>, headers => []}.

-doc "Create a 200 OK response with no body.".
-spec ok() -> nhttp_lib:response().
ok() ->
    #{status => 200, reason => <<"OK">>, headers => []}.

-doc "Create a 200 OK response with body.".
-spec ok(iodata()) -> nhttp_lib:response().
ok(Body) ->
    #{status => 200, reason => <<"OK">>, headers => [], body => Body}.

-doc "Create a 200 OK response with headers and body.".
-spec ok(nhttp_lib:headers(), iodata()) -> nhttp_lib:response().
ok(Headers, Body) ->
    #{status => 200, reason => <<"OK">>, headers => Headers, body => Body}.

%%%-----------------------------------------------------------------------------
%% 3XX REDIRECTS
%%%-----------------------------------------------------------------------------
-doc "Create a 302 Found response.".
-spec found(binary()) -> nhttp_lib:response().
found(Location) ->
    #{
        status => 302,
        reason => <<"Found">>,
        headers => [{<<"location">>, Location}]
    }.

-doc "Create a 301 Moved Permanently response.".
-spec moved_permanently(binary()) -> nhttp_lib:response().
moved_permanently(Location) ->
    #{
        status => 301,
        reason => <<"Moved Permanently">>,
        headers => [{<<"location">>, Location}]
    }.

-doc "Create a 308 Permanent Redirect response.".
-spec permanent_redirect(binary()) -> nhttp_lib:response().
permanent_redirect(Location) ->
    #{
        status => 308,
        reason => <<"Permanent Redirect">>,
        headers => [{<<"location">>, Location}]
    }.

-doc "Create a 303 See Other response.".
-spec see_other(binary()) -> nhttp_lib:response().
see_other(Location) ->
    #{
        status => 303,
        reason => <<"See Other">>,
        headers => [{<<"location">>, Location}]
    }.

-doc "Create a 307 Temporary Redirect response.".
-spec temporary_redirect(binary()) -> nhttp_lib:response().
temporary_redirect(Location) ->
    #{
        status => 307,
        reason => <<"Temporary Redirect">>,
        headers => [{<<"location">>, Location}]
    }.

%%%-----------------------------------------------------------------------------
%% 4XX CLIENT ERRORS
%%%-----------------------------------------------------------------------------
-doc "Create a 400 Bad Request response.".
-spec bad_request() -> nhttp_lib:response().
bad_request() ->
    #{
        status => 400,
        reason => <<"Bad Request">>,
        headers => [],
        body => <<"Bad Request">>
    }.

-doc "Create a 400 Bad Request response with body.".
-spec bad_request(iodata()) -> nhttp_lib:response().
bad_request(Body) ->
    #{status => 400, reason => <<"Bad Request">>, headers => [], body => Body}.

-doc "Create a 409 Conflict response.".
-spec conflict() -> nhttp_lib:response().
conflict() ->
    #{
        status => 409,
        reason => <<"Conflict">>,
        headers => [],
        body => <<"Conflict">>
    }.

-doc "Create a 409 Conflict response with body.".
-spec conflict(iodata()) -> nhttp_lib:response().
conflict(Body) ->
    #{status => 409, reason => <<"Conflict">>, headers => [], body => Body}.

-doc "Create a 403 Forbidden response.".
-spec forbidden() -> nhttp_lib:response().
forbidden() ->
    #{
        status => 403,
        reason => <<"Forbidden">>,
        headers => [],
        body => <<"Forbidden">>
    }.

-doc "Create a 403 Forbidden response with body.".
-spec forbidden(iodata()) -> nhttp_lib:response().
forbidden(Body) ->
    #{status => 403, reason => <<"Forbidden">>, headers => [], body => Body}.

-doc "Create a 410 Gone response.".
-spec gone() -> nhttp_lib:response().
gone() ->
    #{status => 410, reason => <<"Gone">>, headers => [], body => <<"Gone">>}.

-doc "Create a 405 Method Not Allowed response with Allow header.".
-spec method_not_allowed([binary()]) -> nhttp_lib:response().
method_not_allowed(AllowedMethods) ->
    Allow = lists:join(<<", ">>, AllowedMethods),
    #{
        status => 405,
        reason => <<"Method Not Allowed">>,
        headers => [{<<"allow">>, iolist_to_binary(Allow)}],
        body => <<"Method Not Allowed">>
    }.

-doc "Create a 404 Not Found response.".
-spec not_found() -> nhttp_lib:response().
not_found() ->
    #{
        status => 404,
        reason => <<"Not Found">>,
        headers => [],
        body => <<"Not Found">>
    }.

-doc "Create a 404 Not Found response with body.".
-spec not_found(iodata()) -> nhttp_lib:response().
not_found(Body) ->
    #{status => 404, reason => <<"Not Found">>, headers => [], body => Body}.

-doc "Create a 429 Too Many Requests response.".
-spec too_many_requests() -> nhttp_lib:response().
too_many_requests() ->
    #{
        status => 429,
        reason => <<"Too Many Requests">>,
        headers => [],
        body => <<"Too Many Requests">>
    }.

-doc "Create a 429 Too Many Requests response with Retry-After header.".
-spec too_many_requests(non_neg_integer()) -> nhttp_lib:response().
too_many_requests(RetryAfterSecs) ->
    #{
        status => 429,
        reason => <<"Too Many Requests">>,
        headers => [{<<"retry-after">>, integer_to_binary(RetryAfterSecs)}],
        body => <<"Too Many Requests">>
    }.

-doc "Create a 401 Unauthorized response.".
-spec unauthorized() -> nhttp_lib:response().
unauthorized() ->
    #{
        status => 401,
        reason => <<"Unauthorized">>,
        headers => [],
        body => <<"Unauthorized">>
    }.

-doc "Create a 401 Unauthorized response with WWW-Authenticate header.".
-spec unauthorized(binary()) -> nhttp_lib:response().
unauthorized(Challenge) ->
    #{
        status => 401,
        reason => <<"Unauthorized">>,
        headers => [{<<"www-authenticate">>, Challenge}],
        body => <<"Unauthorized">>
    }.

-doc "Create a 422 Unprocessable Content response.".
-spec unprocessable_content() -> nhttp_lib:response().
unprocessable_content() ->
    #{
        status => 422,
        reason => <<"Unprocessable Content">>,
        headers => [],
        body => <<"Unprocessable Content">>
    }.

-doc "Create a 422 Unprocessable Content response with body.".
-spec unprocessable_content(iodata()) -> nhttp_lib:response().
unprocessable_content(Body) ->
    #{
        status => 422,
        reason => <<"Unprocessable Content">>,
        headers => [],
        body => Body
    }.

%%%-----------------------------------------------------------------------------
%% 5XX SERVER ERRORS
%%%-----------------------------------------------------------------------------
-doc "Create a 500 Internal Server Error response.".
-spec internal_error() -> nhttp_lib:response().
internal_error() ->
    #{
        status => 500,
        reason => <<"Internal Server Error">>,
        headers => [],
        body => <<"Internal Server Error">>
    }.

-doc "Create a 500 Internal Server Error response with body.".
-spec internal_error(iodata()) -> nhttp_lib:response().
internal_error(Body) ->
    #{
        status => 500,
        reason => <<"Internal Server Error">>,
        headers => [],
        body => Body
    }.

-doc "Create a 501 Not Implemented response.".
-spec not_implemented() -> nhttp_lib:response().
not_implemented() ->
    #{
        status => 501,
        reason => <<"Not Implemented">>,
        headers => [],
        body => <<"Not Implemented">>
    }.

-doc "Create a 503 Service Unavailable response.".
-spec service_unavailable() -> nhttp_lib:response().
service_unavailable() ->
    #{
        status => 503,
        reason => <<"Service Unavailable">>,
        headers => [],
        body => <<"Service Unavailable">>
    }.

-doc "Create a 503 Service Unavailable response with Retry-After header.".
-spec service_unavailable(non_neg_integer()) -> nhttp_lib:response().
service_unavailable(RetryAfterSecs) ->
    #{
        status => 503,
        reason => <<"Service Unavailable">>,
        headers => [{<<"retry-after">>, integer_to_binary(RetryAfterSecs)}],
        body => <<"Service Unavailable">>
    }.

%%%-----------------------------------------------------------------------------
%% GENERIC BUILDERS
%%%-----------------------------------------------------------------------------
-doc "Create a response with status code only.".
-spec new(nhttp_lib:status()) -> nhttp_lib:response().
new(Status) ->
    #{status => Status, reason => reason_phrase(Status), headers => []}.

-doc "Create a response with status code and body.".
-spec new(nhttp_lib:status(), iodata()) -> nhttp_lib:response().
new(Status, Body) ->
    #{
        status => Status,
        reason => reason_phrase(Status),
        headers => [],
        body => Body
    }.

-doc "Create a response with status code, headers, and body.".
-spec new(nhttp_lib:status(), nhttp_lib:headers(), iodata()) -> nhttp_lib:response().
new(Status, Headers, Body) ->
    #{
        status => Status,
        reason => reason_phrase(Status),
        headers => Headers,
        body => Body
    }.

-doc "Get the reason phrase for a status code.".
-spec reason_phrase(nhttp_lib:status()) -> binary().
reason_phrase(100) -> <<"Continue">>;
reason_phrase(101) -> <<"Switching Protocols">>;
reason_phrase(200) -> <<"OK">>;
reason_phrase(201) -> <<"Created">>;
reason_phrase(202) -> <<"Accepted">>;
reason_phrase(204) -> <<"No Content">>;
reason_phrase(206) -> <<"Partial Content">>;
reason_phrase(301) -> <<"Moved Permanently">>;
reason_phrase(302) -> <<"Found">>;
reason_phrase(303) -> <<"See Other">>;
reason_phrase(304) -> <<"Not Modified">>;
reason_phrase(307) -> <<"Temporary Redirect">>;
reason_phrase(308) -> <<"Permanent Redirect">>;
reason_phrase(400) -> <<"Bad Request">>;
reason_phrase(401) -> <<"Unauthorized">>;
reason_phrase(403) -> <<"Forbidden">>;
reason_phrase(404) -> <<"Not Found">>;
reason_phrase(405) -> <<"Method Not Allowed">>;
reason_phrase(408) -> <<"Request Timeout">>;
reason_phrase(409) -> <<"Conflict">>;
reason_phrase(410) -> <<"Gone">>;
reason_phrase(411) -> <<"Length Required">>;
reason_phrase(413) -> <<"Content Too Large">>;
reason_phrase(414) -> <<"URI Too Long">>;
reason_phrase(415) -> <<"Unsupported Media Type">>;
reason_phrase(416) -> <<"Range Not Satisfiable">>;
reason_phrase(417) -> <<"Expectation Failed">>;
reason_phrase(422) -> <<"Unprocessable Content">>;
reason_phrase(426) -> <<"Upgrade Required">>;
reason_phrase(429) -> <<"Too Many Requests">>;
reason_phrase(500) -> <<"Internal Server Error">>;
reason_phrase(501) -> <<"Not Implemented">>;
reason_phrase(502) -> <<"Bad Gateway">>;
reason_phrase(503) -> <<"Service Unavailable">>;
reason_phrase(504) -> <<"Gateway Timeout">>;
reason_phrase(505) -> <<"HTTP Version Not Supported">>;
reason_phrase(_) -> <<>>.
