%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_resp module.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_resp_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0
]).

-export([
    ok_no_body/1,
    ok_with_body/1,
    ok_with_headers_and_body/1,
    not_found_response/1,
    not_found_with_body/1,
    bad_request_no_body/1,
    bad_request_with_body/1,
    internal_error_response/1,
    internal_error_with_body/1,
    response_with_status_body/1,
    response_with_status_headers_body/1,
    response_with_status_only/1,
    created_location/1,
    created_location_body/1,
    no_content_response/1,
    moved_permanently_response/1,
    found_response/1,
    see_other_response/1,
    temporary_redirect_response/1,
    permanent_redirect_response/1,
    unauthorized_response/1,
    unauthorized_challenge/1,
    forbidden_response/1,
    forbidden_with_body/1,
    method_not_allowed_response/1,
    conflict_response/1,
    conflict_with_body/1,
    gone_response/1,
    unprocessable_content_response/1,
    unprocessable_content_with_body/1,
    too_many_requests_response/1,
    too_many_requests_with_retry/1,
    not_implemented_response/1,
    service_unavailable_response/1,
    service_unavailable_with_retry/1,
    reason_1xx/1,
    reason_2xx/1,
    reason_3xx/1,
    reason_4xx/1,
    reason_5xx/1,
    reason_unknown/1
]).

%%%-----------------------------------------------------------------------------
%%% SUITE SETUP
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, response_helpers},
        {group, status_2xx},
        {group, status_3xx},
        {group, status_4xx},
        {group, status_5xx},
        {group, reason_phrases}
    ].

groups() ->
    [
        {response_helpers, [parallel], [
            ok_no_body,
            ok_with_body,
            ok_with_headers_and_body,
            not_found_response,
            not_found_with_body,
            bad_request_no_body,
            bad_request_with_body,
            internal_error_response,
            internal_error_with_body,
            response_with_status_body,
            response_with_status_headers_body,
            response_with_status_only
        ]},
        {status_2xx, [parallel], [
            created_location,
            created_location_body,
            no_content_response
        ]},
        {status_3xx, [parallel], [
            moved_permanently_response,
            found_response,
            see_other_response,
            temporary_redirect_response,
            permanent_redirect_response
        ]},
        {status_4xx, [parallel], [
            unauthorized_response,
            unauthorized_challenge,
            forbidden_response,
            forbidden_with_body,
            method_not_allowed_response,
            conflict_response,
            conflict_with_body,
            gone_response,
            unprocessable_content_response,
            unprocessable_content_with_body,
            too_many_requests_response,
            too_many_requests_with_retry
        ]},
        {status_5xx, [parallel], [
            not_implemented_response,
            service_unavailable_response,
            service_unavailable_with_retry
        ]},
        {reason_phrases, [parallel], [
            reason_1xx,
            reason_2xx,
            reason_3xx,
            reason_4xx,
            reason_5xx,
            reason_unknown
        ]}
    ].

%%%-----------------------------------------------------------------------------
%%% RESPONSE HELPER TESTS
%%%-----------------------------------------------------------------------------

ok_no_body(_Config) ->
    Resp = nhttp_resp:ok(),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"OK">>, maps:get(reason, Resp)),
    ?assertEqual(<<>>, maps:get(body, Resp, <<>>)),
    ok.

ok_with_body(_Config) ->
    Resp = nhttp_resp:ok(<<"Hello">>),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"OK">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Hello">>, maps:get(body, Resp, <<>>)),

    Resp2 = nhttp_resp:ok([<<"Hello">>, <<" ">>, <<"World">>]),
    ?assertEqual(<<"Hello World">>, iolist_to_binary(maps:get(body, Resp2, <<>>))),
    ok.

ok_with_headers_and_body(_Config) ->
    Headers = [{<<"content-type">>, <<"text/html">>}],
    Resp = nhttp_resp:ok(Headers, <<"<html></html>">>),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"OK">>, maps:get(reason, Resp)),
    ?assertEqual(Headers, maps:get(headers, Resp)),
    ?assertEqual(<<"<html></html>">>, maps:get(body, Resp, <<>>)),
    ok.

not_found_response(_Config) ->
    Resp = nhttp_resp:not_found(),
    ?assertEqual(404, maps:get(status, Resp)),
    ?assertEqual(<<"Not Found">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Not Found">>, maps:get(body, Resp, <<>>)),
    ok.

not_found_with_body(_Config) ->
    Resp = nhttp_resp:not_found(<<"Resource does not exist">>),
    ?assertEqual(404, maps:get(status, Resp)),
    ?assertEqual(<<"Not Found">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Resource does not exist">>, maps:get(body, Resp, <<>>)),
    ok.

bad_request_no_body(_Config) ->
    Resp = nhttp_resp:bad_request(),
    ?assertEqual(400, maps:get(status, Resp)),
    ?assertEqual(<<"Bad Request">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Bad Request">>, maps:get(body, Resp, <<>>)),
    ok.

bad_request_with_body(_Config) ->
    Resp = nhttp_resp:bad_request(<<"Invalid JSON">>),
    ?assertEqual(400, maps:get(status, Resp)),
    ?assertEqual(<<"Bad Request">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Invalid JSON">>, maps:get(body, Resp, <<>>)),
    ok.

internal_error_response(_Config) ->
    Resp = nhttp_resp:internal_error(),
    ?assertEqual(500, maps:get(status, Resp)),
    ?assertEqual(<<"Internal Server Error">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Internal Server Error">>, maps:get(body, Resp, <<>>)),
    ok.

internal_error_with_body(_Config) ->
    Resp = nhttp_resp:internal_error(<<"Database connection failed">>),
    ?assertEqual(500, maps:get(status, Resp)),
    ?assertEqual(<<"Internal Server Error">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Database connection failed">>, maps:get(body, Resp, <<>>)),
    ok.

response_with_status_body(_Config) ->
    Resp = nhttp_resp:new(201, <<"Created resource">>),
    ?assertEqual(201, maps:get(status, Resp)),
    ?assertEqual(<<"Created">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Created resource">>, maps:get(body, Resp, <<>>)),
    ok.

response_with_status_headers_body(_Config) ->
    Headers = [{<<"location">>, <<"/users/123">>}],
    Resp = nhttp_resp:new(301, Headers, <<"Moved">>),
    ?assertEqual(301, maps:get(status, Resp)),
    ?assertEqual(<<"Moved Permanently">>, maps:get(reason, Resp)),
    ?assertEqual(Headers, maps:get(headers, Resp)),
    ?assertEqual(<<"Moved">>, maps:get(body, Resp, <<>>)),
    ok.

response_with_status_only(_Config) ->
    Resp = nhttp_resp:new(204),
    ?assertEqual(204, maps:get(status, Resp)),
    ?assertEqual(<<"No Content">>, maps:get(reason, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% 2XX RESPONSE TESTS
%%%-----------------------------------------------------------------------------

created_location(_Config) ->
    Resp = nhttp_resp:created(<<"/resources/123">>),
    ?assertEqual(201, maps:get(status, Resp)),
    ?assertEqual(<<"Created">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"/resources/123">>}], maps:get(headers, Resp)),
    ok.

created_location_body(_Config) ->
    Resp = nhttp_resp:created(<<"/resources/456">>, <<"Resource created successfully">>),
    ?assertEqual(201, maps:get(status, Resp)),
    ?assertEqual(<<"Created">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"/resources/456">>}], maps:get(headers, Resp)),
    ?assertEqual(<<"Resource created successfully">>, maps:get(body, Resp, <<>>)),
    ok.

no_content_response(_Config) ->
    Resp = nhttp_resp:no_content(),
    ?assertEqual(204, maps:get(status, Resp)),
    ?assertEqual(<<"No Content">>, maps:get(reason, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% 3XX REDIRECT TESTS
%%%-----------------------------------------------------------------------------

moved_permanently_response(_Config) ->
    Resp = nhttp_resp:moved_permanently(<<"https://example.com/new">>),
    ?assertEqual(301, maps:get(status, Resp)),
    ?assertEqual(<<"Moved Permanently">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"https://example.com/new">>}], maps:get(headers, Resp)),
    ok.

found_response(_Config) ->
    Resp = nhttp_resp:found(<<"https://example.com/temp">>),
    ?assertEqual(302, maps:get(status, Resp)),
    ?assertEqual(<<"Found">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"https://example.com/temp">>}], maps:get(headers, Resp)),
    ok.

see_other_response(_Config) ->
    Resp = nhttp_resp:see_other(<<"/results/123">>),
    ?assertEqual(303, maps:get(status, Resp)),
    ?assertEqual(<<"See Other">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"/results/123">>}], maps:get(headers, Resp)),
    ok.

temporary_redirect_response(_Config) ->
    Resp = nhttp_resp:temporary_redirect(<<"/temp/path">>),
    ?assertEqual(307, maps:get(status, Resp)),
    ?assertEqual(<<"Temporary Redirect">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"/temp/path">>}], maps:get(headers, Resp)),
    ok.

permanent_redirect_response(_Config) ->
    Resp = nhttp_resp:permanent_redirect(<<"/perm/path">>),
    ?assertEqual(308, maps:get(status, Resp)),
    ?assertEqual(<<"Permanent Redirect">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"location">>, <<"/perm/path">>}], maps:get(headers, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% 4XX CLIENT ERROR TESTS
%%%-----------------------------------------------------------------------------

unauthorized_response(_Config) ->
    Resp = nhttp_resp:unauthorized(),
    ?assertEqual(401, maps:get(status, Resp)),
    ?assertEqual(<<"Unauthorized">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Unauthorized">>, maps:get(body, Resp, <<>>)),
    ok.

unauthorized_challenge(_Config) ->
    Resp = nhttp_resp:unauthorized(<<"Bearer realm=\"api\"">>),
    ?assertEqual(401, maps:get(status, Resp)),
    ?assertEqual(<<"Unauthorized">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"www-authenticate">>, <<"Bearer realm=\"api\"">>}], maps:get(headers, Resp)),
    ?assertEqual(<<"Unauthorized">>, maps:get(body, Resp, <<>>)),
    ok.

forbidden_response(_Config) ->
    Resp = nhttp_resp:forbidden(),
    ?assertEqual(403, maps:get(status, Resp)),
    ?assertEqual(<<"Forbidden">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Forbidden">>, maps:get(body, Resp, <<>>)),
    ok.

forbidden_with_body(_Config) ->
    Resp = nhttp_resp:forbidden(<<"Access denied to this resource">>),
    ?assertEqual(403, maps:get(status, Resp)),
    ?assertEqual(<<"Forbidden">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Access denied to this resource">>, maps:get(body, Resp, <<>>)),
    ok.

method_not_allowed_response(_Config) ->
    Resp = nhttp_resp:method_not_allowed([<<"GET">>, <<"POST">>]),
    ?assertEqual(405, maps:get(status, Resp)),
    ?assertEqual(<<"Method Not Allowed">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"allow">>, <<"GET, POST">>}], maps:get(headers, Resp)),
    ?assertEqual(<<"Method Not Allowed">>, maps:get(body, Resp, <<>>)),
    ok.

conflict_response(_Config) ->
    Resp = nhttp_resp:conflict(),
    ?assertEqual(409, maps:get(status, Resp)),
    ?assertEqual(<<"Conflict">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Conflict">>, maps:get(body, Resp, <<>>)),
    ok.

conflict_with_body(_Config) ->
    Resp = nhttp_resp:conflict(<<"Resource already exists">>),
    ?assertEqual(409, maps:get(status, Resp)),
    ?assertEqual(<<"Conflict">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Resource already exists">>, maps:get(body, Resp, <<>>)),
    ok.

gone_response(_Config) ->
    Resp = nhttp_resp:gone(),
    ?assertEqual(410, maps:get(status, Resp)),
    ?assertEqual(<<"Gone">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Gone">>, maps:get(body, Resp, <<>>)),
    ok.

unprocessable_content_response(_Config) ->
    Resp = nhttp_resp:unprocessable_content(),
    ?assertEqual(422, maps:get(status, Resp)),
    ?assertEqual(<<"Unprocessable Content">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Unprocessable Content">>, maps:get(body, Resp, <<>>)),
    ok.

unprocessable_content_with_body(_Config) ->
    Resp = nhttp_resp:unprocessable_content(<<"Validation failed">>),
    ?assertEqual(422, maps:get(status, Resp)),
    ?assertEqual(<<"Unprocessable Content">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Validation failed">>, maps:get(body, Resp, <<>>)),
    ok.

too_many_requests_response(_Config) ->
    Resp = nhttp_resp:too_many_requests(),
    ?assertEqual(429, maps:get(status, Resp)),
    ?assertEqual(<<"Too Many Requests">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Too Many Requests">>, maps:get(body, Resp, <<>>)),
    ok.

too_many_requests_with_retry(_Config) ->
    Resp = nhttp_resp:too_many_requests(60),
    ?assertEqual(429, maps:get(status, Resp)),
    ?assertEqual(<<"Too Many Requests">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"retry-after">>, <<"60">>}], maps:get(headers, Resp)),
    ?assertEqual(<<"Too Many Requests">>, maps:get(body, Resp, <<>>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% 5XX SERVER ERROR TESTS
%%%-----------------------------------------------------------------------------

not_implemented_response(_Config) ->
    Resp = nhttp_resp:not_implemented(),
    ?assertEqual(501, maps:get(status, Resp)),
    ?assertEqual(<<"Not Implemented">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Not Implemented">>, maps:get(body, Resp, <<>>)),
    ok.

service_unavailable_response(_Config) ->
    Resp = nhttp_resp:service_unavailable(),
    ?assertEqual(503, maps:get(status, Resp)),
    ?assertEqual(<<"Service Unavailable">>, maps:get(reason, Resp)),
    ?assertEqual(<<"Service Unavailable">>, maps:get(body, Resp, <<>>)),
    ok.

service_unavailable_with_retry(_Config) ->
    Resp = nhttp_resp:service_unavailable(300),
    ?assertEqual(503, maps:get(status, Resp)),
    ?assertEqual(<<"Service Unavailable">>, maps:get(reason, Resp)),
    ?assertEqual([{<<"retry-after">>, <<"300">>}], maps:get(headers, Resp)),
    ?assertEqual(<<"Service Unavailable">>, maps:get(body, Resp, <<>>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% REASON PHRASE TESTS
%%%-----------------------------------------------------------------------------

reason_1xx(_Config) ->
    Resp100 = nhttp_resp:new(100, <<>>),
    ?assertEqual(<<"Continue">>, maps:get(reason, Resp100)),

    Resp101 = nhttp_resp:new(101, <<>>),
    ?assertEqual(<<"Switching Protocols">>, maps:get(reason, Resp101)),
    ok.

reason_2xx(_Config) ->
    Resp200 = nhttp_resp:new(200, <<>>),
    ?assertEqual(<<"OK">>, maps:get(reason, Resp200)),

    Resp201 = nhttp_resp:new(201, <<>>),
    ?assertEqual(<<"Created">>, maps:get(reason, Resp201)),

    Resp202 = nhttp_resp:new(202, <<>>),
    ?assertEqual(<<"Accepted">>, maps:get(reason, Resp202)),

    Resp204 = nhttp_resp:new(204, <<>>),
    ?assertEqual(<<"No Content">>, maps:get(reason, Resp204)),

    Resp206 = nhttp_resp:new(206, <<>>),
    ?assertEqual(<<"Partial Content">>, maps:get(reason, Resp206)),
    ok.

reason_3xx(_Config) ->
    Resp301 = nhttp_resp:new(301, <<>>),
    ?assertEqual(<<"Moved Permanently">>, maps:get(reason, Resp301)),

    Resp302 = nhttp_resp:new(302, <<>>),
    ?assertEqual(<<"Found">>, maps:get(reason, Resp302)),

    Resp303 = nhttp_resp:new(303, <<>>),
    ?assertEqual(<<"See Other">>, maps:get(reason, Resp303)),

    Resp304 = nhttp_resp:new(304, <<>>),
    ?assertEqual(<<"Not Modified">>, maps:get(reason, Resp304)),

    Resp307 = nhttp_resp:new(307, <<>>),
    ?assertEqual(<<"Temporary Redirect">>, maps:get(reason, Resp307)),

    Resp308 = nhttp_resp:new(308, <<>>),
    ?assertEqual(<<"Permanent Redirect">>, maps:get(reason, Resp308)),
    ok.

reason_4xx(_Config) ->
    Resp400 = nhttp_resp:new(400, <<>>),
    ?assertEqual(<<"Bad Request">>, maps:get(reason, Resp400)),

    Resp401 = nhttp_resp:new(401, <<>>),
    ?assertEqual(<<"Unauthorized">>, maps:get(reason, Resp401)),

    Resp403 = nhttp_resp:new(403, <<>>),
    ?assertEqual(<<"Forbidden">>, maps:get(reason, Resp403)),

    Resp404 = nhttp_resp:new(404, <<>>),
    ?assertEqual(<<"Not Found">>, maps:get(reason, Resp404)),

    Resp405 = nhttp_resp:new(405, <<>>),
    ?assertEqual(<<"Method Not Allowed">>, maps:get(reason, Resp405)),

    Resp408 = nhttp_resp:new(408, <<>>),
    ?assertEqual(<<"Request Timeout">>, maps:get(reason, Resp408)),

    Resp409 = nhttp_resp:new(409, <<>>),
    ?assertEqual(<<"Conflict">>, maps:get(reason, Resp409)),

    Resp410 = nhttp_resp:new(410, <<>>),
    ?assertEqual(<<"Gone">>, maps:get(reason, Resp410)),

    Resp411 = nhttp_resp:new(411, <<>>),
    ?assertEqual(<<"Length Required">>, maps:get(reason, Resp411)),

    Resp413 = nhttp_resp:new(413, <<>>),
    ?assertEqual(<<"Content Too Large">>, maps:get(reason, Resp413)),

    Resp414 = nhttp_resp:new(414, <<>>),
    ?assertEqual(<<"URI Too Long">>, maps:get(reason, Resp414)),

    Resp415 = nhttp_resp:new(415, <<>>),
    ?assertEqual(<<"Unsupported Media Type">>, maps:get(reason, Resp415)),

    Resp416 = nhttp_resp:new(416, <<>>),
    ?assertEqual(<<"Range Not Satisfiable">>, maps:get(reason, Resp416)),

    Resp417 = nhttp_resp:new(417, <<>>),
    ?assertEqual(<<"Expectation Failed">>, maps:get(reason, Resp417)),

    Resp422 = nhttp_resp:new(422, <<>>),
    ?assertEqual(<<"Unprocessable Content">>, maps:get(reason, Resp422)),

    Resp426 = nhttp_resp:new(426, <<>>),
    ?assertEqual(<<"Upgrade Required">>, maps:get(reason, Resp426)),

    Resp429 = nhttp_resp:new(429, <<>>),
    ?assertEqual(<<"Too Many Requests">>, maps:get(reason, Resp429)),
    ok.

reason_5xx(_Config) ->
    Resp500 = nhttp_resp:new(500, <<>>),
    ?assertEqual(<<"Internal Server Error">>, maps:get(reason, Resp500)),

    Resp501 = nhttp_resp:new(501, <<>>),
    ?assertEqual(<<"Not Implemented">>, maps:get(reason, Resp501)),

    Resp502 = nhttp_resp:new(502, <<>>),
    ?assertEqual(<<"Bad Gateway">>, maps:get(reason, Resp502)),

    Resp503 = nhttp_resp:new(503, <<>>),
    ?assertEqual(<<"Service Unavailable">>, maps:get(reason, Resp503)),

    Resp504 = nhttp_resp:new(504, <<>>),
    ?assertEqual(<<"Gateway Timeout">>, maps:get(reason, Resp504)),

    Resp505 = nhttp_resp:new(505, <<>>),
    ?assertEqual(<<"HTTP Version Not Supported">>, maps:get(reason, Resp505)),
    ok.

reason_unknown(_Config) ->
    Resp199 = nhttp_resp:new(199, <<>>),
    ?assertEqual(<<>>, maps:get(reason, Resp199)),

    Resp299 = nhttp_resp:new(299, <<>>),
    ?assertEqual(<<>>, maps:get(reason, Resp299)),

    Resp599 = nhttp_resp:new(599, <<>>),
    ?assertEqual(<<>>, maps:get(reason, Resp599)),
    ok.
