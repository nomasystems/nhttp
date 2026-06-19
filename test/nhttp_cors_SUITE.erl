%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_cors module.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_cors_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        preflight_default_test,
        preflight_custom_methods_test,
        preflight_with_credentials_test,
        preflight_with_max_age_test,
        preflight_empty_methods_test,
        headers_default_test,
        headers_custom_test,
        headers_expose_test,
        headers_empty_expose_test,
        full_cors_flow_test
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

preflight_default_test(_Config) ->
    Resp = nhttp_cors:preflight(<<"https://example.com">>),
    ?assertEqual(204, maps:get(status, Resp)),
    Headers = maps:get(headers, Resp),
    ?assertEqual(<<"https://example.com">>, get_header(<<"access-control-allow-origin">>, Headers)),
    ?assertEqual(<<"GET, POST, OPTIONS">>, get_header(<<"access-control-allow-methods">>, Headers)),
    ?assertEqual(<<"content-type">>, get_header(<<"access-control-allow-headers">>, Headers)).

preflight_custom_methods_test(_Config) ->
    Opts = #{
        methods => [<<"GET">>, <<"POST">>, <<"PUT">>, <<"DELETE">>],
        headers => [<<"content-type">>, <<"authorization">>]
    },
    Resp = nhttp_cors:preflight(<<"*">>, Opts),
    Headers = maps:get(headers, Resp),
    ?assertEqual(<<"*">>, get_header(<<"access-control-allow-origin">>, Headers)),
    ?assertEqual(
        <<"GET, POST, PUT, DELETE">>,
        get_header(<<"access-control-allow-methods">>, Headers)
    ),
    ?assertEqual(
        <<"content-type, authorization">>,
        get_header(<<"access-control-allow-headers">>, Headers)
    ).

preflight_with_credentials_test(_Config) ->
    Opts = #{credentials => true},
    Resp = nhttp_cors:preflight(<<"https://app.example.com">>, Opts),
    Headers = maps:get(headers, Resp),
    ?assertEqual(<<"true">>, get_header(<<"access-control-allow-credentials">>, Headers)).

preflight_with_max_age_test(_Config) ->
    Opts = #{max_age => 86400},
    Resp = nhttp_cors:preflight(<<"*">>, Opts),
    Headers = maps:get(headers, Resp),
    ?assertEqual(<<"86400">>, get_header(<<"access-control-max-age">>, Headers)).

preflight_empty_methods_test(_Config) ->
    Opts = #{methods => [], headers => []},
    Resp = nhttp_cors:preflight(<<"*">>, Opts),
    Headers = maps:get(headers, Resp),
    ?assertEqual(<<>>, get_header(<<"access-control-allow-methods">>, Headers)),
    ?assertEqual(<<>>, get_header(<<"access-control-allow-headers">>, Headers)).

headers_default_test(_Config) ->
    Headers = nhttp_cors:headers(<<"*">>),
    ?assertEqual(<<"*">>, get_header(<<"access-control-allow-origin">>, Headers)),
    ?assertEqual(<<"GET, POST, OPTIONS">>, get_header(<<"access-control-allow-methods">>, Headers)).

headers_custom_test(_Config) ->
    Opts = #{
        methods => [<<"GET">>],
        headers => [<<"x-custom-header">>],
        max_age => 3600,
        credentials => true
    },
    Headers = nhttp_cors:headers(<<"https://trusted.example.com">>, Opts),
    ?assertEqual(
        <<"https://trusted.example.com">>, get_header(<<"access-control-allow-origin">>, Headers)
    ),
    ?assertEqual(<<"GET">>, get_header(<<"access-control-allow-methods">>, Headers)),
    ?assertEqual(<<"x-custom-header">>, get_header(<<"access-control-allow-headers">>, Headers)),
    ?assertEqual(<<"3600">>, get_header(<<"access-control-max-age">>, Headers)),
    ?assertEqual(<<"true">>, get_header(<<"access-control-allow-credentials">>, Headers)).

headers_expose_test(_Config) ->
    Opts = #{
        expose_headers => [<<"x-request-id">>, <<"x-rate-limit">>]
    },
    Headers = nhttp_cors:headers(<<"*">>, Opts),
    ?assertEqual(
        <<"x-request-id, x-rate-limit">>,
        get_header(<<"access-control-expose-headers">>, Headers)
    ).

headers_empty_expose_test(_Config) ->
    Opts = #{expose_headers => []},
    Headers = nhttp_cors:headers(<<"*">>, Opts),
    ?assertEqual(undefined, get_header(<<"access-control-expose-headers">>, Headers)).

full_cors_flow_test(_Config) ->
    Origin = <<"https://app.example.com">>,
    Opts = #{
        methods => [<<"GET">>, <<"POST">>],
        headers => [<<"content-type">>, <<"authorization">>],
        credentials => true,
        max_age => 600
    },

    Preflight = nhttp_cors:preflight(Origin, Opts),
    ?assertEqual(204, maps:get(status, Preflight)),

    CorsHeaders = nhttp_cors:headers(Origin, Opts),
    ?assert(length(CorsHeaders) >= 3),

    PreflightHeaders = maps:get(headers, Preflight),
    ?assertNotEqual(undefined, get_header(<<"access-control-allow-origin">>, PreflightHeaders)),
    ?assertNotEqual(undefined, get_header(<<"access-control-allow-methods">>, PreflightHeaders)),
    ?assertNotEqual(undefined, get_header(<<"access-control-allow-headers">>, PreflightHeaders)),
    ?assertNotEqual(
        undefined, get_header(<<"access-control-allow-credentials">>, PreflightHeaders)
    ),
    ?assertNotEqual(undefined, get_header(<<"access-control-max-age">>, PreflightHeaders)).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

get_header(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, Value} -> Value;
        false -> undefined
    end.
