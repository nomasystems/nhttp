%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_limits module.
%%%
%%% Tests header validation and size limits.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_limits_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    default_limits/1,
    error_to_status/1,
    validate_ok/1,
    uri_too_long/1,
    uri_at_limit/1,
    header_name_too_long/1,
    header_value_too_long/1,
    too_many_headers/1,
    header_too_large/1,
    custom_limits/1,
    body_size_default/1,
    body_size_under_limit/1,
    body_size_over_limit/1,
    body_size_custom/1,
    first_failure_wins/1,
    from_opts_resolves_defaults/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [{group, unit}].

groups() ->
    [
        {unit, [sequence], [
            default_limits,
            error_to_status,
            validate_ok,
            uri_too_long,
            uri_at_limit,
            header_name_too_long,
            header_value_too_long,
            too_many_headers,
            header_too_large,
            custom_limits,
            body_size_default,
            body_size_under_limit,
            body_size_over_limit,
            body_size_custom,
            first_failure_wins,
            from_opts_resolves_defaults
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

default_limits(_Config) ->
    Limits = nhttp_limits:default_limits(),
    ?assertEqual(256, maps:get(max_header_name_length, Limits)),
    ?assertEqual(8192, maps:get(max_header_value_length, Limits)),
    ?assertEqual(100, maps:get(max_headers, Limits)),
    ?assertEqual(65536, maps:get(max_header_size, Limits)),
    ?assertEqual(8192, maps:get(max_uri_length, Limits)),
    ?assertEqual(8 * 1024 * 1024, maps:get(max_body_size, Limits)).

error_to_status(_Config) ->
    ?assertEqual(413, nhttp_limits:error_to_status(body_too_large)),
    ?assertEqual(431, nhttp_limits:error_to_status(header_name_too_long)),
    ?assertEqual(431, nhttp_limits:error_to_status(header_too_large)),
    ?assertEqual(431, nhttp_limits:error_to_status(header_value_too_long)),
    ?assertEqual(431, nhttp_limits:error_to_status(too_many_headers)),
    ?assertEqual(414, nhttp_limits:error_to_status(uri_too_long)).

validate_ok(_Config) ->
    Req = #{
        method => get,
        path => <<"/api/users">>,
        headers => [
            {<<"host">>, <<"example.com">>},
            {<<"accept">>, <<"application/json">>}
        ]
    },
    ?assertEqual(ok, nhttp_limits:validate_request(Req, #{})).

uri_too_long(_Config) ->
    LongPath = iolist_to_binary([<<"/path/">>, binary:copy(<<"a">>, 10000)]),
    Req = #{
        method => get,
        path => LongPath,
        headers => []
    },
    ?assertEqual({error, uri_too_long}, nhttp_limits:validate_request(Req, #{})).

uri_at_limit(_Config) ->
    ExactPath = iolist_to_binary([<<"/path/">>, binary:copy(<<"a">>, 8192 - 6)]),
    Req = #{
        method => get,
        path => ExactPath,
        headers => []
    },
    ?assertEqual(ok, nhttp_limits:validate_request(Req, #{})).

header_name_too_long(_Config) ->
    LongName = binary:copy(<<"x">>, 300),
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [{LongName, <<"value">>}]
    },
    ?assertEqual({error, header_name_too_long}, nhttp_limits:validate_request(Req, #{})).

header_value_too_long(_Config) ->
    LongValue = binary:copy(<<"x">>, 10000),
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [{<<"x-custom">>, LongValue}]
    },
    ?assertEqual({error, header_value_too_long}, nhttp_limits:validate_request(Req, #{})).

too_many_headers(_Config) ->
    Headers = [
        {iolist_to_binary([<<"x-h">>, integer_to_binary(N)]), <<"v">>}
     || N <- lists:seq(1, 150)
    ],
    Req = #{
        method => get,
        path => <<"/">>,
        headers => Headers
    },
    ?assertEqual({error, too_many_headers}, nhttp_limits:validate_request(Req, #{})).

header_too_large(_Config) ->
    Headers = [
        {iolist_to_binary([<<"x-header-">>, integer_to_binary(N)]), binary:copy(<<"v">>, 1000)}
     || N <- lists:seq(1, 70)
    ],
    Req = #{
        method => get,
        path => <<"/">>,
        headers => Headers
    },
    ?assertEqual({error, header_too_large}, nhttp_limits:validate_request(Req, #{})).

custom_limits(_Config) ->
    Limits = #{
        max_uri_length => 10,
        max_headers => 2,
        max_header_name_length => 5,
        max_header_value_length => 10
    },

    SmallReq = #{
        method => get,
        path => <<"/">>,
        headers => [{<<"a">>, <<"b">>}]
    },
    ?assertEqual(ok, nhttp_limits:validate_request(SmallReq, Limits)),

    LongUriReq = #{
        method => get,
        path => <<"/this/is/too/long">>,
        headers => []
    },
    ?assertEqual({error, uri_too_long}, nhttp_limits:validate_request(LongUriReq, Limits)),

    ManyHeadersReq = #{
        method => get,
        path => <<"/">>,
        headers => [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}]
    },
    ?assertEqual({error, too_many_headers}, nhttp_limits:validate_request(ManyHeadersReq, Limits)).

body_size_default(_Config) ->
    ?assertEqual(8 * 1024 * 1024, nhttp_limits:max_body_size(#{})),
    ?assertEqual(123, nhttp_limits:max_body_size(#{max_body_size => 123})).

body_size_under_limit(_Config) ->
    Limits = #{max_body_size => 1024},
    ?assertEqual(ok, nhttp_limits:check_body_size(0, Limits)),
    ?assertEqual(ok, nhttp_limits:check_body_size(1023, Limits)),
    ?assertEqual(ok, nhttp_limits:check_body_size(1024, Limits)).

body_size_over_limit(_Config) ->
    Limits = #{max_body_size => 1024},
    ?assertEqual({error, body_too_large}, nhttp_limits:check_body_size(1025, Limits)),
    ?assertEqual({error, body_too_large}, nhttp_limits:check_body_size(10000, Limits)).

body_size_custom(_Config) ->
    ?assertEqual(ok, nhttp_limits:check_body_size(8 * 1024 * 1024, #{})),
    ?assertEqual({error, body_too_large}, nhttp_limits:check_body_size(8 * 1024 * 1024 + 1, #{})).

first_failure_wins(_Config) ->
    Limits = #{
        max_uri_length => 4,
        max_headers => 3,
        max_header_name_length => 4,
        max_header_value_length => 4,
        max_header_size => 20
    },
    Cases = [
        {Path, [kind_header(Kind) || Kind <- Kinds]}
     || Path <- [<<"/ok">>, <<"/long">>], Kinds <- kind_lists(4)
    ],
    lists:foreach(
        fun({Path, Headers}) ->
            Req = #{method => get, path => Path, headers => Headers},
            ?assertEqual(
                {Path, Headers, expected_result(Path, Headers, Limits)},
                {Path, Headers, nhttp_limits:validate_request(Req, Limits)}
            )
        end,
        Cases
    ).

from_opts_resolves_defaults(_Config) ->
    Defaults = nhttp_limits:default_limits(),
    ?assertEqual(Defaults, nhttp_limits:from_opts(#{port => 0})),
    ?assertEqual(
        Defaults#{max_headers => 5, max_body_size => 10},
        nhttp_limits:from_opts(#{port => 0, max_headers => 5, max_body_size => 10})
    ).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

expected_result(Path, Headers, Limits) ->
    #{
        max_uri_length := MaxUri,
        max_headers := MaxHeaders,
        max_header_name_length := MaxName,
        max_header_value_length := MaxValue,
        max_header_size := MaxSize
    } = Limits,
    Failures = [
        {byte_size(Path) > MaxUri, uri_too_long},
        {length(Headers) > MaxHeaders, too_many_headers},
        {lists:any(fun({N, _}) -> byte_size(N) > MaxName end, Headers), header_name_too_long},
        {lists:any(fun({_, V}) -> byte_size(V) > MaxValue end, Headers), header_value_too_long},
        {
            lists:sum([byte_size(N) + byte_size(V) + 4 || {N, V} <- Headers]) > MaxSize,
            header_too_large
        }
    ],
    case [Reason || {true, Reason} <- Failures] of
        [] -> ok;
        [First | _] -> {error, First}
    end.

kind_header(ok) -> {<<"ab">>, <<"cd">>};
kind_header(long_name) -> {<<"abcde">>, <<"cd">>};
kind_header(long_value) -> {<<"ab">>, <<"cdefg">>};
kind_header(long_both) -> {<<"abcde">>, <<"cdefg">>}.

kind_lists(0) ->
    [[]];
kind_lists(N) ->
    [
        []
        | [
            [Kind | Rest]
         || Kind <- [ok, long_name, long_value, long_both], Rest <- kind_lists(N - 1)
        ]
    ].
