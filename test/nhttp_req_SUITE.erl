-module(nhttp_req_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0
]).

-export([
    accessors_basic/1,
    body_default/1,
    connect_protocol_default/1,
    header_case_insensitive/1,
    header_default/1,
    header_missing/1,
    is_websocket_h1_upgrade/1,
    is_websocket_h1_no_upgrade/1,
    is_websocket_h2_h3_extended_connect/1,
    is_websocket_h2_h3_other_method/1,
    is_websocket_h2_h3_no_protocol/1,
    peer_default/1,
    trailers_default/1,
    version_default/1
]).

all() ->
    [
        accessors_basic,
        body_default,
        connect_protocol_default,
        header_case_insensitive,
        header_default,
        header_missing,
        is_websocket_h1_upgrade,
        is_websocket_h1_no_upgrade,
        is_websocket_h2_h3_extended_connect,
        is_websocket_h2_h3_other_method,
        is_websocket_h2_h3_no_protocol,
        peer_default,
        trailers_default,
        version_default
    ].

accessors_basic(_Config) ->
    Req = #{
        method => get,
        path => <<"/foo">>,
        scheme => https,
        authority => <<"example.com:443">>,
        headers => [{<<"x-test">>, <<"yes">>}],
        body => <<"hello">>,
        version => http2,
        peer => {{127, 0, 0, 1}, 54321},
        trailers => [{<<"x-trace">>, <<"abc">>}]
    },
    ?assertEqual(get, nhttp_req:method(Req)),
    ?assertEqual(<<"/foo">>, nhttp_req:path(Req)),
    ?assertEqual(https, nhttp_req:scheme(Req)),
    ?assertEqual(<<"example.com:443">>, nhttp_req:authority(Req)),
    ?assertEqual([{<<"x-test">>, <<"yes">>}], nhttp_req:headers(Req)),
    ?assertEqual(<<"hello">>, nhttp_req:body(Req)),
    ?assertEqual(http2, nhttp_req:version(Req)),
    ?assertEqual({{127, 0, 0, 1}, 54321}, nhttp_req:peer(Req)),
    ?assertEqual([{<<"x-trace">>, <<"abc">>}], nhttp_req:trailers(Req)),
    ok.

body_default(_Config) ->
    Req = base_request(),
    ?assertEqual(<<>>, nhttp_req:body(Req)),
    Streaming = Req#{body => streaming},
    ?assertEqual(streaming, nhttp_req:body(Streaming)),
    ok.

connect_protocol_default(_Config) ->
    Req = base_request(),
    ?assertEqual(undefined, nhttp_req:connect_protocol(Req)),
    With = Req#{connect_protocol => <<"websocket">>},
    ?assertEqual(<<"websocket">>, nhttp_req:connect_protocol(With)),
    ok.

header_case_insensitive(_Config) ->
    Req = (base_request())#{
        headers => [{<<"content-type">>, <<"application/json">>}]
    },
    ?assertEqual(<<"application/json">>, nhttp_req:header(<<"content-type">>, Req)),
    ?assertEqual(
        <<"application/json">>, nhttp_req:header(<<"Content-Type">>, Req)
    ),
    ok.

header_default(_Config) ->
    Req = base_request(),
    ?assertEqual(<<"fallback">>, nhttp_req:header(<<"x-missing">>, Req, <<"fallback">>)),
    ok.

header_missing(_Config) ->
    Req = base_request(),
    ?assertEqual(undefined, nhttp_req:header(<<"x-missing">>, Req)),
    ok.

is_websocket_h1_upgrade(_Config) ->
    Req = (base_request())#{
        method => get,
        headers => [
            {<<"connection">>, <<"upgrade">>},
            {<<"upgrade">>, <<"websocket">>}
        ]
    },
    ?assert(nhttp_req:is_websocket(Req)),
    Req1 = Req#{headers => [{<<"upgrade">>, <<"WebSocket">>}]},
    ?assert(nhttp_req:is_websocket(Req1)),
    ok.

is_websocket_h1_no_upgrade(_Config) ->
    Req = (base_request())#{method => get, headers => []},
    ?assertNot(nhttp_req:is_websocket(Req)),
    ok.

is_websocket_h2_h3_extended_connect(_Config) ->
    Req = (base_request())#{
        method => connect,
        connect_protocol => <<"websocket">>
    },
    ?assert(nhttp_req:is_websocket(Req)),
    ok.

is_websocket_h2_h3_other_method(_Config) ->
    Req = (base_request())#{method => post},
    ?assertNot(nhttp_req:is_websocket(Req)),
    ok.

is_websocket_h2_h3_no_protocol(_Config) ->
    Req = (base_request())#{method => connect},
    ?assertNot(nhttp_req:is_websocket(Req)),
    ok.

peer_default(_Config) ->
    Req = base_request(),
    ?assertEqual(undefined, nhttp_req:peer(Req)),
    ok.

trailers_default(_Config) ->
    Req = base_request(),
    ?assertEqual([], nhttp_req:trailers(Req)),
    ok.

version_default(_Config) ->
    Req = base_request(),
    ?assertEqual(undefined, nhttp_req:version(Req)),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL
%%%-----------------------------------------------------------------------------

base_request() ->
    #{
        method => get,
        path => <<"/">>,
        scheme => http,
        authority => <<"example.com">>,
        headers => []
    }.
