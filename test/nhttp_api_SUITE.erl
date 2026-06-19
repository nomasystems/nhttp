%%%-----------------------------------------------------------------------------
%%% @doc Coverage suite for the public top-level wrappers in nhttp.erl.
%%%
%%% Each test exercises one of the thin delegates so the wrapper module gets
%%% line coverage end-to-end. Functional behaviour is asserted only at the
%%% level the wrapper is responsible for.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_api_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    active_connections/1,
    active_requests/1,
    active_ws_sessions/1,
    child_spec_unnamed/1,
    child_spec_named/1,
    drain_default_timeout/1,
    header_lookup/1,
    header_lookup_with_default/1,
    start_link_named_local/1,
    start_link_named_global/1,
    start_link_named_via/1,
    start_link_named_atom/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(_Req, State) ->
    {reply, nhttp_resp:ok(<<"hello">>), State}.

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, stats},
        {group, child_spec},
        {group, drain},
        {group, header},
        {group, start_link_named}
    ].

groups() ->
    [
        {stats, [sequence], [
            active_connections,
            active_requests,
            active_ws_sessions
        ]},
        {child_spec, [sequence], [
            child_spec_unnamed,
            child_spec_named
        ]},
        {drain, [sequence], [
            drain_default_timeout
        ]},
        {header, [sequence], [
            header_lookup,
            header_lookup_with_default
        ]},
        {start_link_named, [sequence], [
            start_link_named_local,
            start_link_named_global,
            start_link_named_via,
            start_link_named_atom
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(nhttp),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% STATS
%%%-----------------------------------------------------------------------------

active_connections(_Config) ->
    Initial = nhttp:active_connections(),
    ?assert(is_integer(Initial)),
    ?assert(Initial >= 0),

    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp:active_connections() > Initial end, 2000
    ),

    gen_tcp:close(Sock),
    ok = nhttp_test_helpers:wait_until(
        fun() -> nhttp:active_connections() =:= Initial end, 2000
    ),
    nhttp:stop(Pid),
    ok.

active_requests(_Config) ->
    Value = nhttp:active_requests(),
    ?assert(is_integer(Value)),
    ?assert(Value >= 0),
    ok.

active_ws_sessions(_Config) ->
    Value = nhttp:active_ws_sessions(),
    ?assert(is_integer(Value)),
    ?assert(Value >= 0),
    ok.

%%%-----------------------------------------------------------------------------
%%% CHILD SPECS
%%%-----------------------------------------------------------------------------

child_spec_unnamed(_Config) ->
    Opts = #{port => 0, handler => ?MODULE},
    Spec = nhttp:child_spec(Opts),
    ?assertMatch(#{id := {nhttp_listener, _}}, Spec),
    ?assertMatch(#{start := {nhttp_listener, start_link, [Opts]}}, Spec),
    ?assertMatch(#{type := supervisor}, Spec),
    ?assertMatch(#{restart := permanent}, Spec),
    ?assertMatch(#{shutdown := infinity}, Spec),
    ok.

child_spec_named(_Config) ->
    Name = api_named_spec,
    Opts = #{port => 0, handler => ?MODULE},
    Spec = nhttp:child_spec(Name, Opts),
    ?assertMatch(#{id := {nhttp_listener, Name}}, Spec),
    ?assertMatch(#{start := {nhttp_listener, start_link, [Name, Opts]}}, Spec),
    ?assertMatch(#{type := supervisor}, Spec),
    ok.

%%%-----------------------------------------------------------------------------
%%% DRAIN
%%%-----------------------------------------------------------------------------

drain_default_timeout(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1]
    }),
    {ok, _Port} = nhttp:get_port(Pid),

    ok = nhttp:drain(Pid),

    ?assertEqual({error, {server, no_acceptors}}, nhttp:get_port(Pid)),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HEADER LOOKUP
%%%-----------------------------------------------------------------------------

header_lookup(_Config) ->
    Req = #{headers => [{<<"host">>, <<"example.com">>}]},
    ?assertEqual(<<"example.com">>, nhttp:header(<<"host">>, Req)),
    ?assertEqual(<<"example.com">>, nhttp:header(<<"Host">>, Req)),
    ?assertEqual(undefined, nhttp:header(<<"missing">>, Req)),
    ok.

header_lookup_with_default(_Config) ->
    Req = #{headers => [{<<"x-trace">>, <<"abc">>}]},
    ?assertEqual(<<"abc">>, nhttp:header(<<"x-trace">>, Req, <<"fallback">>)),
    ?assertEqual(<<"fallback">>, nhttp:header(<<"missing">>, Req, <<"fallback">>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% start_link/2 NAMED FORMS
%%%-----------------------------------------------------------------------------

start_link_named_local(_Config) ->
    Name = {local, nhttp_api_local},
    {ok, Pid} = nhttp:start_link(Name, #{port => 0, handler => ?MODULE}),
    ?assertEqual(Pid, whereis(nhttp_api_local)),
    nhttp:stop(Pid),
    ok.

start_link_named_global(_Config) ->
    Name = {global, nhttp_api_global},
    {ok, Pid} = nhttp:start_link(Name, #{port => 0, handler => ?MODULE}),
    ?assertEqual(Pid, global:whereis_name(nhttp_api_global)),
    nhttp:stop(Pid),
    ok.

start_link_named_via(_Config) ->
    Name = {via, global, nhttp_api_via},
    {ok, Pid} = nhttp:start_link(Name, #{port => 0, handler => ?MODULE}),
    ?assertEqual(Pid, global:whereis_name(nhttp_api_via)),
    nhttp:stop(Pid),
    ok.

start_link_named_atom(_Config) ->
    Name = nhttp_api_atom,
    {ok, Pid} = nhttp:start_link(Name, #{port => 0, handler => ?MODULE}),
    ?assertEqual(Pid, whereis(nhttp_api_atom)),
    nhttp:stop(Pid),
    ok.
