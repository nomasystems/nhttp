-module(nhttp_registry_SUITE).

%%%-----------------------------------------------------------------------------
%%% INCLUDES
%%%-----------------------------------------------------------------------------
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% COMMON TEST CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    end_per_testcase/2,
    init_per_testcase/2
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    listener_publishes_children/1,
    listener_teardown_drops_table/1,
    lookup_returns_undefined_when_unregistered/1,
    register_overwrites_previous_value/1,
    table_dies_with_owner/1,
    two_listeners_are_isolated/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).
-export([handle_request/2, init/1]).

init(_) -> {ok, undefined}.
handle_request(_Req, State) -> {reply, nhttp_resp:ok(<<"OK">>), State}.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        listener_publishes_children,
        listener_teardown_drops_table,
        lookup_returns_undefined_when_unregistered,
        register_overwrites_previous_value,
        table_dies_with_owner,
        two_listeners_are_isolated
    ].

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    case erase(listener_pids) of
        undefined ->
            ok;
        Pids when is_list(Pids) ->
            lists:foreach(
                fun(Pid) ->
                    case is_pid(Pid) andalso is_process_alive(Pid) of
                        true -> nhttp:stop(Pid);
                        false -> ok
                    end
                end,
                Pids
            )
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

listener_publishes_children(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pids, [ListenerPid]),
    Tab = listener_table(ListenerPid),
    AccSupPid = nhttp_registry:lookup_acceptor_sup(Tab),
    ConnSupPid = nhttp_registry:lookup_conn_sup(Tab),
    TrackerPid = nhttp_registry:lookup_conn_tracker(Tab),
    Counter = nhttp_registry:lookup_counter(Tab),
    ?assert(is_pid(AccSupPid) andalso is_process_alive(AccSupPid)),
    ?assert(is_pid(ConnSupPid) andalso is_process_alive(ConnSupPid)),
    ?assert(is_pid(TrackerPid) andalso is_process_alive(TrackerPid)),
    ?assertNotEqual(undefined, Counter),
    3 = length(lists:usort([AccSupPid, ConnSupPid, TrackerPid])),
    [TransportSup | _] = nhttp_test_helpers:transport_sups(ListenerPid),
    {_, ListConnSupPid, _, _} =
        lists:keyfind(nhttp_conn_sup, 1, supervisor:which_children(TransportSup)),
    ?assertEqual(ConnSupPid, ListConnSupPid),
    ok.

listener_teardown_drops_table(_Config) ->
    {ok, ListenerPid} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pids, [ListenerPid]),
    Tab = listener_table(ListenerPid),
    ?assertNotEqual(undefined, ets:info(Tab)),
    ok = nhttp:stop(ListenerPid),
    erase(listener_pids),
    ?assertEqual(undefined, ets:info(Tab)),
    ok.

lookup_returns_undefined_when_unregistered(_Config) ->
    Tab = nhttp_registry:new(),
    ?assertEqual(undefined, nhttp_registry:lookup_acceptor_sup(Tab)),
    ?assertEqual(undefined, nhttp_registry:lookup_conn_sup(Tab)),
    ?assertEqual(undefined, nhttp_registry:lookup_conn_tracker(Tab)),
    ?assertEqual(undefined, nhttp_registry:lookup_counter(Tab)),
    ?assertEqual(undefined, nhttp_registry:lookup_name(Tab)),
    true = ets:delete(Tab),
    ok.

register_overwrites_previous_value(_Config) ->
    Tab = nhttp_registry:new(),
    P1 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    P2 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ok = nhttp_registry:register_conn_sup(Tab, P1),
    ?assertEqual(P1, nhttp_registry:lookup_conn_sup(Tab)),
    ok = nhttp_registry:register_conn_sup(Tab, P2),
    ?assertEqual(P2, nhttp_registry:lookup_conn_sup(Tab)),
    P1 ! stop,
    P2 ! stop,
    true = ets:delete(Tab),
    ok.

table_dies_with_owner(_Config) ->
    Parent = self(),
    Owner = spawn(fun() ->
        T = nhttp_registry:new(),
        Parent ! {tab, T},
        receive
            stop -> ok
        end
    end),
    Tab =
        receive
            {tab, T} -> T
        after 1000 ->
            ct:fail(no_table)
        end,
    ?assertNotEqual(undefined, ets:info(Tab)),
    Owner ! stop,
    MRef = erlang:monitor(process, Owner),
    receive
        {'DOWN', MRef, process, Owner, _} -> ok
    after 1000 ->
        ct:fail(owner_didnt_exit)
    end,
    ?assertEqual(undefined, ets:info(Tab)),
    ok.

two_listeners_are_isolated(_Config) ->
    {ok, PidA} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1
    }),
    {ok, PidB} = nhttp:start_link(#{
        port => 0,
        handler => ?MODULE,
        versions => [http1_1],
        acceptor_count => 1
    }),
    put(listener_pids, [PidA, PidB]),
    TabA = listener_table(PidA),
    TabB = listener_table(PidB),
    ?assertNotEqual(TabA, TabB),
    ConnSupA = nhttp_registry:lookup_conn_sup(TabA),
    ConnSupB = nhttp_registry:lookup_conn_sup(TabB),
    TrackerA = nhttp_registry:lookup_conn_tracker(TabA),
    TrackerB = nhttp_registry:lookup_conn_tracker(TabB),
    AccSupA = nhttp_registry:lookup_acceptor_sup(TabA),
    AccSupB = nhttp_registry:lookup_acceptor_sup(TabB),
    CounterA = nhttp_registry:lookup_counter(TabA),
    CounterB = nhttp_registry:lookup_counter(TabB),
    ?assert(is_pid(ConnSupA) andalso is_pid(ConnSupB)),
    ?assert(is_pid(TrackerA) andalso is_pid(TrackerB)),
    ?assert(is_pid(AccSupA) andalso is_pid(AccSupB)),
    ?assertNotEqual(ConnSupA, ConnSupB),
    ?assertNotEqual(TrackerA, TrackerB),
    ?assertNotEqual(AccSupA, AccSupB),
    ?assertNotEqual(CounterA, CounterB),
    ok = nhttp:stop(PidA),
    put(listener_pids, [PidB]),
    ?assertEqual(undefined, ets:info(TabA)),
    ?assertNotEqual(undefined, ets:info(TabB)),
    ?assertEqual(ConnSupB, nhttp_registry:lookup_conn_sup(TabB)),
    ?assertEqual(TrackerB, nhttp_registry:lookup_conn_tracker(TabB)),
    ?assertEqual(AccSupB, nhttp_registry:lookup_acceptor_sup(TabB)),
    ?assertEqual(CounterB, nhttp_registry:lookup_counter(TabB)),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL HELPERS
%%%-----------------------------------------------------------------------------

listener_table(ListenerPid) ->
    [TransportSup | _] = nhttp_test_helpers:transport_sups(ListenerPid),
    {links, Links} = process_info(TransportSup, links),
    Tables = [T || T <- ets:all(), ets:info(T, owner) =:= TransportSup],
    case Tables of
        [Tab] ->
            Tab;
        _ ->
            ct:pal("links=~p tables=~p", [Links, Tables]),
            ct:fail({expected_one_owned_table, Tables})
    end.
