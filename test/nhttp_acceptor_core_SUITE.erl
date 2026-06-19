%%%-----------------------------------------------------------------------------
%%% @doc Integration tests for nhttp_acceptor_core's accept-error handling.
%%%
%%% Drives the proc_lib accept loop with a scripted mock transport
%%% (`nhttp_acceptor_core_mock`) that returns errors on demand, and
%%% asserts the loop backs off (no busy loop), throttles its log output,
%%% and resets the backoff after a successful accept.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_acceptor_core_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([log/2]).

-export([
    backoff_throttles_persistent_error/1,
    backoff_resets_after_success/1,
    responsive_during_backoff/1
]).

-define(HANDLER, nhttp_acceptor_core_test_handler).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------
all() ->
    [{group, accept_errors}].

groups() ->
    [
        {accept_errors, [sequence], [
            backoff_throttles_persistent_error,
            backoff_resets_after_success,
            responsive_during_backoff
        ]}
    ].

init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    PrimaryLevel = logger:get_primary_config(),
    ok = logger:set_primary_config(level, all),
    ok = logger:add_handler(?HANDLER, ?MODULE, #{
        config => #{recipient => self()},
        level => all
    }),
    RegTab = nhttp_registry:new(),
    Dummy = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    Counter = nhttp_listener_counter:new(infinity),
    ok = nhttp_registry:register_conn_sup(RegTab, Dummy),
    ok = nhttp_registry:register_conn_tracker(RegTab, Dummy),
    ok = nhttp_registry:register_counter(RegTab, Counter),
    ok = nhttp_registry:register_name(RegTab, mock_listener),
    MockTab = ets:new(mock_ctl, [public, set]),
    true = ets:insert(MockTab, {calls, 0}),
    true = ets:insert(MockTab, {recipient, self()}),
    [
        {prev_primary, PrimaryLevel},
        {reg_tab, RegTab},
        {mock_tab, MockTab},
        {dummy, Dummy}
        | Config
    ].

end_per_testcase(_Case, Config) ->
    _ = logger:remove_handler(?HANDLER),
    Dummy = ?config(dummy, Config),
    Dummy ! stop,
    case ?config(prev_primary, Config) of
        #{level := Level} -> ok = logger:set_primary_config(level, Level);
        _ -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% LOGGER HANDLER CALLBACK
%%%-----------------------------------------------------------------------------
log(#{msg := {report, #{event := accept_error} = Report}}, #{config := #{recipient := Pid}}) ->
    Pid ! {accept_error_logged, Report},
    ok;
log(_, _) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------
backoff_throttles_persistent_error(Config) ->
    Pid = start_acceptor(Config, persistent_error),
    timer:sleep(2500),
    stop_acceptor(Pid),
    Calls = drain_calls(),
    Logs = drain_logs(),
    ?assert(length(Calls) >= 3),
    ?assert(length(Calls) =< 12),
    ?assertEqual(1, length(Logs)).

backoff_resets_after_success(Config) ->
    Pid = start_acceptor(Config, recover),
    Calls = collect_until(6, 5000),
    stop_acceptor(Pid),
    GapBeforeReset = gap(Calls, 3, 4),
    GapAfterReset = gap(Calls, 5, 6),
    ?assert(GapAfterReset < GapBeforeReset).

responsive_during_backoff(Config) ->
    Pid = start_acceptor(Config, persistent_error),
    timer:sleep(1500),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual({ok, 0}, nhttp_acceptor_core:get_listen_port(Pid)),
    GetPortElapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(GetPortElapsed < 500),
    ?assertMatch(Tuple when is_tuple(Tuple), sys:get_state(Pid, 1000)),
    stop_acceptor(Pid),
    _ = drain_calls(),
    _ = drain_logs(),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------
start_acceptor(Config, Script) ->
    RegTab = ?config(reg_tab, Config),
    MockTab = ?config(mock_tab, Config),
    true = ets:insert(MockTab, {script, Script}),
    Opts = #{actual_port => 0, mock_tab => MockTab},
    {ok, Pid} = nhttp_acceptor_core:start_link(nhttp_acceptor_core_mock, RegTab, Opts),
    Pid.

stop_acceptor(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    ok.

drain_calls() ->
    receive
        {acc_call, N, Mode, T} -> [{N, Mode, T} | drain_calls()]
    after 0 ->
        []
    end.

drain_logs() ->
    receive
        {accept_error_logged, Report} -> [Report | drain_logs()]
    after 0 ->
        []
    end.

collect_until(MaxN, Timeout) ->
    collect_until(MaxN, Timeout, []).

collect_until(MaxN, Timeout, Acc) ->
    receive
        {acc_call, MaxN, Mode, T} ->
            lists:reverse([{MaxN, Mode, T} | Acc]);
        {acc_call, N, Mode, T} ->
            collect_until(MaxN, Timeout, [{N, Mode, T} | Acc])
    after Timeout ->
        error({timeout_collecting_calls, MaxN, lists:reverse(Acc)})
    end.

gap(Calls, FromN, ToN) ->
    {FromN, _, FromT} = lists:keyfind(FromN, 1, Calls),
    {ToN, _, ToT} = lists:keyfind(ToN, 1, Calls),
    ToT - FromT.
