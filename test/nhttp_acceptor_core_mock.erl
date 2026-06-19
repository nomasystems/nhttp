-module(nhttp_acceptor_core_mock).

-moduledoc false.

-behaviour(nhttp_acceptor_core).

-export([
    do_accept/1,
    init_sub/1,
    reject/1,
    spawn_conn/2
]).

-spec init_sub(nhttp:opts()) -> ets:tid().
init_sub(Opts) ->
    maps:get(mock_tab, Opts).

-spec do_accept(ets:tid()) -> {ok, mock_accepted} | {error, enobufs}.
do_accept(Tab) ->
    N = ets:update_counter(Tab, calls, 1),
    Pid = ets:lookup_element(Tab, recipient, 2),
    Mode = call_mode(ets:lookup_element(Tab, script, 2), N),
    Pid ! {acc_call, N, Mode, erlang:monotonic_time(millisecond)},
    case Mode of
        error -> {error, enobufs};
        ok -> {ok, mock_accepted}
    end.

-spec reject(term()) -> ok.
reject(_Accepted) ->
    ok.

-spec spawn_conn(term(), nhttp_acceptor_core:spawn_ctx()) -> ok.
spawn_conn(_Accepted, _Ctx) ->
    ok.

-spec call_mode(persistent_error | recover, pos_integer()) -> ok | error.
call_mode(persistent_error, _N) ->
    error;
call_mode(recover, 4) ->
    ok;
call_mode(recover, _N) ->
    error.
