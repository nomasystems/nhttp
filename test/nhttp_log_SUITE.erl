%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_log.
%%%
%%% Each test installs a transient logger handler that forwards captured
%%% reports to the test process, invokes one nhttp_log:* helper, and
%%% verifies that the resulting log report carries the expected event tag,
%%% the connection-context fields it was given, and the event-specific
%%% fields. Stacktraces are NOT captured server-side, so we also assert
%%% the `stacktrace` key is absent from the handler-crash event.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_log_SUITE).

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
    accept_error/1,
    handler_crashed/1,
    handler_crashed_no_stacktrace/1,
    h1_push_worker_stuck/1,
    request_ctx_enriches/1,
    request_id_unique/1,
    sock_send_failed/1,
    stream_push_producer_crashed/1,
    stream_push_rejected/1,
    ws_send_failed/1
]).

-define(HANDLER, nhttp_log_test_handler).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [{group, unit}].

groups() ->
    [
        {unit, [sequence], [
            accept_error,
            handler_crashed,
            handler_crashed_no_stacktrace,
            h1_push_worker_stuck,
            request_ctx_enriches,
            request_id_unique,
            sock_send_failed,
            stream_push_producer_crashed,
            stream_push_rejected,
            ws_send_failed
        ]}
    ].

init_per_testcase(_Case, Config) ->
    PrimaryLevel = logger:get_primary_config(),
    ok = logger:set_primary_config(level, all),
    ok = logger:add_handler(?HANDLER, ?MODULE, #{
        config => #{recipient => self()},
        level => all
    }),
    [{prev_primary, PrimaryLevel} | Config].

end_per_testcase(_Case, Config) ->
    _ = logger:remove_handler(?HANDLER),
    case ?config(prev_primary, Config) of
        #{level := Level} -> ok = logger:set_primary_config(level, Level);
        _ -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% LOGGER HANDLER CALLBACK
%%%-----------------------------------------------------------------------------

log(#{level := Level, msg := {report, Report}}, #{config := #{recipient := Pid}}) ->
    Pid ! {nhttp_log_event, Level, Report},
    ok;
log(_, _) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

accept_error(_Config) ->
    Ctx = #{listener_name => api_listener},
    ok = nhttp_log:accept_error(Ctx, enobufs),
    Report = recv_report(warning),
    ?assertEqual(accept_error, maps:get(event, Report)),
    ?assertEqual(enobufs, maps:get(reason, Report)),
    ?assertEqual(api_listener, maps:get(listener_name, Report)).

handler_crashed(_Config) ->
    Ctx = (sample_ctx())#{handler => my_handler},
    ok = nhttp_log:handler_crashed(Ctx, handle_request, error, badarg),
    Report = recv_report(error),
    ?assertEqual(handler_crashed, maps:get(event, Report)),
    ?assertEqual(my_handler, maps:get(handler, Report)),
    ?assertEqual(handle_request, maps:get(callback, Report)),
    ?assertEqual(error, maps:get(class, Report)),
    ?assertEqual(badarg, maps:get(crash_reason, Report)),
    assert_ctx_fields(Report, Ctx).

handler_crashed_no_stacktrace(_Config) ->
    ok = nhttp_log:handler_crashed(sample_ctx(), f, exit, normal),
    Report = recv_report(error),
    ?assertNot(maps:is_key(stacktrace, Report)).

request_ctx_enriches(_Config) ->
    Base = sample_ctx(),
    Request = #{method => get, path => <<"/foo">>},
    Ctx = nhttp_log:request_ctx(Base, Request, 5),
    ?assertEqual(get, maps:get(method, Ctx)),
    ?assertEqual(<<"/foo">>, maps:get(path, Ctx)),
    ?assertEqual(5, maps:get(stream_id, Ctx)),
    ?assert(is_binary(maps:get(request_id, Ctx))),
    Ctx1 = nhttp_log:request_ctx(Base, Request, undefined),
    ?assertNot(maps:is_key(stream_id, Ctx1)),
    Ctx2 = nhttp_log:request_ctx(Base, #{}, undefined),
    ?assertNot(maps:is_key(method, Ctx2)),
    ?assertNot(maps:is_key(path, Ctx2)),
    ?assert(is_binary(maps:get(request_id, Ctx2))).

request_id_unique(_Config) ->
    Id1 = nhttp_log:request_id(),
    Id2 = nhttp_log:request_id(),
    ?assert(is_binary(Id1)),
    ?assertNotEqual(Id1, Id2),
    ?assertEqual(16, byte_size(Id1)).

h1_push_worker_stuck(_Config) ->
    Ctx = sample_ctx(),
    Pid = self(),
    ok = nhttp_log:h1_push_worker_stuck(Ctx, Pid),
    Report = recv_report(warning),
    ?assertEqual(h1_push_worker_stuck, maps:get(event, Report)),
    ?assertEqual(Pid, maps:get(worker_pid, Report)),
    assert_ctx_fields(Report, Ctx).

sock_send_failed(_Config) ->
    Ctx = sample_ctx(),
    ok = nhttp_log:sock_send_failed(Ctx, closed),
    Report = recv_report(debug),
    ?assertEqual(sock_send_failed, maps:get(event, Report)),
    ?assertEqual(closed, maps:get(reason, Report)),
    assert_ctx_fields(Report, Ctx).

stream_push_producer_crashed(_Config) ->
    Ctx = sample_ctx(),
    ok = nhttp_log:stream_push_producer_crashed(Ctx, killed),
    Report = recv_report(warning),
    ?assertEqual(stream_push_producer_crashed, maps:get(event, Report)),
    ?assertEqual(killed, maps:get(reason, Report)),
    assert_ctx_fields(Report, Ctx).

stream_push_rejected(_Config) ->
    Ctx = sample_ctx(),
    ok = nhttp_log:stream_push_rejected(Ctx, get, <<"/csv">>, http1_0_unsupported),
    Report = recv_report(warning),
    ?assertEqual(stream_push_rejected, maps:get(event, Report)),
    ?assertEqual(get, maps:get(method, Report)),
    ?assertEqual(<<"/csv">>, maps:get(path, Report)),
    ?assertEqual(http1_0_unsupported, maps:get(reject_reason, Report)),
    assert_ctx_fields(Report, Ctx).

ws_send_failed(_Config) ->
    Ctx = sample_ctx(),
    ok = nhttp_log:ws_send_failed(Ctx, closed),
    Report = recv_report(debug),
    ?assertEqual(ws_send_failed, maps:get(event, Report)),
    ?assertEqual(closed, maps:get(reason, Report)),
    assert_ctx_fields(Report, Ctx).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

assert_ctx_fields(Report, Ctx) ->
    maps:foreach(
        fun(K, V) -> ?assertEqual(V, maps:get(K, Report)) end,
        Ctx
    ).

recv_report(ExpectedLevel) ->
    receive
        {nhttp_log_event, Level, Report} ->
            ?assertEqual(ExpectedLevel, Level),
            Report
    after 1000 ->
        error(no_log_event_received)
    end.

sample_ctx() ->
    #{
        listener_name => api_listener,
        version => http2,
        peer => {{127, 0, 0, 1}, 54321}
    }.
