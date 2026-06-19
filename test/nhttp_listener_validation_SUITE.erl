%%%-----------------------------------------------------------------------------
%%% @doc Deterministic listener-validation and acceptor lifecycle paths.
%%%
%%% Pure option-validation rejections (`nhttp_listener') and the
%%% acceptor OTP system-message / stop surface (`nhttp_acceptor_core').
%%% No network timing, so these contribute a stable coverage floor.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_listener_validation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    init/1,
    handle_request/2
]).

-export([
    acceptor_sys_lifecycle/1,
    acceptor_sys_terminate/1,
    listener_invalid_tls/1,
    listener_invalid_versions/1,
    listener_listen_failed/1,
    listener_named_start/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        acceptor_sys_lifecycle,
        acceptor_sys_terminate,
        listener_invalid_tls,
        listener_invalid_versions,
        listener_listen_failed,
        listener_named_start
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HANDLER (minimal, for the valid-start cases)
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(_Request, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

listener_invalid_versions(_Config) ->
    assert_error_contains(
        "invalid_versions",
        nhttp:start_link(#{port => 0, handler => ?MODULE, versions => [bogus_version]})
    ),
    assert_error_contains(
        "invalid_versions",
        nhttp:start_link(#{port => 0, handler => ?MODULE, versions => []})
    ),
    ok.

listener_invalid_tls(_Config) ->
    assert_error_contains(
        "invalid_tls",
        nhttp:start_link(#{
            port => 0, handler => ?MODULE, versions => [http2], tls => not_a_map
        })
    ),
    assert_error_contains(
        "certfile",
        nhttp:start_link(#{
            port => 0, handler => ?MODULE, versions => [http2], tls => #{}
        })
    ),
    assert_error_contains(
        "keyfile",
        nhttp:start_link(#{
            port => 0,
            handler => ?MODULE,
            versions => [http2],
            tls => #{certfile => "nope.pem"}
        })
    ),
    ok.

listener_listen_failed(_Config) ->
    {ok, PidA} = nhttp:start_link(#{
        port => 0, handler => ?MODULE, versions => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(PidA),
    Res = nhttp:start_link(#{
        port => Port, handler => ?MODULE, versions => [http1_1]
    }),
    assert_error_contains("listen_failed", Res),
    nhttp:stop(PidA),
    ok.

listener_named_start(_Config) ->
    LocalName = list_to_atom(
        "nhttp_vt_local_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    GlobalName =
        {global,
            list_to_atom(
                "nhttp_vt_global_" ++ integer_to_list(erlang:unique_integer([positive]))
            )},
    ViaName =
        {via, global,
            list_to_atom(
                "nhttp_vt_via_" ++ integer_to_list(erlang:unique_integer([positive]))
            )},
    Opts = #{port => 0, handler => ?MODULE, versions => [http1_1]},
    {ok, PidL} = nhttp:start_link({local, LocalName}, Opts),
    {ok, PidG} = nhttp:start_link(GlobalName, Opts),
    {ok, PidV} = nhttp:start_link(ViaName, Opts),
    ?assert(is_pid(PidL) andalso is_pid(PidG) andalso is_pid(PidV)),
    nhttp:stop(PidL),
    nhttp:stop(PidG),
    nhttp:stop(PidV),
    ok.

assert_error_contains(Substr, Result) ->
    ?assertMatch({error, _}, Result),
    Flat = lists:flatten(io_lib:format("~p", [Result])),
    ?assertNotEqual(nomatch, string:find(Flat, Substr)).

acceptor_sys_lifecycle(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0, handler => ?MODULE, versions => [http1_1], acceptor_count => 1
    }),
    [AccPid] = nhttp_test_helpers:wait_until_acceptors(Pid, 1, 2000),
    ?assertMatch({status, AccPid, _, _}, sys:get_status(AccPid)),
    {ok, _Port} = nhttp_acceptor:get_listen_port(AccPid),
    ok = sys:suspend(AccPid),
    ok = sys:change_code(AccPid, nhttp_acceptor_core, undefined, []),
    ok = sys:resume(AccPid),
    Ref = monitor(process, AccPid),
    ok = nhttp_acceptor:stop_accepting(AccPid),
    receive
        {'DOWN', Ref, process, AccPid, _} -> ok
    after 2000 ->
        error(acceptor_did_not_stop)
    end,
    nhttp:stop(Pid),
    ok.

acceptor_sys_terminate(_Config) ->
    {ok, Pid} = nhttp:start_link(#{
        port => 0, handler => ?MODULE, versions => [http1_1], acceptor_count => 1
    }),
    [AccPid] = nhttp_test_helpers:wait_until_acceptors(Pid, 1, 2000),
    Ref = monitor(process, AccPid),
    ok = sys:terminate(AccPid, shutdown),
    receive
        {'DOWN', Ref, process, AccPid, shutdown} -> ok
    after 2000 ->
        error(acceptor_did_not_terminate)
    end,
    nhttp:stop(Pid),
    ok.
