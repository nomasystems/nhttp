%%%-----------------------------------------------------------------------------
%%% @doc HTTP/3 dispatch and error-mapping lifecycle paths.
%%%
%%% Covers the `nhttp_conn_h3' dispatch branches that need an unusual
%%% handler result or request shape (websocket upgrade on a plain H3
%%% request, request-limit rejection, accept-body that never terminates)
%%% and the exhaustive `nhttp_conn_h3:h3_error_to_code/1' mapping. The
%%% suite doubles as its own `nhttp_handler'.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_h3_lifecycle_SUITE).

-behaviour(nhttp_handler).

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
    h3_bad_upgrade/1,
    h3_bad_upgrade_opts/1,
    h3_error_to_code_exhaustive/1,
    h3_request_cancelled/1,
    h3_request_incomplete/1,
    h3_uri_too_long/1
]).

-export([
    init/1,
    handle_request/2,
    handle_request_body/3,
    terminate/2
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        h3_bad_upgrade,
        h3_bad_upgrade_opts,
        h3_error_to_code_exhaustive,
        h3_request_cancelled,
        h3_request_incomplete,
        h3_uri_too_long
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    {CertFile, KeyFile} = nhttp_test_helpers:certs(),
    case filelib:is_file(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found. Run test/conf/gen_test_certs.sh"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

h3_error_to_code_exhaustive(_Config) ->
    Mapping = [
        {h3_no_error, 16#0100},
        {h3_general_protocol_error, 16#0101},
        {h3_internal_error, 16#0102},
        {h3_stream_creation_error, 16#0103},
        {h3_closed_critical_stream, 16#0104},
        {h3_frame_unexpected, 16#0105},
        {h3_frame_error, 16#0106},
        {h3_excessive_load, 16#0107},
        {h3_id_error, 16#0108},
        {h3_settings_error, 16#0109},
        {h3_missing_settings, 16#010a},
        {h3_request_rejected, 16#010b},
        {h3_request_cancelled, 16#010c},
        {h3_request_incomplete, 16#010d},
        {h3_message_error, 16#010e},
        {h3_connect_error, 16#010f},
        {h3_version_fallback, 16#0110},
        {qpack_decompression_failed, 16#0200},
        {qpack_encoder_stream_error, 16#0201},
        {qpack_decoder_stream_error, 16#0202}
    ],
    lists:foreach(
        fun({Atom, Code}) ->
            ?assertEqual(Code, nhttp_conn_h3:h3_error_to_code(Atom))
        end,
        Mapping
    ),
    ok.

h3_bad_upgrade(Config) ->
    {Pid, Port} = start(Config, #{}),
    try
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        Res = nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/bad-upgrade">>, <<>>),
        ?assertMatch({error, _}, Res),
        ?assert(is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_bad_upgrade_opts(Config) ->
    {Pid, Port} = start(Config, #{}),
    try
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        Res = nhttp_h3_test_client:request(QConn, H3, <<"GET">>, <<"/bad-upgrade-opts">>, <<>>),
        ?assertMatch({error, _}, Res),
        ?assert(is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_uri_too_long(Config) ->
    {Pid, Port} = start(Config, #{max_uri_length => 10}),
    try
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        {ok, Status, _Hs, _Body, _H3_1} =
            nhttp_h3_test_client:request(
                QConn, H3, <<"GET">>, <<"/this-path-is-far-too-long">>, <<>>
            ),
        ?assertEqual(414, Status),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_request_incomplete(Config) ->
    {Pid, Port} = start(Config, #{}),
    try
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        Res = nhttp_h3_test_client:request(
            QConn, H3, <<"POST">>, <<"/accept-forever">>, <<"body-bytes">>
        ),
        ?assertMatch({error, _}, Res),
        ?assert(is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

h3_request_cancelled(Config) ->
    {Pid, Port} = start(Config, #{}),
    try
        {QConn, H3} = nhttp_h3_test_client:connect(Port),
        {ok, StreamId, _H3_1} =
            nhttp_h3_test_client:open_request(
                QConn, H3, <<"POST">>, <<"/accept-forever">>, [], <<>>, nofin
            ),
        timer:sleep(200),
        ok = nhttp_h3_test_client:reset_stream(QConn, StreamId, 16#010c),
        timer:sleep(300),
        ?assert(is_process_alive(Pid)),
        nhttp_h3_test_client:close(QConn)
    after
        nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/bad-upgrade">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/bad-upgrade-opts">>}, State) ->
    {upgrade, websocket, #{}, State};
handle_request(#{path := <<"/accept-forever">>}, State) ->
    {accept_body, [], State};
handle_request(_Request, State) ->
    {reply, nhttp_resp:ok(<<"ok">>), State}.

handle_request_body(_Event, BodyState, State) ->
    {accept_body, BodyState, State}.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start(Config, Extra) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Pid} = nhttp:start_link(
        maps:merge(
            #{
                port => 0,
                handler => ?MODULE,
                versions => [http3],
                tls => #{certfile => CertFile, keyfile => KeyFile},
                acceptor_count => 1
            },
            Extra
        )
    ),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.
