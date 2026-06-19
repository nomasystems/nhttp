%%%-----------------------------------------------------------------------------
%%% @doc Unit tests for pure helpers in the connection-handling modules.
%%% These functions are deterministic and exercised directly, away from the
%%% async protocol loops where their branches are otherwise hard to reach.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

-compile([export_all, nowarn_export_all]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        classify_term_reason_test,
        init_exit_reason_test,
        exit_reason_test,
        select_family_test,
        version_family_mapping_test,
        route_silent_test,
        ws_str_or_atom_test,
        ws_default_runtime_opts_test,
        ws_apply_runtime_opts_test,
        ws_oversize_test,
        ws_apply_handler_result_test,
        stream_worker_body_fin_unconsumed_test,
        stream_worker_body_peer_aborted_test,
        stream_worker_body_handler_crash_test,
        stream_worker_trailers_after_close_test
    ].

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

classify_term_reason_test(_Config) ->
    ?assertEqual(normal, nhttp_conn_otel:classify_term_reason(normal)),
    ?assertEqual(timeout, nhttp_conn_otel:classify_term_reason(idle_timeout)),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason({socket_error, closed})),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason({h2_connection_error, foo})),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason({h2_error, bar})),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason({h3_connection_error, baz})),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason({protocol_error, qux})),
    ?assertEqual(error, nhttp_conn_otel:classify_term_reason(some_other_reason)).

init_exit_reason_test(_Config) ->
    ?assertEqual(normal, nhttp_conn:init_exit_reason(closed)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(econnreset)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(econnaborted)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(epipe)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(etimedout)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(einval)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason(enotconn)),
    ?assertEqual(normal, nhttp_conn:init_exit_reason({handshake_error, closed})),
    ?assertEqual(normal, nhttp_conn:init_exit_reason({proxy_protocol, {recv, closed}})),
    ?assertEqual(shutdown, nhttp_conn:init_exit_reason(shutdown)),
    ?assertEqual({shutdown, drain}, nhttp_conn:init_exit_reason({shutdown, drain})),
    ?assertEqual(
        {handshake_error, {tls_alert, bad}},
        nhttp_conn:init_exit_reason({handshake_error, {tls_alert, bad}})
    ),
    ?assertEqual(
        {proxy_protocol, leftover_bytes},
        nhttp_conn:init_exit_reason({proxy_protocol, leftover_bytes})
    ),
    ?assertEqual(socket_transfer_timeout, nhttp_conn:init_exit_reason(socket_transfer_timeout)).

exit_reason_test(_Config) ->
    ?assertEqual(shutdown, nhttp_conn:exit_reason(shutdown)),
    ?assertEqual({shutdown, drain}, nhttp_conn:exit_reason({shutdown, drain})),
    ?assertEqual(normal, nhttp_conn:exit_reason(idle_timeout)),
    ?assertEqual(normal, nhttp_conn:exit_reason({socket_error, closed})).

select_family_test(_Config) ->
    ?assertEqual({ok, http2}, nhttp_conn:select_family({ok, <<"h2">>}, [http2, http1])),
    ?assertEqual(
        {error, {unsupported_alpn, <<"h2">>}},
        nhttp_conn:select_family({ok, <<"h2">>}, [http1])
    ),
    ?assertEqual({ok, http1}, nhttp_conn:select_family({ok, <<"http/1.1">>}, [http1, http2])),
    ?assertEqual(
        {error, {unsupported_alpn, <<"http/1.1">>}},
        nhttp_conn:select_family({ok, <<"http/1.1">>}, [http2])
    ),
    ?assertEqual(
        {error, {unsupported_alpn, <<"http/1.0">>}},
        nhttp_conn:select_family({ok, <<"http/1.0">>}, [http1])
    ),
    ?assertEqual(
        {error, {unsupported_alpn, <<"spdy/3">>}},
        nhttp_conn:select_family({ok, <<"spdy/3">>}, [http1, http2])
    ),
    ?assertEqual({ok, http2}, nhttp_conn:select_family({error, no_alpn}, [http2, http1])),
    ?assertEqual({ok, http1}, nhttp_conn:select_family({error, no_alpn}, [http1, http2])),
    ?assertEqual({ok, http1}, nhttp_conn:select_family({error, no_alpn}, [http3])).

version_family_mapping_test(_Config) ->
    ?assertEqual(http1, nhttp_conn:version_to_family(http1_0)),
    ?assertEqual(http1, nhttp_conn:version_to_family(http1_1)),
    ?assertEqual(http2, nhttp_conn:version_to_family(http2)),
    ?assertEqual(http3, nhttp_conn:version_to_family(http3)),
    ?assertEqual(http1_1, nhttp_conn:family_to_version(http1)),
    ?assertEqual(http2, nhttp_conn:family_to_version(http2)),
    ?assertEqual(http1_1, nhttp_conn:family_to_version(websocket)),
    ?assertEqual(undefined, nhttp_conn:family_to_version(undefined)).

route_silent_test(_Config) ->
    Self = self(),
    Workers = #{Self => 7},
    ?assertEqual(7, nhttp_conn_workers:route_silent(Self, Workers, state, fun(Sid) -> Sid end)),
    ?assertEqual(
        state, nhttp_conn_workers:route_silent(some_pid, Workers, state, fun(Sid) -> Sid end)
    ).

ws_str_or_atom_test(_Config) ->
    ?assertEqual(<<"normal">>, nhttp_conn_ws:str_or_atom(normal)),
    Rendered = nhttp_conn_ws:str_or_atom({some, tuple, 1}),
    ?assert(is_binary(Rendered)),
    ?assertEqual(<<"{some,tuple,1}">>, Rendered).

ws_default_runtime_opts_test(_Config) ->
    ?assertEqual(
        #{deliver_ping => false, deliver_pong => false, max_message_size => infinity},
        nhttp_conn_ws:default_runtime_opts()
    ).

ws_apply_runtime_opts_test(_Config) ->
    Current = nhttp_conn_ws:default_runtime_opts(),
    ?assertEqual(Current, nhttp_conn_ws:apply_runtime_opts(#{}, Current)),
    Merged = nhttp_conn_ws:apply_runtime_opts(
        #{deliver_ping => true, max_message_size => 1024, ignored => key}, Current
    ),
    ?assertEqual(
        #{deliver_ping => true, deliver_pong => false, max_message_size => 1024}, Merged
    ).

ws_oversize_test(_Config) ->
    ?assertNot(nhttp_conn_ws:oversize(<<"data">>, #{max_message_size => infinity})),
    ?assertNot(nhttp_conn_ws:oversize(<<"data">>, #{max_message_size => 10})),
    ?assert(nhttp_conn_ws:oversize(<<"too long payload">>, #{max_message_size => 4})).

ws_apply_handler_result_test(_Config) ->
    View = #{
        handler_state => old, runtime_opts => nhttp_conn_ws:default_runtime_opts(), session => s
    },

    ?assertEqual(
        [{update_view, View#{handler_state := new}}],
        nhttp_conn_ws:apply_handler_result({ok, new}, View)
    ),

    ?assertEqual(
        [
            {update_view, View#{handler_state := new}},
            {send_frame, {text, <<"a">>}},
            {send_frame, {text, <<"b">>}}
        ],
        nhttp_conn_ws:apply_handler_result(
            {reply, [{text, <<"a">>}, {text, <<"b">>}], new}, View
        )
    ),

    ?assertEqual(
        [{update_view, View#{handler_state := new}}, {send_frame, {text, <<"x">>}}],
        nhttp_conn_ws:apply_handler_result({reply, {text, <<"x">>}, new}, View)
    ),

    ?assertEqual(
        [
            {update_view, View#{handler_state := new}},
            {send_close, ?WS_CLOSE_NORMAL, <<"bye">>},
            {close_session, {local, ?WS_CLOSE_NORMAL, <<"bye">>}}
        ],
        nhttp_conn_ws:apply_handler_result({close, ?WS_CLOSE_NORMAL, <<"bye">>, new}, View)
    ),

    ?assertEqual(
        [
            {update_view, View#{handler_state := new}},
            {send_close, ?WS_CLOSE_GOING_AWAY, <<"Server Going Away">>},
            {close_session, {handler_stop, my_reason}}
        ],
        nhttp_conn_ws:apply_handler_result({stop, my_reason, new}, View)
    ),

    ?assertEqual(
        [
            {send_close, ?WS_CLOSE_INTERNAL_ERROR, <<"Internal Server Error">>},
            {close_session, {handler_crash, {error, boom}}}
        ],
        nhttp_conn_ws:apply_handler_result({nhttp_handler_exception, error, boom}, View)
    ).

%%%-----------------------------------------------------------------------------
%%% nhttp_stream_worker: the test process plays the connection role.
%%%-----------------------------------------------------------------------------

stream_worker_body_fin_unconsumed_test(_Config) ->
    {WPid, Ref} = start_body_worker(always_accept),
    WPid ! {body_chunk, Ref, {fin, <<>>}},
    expect_body_chunk_ack(WPid, Ref),
    ?assertEqual(
        {abort, body_unconsumed, always_accept}, expect_request_result(1, Ref)
    ),
    await_exit(WPid).

stream_worker_body_peer_aborted_test(_Config) ->
    {WPid, Ref} = start_body_worker(always_accept),
    WPid ! {body_chunk, Ref, {abort, <<>>}},
    expect_body_chunk_ack(WPid, Ref),
    ?assertEqual(
        {abort, peer_aborted, always_accept}, expect_request_result(1, Ref)
    ),
    await_exit(WPid).

stream_worker_body_handler_crash_test(_Config) ->
    {WPid, Ref} = start_body_worker(crash),
    WPid ! {body_chunk, Ref, {data, <<"x">>}},
    expect_body_chunk_ack(WPid, Ref),
    ?assertMatch(
        {nhttp_handler_exception, error, boom}, expect_request_result(1, Ref)
    ),
    await_exit(WPid).

stream_worker_trailers_after_close_test(_Config) ->
    Self = self(),
    Ref = make_ref(),
    Producer = fun(SendChunk) ->
        {error, closed} = SendChunk(<<"data">>),
        {trailers, [{<<"x">>, <<"y">>}]}
    end,
    WPid = nhttp_stream_worker:start(Self, Ref, Producer),
    receive
        {send_chunk, WPid, Ref, <<"data">>} ->
            WPid ! {chunk_ack, Ref, {error, closed}}
    after 1000 ->
        ct:fail(no_send_chunk)
    end,
    await_exit(WPid),
    receive
        {send_trailers, WPid, Ref, _} -> ct:fail(unexpected_trailers)
    after 100 ->
        ok
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start_body_worker(Mode) ->
    Ref = make_ref(),
    WPid = nhttp_stream_worker:start_request(
        self(), 1, Ref, nhttp_stream_worker_test_handler, #{}, Mode
    ),
    ?assertEqual({accept_body, body0, Mode}, expect_request_result(1, Ref)),
    {WPid, Ref}.

expect_request_result(StreamId, Ref) ->
    receive
        {request_result, StreamId, Ref, Result} -> Result
    after 1000 ->
        ct:fail(no_request_result)
    end.

expect_body_chunk_ack(WPid, Ref) ->
    receive
        {body_chunk_ack, WPid, Ref} -> ok
    after 1000 ->
        ct:fail(no_body_chunk_ack)
    end.

await_exit(WPid) ->
    MRef = erlang:monitor(process, WPid),
    receive
        {'DOWN', MRef, process, WPid, _} -> ok
    after 1000 ->
        ct:fail(worker_did_not_exit)
    end.
