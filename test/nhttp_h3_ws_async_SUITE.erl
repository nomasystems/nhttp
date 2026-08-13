-module(nhttp_h3_ws_async_SUITE).

-moduledoc """
Phase-5 verification suite for the H3 (RFC 9220 Extended CONNECT)
WebSocket dispatch in `nhttp_conn_h3`. Mirrors the H1 / H2 phase
verification suites so the new lifecycle, external send/info/close,
multi-session broadcast, and stream-reset paths are covered.

Reuses the transport-agnostic `nhttp_h1_ws_async_handler` test handler.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("../src/nhttp_ws_codes.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    open_event/1,
    frame_echo_text/1,
    frame_echo_binary/1,
    peer_close_with_code/1,
    local_close_via_callback/1,
    external_send/1,
    external_info_push/1,
    external_close/1,
    oversized_message_close/1,
    handler_crash_close/1,
    deliver_ping_observable/1,
    deliver_pong_observable/1,
    send_async_returns_ok/1,
    stale_session_handle/1,
    stream_reset_h3/1,
    multi_session_broadcast/1,
    multi_session_addressed_info/1,
    multi_reply_list/1,
    plain_open_no_opts/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [{group, h3_ws_async}].

groups() ->
    [
        {h3_ws_async, [], [
            open_event,
            frame_echo_text,
            frame_echo_binary,
            peer_close_with_code,
            local_close_via_callback,
            external_send,
            external_info_push,
            external_close,
            oversized_message_close,
            handler_crash_close,
            deliver_ping_observable,
            deliver_pong_observable,
            send_async_returns_ok,
            stale_session_handle,
            stream_reset_h3,
            multi_session_broadcast,
            multi_session_addressed_info,
            multi_reply_list,
            plain_open_no_opts
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(h3_ws_async, Config) ->
    TestConfDir = filename:join(filename:dirname(code:which(?MODULE)), "conf"),
    CertFile = filename:join(TestConfDir, "server.pem"),
    KeyFile = filename:join(TestConfDir, "server.key"),
    case filelib:is_file(CertFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "SSL certificates not found"}
    end.

end_per_group(_Group, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

open_event(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    ?assertEqual(h3, nhttp_ws:transport(Session)),
    ?assert(is_pid(nhttp_ws:owner(Session))),
    teardown_ws(Ctx).

frame_echo_text(Config) ->
    Ctx = setup_ws(Config, echo),
    ok = h3_ws_send_frame(Ctx, {text, <<"hello">>}),
    ?assertEqual({text, <<"hello">>}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

frame_echo_binary(Config) ->
    Ctx = setup_ws(Config, echo),
    Payload = crypto:strong_rand_bytes(64),
    ok = h3_ws_send_frame(Ctx, {binary, Payload}),
    ?assertEqual({binary, Payload}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

peer_close_with_code(Config) ->
    Ctx = setup_ws(Config, echo),
    ok = h3_ws_send_frame(Ctx, {close, ?WS_CLOSE_NORMAL, <<"bye">>}),
    ?assertEqual({close, ?WS_CLOSE_NORMAL, <<"bye">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch(
        {closed, {peer, ?WS_CLOSE_NORMAL, <<"bye">>}}, recv_observer_event(closed)
    ),
    teardown_ws(Ctx).

local_close_via_callback(Config) ->
    Ctx = setup_ws(Config, close_on_text),
    ok = h3_ws_send_frame(Ctx, {text, <<"trigger">>}),
    ?assertEqual({close, 4000, <<"closing on text">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch({closed, {local, 4000, <<"closing on text">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

external_send(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:send(Session, {text, <<"pushed">>}),
    ?assertEqual({text, <<"pushed">>}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

external_info_push(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:info(Session, {push_text, <<"via info">>}),
    ?assertEqual({text, <<"via info">>}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

external_close(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    ok = nhttp_ws:close(Session, 4001, <<"external">>),
    ?assertEqual({close, 4001, <<"external">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch({closed, {local, 4001, <<"external">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

oversized_message_close(Config) ->
    Ctx = setup_ws(Config, max_msg_256),
    ok = h3_ws_send_frame(Ctx, {text, binary:copy(<<"x">>, 1024)}),
    ?assertEqual({close, 1009, <<"Message Too Big">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch({closed, {fail, 1009, <<"Message Too Big">>}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

handler_crash_close(Config) ->
    Ctx = setup_ws(Config, crash_text),
    ok = h3_ws_send_frame(Ctx, {text, <<"boom">>}),
    ?assertEqual({close, 1011, <<"Internal Server Error">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch({closed, {handler_crash, _}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

deliver_ping_observable(Config) ->
    Ctx = setup_ws(Config, deliver_ping),
    ok = h3_ws_send_frame(Ctx, {ping, <<"hi">>}),
    ?assertEqual({pong, <<"hi">>}, h3_ws_recv_frame(Ctx)),
    ?assertMatch({frame, {ping, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Ctx).

send_async_returns_ok(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    ?assertEqual(ok, nhttp_ws:send_async(Session, {text, <<"async">>})),
    ?assertEqual({text, <<"async">>}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

stale_session_handle(Config) ->
    Ctx = setup_ws(Config, echo),
    Session = ctx_session(Ctx),
    Stale = nhttp_ws:new_session(
        nhttp_ws:transport(Session),
        nhttp_ws:owner(Session),
        nhttp_ws:stream_id(Session),
        make_ref()
    ),
    ?assertEqual({error, gone}, nhttp_ws:send(Stale, ping)),
    teardown_ws(Ctx).

stream_reset_h3(Config) ->
    Ctx = setup_ws(Config, echo),
    StreamId = ctx_stream(Ctx),
    QConn = ctx_qconn(Ctx),
    ok = nhttp_h3_test_client:reset_stream(QConn, StreamId, 16#10c),
    ?assertMatch({closed, {h3_reset, _}}, recv_observer_event(closed)),
    teardown_ws(Ctx).

multi_session_broadcast(Config) ->
    Ctx0 = setup_ws(Config, echo),
    Ctx = open_extra_stream(Ctx0),
    Session1 = ctx_session(Ctx),
    ok = nhttp_ws:broadcast(Session1, {observe, broadcast_msg}),
    ?assertMatch({info, {observe, broadcast_msg}}, recv_observer_event(info)),
    ?assertMatch({info, {observe, broadcast_msg}}, recv_observer_event(info)),
    teardown_ws(Ctx).

multi_session_addressed_info(Config) ->
    Ctx0 = setup_ws(Config, echo),
    Ctx = open_extra_stream(Ctx0),
    Sessions = ctx_sessions(Ctx),
    Session2 = lists:nth(2, Sessions),
    ok = nhttp_ws:info(Session2, {observe, only_2}),
    ?assertMatch({info, {observe, only_2}}, recv_observer_event(info)),
    ?assertEqual(timeout, recv_observer_event_or_timeout(200)),
    teardown_ws(Ctx).

deliver_pong_observable(Config) ->
    Ctx = setup_ws(Config, deliver_pong),
    ok = h3_ws_send_frame(Ctx, {pong, <<"hi">>}),
    ?assertMatch({frame, {pong, <<"hi">>}}, recv_observer_event(frame)),
    teardown_ws(Ctx).

multi_reply_list(Config) ->
    Ctx = setup_ws(Config, multi_reply),
    ok = h3_ws_send_frame(Ctx, {text, <<"x">>}),
    F1 = h3_ws_recv_frame(Ctx),
    F2 = h3_ws_recv_frame(Ctx),
    ?assertEqual({text, <<"first:x">>}, F1),
    ?assertEqual({text, <<"second:x">>}, F2),
    teardown_ws(Ctx).

plain_open_no_opts(Config) ->
    Ctx = setup_ws(Config, plain_open),
    ok = h3_ws_send_frame(Ctx, {ping, <<"p">>}),
    ?assertEqual({pong, <<"p">>}, h3_ws_recv_frame(Ctx)),
    teardown_ws(Ctx).

%%%-----------------------------------------------------------------------------
%%% H3 CONNECTION / WS UPGRADE / FRAMING HELPERS
%%%-----------------------------------------------------------------------------

-record(ctx, {
    server :: pid(),
    qconn :: term(),
    h3 :: term(),
    streams = [] :: [{nhttp_lib:stream_id(), nhttp_ws:session()}],
    inbox = #{} :: #{nhttp_lib:stream_id() => [term()]}
}).

ctx_qconn(#ctx{qconn = Q}) -> Q.
ctx_session(#ctx{streams = [{_, S} | _]}) -> S.
ctx_sessions(#ctx{streams = Ss}) -> [S || {_, S} <- Ss].
ctx_stream(#ctx{streams = [{Id, _} | _]}) -> Id.

setup_ws(Config, Mode) ->
    Observer = self(),
    flush_observer(),
    {Server, QConn, H3} = h3_connect(Config, Mode, Observer),
    open_extra_stream(#ctx{server = Server, qconn = QConn, h3 = H3, streams = []}).

open_extra_stream(#ctx{qconn = QConn, h3 = H3, streams = Streams} = Ctx) ->
    {ok, StreamId} = nhttp_h3_test_client:open_stream(QConn, bidi),
    Headers = [
        {<<":method">>, <<"CONNECT">>},
        {<<":protocol">>, <<"websocket">>},
        {<<":path">>, <<"/ws">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>}
    ],
    {ok, H3_1} = nhttp_h3_test_client:send_h3_headers(QConn, H3, StreamId, Headers, nofin),
    Session =
        receive
            {ws, _Conn, {open, S}} -> S
        after 2000 ->
            ct:fail({no_open_event, StreamId})
        end,
    Ctx#ctx{h3 = H3_1, streams = Streams ++ [{StreamId, Session}]}.

teardown_ws(#ctx{server = Server, qconn = QConn}) ->
    nhttp_h3_test_client:close(QConn),
    nhttp:stop(Server),
    flush_observer().

h3_connect(Config, Mode, Observer) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, Server} = nhttp:start_link(#{
        port => 0,
        tls => #{certfile => CertFile, keyfile => KeyFile},
        versions => [http3],
        handler => nhttp_h1_ws_async_handler,
        handler_args => #{observer => Observer, mode => Mode},
        acceptor_count => 1
    }),
    {ok, Port} = nhttp:get_port(Server),
    {QConn, H3} = nhttp_h3_test_client:connect(Port),
    {Server, QConn, H3}.

h3_ws_send_frame(#ctx{qconn = QConn, h3 = H3, streams = [{StreamId, _} | _]} = Ctx, Msg) ->
    Bin = iolist_to_binary(nhttp_ws:encode_masked(Msg)),
    {ok, _H3_1} = nhttp_h3_test_client:send_h3_data(QConn, H3, StreamId, Bin, nofin),
    _ = Ctx,
    ok.

h3_ws_recv_frame(#ctx{streams = [{StreamId, _} | _]} = Ctx) ->
    h3_ws_recv_frame_for(Ctx, StreamId).

h3_ws_recv_frame_for(Ctx, StreamId) ->
    case h3_try_recv_frame_for(Ctx, StreamId, 2000) of
        timeout -> ct:fail({recv_frame_timeout, StreamId});
        Frame -> Frame
    end.

h3_try_recv_frame_for(Ctx, StreamId, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    h3_try_recv_loop(Ctx, StreamId, Deadline).

h3_try_recv_loop(#ctx{qconn = QConn, h3 = H3} = Ctx, StreamId, Deadline) ->
    case pop_inbox(StreamId) of
        {ok, Frame} ->
            Frame;
        none ->
            Remaining = max(Deadline - erlang:monotonic_time(millisecond), 0),
            case nhttp_h3_test_client:recv_events(QConn, H3, StreamId, Remaining) of
                {ok, Events, H3_1} ->
                    case extract_ws_frame(Events, StreamId) of
                        {ok, Frame} -> Frame;
                        none -> h3_try_recv_loop(Ctx#ctx{h3 = H3_1}, StreamId, Deadline)
                    end;
                {error, _} ->
                    timeout
            end
    end.

extract_ws_frame(Events, StreamId) ->
    [stash_frames_from_event(E) || E <- Events],
    pop_inbox(StreamId).

stash_frames_from_event({data, StreamId, Data, _Fin}) ->
    decode_and_push(StreamId, Data);
stash_frames_from_event(_) ->
    ok.

decode_and_push(StreamId, Data) ->
    case nhttp_ws:decode_unmasked(Data) of
        {ok, Frame, <<>>} ->
            push_inbox(StreamId, Frame);
        {ok, Frame, Rest} ->
            push_inbox(StreamId, Frame),
            decode_and_push(StreamId, Rest);
        _ ->
            ok
    end.

push_inbox(StreamId, Frame) ->
    Key = {?MODULE, inbox, StreamId},
    Q =
        case erlang:get(Key) of
            undefined -> [];
            L -> L
        end,
    erlang:put(Key, Q ++ [Frame]).

pop_inbox(StreamId) ->
    Key = {?MODULE, inbox, StreamId},
    case erlang:get(Key) of
        [Frame | Rest] ->
            erlang:put(Key, Rest),
            {ok, Frame};
        _ ->
            none
    end.

%%%-----------------------------------------------------------------------------
%%% OBSERVER MAILBOX HELPERS
%%%-----------------------------------------------------------------------------

recv_observer_event(Tag) ->
    receive
        {ws, _Conn, {Tag, _} = Event} -> Event;
        {ws, _Conn, Tag = Event} -> Event
    after 2000 ->
        ct:fail({timeout_waiting_for, Tag})
    end.

recv_observer_event_or_timeout(Timeout) ->
    receive
        {ws, _Conn, _} = Event -> Event
    after Timeout ->
        timeout
    end.

flush_observer() ->
    receive
        {ws, _, _} -> flush_observer()
    after 0 -> ok
    end.
