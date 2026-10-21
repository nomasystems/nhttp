-module(nhttp_h2c_SUITE).

-moduledoc """
HTTP/2 over cleartext TCP with prior knowledge (RFC 9113 §3.3): a listener
with `versions => [http2]` and no `tls`.
""".

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
    concurrent_streams/1,
    drain_sends_goaway/1,
    get_request/1,
    h1_request_gets_goaway/1,
    post_with_body/1,
    request_scheme_is_http/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).

-export([
    handle_request/2,
    handle_request_body/3,
    init/1
]).

-define(RECV_TIMEOUT, 5000).
-define(CONCURRENT_STREAMS, 100).
-define(PROTOCOL_ERROR, 1).

init(TestPid) ->
    {ok, #{test => TestPid}}.

handle_request(#{path := <<"/reply">>}, State) ->
    {reply, nhttp_resp:ok(<<"OK">>), State};
handle_request(#{path := <<"/scheme">>} = Req, #{test := TestPid} = State) ->
    TestPid ! {scheme, nhttp_req:scheme(Req)},
    {reply, nhttp_resp:ok(<<"OK">>), State};
handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State}.

handle_request_body({data, Data}, Acc, State) ->
    {accept_body, [Data | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    {reply, nhttp_resp:ok(iolist_to_binary(lists:reverse(Acc))), State}.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------
all() ->
    [
        get_request,
        post_with_body,
        concurrent_streams,
        request_scheme_is_http,
        drain_sends_goaway,
        h1_request_gets_goaway
    ].

init_per_testcase(_TestCase, Config) ->
    {Pid, Port} = nhttp_test_helpers:h2c_start_server(?MODULE, #{handler_args => self()}),
    [{server, Pid}, {port, Port} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = nhttp:stop(?config(server, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
get_request(Config) ->
    {ok, Sock} = nhttp_test_helpers:h2c_connect(?config(port, Config)),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/reply">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"200">>, nhttp_test_helpers:h2_response_status(Frames)),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    ok = gen_tcp:close(Sock).

post_with_body(Config) ->
    {ok, Sock} = nhttp_test_helpers:h2c_connect(?config(port, Config)),
    ok = nhttp_test_helpers:h2_send_post(Sock, 1, <<"/echo">>, <<"hello">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"hello">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    ok = gen_tcp:close(Sock).

concurrent_streams(Config) ->
    {ok, Sock} = nhttp_test_helpers:h2c_connect(?config(port, Config)),
    StreamIds = lists:seq(1, 2 * ?CONCURRENT_STREAMS - 1, 2),
    lists:foreach(
        fun(Id) -> ok = nhttp_test_helpers:h2_open_stream(Sock, Id, <<"/echo">>) end,
        StreamIds
    ),
    lists:foreach(
        fun(Id) ->
            ok = nhttp_test_helpers:h2_send_data(Sock, Id, integer_to_binary(Id), true)
        end,
        StreamIds
    ),
    Frames = recv_until_done(Sock, StreamIds, <<>>, []),
    lists:foreach(
        fun(Id) ->
            ?assertEqual(integer_to_binary(Id), nhttp_test_helpers:h2_response_body(Frames, Id))
        end,
        StreamIds
    ),
    ok = gen_tcp:close(Sock).

request_scheme_is_http(Config) ->
    {ok, Sock} = nhttp_test_helpers:h2c_connect(?config(port, Config)),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/scheme">>),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames, 1)),
    receive
        {scheme, Scheme} -> ?assertEqual(http, Scheme)
    after ?RECV_TIMEOUT -> ct:fail(handler_not_called)
    end,
    ok = gen_tcp:close(Sock).

drain_sends_goaway(Config) ->
    Server = ?config(server, Config),
    {ok, Sock} = nhttp_test_helpers:h2c_connect(?config(port, Config)),
    ok = nhttp_test_helpers:h2_send_request(Sock, 1, <<"/reply">>),
    Frames1 = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ?assertEqual(<<"OK">>, nhttp_test_helpers:h2_response_body(Frames1, 1)),
    ok = nhttp:drain(Server, ?RECV_TIMEOUT),
    Frames2 = nhttp_test_helpers:h2_recv(Sock, ?RECV_TIMEOUT),
    ?assertMatch([{goaway, 0, <<0:1, 1:31, 0:32>>} | _], [F || {goaway, _, _} = F <- Frames2]),
    ok = gen_tcp:close(Sock).

h1_request_gets_goaway(Config) ->
    Sock = nhttp_test_helpers:tcp_connect(?config(port, Config)),
    ok = gen_tcp:send(Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    {Frames, _Rest} = nhttp_test_helpers:decode_h2_frames(nhttp_test_helpers:drain_tcp(Sock)),
    ?assertMatch(
        [{goaway, 0, <<_:32, ?PROTOCOL_ERROR:32>>}], [F || {goaway, _, _} = F <- Frames]
    ),
    ok = gen_tcp:close(Sock).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------
recv_until_done(Sock, StreamIds, Buf, Acc) ->
    case lists:all(fun(Id) -> nhttp_test_helpers:h2_stream_done(Acc, Id) end, StreamIds) of
        true ->
            Acc;
        false ->
            {ok, Data} = gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT),
            {Frames, Rest} = nhttp_test_helpers:decode_h2_frames(<<Buf/binary, Data/binary>>),
            recv_until_done(Sock, StreamIds, Rest, Acc ++ Frames)
    end.
