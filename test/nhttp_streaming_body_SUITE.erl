%%%-----------------------------------------------------------------------------
%%% @doc Test suite for HTTP/1.1 streaming request bodies.
%%%
%%% Covers the `accept_body` + `handle_request_body/3` path:
%%%   * Content-Length-bound and `Transfer-Encoding: chunked` framing.
%%%   * Trailers delivered via `{fin, Trailers}`.
%%%   * `accept_body` re-entry per chunk threading body state.
%%%   * Peer abort mid-body delivering an `{abort, _}` event.
%%%   * `max_body_size` enforcement (413 + close).
%%%   * Non-`accept_body` return mid-body (RFC 9112 §6.3 close fallback).
%%%   * Non-`accept_body` return from `handle_request/2` with body in flight
%%%     (RFC 9112 §6.3 close fallback).
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_streaming_body_SUITE).

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
    h1_content_length_single/1,
    h1_content_length_split/1,
    h1_chunked_simple/1,
    h1_chunked_with_trailers/1,
    h1_accept_body_reentry/1,
    h1_peer_abort/1,
    h1_max_body_size_413/1,
    h1_reply_mid_body/1,
    h1_reply_without_accept_body/1,
    h1_chunked_bad_size/1,
    h1_content_length_zero_keepalive/1,
    h1_accept_body_terminal_abort/1
]).

-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

-define(PROBE_KEY, {?MODULE, abort_probe}).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/echo-reentry">>}, State) ->
    {accept_body, {0, []}, State};
handle_request(#{path := <<"/echo-trailers">>}, State) ->
    {accept_body, {0, []}, State};
handle_request(#{path := <<"/reply-mid-body">>}, State) ->
    {accept_body, reply_on_data, State};
handle_request(#{path := <<"/reply-immediate">>}, State) ->
    {reply, nhttp_resp:ok(<<"immediate">>), State};
handle_request(#{path := <<"/echo-never-finish">>}, State) ->
    {accept_body, never, State};
handle_request(#{path := <<"/abort-on-data">>}, State) ->
    {accept_body, [], State}.

handle_request_body({data, _Chunk}, reply_on_data, State) ->
    {reply, nhttp_resp:ok(<<"early">>), State};
handle_request_body({fin, _Trailers}, reply_on_data, State) ->
    {reply, nhttp_resp:ok(<<"empty">>), State};
handle_request_body({abort, Reason}, reply_on_data, State) ->
    {abort, Reason, State};
handle_request_body({data, _Chunk}, never, State) ->
    {accept_body, never, State};
handle_request_body({fin, _Trailers}, never, State) ->
    {accept_body, never, State};
handle_request_body({abort, _Reason}, never, State) ->
    {accept_body, never, State};
handle_request_body({data, Chunk}, Acc, State) when is_list(Acc) ->
    probe_send({data, byte_size(Chunk)}),
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) when is_list(Acc) ->
    probe_send(fin),
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, Acc, State) when is_list(Acc) ->
    probe_send({abort, Reason}),
    {abort, Reason, State};
handle_request_body({data, Chunk}, {Count, Acc}, State) ->
    {accept_body, {Count + 1, [Chunk | Acc]}, State};
handle_request_body({fin, Trailers}, {Count, Acc}, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    Resp = #{
        status => 200,
        headers => [
            {<<"x-chunks">>, integer_to_binary(Count)},
            {<<"x-trailers">>, integer_to_binary(length(Trailers))}
        ],
        body => Body
    },
    {reply, Resp, State};
handle_request_body({abort, Reason}, {_Count, _Acc}, State) ->
    {abort, Reason, State}.

probe_send(Event) ->
    case persistent_term:get(?PROBE_KEY, undefined) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            Pid ! {handler_event, Event},
            ok
    end.

%%%-----------------------------------------------------------------------------
%%% SUITE SETUP
%%%-----------------------------------------------------------------------------

all() ->
    [
        h1_content_length_single,
        h1_content_length_split,
        h1_chunked_simple,
        h1_chunked_with_trailers,
        h1_accept_body_reentry,
        h1_peer_abort,
        h1_max_body_size_413,
        h1_reply_mid_body,
        h1_reply_without_accept_body,
        h1_chunked_bad_size,
        h1_content_length_zero_keepalive,
        h1_accept_body_terminal_abort
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    persistent_term:put(?PROBE_KEY, self()),
    flush_probe(),
    Config.

end_per_testcase(_TC, _Config) ->
    _ = persistent_term:erase(?PROBE_KEY),
    flush_probe(),
    ok.

flush_probe() ->
    receive
        {handler_event, _} -> flush_probe()
    after 0 -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

h1_content_length_single(_Config) ->
    {Pid, Port} = start_server(#{}),
    Body = <<"hello streaming body">>,
    {ok, Sock} = connect(Port),
    Req = post_request(<<"/echo">>, Body, byte_size(Body)),
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, Body)),
    ?assertEqual([{data, byte_size(Body)}, fin], collect_probe()),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_content_length_split(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Chunk1 = <<"part-one-">>,
    Chunk2 = <<"part-two">>,
    Total = byte_size(Chunk1) + byte_size(Chunk2),
    Headers = [
        <<"POST /echo-reentry HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Content-Length: ">>,
        integer_to_binary(Total),
        <<"\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, Headers),
    ok = gen_tcp:send(Sock, Chunk1),
    timer:sleep(50),
    ok = gen_tcp:send(Sock, Chunk2),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, <<Chunk1/binary, Chunk2/binary>>)),
    ?assert(extract_header_value(<<"x-chunks">>, Resp) >= 2),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_chunked_simple(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Headers = [
        <<"POST /echo HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Transfer-Encoding: chunked\r\n\r\n">>
    ],
    Chunks = [
        encode_chunk(<<"abc">>),
        encode_chunk(<<"defghij">>),
        encode_chunk(<<"klmn">>),
        last_chunk()
    ],
    ok = gen_tcp:send(Sock, [Headers, Chunks]),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"abcdefghijklmn">>)),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_chunked_with_trailers(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Headers = [
        <<"POST /echo-trailers HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Transfer-Encoding: chunked\r\n">>,
        <<"Trailer: x-tag\r\n\r\n">>
    ],
    Body = [
        encode_chunk(<<"streamed">>),
        encode_chunk(<<"-with-">>),
        encode_chunk(<<"trailer">>),
        <<"0\r\nx-tag: complete\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, [Headers, Body]),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"streamed-with-trailer">>)),
    ?assertEqual(1, extract_header_value(<<"x-trailers">>, Resp)),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_accept_body_reentry(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Body = <<"first|second|third">>,
    Req = post_request(<<"/echo-reentry">>, Body, byte_size(Body)),
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, Body)),
    ?assert(extract_header_value(<<"x-chunks">>, Resp) >= 1),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_peer_abort(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Headers = [
        <<"POST /echo HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Content-Length: 1024\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, [Headers, <<"prefix-">>]),
    timer:sleep(50),
    gen_tcp:close(Sock),
    Events = collect_probe(),
    ?assertMatch([{data, _} | _], Events),
    ?assertNotEqual(nomatch, find_abort(Events)),
    nhttp:stop(Pid),
    ok.

h1_max_body_size_413(_Config) ->
    {Pid, Port} = start_server(#{max_body_size => 16}),
    {ok, Sock} = connect(Port),
    Body = <<"this body is definitely longer than sixteen bytes">>,
    Req = post_request(<<"/echo">>, Body, byte_size(Body)),
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp} = recv_response(Sock, 5000),
    ?assertMatch(<<"HTTP/1.1 413", _/binary>>, Resp),
    wait_for_close(Sock, 2000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_reply_mid_body(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Headers = [
        <<"POST /reply-mid-body HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Content-Length: 200\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, [Headers, <<"early-bytes">>]),
    {ok, Resp} = recv_response(Sock, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"early">>)),
    wait_for_close(Sock, 2000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_reply_without_accept_body(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Body = <<"unread body bytes go here">>,
    Req = post_request(<<"/reply-immediate">>, Body, byte_size(Body)),
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp} = recv_response(Sock, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"immediate">>)),
    wait_for_close(Sock, 2000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_chunked_bad_size(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Headers = [
        <<"POST /echo HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Transfer-Encoding: chunked\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, [Headers, <<"zz\r\n">>]),
    Outcome = recv_response_or_close(Sock, 5000),
    ?assert(
        Outcome =:= closed orelse
            match_prefix(<<"HTTP/1.1 400">>, Outcome)
    ),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_content_length_zero_keepalive(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Req = [
        <<"POST /reply-immediate HTTP/1.1\r\n">>,
        <<"Host: x\r\n">>,
        <<"Content-Length: 0\r\n\r\n">>
    ],
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp1} = recv_response(Sock, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp1),
    ?assertNotEqual(nomatch, binary:match(Resp1, <<"immediate">>)),
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp2} = recv_response(Sock, 5000),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Resp2),
    ?assertNotEqual(nomatch, binary:match(Resp2, <<"immediate">>)),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

h1_accept_body_terminal_abort(_Config) ->
    {Pid, Port} = start_server(#{}),
    {ok, Sock} = connect(Port),
    Body = <<"complete-body">>,
    Req = post_request(<<"/echo-never-finish">>, Body, byte_size(Body)),
    ok = gen_tcp:send(Sock, Req),
    wait_for_close(Sock, 2000),
    gen_tcp:close(Sock),
    nhttp:stop(Pid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

start_server(Extra) ->
    Opts = maps:merge(
        #{
            port => 0,
            handler => ?MODULE,
            versions => [http1_1],
            timeouts => #{idle => 5000}
        },
        Extra
    ),
    {ok, Pid} = nhttp:start_link(Opts),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port}.

connect(Port) ->
    gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]).

post_request(Path, Body, Length) ->
    [
        <<"POST ">>,
        Path,
        <<" HTTP/1.1\r\n">>,
        <<"Host: localhost\r\n">>,
        <<"Content-Length: ">>,
        integer_to_binary(Length),
        <<"\r\n\r\n">>,
        Body
    ].

encode_chunk(Data) ->
    Size = integer_to_binary(byte_size(Data), 16),
    [Size, <<"\r\n">>, Data, <<"\r\n">>].

last_chunk() ->
    <<"0\r\n\r\n">>.

recv_response(Sock, Timeout) ->
    recv_response(Sock, Timeout, <<>>).

recv_response(Sock, Timeout, Acc) ->
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            Combined = <<Acc/binary, Data/binary>>,
            case binary:match(Combined, <<"\r\n\r\n">>) of
                nomatch -> recv_response(Sock, Timeout, Combined);
                _ -> {ok, Combined}
            end;
        {error, closed} when Acc =/= <<>> ->
            {ok, Acc};
        {error, Reason} ->
            {error, Reason}
    end.

recv_response_or_close(Sock, Timeout) ->
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} -> Data;
        {error, closed} -> closed;
        {error, Reason} -> error({recv_response_or_close, Reason})
    end.

match_prefix(_Prefix, closed) ->
    false;
match_prefix(Prefix, Data) when is_binary(Data) ->
    PrefixSize = byte_size(Prefix),
    case Data of
        <<Prefix:PrefixSize/binary, _/binary>> -> true;
        _ -> false
    end.

wait_for_close(Sock, Timeout) ->
    case gen_tcp:recv(Sock, 0, Timeout) of
        {error, closed} -> ok;
        {ok, _Extra} -> wait_for_close(Sock, Timeout);
        {error, Reason} -> error({wait_for_close, Reason})
    end.

collect_probe() ->
    collect_probe([], 500).

collect_probe(Acc, Timeout) ->
    receive
        {handler_event, Ev} -> collect_probe([Ev | Acc], Timeout)
    after Timeout ->
        lists:reverse(Acc)
    end.

find_abort(Events) ->
    case [E || {abort, _} = E <- Events] of
        [] -> nomatch;
        [_ | _] = Aborts -> Aborts
    end.

extract_header_value(Name, Resp) ->
    Lower = nhttp_headers:to_lower(Name),
    {ok, HeaderSection} = split_head(Resp),
    Lines = binary:split(HeaderSection, <<"\r\n">>, [global]),
    extract_header_lines(Lines, Lower).

extract_header_lines([], _Name) ->
    undefined;
extract_header_lines([Line | Rest], Name) ->
    case binary:split(Line, <<":">>) of
        [HName, HValue] ->
            case nhttp_headers:to_lower(HName) of
                Name ->
                    Trimmed = string:trim(HValue),
                    binary_to_integer(iolist_to_binary(Trimmed));
                _ ->
                    extract_header_lines(Rest, Name)
            end;
        _ ->
            extract_header_lines(Rest, Name)
    end.

split_head(Resp) ->
    case binary:split(Resp, <<"\r\n\r\n">>) of
        [Head, _Body] -> {ok, Head};
        _ -> {error, no_head}
    end.
