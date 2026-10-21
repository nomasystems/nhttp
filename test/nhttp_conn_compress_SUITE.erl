-module(nhttp_conn_compress_SUITE).

-moduledoc """
Automatic response compression and the RFC 9110 rules on its path: a body
the handler already encoded, `Vary`, `no-transform`, statuses without a
body or with a range, HEAD, and streamed bodies.
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
    groups/0,
    init_per_group/2,
    end_per_group/2,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    compressed_reply_has_vary/1,
    encoded_body_unchanged/1,
    head_reply_matches_get/1,
    ineligible_reply_has_no_vary/1,
    no_content_status_unchanged/1,
    no_transform_unchanged/1,
    partial_content_unchanged/1,
    pre_encoded_body_kept/1,
    stream_not_compressed/1,
    vary_added_on_compress/1,
    vary_added_on_identity/1,
    vary_kept_when_listed/1,
    vary_merged_with_handler_value/1
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER
%%%-----------------------------------------------------------------------------
-behaviour(nhttp_handler).

-export([
    handle_request/2,
    init/1
]).

-define(RECV_TIMEOUT, 5000).
-define(THRESHOLD, 1024).
-define(LEVEL, 6).
-define(BODY, binary:copy(<<"compressible text ">>, 128)).
-define(BR_BODY, binary:copy(<<"pretend brotli octets ">>, 128)).
-define(STREAM_CHUNK, binary:copy(<<"s">>, 2048)).

init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/pre-encoded">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"br">>}],
    {reply, #{status => 200, headers => Headers, body => ?BR_BODY}, State};
handle_request(#{path := <<"/large">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {reply, #{status => 200, headers => Headers, body => ?BODY}, State};
handle_request(#{path := <<"/stream">>}, State) ->
    Producer = fun(Send) ->
        ok = Send(?STREAM_CHUNK),
        ok = Send(?STREAM_CHUNK),
        ok
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State}.

%%%-----------------------------------------------------------------------------
%%% SUITE CALLBACKS
%%%-----------------------------------------------------------------------------
all() ->
    [{group, unit}, {group, h1}, {group, h2}, {group, h3}].

groups() ->
    Wire = [pre_encoded_body_kept, compressed_reply_has_vary, stream_not_compressed],
    [
        {unit, [parallel], [
            encoded_body_unchanged,
            no_content_status_unchanged,
            partial_content_unchanged,
            no_transform_unchanged,
            vary_added_on_compress,
            vary_added_on_identity,
            vary_merged_with_handler_value,
            vary_kept_when_listed,
            ineligible_reply_has_no_vary,
            head_reply_matches_get
        ]},
        {h1, [], Wire},
        {h2, [], Wire},
        {h3, [], Wire}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(unit, Config) ->
    Config;
init_per_group(Proto, Config) ->
    [{proto, Proto} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    case proplists:get_value(proto, Config) of
        undefined ->
            Config;
        Proto ->
            {Pid, Port} = start_server(Proto),
            [{server, Pid}, {port, Port} | Config]
    end.

end_per_testcase(_TestCase, Config) ->
    case proplists:get_value(server, Config) of
        undefined -> ok;
        Pid -> nhttp:stop(Pid)
    end.

%%%-----------------------------------------------------------------------------
%%% UNIT TEST CASES
%%%-----------------------------------------------------------------------------
encoded_body_unchanged(_Config) ->
    lists:foreach(
        fun(Coding) ->
            Response = reply(200, [{<<"Content-Encoding">>, Coding}]),
            ?assertEqual(Response, compress(Response, gzip_request()))
        end,
        [<<"br">>, <<"gzip">>, <<"identity">>]
    ).

no_content_status_unchanged(_Config) ->
    lists:foreach(
        fun(Status) ->
            Response = reply(Status, []),
            ?assertEqual(Response, compress(Response, gzip_request()))
        end,
        [103, 204, 304]
    ).

partial_content_unchanged(_Config) ->
    Response = reply(206, [{<<"content-range">>, <<"bytes 0-2303/9000">>}]),
    ?assertEqual(Response, compress(Response, gzip_request())).

no_transform_unchanged(_Config) ->
    Response = reply(200, [{<<"Cache-Control">>, <<"public, No-Transform, max-age=60">>}]),
    ?assertEqual(Response, compress(Response, gzip_request())).

vary_added_on_compress(_Config) ->
    #{headers := Headers, body := Body} = compress(reply(200, []), gzip_request()),
    ?assertEqual([<<"gzip">>], values(<<"content-encoding">>, Headers)),
    ?assertEqual([<<"accept-encoding">>], values(<<"vary">>, Headers)),
    ?assertEqual(?BODY, zlib:gunzip(Body)).

vary_added_on_identity(_Config) ->
    Request = #{headers => []},
    #{headers := Headers, body := Body} = compress(reply(200, []), Request),
    ?assertEqual([], values(<<"content-encoding">>, Headers)),
    ?assertEqual([<<"accept-encoding">>], values(<<"vary">>, Headers)),
    ?assertEqual(?BODY, Body).

vary_merged_with_handler_value(_Config) ->
    Response = reply(200, [{<<"Vary">>, <<"Origin">>}]),
    #{headers := Headers} = compress(Response, gzip_request()),
    ?assertEqual([<<"gzip">>], values(<<"content-encoding">>, Headers)),
    ?assertEqual(
        [<<"accept-encoding">>, <<"origin">>],
        lists:sort(vary_members(Headers))
    ).

vary_kept_when_listed(_Config) ->
    lists:foreach(
        fun(Vary) ->
            #{headers := Headers} = compress(reply(200, [{<<"vary">>, Vary}]), gzip_request()),
            ?assertEqual([<<"gzip">>], values(<<"content-encoding">>, Headers)),
            ?assertEqual([Vary], values(<<"vary">>, Headers))
        end,
        [<<"Origin, Accept-Encoding">>, <<"*">>]
    ).

ineligible_reply_has_no_vary(_Config) ->
    Small = #{
        status => 200,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"tiny">>
    },
    ?assertEqual(Small, compress(Small, gzip_request())),
    Binary = reply(200, [{<<"content-type">>, <<"image/png">>}]),
    ?assertEqual(Binary, compress(Binary, gzip_request())).

head_reply_matches_get(_Config) ->
    #{headers := GetHeaders} = compress(reply(200, []), gzip_request()),
    #{headers := HeadHeaders} = compress(reply(200, []), (gzip_request())#{method => head}),
    ?assertEqual(GetHeaders, HeadHeaders).

%%%-----------------------------------------------------------------------------
%%% WIRE TEST CASES
%%%-----------------------------------------------------------------------------
pre_encoded_body_kept(Config) ->
    {200, Headers, Body} = request(Config, <<"/pre-encoded">>, <<"br;q=1, gzip;q=0.8">>),
    ?assertEqual([<<"br">>], values(<<"content-encoding">>, Headers)),
    ?assertEqual(?BR_BODY, Body).

compressed_reply_has_vary(Config) ->
    {200, Headers, Body} = request(Config, <<"/large">>, <<"gzip">>),
    ?assertEqual([<<"gzip">>], values(<<"content-encoding">>, Headers)),
    ?assertEqual([<<"accept-encoding">>], values(<<"vary">>, Headers)),
    ?assertEqual(?BODY, zlib:gunzip(Body)).

stream_not_compressed(Config) ->
    {200, Headers, _Body} = request(Config, <<"/stream">>, <<"gzip">>),
    ?assertEqual([], values(<<"content-encoding">>, Headers)).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------
compress(Response, Request) ->
    Config = nhttp_conn_compress:config(
        true, ?LEVEL, ?THRESHOLD, nhttp_compress:default_mime_types()
    ),
    nhttp_conn_compress:maybe_compress(Response, Request, Config).

gzip_request() ->
    #{method => get, headers => [{<<"accept-encoding">>, <<"gzip">>}]}.

reply(Status, Extra) ->
    Headers =
        case
            lists:any(fun({K, _}) -> nhttp_headers:to_lower(K) =:= <<"content-type">> end, Extra)
        of
            true -> Extra;
            false -> [{<<"content-type">>, <<"text/plain">>} | Extra]
        end,
    #{status => Status, headers => Headers, body => ?BODY}.

values(Name, Headers) ->
    [V || {K, V} <- Headers, nhttp_headers:to_lower(K) =:= Name].

vary_members(Headers) ->
    [
        nhttp_headers:to_lower(string:trim(M))
     || V <- values(<<"vary">>, Headers), M <- binary:split(V, <<",">>, [global])
    ].

start_server(Proto) ->
    {CertFile, KeyFile} = nhttp_test_helpers:certs(),
    Tls = #{tls => #{certfile => CertFile, keyfile => KeyFile}},
    Opts =
        case Proto of
            h1 -> #{versions => [http1_1]};
            h2 -> Tls#{versions => [http2]};
            h3 -> Tls#{versions => [http3]}
        end,
    {ok, Pid} = nhttp:start_link(Opts#{port => 0, handler => ?MODULE}),
    {ok, Port} =
        case Proto of
            h3 -> nhttp:get_port(Pid, quic);
            _ -> nhttp:get_port(Pid)
        end,
    {Pid, Port}.

request(Config, Path, AcceptEncoding) ->
    Port = ?config(port, Config),
    Extra = [{<<"accept-encoding">>, AcceptEncoding}],
    case ?config(proto, Config) of
        h1 -> h1_get(Port, Path, Extra);
        h2 -> h2_get(Port, Path, Extra);
        h3 -> h3_get(Port, Path, Extra)
    end.

h1_get(Port, Path, Extra) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Fields = [[K, <<": ">>, V, <<"\r\n">>] || {K, V} <- Extra],
    ok = gen_tcp:send(Sock, [
        <<"GET ">>,
        Path,
        <<" HTTP/1.1\r\nhost: localhost\r\n">>,
        Fields,
        <<"\r\n">>
    ]),
    {Status, Headers, Rest} = h1_recv_head(Sock, <<>>),
    Body =
        case nhttp_headers:get(<<"content-length">>, Headers) of
            undefined -> Rest;
            Length -> h1_recv_body(Sock, binary_to_integer(Length), Rest)
        end,
    ok = gen_tcp:close(Sock),
    {Status, Headers, Body}.

h1_recv_head(Sock, Acc) ->
    {ok, Data} = gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT),
    Raw = <<Acc/binary, Data/binary>>,
    case nhttp_h1:parse_response_head(Raw) of
        {ok, Status, _Reason, _Version, Headers, Rest} -> {Status, Headers, Rest};
        {more, _} -> h1_recv_head(Sock, Raw)
    end.

h1_recv_body(_Sock, Length, Acc) when byte_size(Acc) >= Length ->
    Acc;
h1_recv_body(Sock, Length, Acc) ->
    {ok, Data} = gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT),
    h1_recv_body(Sock, Length, <<Acc/binary, Data/binary>>).

h2_get(Port, Path, Extra) ->
    {ok, Sock} = nhttp_test_helpers:h2_connect(Port),
    {ok, Enc} = nhttp_hpack:new(),
    Request = [
        {<<":method">>, <<"GET">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path}
        | Extra
    ],
    {ok, IoList, _Enc1} = nhttp_hpack:encode(Request, Enc),
    Block = iolist_to_binary(IoList),
    ok = nhttp_test_helpers:h2_send_raw(
        Sock, <<(byte_size(Block)):24, 1, 5, 0:1, 1:31, Block/binary>>
    ),
    Frames = nhttp_test_helpers:h2_recv_stream(Sock, 1, ?RECV_TIMEOUT),
    ok = ssl:close(Sock),
    [HeaderBlock | _] = [P || {headers, 1, P, _} <- Frames],
    {ok, Dec} = nhttp_hpack:new(),
    {ok, Fields, _Dec1} = nhttp_hpack:decode(HeaderBlock, Dec),
    {<<":status">>, Status} = lists:keyfind(<<":status">>, 1, Fields),
    Headers = [F || {<<C, _/binary>>, _} = F <- Fields, C =/= $:],
    {binary_to_integer(Status), Headers, nhttp_test_helpers:h2_response_body(Frames, 1)}.

h3_get(Port, Path, Extra) ->
    {QConn, H3} = nhttp_h3_test_client:connect(Port),
    {ok, Status, Headers, Body, _H3} =
        nhttp_h3_test_client:request(QConn, H3, <<"GET">>, Path, Extra, <<>>),
    ok = nhttp_h3_test_client:close(QConn),
    {Status, Headers, Body}.
