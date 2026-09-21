-module(nhttp_test_helpers).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------
-export([
    wait_until/1,
    wait_until/2,
    wait_until_down/1,
    wait_until_down/2,
    which_acceptors/1
]).

-export([
    certs/0,
    conn_pids/1,
    start/1,
    transport_sups/1,
    wait_for_conns/3,
    wait_for_no_conns/2,
    wait_until_acceptors/3
]).

-export([
    decode_h2_frames/1,
    drain_tcp/1,
    drain_tcp/2,
    h2_connect/1,
    h2_open_stream/3,
    h2_recv/2,
    h2_recv_stream/3,
    h2_response_body/2,
    h2_response_status/1,
    h2_send_data/4,
    h2_send_headers/5,
    h2_send_post/4,
    h2_send_raw/2,
    h2_send_request/3,
    h2_send_rst_stream/3,
    h2_send_settings/2,
    h2_send_window_update/3,
    h2_server_settings/1,
    h2_start_server/2,
    h2_stream_done/2,
    tcp_connect/1
]).

-define(DEFAULT_TIMEOUT, 5000).
-define(H2_IDLE_TIMEOUT, 5000).
-define(POLL_INTERVAL, 25).
-define(SETTINGS_RECV_MS, 1000).
-define(STREAM_TAIL_MS, 200).

-type h2_frame() ::
    {data, non_neg_integer(), binary(), fin | nofin}
    | {headers, non_neg_integer(), binary(), fin | nofin}
    | {rst_stream, non_neg_integer(), non_neg_integer()}
    | {settings, non_neg_integer(), binary()}
    | {goaway, non_neg_integer(), binary()}
    | {window_update, non_neg_integer(), non_neg_integer()}
    | {other, non_neg_integer(), non_neg_integer(), binary()}.

-export_type([h2_frame/0]).

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

wait_until(Fun) ->
    wait_until(Fun, ?DEFAULT_TIMEOUT).

wait_until(Fun, Timeout) when Timeout =< 0 ->
    case Fun() of
        true -> ok;
        Other -> error({wait_until_timeout, Other})
    end;
wait_until(Fun, Timeout) ->
    case Fun() of
        true ->
            ok;
        _ ->
            Step = min(?POLL_INTERVAL, Timeout),
            timer:sleep(Step),
            wait_until(Fun, Timeout - Step)
    end.

wait_until_down(Pid) ->
    wait_until_down(Pid, ?DEFAULT_TIMEOUT).

wait_until_down(Pid, Timeout) when is_pid(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, Reason} ->
            {ok, Reason}
    after Timeout ->
        true = erlang:demonitor(Ref, [flush]),
        {error, timeout}
    end.

-spec which_acceptors(pid()) -> [pid()].
which_acceptors(ListenerPid) ->
    lists:append([acceptors_in(TSup) || TSup <- transport_sups(ListenerPid)]).

acceptors_in(TransportSup) ->
    case lists:keyfind(nhttp_acceptor_sup, 1, supervisor:which_children(TransportSup)) of
        {_, AccSupPid, _, _} when is_pid(AccSupPid) ->
            [P || {_, P, worker, _} <- supervisor:which_children(AccSupPid), is_pid(P)];
        _ ->
            []
    end.

-spec transport_sups(pid()) -> [pid()].
transport_sups(ListenerPid) ->
    [
        Pid
     || {_Id, Pid, supervisor, [nhttp_transport_sup]} <- supervisor:which_children(ListenerPid),
        is_pid(Pid)
    ].

%%%-----------------------------------------------------------------------------
%%% SERVER LIFECYCLE
%%%-----------------------------------------------------------------------------

-spec certs() -> {file:filename(), file:filename()}.
certs() ->
    ConfDir = filename:join(filename:dirname(code:which(?MODULE)), "conf"),
    {filename:join(ConfDir, "server.pem"), filename:join(ConfDir, "server.key")}.

-spec start(nhttp:opts()) -> {ok, pid(), inet:port_number()}.
start(Opts) ->
    {ok, Pid} = nhttp:start_link(maps:merge(#{port => 0}, Opts)),
    {ok, Port} = nhttp:get_port(Pid),
    {ok, Pid, Port}.

-doc "Start an HTTP/2-only TLS listener on a free port with the test certificates.".
-spec h2_start_server(module(), nhttp:opts()) -> {pid(), inet:port_number()}.
h2_start_server(Handler, Extra) ->
    {CertFile, KeyFile} = certs(),
    {ok, Pid, Port} = start(
        maps:merge(
            #{
                handler => Handler,
                tls => #{certfile => CertFile, keyfile => KeyFile},
                versions => [http2],
                timeouts => #{idle => ?H2_IDLE_TIMEOUT}
            },
            Extra
        )
    ),
    {Pid, Port}.

-spec conn_pids(pid()) -> [pid()].
conn_pids(ListenerPid) ->
    lists:append([conns_in(TSup) || TSup <- transport_sups(ListenerPid)]).

conns_in(TransportSup) ->
    case lists:keyfind(nhttp_conn_sup, 1, supervisor:which_children(TransportSup)) of
        {_, ConnSupPid, _, _} when is_pid(ConnSupPid) ->
            [P || {_, P, _, _} <- supervisor:which_children(ConnSupPid), is_pid(P)];
        _ ->
            []
    end.

-spec wait_for_conns(pid(), non_neg_integer(), timeout()) -> [pid()].
wait_for_conns(ListenerPid, N, Timeout) ->
    ok = wait_until(fun() -> length(conn_pids(ListenerPid)) =:= N end, Timeout),
    conn_pids(ListenerPid).

-spec wait_for_no_conns(pid(), timeout()) -> ok.
wait_for_no_conns(ListenerPid, Timeout) ->
    wait_until(fun() -> conn_pids(ListenerPid) =:= [] end, Timeout).

-spec wait_until_acceptors(pid(), non_neg_integer(), timeout()) -> [pid()].
wait_until_acceptors(ListenerPid, N, Timeout) ->
    ok = wait_until(fun() -> length(which_acceptors(ListenerPid)) =:= N end, Timeout),
    which_acceptors(ListenerPid).

%%%-----------------------------------------------------------------------------
%%% RAW CLIENTS
%%%-----------------------------------------------------------------------------

-spec tcp_connect(inet:port_number()) -> gen_tcp:socket().
tcp_connect(Port) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    Sock.

-spec drain_tcp(gen_tcp:socket()) -> binary().
drain_tcp(Sock) ->
    drain_tcp(Sock, <<>>).

-spec drain_tcp(gen_tcp:socket(), binary()) -> binary().
drain_tcp(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 1000) of
        {ok, Bin} -> drain_tcp(Sock, <<Acc/binary, Bin/binary>>);
        {error, _} -> Acc
    end.

-spec h2_connect(inet:port_number()) -> {ok, ssl:sslsocket()}.
h2_connect(Port) ->
    {ok, Sock} = h2_connect_preface(Port),
    _ = ssl:recv(Sock, 0, 2000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    {ok, Sock}.

-doc """
Open a connection and return the parameters of the SETTINGS frame the
server sends in its preface, as `{Identifier, Value}` pairs.
""".
-spec h2_server_settings(inet:port_number()) -> [{non_neg_integer(), non_neg_integer()}].
h2_server_settings(Port) ->
    {ok, Sock} = h2_connect_preface(Port),
    Frames = h2_recv(Sock, ?SETTINGS_RECV_MS),
    ok = ssl:close(Sock),
    [{Id, Value} || {settings, 0, Payload} <- Frames, <<Id:16, Value:32>> <= Payload].

-spec h2_send_request(ssl:sslsocket(), non_neg_integer(), binary()) -> ok.
h2_send_request(Sock, StreamId, Path) ->
    Header = h2_header_block(Path),
    Frame = <<(byte_size(Header)):24, 1, 5, 0:1, StreamId:31, Header/binary>>,
    ssl:send(Sock, Frame).

-spec h2_open_stream(ssl:sslsocket(), non_neg_integer(), binary()) -> ok.
h2_open_stream(Sock, StreamId, Path) ->
    Header = h2_header_block(Path),
    Frame = <<(byte_size(Header)):24, 1, 4, 0:1, StreamId:31, Header/binary>>,
    ssl:send(Sock, Frame).

-spec h2_send_raw(ssl:sslsocket(), iodata()) -> ok.
h2_send_raw(Sock, Bytes) ->
    ssl:send(Sock, Bytes).

-spec h2_send_rst_stream(ssl:sslsocket(), non_neg_integer(), non_neg_integer()) -> ok.
h2_send_rst_stream(Sock, StreamId, ErrorCode) ->
    ssl:send(Sock, <<4:24, 3, 0, 0:1, StreamId:31, ErrorCode:32>>).

-spec h2_send_settings(ssl:sslsocket(), [{non_neg_integer(), non_neg_integer()}]) -> ok.
h2_send_settings(Sock, Settings) ->
    Payload = <<<<Id:16, Value:32>> || {Id, Value} <- Settings>>,
    ssl:send(Sock, <<(byte_size(Payload)):24, 4, 0, 0:1, 0:31, Payload/binary>>).

-spec h2_send_window_update(ssl:sslsocket(), non_neg_integer(), non_neg_integer()) -> ok.
h2_send_window_update(Sock, StreamId, Increment) ->
    ssl:send(Sock, <<4:24, 8, 0, 0:1, StreamId:31, 0:1, Increment:31>>).

-spec h2_recv(ssl:sslsocket(), timeout()) -> [h2_frame()].
h2_recv(Sock, Timeout) ->
    h2_recv(Sock, Timeout, <<>>, []).

-doc """
Receive frames until the stream ends (END_STREAM or RST_STREAM), then
drain the socket for a short tail so frames sent right after are seen.
""".
-spec h2_recv_stream(ssl:sslsocket(), non_neg_integer(), timeout()) -> [h2_frame()].
h2_recv_stream(Sock, StreamId, Timeout) ->
    Frames = recv_stream_until_done(Sock, StreamId, Timeout, <<>>, []),
    Frames ++ recv_stream_tail(Sock, <<>>, []).

-spec h2_response_body([h2_frame()], non_neg_integer()) -> binary().
h2_response_body(Frames, StreamId) ->
    iolist_to_binary([P || {data, SId, P, _} <- Frames, SId =:= StreamId]).

-doc "The `:status` of the first HEADERS frame, decoded with a fresh HPACK table.".
-spec h2_response_status([h2_frame()]) -> binary() | undefined.
h2_response_status(Frames) ->
    case [P || {headers, _, P, _} <- Frames] of
        [Block | _] ->
            {ok, Dec} = nhttp_hpack:new(),
            case nhttp_hpack:decode(Block, Dec) of
                {ok, Headers, _} -> proplists:get_value(<<":status">>, Headers);
                {error, _} -> undefined
            end;
        [] ->
            undefined
    end.

-doc "True once `Frames` carries END_STREAM or RST_STREAM for `StreamId`.".
-spec h2_stream_done([h2_frame()], non_neg_integer()) -> boolean().
h2_stream_done(Frames, StreamId) ->
    lists:any(
        fun
            ({data, SId, _, fin}) when SId =:= StreamId -> true;
            ({headers, SId, _, fin}) when SId =:= StreamId -> true;
            ({rst_stream, SId, _}) when SId =:= StreamId -> true;
            (_) -> false
        end,
        Frames
    ).

-spec h2_send_data(ssl:sslsocket(), non_neg_integer(), binary(), boolean()) ->
    ok | {error, term()}.
h2_send_data(Sock, StreamId, Data, EndStream) ->
    Flags =
        case EndStream of
            true -> 16#01;
            false -> 16#00
        end,
    Frame = <<(byte_size(Data)):24, 0, Flags, 0:1, StreamId:31, Data/binary>>,
    ssl:send(Sock, Frame).

-doc "Send a POST HEADERS frame with `content-length` set to `Length`.".
-spec h2_send_headers(
    ssl:sslsocket(), non_neg_integer(), binary(), non_neg_integer(), boolean()
) -> ok | {error, term()}.
h2_send_headers(Sock, StreamId, Path, Length, EndStream) ->
    {ok, Enc} = nhttp_hpack:new(),
    Headers = [
        {<<":method">>, <<"POST">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path},
        {<<"content-length">>, integer_to_binary(Length)}
    ],
    {ok, IOList, _Enc1} = nhttp_hpack:encode(Headers, Enc),
    Block = iolist_to_binary(IOList),
    Flags =
        case EndStream of
            true -> 16#05;
            false -> 16#04
        end,
    Frame = <<(byte_size(Block)):24, 1, Flags, 0:1, StreamId:31, Block/binary>>,
    ssl:send(Sock, Frame).

-spec h2_send_post(ssl:sslsocket(), non_neg_integer(), binary(), binary()) ->
    ok | {error, term()}.
h2_send_post(Sock, StreamId, Path, Body) ->
    ok = h2_send_headers(Sock, StreamId, Path, byte_size(Body), false),
    h2_send_data(Sock, StreamId, Body, true).

-spec decode_h2_frames(binary()) -> {[h2_frame()], binary()}.
decode_h2_frames(Bin) ->
    decode_h2_frames(Bin, []).

%%%-----------------------------------------------------------------------------
%%% INTERNAL
%%%-----------------------------------------------------------------------------

-spec h2_header_block(binary()) -> binary().
h2_header_block(Path) ->
    PathLen = byte_size(Path),
    <<16#82, 16#87, 16#44, PathLen, Path/binary>>.

-spec h2_recv(ssl:sslsocket(), timeout(), binary(), [h2_frame()]) -> [h2_frame()].
h2_recv(_Sock, Timeout, Buf, Acc) when Timeout =< 0 ->
    {Frames, _Rest} = decode_h2_frames(Buf),
    Acc ++ Frames;
h2_recv(Sock, Timeout, Buf, Acc) ->
    T0 = erlang:monotonic_time(millisecond),
    case ssl:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            {Frames, Rest} = decode_h2_frames(<<Buf/binary, Data/binary>>),
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            h2_recv(Sock, Timeout - Elapsed, Rest, Acc ++ Frames);
        {error, _} ->
            {Frames, _Rest} = decode_h2_frames(Buf),
            Acc ++ Frames
    end.

-spec decode_h2_frames(binary(), [h2_frame()]) -> {[h2_frame()], binary()}.
decode_h2_frames(
    <<Len:24, Type:8, Flags:8, _R:1, StreamId:31, Payload:Len/binary, Rest/binary>>, Acc
) ->
    decode_h2_frames(Rest, [decode_h2_frame(Type, Flags, StreamId, Payload) | Acc]);
decode_h2_frames(Other, Acc) ->
    {lists:reverse(Acc), Other}.

-spec decode_h2_frame(non_neg_integer(), non_neg_integer(), non_neg_integer(), binary()) ->
    h2_frame().
decode_h2_frame(0, Flags, StreamId, Payload) ->
    {data, StreamId, Payload, h2_fin(Flags)};
decode_h2_frame(1, Flags, StreamId, Payload) ->
    {headers, StreamId, Payload, h2_fin(Flags)};
decode_h2_frame(3, _Flags, StreamId, <<Code:32>>) ->
    {rst_stream, StreamId, Code};
decode_h2_frame(4, _Flags, StreamId, Payload) ->
    {settings, StreamId, Payload};
decode_h2_frame(7, _Flags, StreamId, Payload) ->
    {goaway, StreamId, Payload};
decode_h2_frame(8, _Flags, StreamId, <<Inc:32>>) ->
    {window_update, StreamId, Inc};
decode_h2_frame(Type, _Flags, StreamId, Payload) ->
    {other, StreamId, Type, Payload}.

-spec h2_connect_preface(inet:port_number()) -> {ok, ssl:sslsocket()}.
h2_connect_preface(Port) ->
    {ok, Sock} = ssl:connect(
        "127.0.0.1",
        Port,
        [
            binary,
            {active, false},
            {verify, verify_none},
            {alpn_advertised_protocols, [<<"h2">>]}
        ],
        5000
    ),
    ok = ssl:send(Sock, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 0, 0, 0, 0, 0>>),
    {ok, Sock}.

-spec h2_fin(non_neg_integer()) -> fin | nofin.
h2_fin(Flags) when Flags band 1 =:= 1 -> fin;
h2_fin(_Flags) -> nofin.

-spec recv_stream_until_done(
    ssl:sslsocket(), non_neg_integer(), timeout(), binary(), [h2_frame()]
) -> [h2_frame()].
recv_stream_until_done(Sock, StreamId, Timeout, Buf, Acc) ->
    case h2_stream_done(Acc, StreamId) of
        true ->
            Acc;
        false ->
            case ssl:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    {Frames, Rest} = decode_h2_frames(<<Buf/binary, Data/binary>>),
                    recv_stream_until_done(Sock, StreamId, Timeout, Rest, Acc ++ Frames);
                {error, _} ->
                    Acc
            end
    end.

-spec recv_stream_tail(ssl:sslsocket(), binary(), [h2_frame()]) -> [h2_frame()].
recv_stream_tail(Sock, Buf, Acc) ->
    case ssl:recv(Sock, 0, ?STREAM_TAIL_MS) of
        {ok, Data} ->
            {Frames, Rest} = decode_h2_frames(<<Buf/binary, Data/binary>>),
            recv_stream_tail(Sock, Rest, Acc ++ Frames);
        {error, _} ->
            Acc
    end.
