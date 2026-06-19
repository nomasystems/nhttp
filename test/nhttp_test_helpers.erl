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
    h2_send_raw/2,
    h2_send_request/3,
    h2_send_rst_stream/3,
    h2_send_window_update/3,
    tcp_connect/1
]).

-define(DEFAULT_TIMEOUT, 5000).
-define(POLL_INTERVAL, 25).

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
    _ = ssl:recv(Sock, 0, 2000),
    ok = ssl:send(Sock, <<0, 0, 0, 4, 1, 0, 0, 0, 0>>),
    {ok, Sock}.

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

-spec h2_send_window_update(ssl:sslsocket(), non_neg_integer(), non_neg_integer()) -> ok.
h2_send_window_update(Sock, StreamId, Increment) ->
    ssl:send(Sock, <<4:24, 8, 0, 0:1, StreamId:31, 0:1, Increment:31>>).

-spec h2_recv(ssl:sslsocket(), timeout()) -> [h2_frame()].
h2_recv(Sock, Timeout) ->
    h2_recv(Sock, Timeout, <<>>, []).

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

-spec h2_fin(non_neg_integer()) -> fin | nofin.
h2_fin(Flags) when Flags band 1 =:= 1 -> fin;
h2_fin(_Flags) -> nofin.
