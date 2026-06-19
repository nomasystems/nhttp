-module(nhttp_h3_test_client).

-moduledoc """
Synchronous HTTP/3 test client over a `nquic_ctx_driver' owner loop.

The driver owns a production `nquic:ctx()' and exposes a blocking
per-stream API (`connect/recv/accept_stream/send/...'). This module
layers the `nhttp_h3' client state machine on top: it opens the three
local control/QPACK streams, drains the server preamble, and pumps the
response stream while interleaving the server's unidirectional streams
(SETTINGS / QPACK encoder-decoder) so header decode never deadlocks.

Server-initiated unidirectional stream ids are discovered lazily via
`nquic_ctx_driver:accept_stream/2' and remembered in the process
dictionary keyed by the driver pid, so every pump cycle keeps feeding
them to `nhttp_h3'.
""".

-export([
    connect/1,
    connect/2,
    connect_raw/1,
    drain/4,
    close/1,
    request/5,
    request/6,
    open_request/7,
    send_h3_headers/5,
    send_h3_data/5,
    open_stream/2,
    send_raw/3,
    send_raw_fin/3,
    reset_stream/3,
    recv_response/4,
    recv_response_with_trailers/4,
    recv_headers/4,
    recv_events/4,
    collect_responses/4,
    drain_some_data/4,
    expect_connection_close/2,
    execute_actions/2
]).

-export_type([client/0]).

-type client() :: nquic_ctx_driver:driver().
-type h3() :: nhttp_h3:conn().
-type headers() :: [{binary(), binary()}].

-record(acc, {
    status :: non_neg_integer() | undefined,
    headers = [] :: headers(),
    body = [] :: [binary()],
    trailers = undefined :: headers() | undefined,
    done = false :: boolean(),
    reset = undefined :: non_neg_integer() | undefined
}).

-define(PREAMBLE_BUDGET_MS, 500).
-define(BLOCK_TICK_MS, 100).

%%%-----------------------------------------------------------------------------
%% CONNECTION
%%%-----------------------------------------------------------------------------

-spec connect(inet:port_number()) -> {client(), h3()}.
connect(Port) ->
    connect(Port, #{}).

-spec connect(inet:port_number(), map()) -> {client(), h3()}.
connect(Port, ExtraOpts) ->
    Base = #{timeout => 5000, tls => #{alpn => [<<"h3">>], verify => verify_none}},
    Opts = maps:merge(Base, ExtraOpts),
    {ok, Drv} = nquic_ctx_driver:connect("localhost", Port, Opts),
    H3 = nhttp_h3:new(client, #{}),
    {ok, CtrlId} = nquic_ctx_driver:open_stream(Drv, #{type => uni}),
    {ok, EncId} = nquic_ctx_driver:open_stream(Drv, #{type => uni}),
    {ok, DecId} = nquic_ctx_driver:open_stream(Drv, #{type => uni}),
    {ok, H3_1, InitActions} = nhttp_h3:init_local_streams(H3, #{
        control => CtrlId, encoder => EncId, decoder => DecId
    }),
    ok = execute_actions(Drv, InitActions),
    set_unis(Drv, []),
    H3_2 = drain_preamble(Drv, H3_1, deadline(?PREAMBLE_BUDGET_MS)),
    {Drv, H3_2}.

-spec connect_raw(inet:port_number()) -> {ok, client()} | {error, term()}.
connect_raw(Port) ->
    nquic_ctx_driver:connect(
        "localhost", Port, #{timeout => 5000, tls => #{alpn => [<<"h3">>], verify => verify_none}}
    ).

-spec drain(client(), h3(), nhttp_lib:stream_id(), timeout()) -> h3().
drain(Drv, H3, StreamId, Timeout) ->
    drain_loop(Drv, H3, StreamId, deadline(Timeout)).

drain_loop(Drv, H3, StreamId, DeadlineT) ->
    case remaining(DeadlineT) =< 0 of
        true ->
            H3;
        false ->
            T = min(?BLOCK_TICK_MS, max(1, remaining(DeadlineT))),
            case recv_events(Drv, H3, StreamId, T) of
                {ok, _Events, H3_1} -> drain_loop(Drv, H3_1, StreamId, DeadlineT);
                {error, closed} -> H3;
                {error, _} -> drain_loop(Drv, H3, StreamId, DeadlineT)
            end
    end.

-spec close(client()) -> ok.
close(Drv) ->
    forget_unis(Drv),
    nquic_ctx_driver:close(Drv).

%%%-----------------------------------------------------------------------------
%% REQUESTS
%%%-----------------------------------------------------------------------------

-spec request(client(), h3(), binary(), binary(), binary()) ->
    {ok, non_neg_integer(), headers(), binary(), h3()} | {error, term()}.
request(Drv, H3, Method, Path, Body) ->
    request(Drv, H3, Method, Path, [], Body).

-spec request(client(), h3(), binary(), binary(), headers(), binary()) ->
    {ok, non_neg_integer(), headers(), binary(), h3()} | {error, term()}.
request(Drv, H3, Method, Path, ExtraHeaders, Body) ->
    case open_request(Drv, H3, Method, Path, ExtraHeaders, Body, fin) of
        {ok, StreamId, H3_1} ->
            recv_response(Drv, H3_1, StreamId, 5000);
        {error, _} = Err ->
            Err
    end.

-spec open_request(client(), h3(), binary(), binary(), headers(), binary(), nhttp_h3:fin()) ->
    {ok, nhttp_lib:stream_id(), h3()} | {error, term()}.
open_request(Drv, H3, Method, Path, ExtraHeaders, Body, Fin) ->
    {ok, StreamId} = nquic_ctx_driver:open_stream(Drv, #{type => bidi}),
    Headers =
        [
            {<<":method">>, Method},
            {<<":path">>, Path},
            {<<":scheme">>, <<"https">>},
            {<<":authority">>, <<"localhost">>}
        ] ++ ExtraHeaders,
    HeaderFin =
        case Body of
            <<>> -> Fin;
            _ -> nofin
        end,
    {ok, H3_1, HeaderActions} = nhttp_h3:send_headers(H3, StreamId, Headers, HeaderFin),
    ok = execute_actions(Drv, HeaderActions),
    case Body of
        <<>> ->
            {ok, StreamId, H3_1};
        _ ->
            {ok, H3_2, DataActions} = nhttp_h3:send_data(H3_1, StreamId, Body, Fin),
            ok = execute_actions(Drv, DataActions),
            {ok, StreamId, H3_2}
    end.

-spec send_h3_headers(client(), h3(), nhttp_lib:stream_id(), headers(), nhttp_h3:fin()) ->
    {ok, h3()}.
send_h3_headers(Drv, H3, StreamId, Headers, Fin) ->
    {ok, H3_1, Actions} = nhttp_h3:send_headers(H3, StreamId, Headers, Fin),
    ok = execute_actions(Drv, Actions),
    {ok, H3_1}.

-spec send_h3_data(client(), h3(), nhttp_lib:stream_id(), iodata(), nhttp_h3:fin()) ->
    {ok, h3()}.
send_h3_data(Drv, H3, StreamId, Data, Fin) ->
    {ok, H3_1, Actions} = nhttp_h3:send_data(H3, StreamId, Data, Fin),
    ok = execute_actions(Drv, Actions),
    {ok, H3_1}.

-spec open_stream(client(), bidi | uni) -> {ok, nhttp_lib:stream_id()}.
open_stream(Drv, Type) ->
    nquic_ctx_driver:open_stream(Drv, #{type => Type}).

-spec send_raw(client(), nhttp_lib:stream_id(), iodata()) -> ok | {error, term()}.
send_raw(Drv, StreamId, Data) ->
    nquic_ctx_driver:send(Drv, StreamId, Data).

-spec send_raw_fin(client(), nhttp_lib:stream_id(), iodata()) -> ok | {error, term()}.
send_raw_fin(Drv, StreamId, Data) ->
    nquic_ctx_driver:send_fin(Drv, StreamId, Data).

-spec reset_stream(client(), nhttp_lib:stream_id(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(Drv, StreamId, ErrorCode) ->
    nquic_ctx_driver:reset_stream(Drv, StreamId, ErrorCode).

%%%-----------------------------------------------------------------------------
%% RESPONSE PUMP
%%%-----------------------------------------------------------------------------

-spec recv_response(client(), h3(), nhttp_lib:stream_id(), timeout()) ->
    {ok, non_neg_integer(), headers(), binary(), h3()}
    | {error, {stream_reset, non_neg_integer()}}
    | {error, timeout}
    | {error, closed}.
recv_response(Drv, H3, StreamId, Timeout) ->
    case pump(Drv, H3, StreamId, deadline(Timeout), #acc{}) of
        {ok, #acc{status = S, headers = Hs, body = B}, H3_1} ->
            {ok, S, Hs, iolist_to_binary(lists:reverse(B)), H3_1};
        {error, _} = Err ->
            Err
    end.

-spec recv_response_with_trailers(client(), h3(), nhttp_lib:stream_id(), timeout()) ->
    {ok, non_neg_integer(), headers(), binary(), headers(), h3()}
    | {error, term()}.
recv_response_with_trailers(Drv, H3, StreamId, Timeout) ->
    case pump(Drv, H3, StreamId, deadline(Timeout), #acc{}) of
        {ok, #acc{status = S, headers = Hs, body = B, trailers = T}, H3_1} ->
            Trailers =
                case T of
                    undefined -> [];
                    _ -> T
                end,
            {ok, S, Hs, iolist_to_binary(lists:reverse(B)), Trailers, H3_1};
        {error, _} = Err ->
            Err
    end.

-spec recv_headers(client(), h3(), nhttp_lib:stream_id(), timeout()) ->
    {ok, non_neg_integer(), headers(), h3()} | {error, term()}.
recv_headers(Drv, H3, StreamId, Timeout) ->
    DeadlineT = deadline(Timeout),
    recv_headers_loop(Drv, H3, StreamId, DeadlineT).

recv_headers_loop(Drv, H3, StreamId, DeadlineT) ->
    case recv_events(Drv, H3, StreamId, remaining(DeadlineT)) of
        {ok, Events, H3_1} ->
            case find_response(Events, StreamId) of
                {ok, Status, Headers} ->
                    {ok, Status, Headers, H3_1};
                not_found ->
                    case lists:keymember(stream_reset, 1, Events) of
                        true -> {error, {stream_reset, reset_code(Events, StreamId)}};
                        false -> recv_headers_loop(Drv, H3_1, StreamId, DeadlineT)
                    end
            end;
        {error, _} = Err ->
            Err
    end.

-spec collect_responses(client(), h3(), [nhttp_lib:stream_id()], timeout()) ->
    {#{nhttp_lib:stream_id() => {non_neg_integer(), headers(), binary()}}, h3()}.
collect_responses(Drv, H3, StreamIds, Timeout) ->
    Accs = maps:from_list([{Sid, #acc{}} || Sid <- StreamIds]),
    collect_loop(Drv, H3, deadline(Timeout), Accs).

collect_loop(Drv, H3, DeadlineT, Accs) ->
    Pending = [Sid || {Sid, #acc{done = false}} <- maps:to_list(Accs)],
    case Pending =:= [] orelse remaining(DeadlineT) =< 0 of
        true ->
            {finalize(Accs), H3};
        false ->
            {H3_1, Events} = drain_unis(Drv, H3),
            Accs1 = apply_all(Events, Accs),
            {H3_2, Accs2} = collect_step(Drv, H3_1, Pending, DeadlineT, Accs1),
            collect_loop(Drv, H3_2, DeadlineT, Accs2)
    end.

collect_step(_Drv, H3, [], _DeadlineT, Accs) ->
    {H3, Accs};
collect_step(Drv, H3, [Sid | Rest], DeadlineT, Accs) ->
    case maps:get(Sid, Accs) of
        #acc{done = true} ->
            collect_step(Drv, H3, Rest, DeadlineT, Accs);
        _ ->
            T = min(?BLOCK_TICK_MS, max(0, remaining(DeadlineT))),
            case nquic_ctx_driver:recv(Drv, Sid, T) of
                {ok, Data, FinA} when Data =/= <<>>; FinA =:= fin ->
                    {H3_1, Events} = feed(Drv, H3, Sid, Data, to_fin(FinA)),
                    Accs1 = apply_all(Events, Accs),
                    collect_step(Drv, H3_1, Rest, DeadlineT, Accs1);
                {ok, <<>>, _} ->
                    collect_step(Drv, H3, Rest, DeadlineT, Accs);
                {error, _} ->
                    collect_step(Drv, H3, Rest, DeadlineT, Accs)
            end
    end.

finalize(Accs) ->
    maps:map(
        fun(_Sid, #acc{status = S, headers = Hs, body = B}) ->
            {S, Hs, iolist_to_binary(lists:reverse(B))}
        end,
        Accs
    ).

-spec drain_some_data(client(), h3(), nhttp_lib:stream_id(), timeout()) -> h3().
drain_some_data(Drv, H3, StreamId, Timeout) ->
    DeadlineT = deadline(Timeout),
    drain_some_loop(Drv, H3, StreamId, DeadlineT).

drain_some_loop(Drv, H3, StreamId, DeadlineT) ->
    case remaining(DeadlineT) =< 0 of
        true ->
            H3;
        false ->
            case recv_events(Drv, H3, StreamId, remaining(DeadlineT)) of
                {ok, Events, H3_1} ->
                    case has_payload(Events, StreamId) of
                        true -> H3_1;
                        false -> drain_some_loop(Drv, H3_1, StreamId, DeadlineT)
                    end;
                {error, _} ->
                    H3
            end
    end.

-spec recv_events(client(), h3(), nhttp_lib:stream_id(), timeout()) ->
    {ok, [nhttp_h3:event()], h3()} | {error, timeout} | {error, closed}.
recv_events(Drv, H3, StreamId, Timeout) ->
    DeadlineT = deadline(Timeout),
    {H3_1, UniEvents} = drain_unis(Drv, H3),
    case UniEvents of
        [_ | _] ->
            {ok, UniEvents, H3_1};
        [] ->
            T = min(?BLOCK_TICK_MS, max(0, remaining(DeadlineT))),
            case nquic_ctx_driver:recv(Drv, StreamId, T) of
                {ok, Data, FinA} when Data =/= <<>>; FinA =:= fin ->
                    {H3_2, Events} = feed(Drv, H3_1, StreamId, Data, to_fin(FinA)),
                    {ok, Events, H3_2};
                {ok, <<>>, _} ->
                    recv_events_continue(Drv, H3_1, StreamId, DeadlineT);
                {error, timeout} ->
                    recv_events_continue(Drv, H3_1, StreamId, DeadlineT);
                {error, closed} ->
                    {error, closed};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

recv_events_continue(Drv, H3, StreamId, DeadlineT) ->
    case remaining(DeadlineT) =< 0 of
        true -> {error, timeout};
        false -> recv_events(Drv, H3, StreamId, remaining(DeadlineT))
    end.

-spec expect_connection_close(client(), timeout()) -> ok | {error, no_close}.
expect_connection_close(Drv, Timeout) ->
    DeadlineT = deadline(Timeout),
    expect_close_loop(Drv, DeadlineT).

expect_close_loop(Drv, DeadlineT) ->
    case remaining(DeadlineT) =< 0 of
        true ->
            {error, no_close};
        false ->
            case nquic_ctx_driver:accept_stream(Drv, remaining(DeadlineT)) of
                {error, closed} -> ok;
                {error, timeout} -> {error, no_close};
                {ok, _Sid} -> expect_close_loop(Drv, DeadlineT)
            end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL PUMP ENGINE
%%%-----------------------------------------------------------------------------

pump(Drv, H3, StreamId, DeadlineT, Acc) ->
    {H3_1, UniEvents} = drain_unis(Drv, H3),
    Acc1 = apply_events(UniEvents, StreamId, Acc),
    case done_or_error(Acc1, H3_1) of
        {done, Result} ->
            Result;
        continue ->
            pump_block(Drv, H3_1, StreamId, DeadlineT, Acc1)
    end.

pump_block(Drv, H3, StreamId, DeadlineT, Acc) ->
    case remaining(DeadlineT) =< 0 of
        true ->
            {error, timeout};
        false ->
            T = min(?BLOCK_TICK_MS, max(1, remaining(DeadlineT))),
            case nquic_ctx_driver:recv(Drv, StreamId, T) of
                {ok, Data, FinA} when Data =/= <<>>; FinA =:= fin ->
                    {H3_1, Events} = feed(Drv, H3, StreamId, Data, to_fin(FinA)),
                    Acc1 = apply_events(Events, StreamId, Acc),
                    case done_or_error(Acc1, H3_1) of
                        {done, Result} -> Result;
                        continue -> pump(Drv, H3_1, StreamId, DeadlineT, Acc1)
                    end;
                {ok, <<>>, _} ->
                    pump(Drv, H3, StreamId, DeadlineT, Acc);
                {error, timeout} ->
                    pump(Drv, H3, StreamId, DeadlineT, Acc);
                {error, closed} ->
                    {error, closed};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

done_or_error(#acc{reset = Code}, _H3) when Code =/= undefined ->
    {done, {error, {stream_reset, Code}}};
done_or_error(#acc{done = true} = Acc, H3) ->
    {done, {ok, Acc, H3}};
done_or_error(#acc{}, _H3) ->
    continue.

%%%-----------------------------------------------------------------------------
%% INTERNAL SERVER UNI-STREAM INTERLEAVING
%%%-----------------------------------------------------------------------------

drain_preamble(Drv, H3, DeadlineT) ->
    case remaining(DeadlineT) =< 0 of
        true ->
            H3;
        false ->
            T = min(200, remaining(DeadlineT)),
            case nquic_ctx_driver:accept_stream(Drv, T) of
                {ok, Sid} ->
                    add_uni(Drv, Sid),
                    {H3_1, _Events} = drain_stream(Drv, H3, Sid),
                    drain_preamble(Drv, H3_1, DeadlineT);
                {error, _} ->
                    H3
            end
    end.

drain_unis(Drv, H3) ->
    H3_1 = accept_new_unis(Drv, H3),
    lists:foldl(
        fun(Sid, {H3Acc, EvAcc}) ->
            {H3Acc1, Ev} = drain_stream(Drv, H3Acc, Sid),
            {H3Acc1, EvAcc ++ Ev}
        end,
        {H3_1, []},
        get_unis(Drv)
    ).

accept_new_unis(Drv, H3) ->
    case nquic_ctx_driver:accept_stream(Drv, 0) of
        {ok, Sid} ->
            add_uni(Drv, Sid),
            accept_new_unis(Drv, H3);
        {error, _} ->
            H3
    end.

drain_stream(Drv, H3, Sid) ->
    case nquic_ctx_driver:recv(Drv, Sid, 0) of
        {ok, Data, FinA} when Data =/= <<>>; FinA =:= fin ->
            {H3_1, Events} = feed(Drv, H3, Sid, Data, to_fin(FinA)),
            case FinA of
                fin ->
                    {H3_1, Events};
                nofin ->
                    {H3_2, More} = drain_stream(Drv, H3_1, Sid),
                    {H3_2, Events ++ More}
            end;
        _ ->
            {H3, []}
    end.

feed(Drv, H3, Sid, Data, Fin) ->
    case nhttp_h3:recv(H3, Sid, Data, Fin) of
        {ok, Events, H3_1, Actions} ->
            ok = execute_actions(Drv, Actions),
            {H3_1, Events};
        {error, _Reason} ->
            {H3, []}
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL EVENT ACCUMULATION
%%%-----------------------------------------------------------------------------

apply_events([], _StreamId, Acc) ->
    Acc;
apply_events(
    [{response, Sid, #{status := Status, headers := Hs}, Fin} | Rest], Sid, Acc
) ->
    apply_events(Rest, Sid, Acc#acc{
        status = Status, headers = Hs, done = Acc#acc.done orelse Fin =:= fin
    });
apply_events([{data, Sid, Data, Fin} | Rest], Sid, #acc{body = B} = Acc) ->
    apply_events(Rest, Sid, Acc#acc{
        body = [Data | B], done = Acc#acc.done orelse Fin =:= fin
    });
apply_events([{trailers, Sid, T} | Rest], Sid, Acc) ->
    apply_events(Rest, Sid, Acc#acc{trailers = T, done = true});
apply_events([{stream_reset, Sid, Code} | Rest], Sid, Acc) ->
    apply_events(Rest, Sid, Acc#acc{reset = Code});
apply_events([_ | Rest], Sid, Acc) ->
    apply_events(Rest, Sid, Acc).

apply_all(Events, Accs) ->
    maps:map(fun(Sid, Acc) -> apply_events(Events, Sid, Acc) end, Accs).

find_response([], _Sid) ->
    not_found;
find_response([{response, Sid, #{status := Status, headers := Hs}, _Fin} | _], Sid) ->
    {ok, Status, Hs};
find_response([_ | Rest], Sid) ->
    find_response(Rest, Sid).

reset_code([{stream_reset, Sid, Code} | _], Sid) -> Code;
reset_code([_ | Rest], Sid) -> reset_code(Rest, Sid);
reset_code([], _Sid) -> 0.

has_payload([{data, Sid, _, _} | _], Sid) -> true;
has_payload([{response, Sid, _, _} | _], Sid) -> true;
has_payload([{trailers, Sid, _} | _], Sid) -> true;
has_payload([{stream_reset, Sid, _} | _], Sid) -> true;
has_payload([_ | Rest], Sid) -> has_payload(Rest, Sid);
has_payload([], _Sid) -> false.

%%%-----------------------------------------------------------------------------
%% INTERNAL ACTIONS / HELPERS
%%%-----------------------------------------------------------------------------

-spec execute_actions(client(), [nhttp_h3:action()]) -> ok.
execute_actions(_Drv, []) ->
    ok;
execute_actions(Drv, [{send, StreamId, Data} | Rest]) ->
    ok = nquic_ctx_driver:send(Drv, StreamId, Data),
    execute_actions(Drv, Rest);
execute_actions(Drv, [{send_fin, StreamId, Data} | Rest]) ->
    ok = nquic_ctx_driver:send_fin(Drv, StreamId, Data),
    execute_actions(Drv, Rest);
execute_actions(_Drv, [{close_connection, _Code, _Reason} | _]) ->
    ok.

to_fin(fin) -> fin;
to_fin(nofin) -> nofin;
to_fin(true) -> fin;
to_fin(false) -> nofin.

deadline(infinity) -> infinity;
deadline(Ms) when is_integer(Ms) -> erlang:monotonic_time(millisecond) + Ms.

remaining(infinity) -> infinity;
remaining(Deadline) -> Deadline - erlang:monotonic_time(millisecond).

%%%-----------------------------------------------------------------------------
%% INTERNAL SERVER UNI REGISTRY (per-driver, process dictionary)
%%%-----------------------------------------------------------------------------

uni_key(Drv) -> {?MODULE, unis, Drv}.

set_unis(Drv, Ids) ->
    _ = put(uni_key(Drv), Ids),
    ok.

get_unis(Drv) ->
    case get(uni_key(Drv)) of
        undefined -> [];
        Ids -> Ids
    end.

add_uni(Drv, Sid) ->
    Ids = get_unis(Drv),
    case lists:member(Sid, Ids) of
        true -> ok;
        false -> set_unis(Drv, Ids ++ [Sid])
    end.

forget_unis(Drv) ->
    _ = erase(uni_key(Drv)),
    ok.
