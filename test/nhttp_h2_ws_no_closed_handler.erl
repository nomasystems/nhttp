-module(nhttp_h2_ws_no_closed_handler).

-moduledoc """
Minimal WebSocket test handler that upgrades and notifies an observer on
open but deliberately does NOT export `handle_ws_closed/3`.

Used to exercise the H2 RST_STREAM branch where
`erlang:function_exported(Handler, handle_ws_closed, 3)` is `false`: the
connection drops the stream without invoking any close callback.

Configured via `handler_args = #{observer => Pid|Name}`.
""".

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2,
    handle_ws_open/2,
    handle_ws_frame/3
]).

-spec init(map() | term()) -> {ok, map()}.
init(Args) when is_map(Args) ->
    {ok, #{observer => maps:get(observer, Args, undefined)}};
init(_Args) ->
    {ok, #{observer => undefined}}.

-spec handle_request(nhttp_lib:request(), map()) ->
    {upgrade, websocket, map()} | {reply, nhttp_lib:response(), map()}.
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, #{status => 404, headers => [], body => <<"not found">>}, State}.

-spec handle_ws_open(nhttp_ws:session(), map()) -> {ok, map()}.
handle_ws_open(Session, #{observer := Obs} = State) ->
    notify(Obs, {open, Session}),
    {ok, State#{session => Session}}.

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), map()) ->
    nhttp_handler:ws_result(map()).
handle_ws_frame({Type, Data}, _Session, State) when Type =:= text orelse Type =:= binary ->
    {reply, {Type, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

-spec notify(undefined | pid() | atom(), term()) -> ok.
notify(undefined, _Msg) ->
    ok;
notify(Pid, Msg) when is_pid(Pid) ->
    Pid ! {ws, self(), Msg},
    ok;
notify(Name, Msg) when is_atom(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid -> Pid ! {ws, self(), Msg}
    end,
    ok.
