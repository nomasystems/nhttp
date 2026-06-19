-module(nhttp_h1_ws_async_handler).

-moduledoc """
Test handler for the phase-3 H1 async WebSocket dispatch.

Forwards every lifecycle event to a configured observer pid (or registered
name) so the test can observe `handle_ws_open`, `handle_ws_frame`,
`handle_ws_info`, `handle_ws_closed`, and `terminate` from outside the
connection process.

Configured via `handler_args = #{observer => Pid|Name, mode => Mode}` on
the listener, where `Mode` selects per-test variations:

- `echo`         (default):echoes text/binary frames back.
- `crash_text`  :`handle_ws_frame/3` raises on a text frame.
- `deliver_ping`:sets `deliver_ping => true` so PING frames flow through
                   `handle_ws_frame/3` in addition to the auto-PONG.
- `deliver_pong`:sets `deliver_pong => true` so PONG frames are observed.
- `max_msg_256` :sets `max_message_size => 256` to trip the §7.4.1 1009.
- `close_on_text`: `handle_ws_frame/3` returns `{close, 4000, _, _}` on
                   text, exercising the local-close path.
- `stop_on_text` : `handle_ws_frame/3` returns `{stop, stop_requested, _}`
                   on text, exercising the handler-stop close path (1001).
- `multi_reply` :`handle_ws_frame/3` returns `{reply, [F1, F2], _}`
                   (list of frames in one return), exercising the list
                   branch of `apply_ws_result/3`.
- `plain_open`  :`handle_ws_open/2` returns `{ok, State}` without the
                   optional runtime-opts map.
- `session_opts_upgrade`: `handle_request/2` returns the 4-tuple
                   `{upgrade, websocket, SessionOpts, State}` with a
                   non-empty opts map (`subprotocol` + `extensions`),
                   exercising the `connect/3` opts path and the
                   subprotocol/extension response-header branches.
- `crash_on_closed`: `handle_ws_closed/3` raises, exercising the
                   handler-crash branch of the H2 RST_STREAM path.
""".

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2,
    handle_ws_open/2,
    handle_ws_frame/3,
    handle_ws_info/3,
    handle_ws_closed/3,
    terminate/2
]).

-spec init(map() | term()) -> {ok, map()}.
init(Args) when is_map(Args) ->
    {ok, #{
        observer => maps:get(observer, Args, undefined),
        mode => maps:get(mode, Args, echo)
    }};
init(_Args) ->
    {ok, #{observer => undefined, mode => echo}}.

-spec handle_request(nhttp_lib:request(), map()) ->
    {upgrade, websocket, map()}
    | {upgrade, websocket, nhttp_ws:ws_session_opts(), map()}
    | {reply, nhttp_lib:response(), map()}.
handle_request(#{path := <<"/ws">>}, #{mode := session_opts_upgrade} = State) ->
    {upgrade, websocket, #{subprotocol => <<"chat">>, extensions => [<<"x-foo">>]}, State};
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, #{status => 404, headers => [], body => <<"not found">>}, State}.

-spec handle_ws_open(nhttp_ws:session(), map()) ->
    {ok, map(), nhttp_ws:ws_runtime_opts()} | {ok, map()}.
handle_ws_open(Session, #{observer := Obs, mode := plain_open} = State) ->
    notify(Obs, {open, Session}),
    {ok, State#{session => Session}};
handle_ws_open(Session, #{observer := Obs, mode := Mode} = State) ->
    notify(Obs, {open, Session}),
    Opts = runtime_opts_for(Mode),
    {ok, State#{session => Session}, Opts}.

-spec handle_ws_frame(nhttp_ws:ws_frame(), nhttp_ws:session(), map()) ->
    nhttp_handler:ws_result(map()).
handle_ws_frame(_Frame, _Session, #{mode := crash_text} = _State) ->
    erlang:error(intentional_crash);
handle_ws_frame({text, _Data}, _Session, #{mode := close_on_text} = State) ->
    {close, 4000, <<"closing on text">>, State};
handle_ws_frame({text, _Data}, _Session, #{mode := stop_on_text} = State) ->
    {stop, stop_requested, State};
handle_ws_frame({text, Data}, _Session, #{mode := multi_reply} = State) ->
    {reply, [{text, <<"first:", Data/binary>>}, {text, <<"second:", Data/binary>>}], State};
handle_ws_frame({Type, Data} = Frame, _Session, #{observer := Obs, mode := echo} = State) when
    Type =:= text orelse Type =:= binary
->
    notify(Obs, {frame, Frame}),
    {reply, {Type, Data}, State};
handle_ws_frame(Frame, _Session, #{observer := Obs} = State) ->
    notify(Obs, {frame, Frame}),
    {ok, State}.

-spec handle_ws_info(term(), nhttp_ws:session(), map()) -> nhttp_handler:ws_result(map()).
handle_ws_info({push_text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_info(Other, _Session, #{observer := Obs} = State) ->
    notify(Obs, {info, Other}),
    {ok, State}.

-spec handle_ws_closed(nhttp_handler:ws_close_reason(), nhttp_ws:session(), map()) -> ok.
handle_ws_closed(_Reason, _Session, #{mode := crash_on_closed}) ->
    erlang:error(intentional_closed_crash);
handle_ws_closed(Reason, _Session, #{observer := Obs}) ->
    notify(Obs, {closed, Reason}),
    ok.

-spec terminate(term(), map()) -> ok.
terminate(_Reason, #{observer := Obs}) ->
    notify(Obs, terminated),
    ok;
terminate(_Reason, _State) ->
    ok.

-spec runtime_opts_for(atom()) -> nhttp_ws:ws_runtime_opts().
runtime_opts_for(deliver_ping) -> #{deliver_ping => true};
runtime_opts_for(deliver_pong) -> #{deliver_pong => true};
runtime_opts_for(max_msg_256) -> #{max_message_size => 256};
runtime_opts_for(_) -> #{}.

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
