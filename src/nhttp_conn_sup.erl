-module(nhttp_conn_sup).

-moduledoc false.

-behaviour(supervisor).

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_defaults.hrl").

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    start_conn/3,
    start_link/2
]).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Start a connection child.

The `Args` list is appended to the child spec's args. For `nhttp_conn`
it must be `[Name, Socket, Opts]`, for `nhttp_conn_h3` `[Name, Ctx, Opts]`.
`Name` is the listener name (or `undefined`), carried only for diagnostics.
On success the conn is registered with the listener's tracker so its
counter slot is released when it exits.
""".
-spec start_conn(pid(), pid(), [term()]) -> {ok, pid()} | {error, term()}.
start_conn(ConnSupPid, TrackerPid, Args) ->
    case supervisor:start_child(ConnSupPid, Args) of
        {ok, Pid} ->
            ok = nhttp_conn_tracker:track(TrackerPid, Pid),
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.

-spec start_link(nhttp_registry:tab(), module()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Tab, ConnModule) ->
    supervisor:start_link(?MODULE, {Tab, ConnModule}).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({nhttp_registry:tab(), module()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Tab, ConnModule}) ->
    ok = nhttp_registry:register_conn_sup(Tab, self()),
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,
        period => 1
    },
    ChildSpec = #{
        id => ConnModule,
        start => {ConnModule, start_link_supervised, []},
        restart => temporary,
        shutdown => ?WORKER_SHUTDOWN_TIMEOUT,
        type => worker,
        modules => [ConnModule]
    },
    {ok, {SupFlags, [ChildSpec]}}.
