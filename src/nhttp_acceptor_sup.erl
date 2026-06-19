-module(nhttp_acceptor_sup).

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
    start_link/4
]).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec start_link(
    nhttp_registry:tab(), module(), nhttp:opts(), pos_integer()
) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Tab, AcceptorModule, Opts, Count) ->
    supervisor:start_link(?MODULE, {Tab, AcceptorModule, Opts, Count}).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init(
    {nhttp_registry:tab(), module(), nhttp:opts(), pos_integer()}
) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Tab, AcceptorModule, Opts, Count}) ->
    ok = nhttp_registry:register_acceptor_sup(Tab, self()),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    ChildSpecs = [
        #{
            id => {AcceptorModule, N},
            start => {AcceptorModule, start_link, [Tab, Opts]},
            restart => transient,
            shutdown => ?WORKER_SHUTDOWN_TIMEOUT,
            type => worker,
            modules => [AcceptorModule]
        }
     || N <- lists:seq(1, Count)
    ],
    {ok, {SupFlags, ChildSpecs}}.
