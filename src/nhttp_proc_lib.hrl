%%%-----------------------------------------------------------------------------
%%% Internal proc_lib / sys helpers for nhttp.
%%%
%%% Centralises boilerplate that every nhttp proc_lib process repeats
%%% verbatim. Module-local `-spec` declarations stay in each module so
%%% dialyzer keeps a per-module surface (the macro cannot carry a spec
%%% without losing the surrounding `#state{}` record).
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_PROC_LIB_HRL).
-define(NHTTP_PROC_LIB_HRL, true).

-define(NHTTP_SYSTEM_CODE_CHANGE_NOOP,
    system_code_change(State, _Module, _OldVsn, _Extra) ->
        {ok, State}
).

-endif.
