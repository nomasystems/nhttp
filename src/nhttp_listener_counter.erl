-module(nhttp_listener_counter).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    acquire/1,
    count/1,
    new/1,
    release/1,
    reset/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([counter/0]).

-type counter() :: atomics:atomics_ref().

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(COUNTER_INDEX, 1).
-define(MAX_INDEX, 2).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Try to acquire a connection slot.
Returns `ok` on success, or `nhttp_error:at_capacity()` (which is
`{error, {server, #{type := at_capacity}}}`) when at max.
""".
-spec acquire(counter()) -> ok | nhttp_error:t().
acquire(AtomicsRef) ->
    Max = atomics:get(AtomicsRef, ?MAX_INDEX),
    case Max of
        -1 ->
            atomics:add(AtomicsRef, ?COUNTER_INDEX, 1),
            ok;
        _ ->
            acquire_with_limit(AtomicsRef, Max)
    end.

-doc "Get the current connection count.".
-spec count(counter()) -> non_neg_integer().
count(AtomicsRef) ->
    max(0, atomics:get(AtomicsRef, ?COUNTER_INDEX)).

-doc """
Create a new connection counter.
Max can be `infinity` for unlimited connections.
""".
-spec new(pos_integer() | infinity) -> counter().
new(infinity) ->
    AtomicsRef = atomics:new(2, [{signed, true}]),
    atomics:put(AtomicsRef, ?COUNTER_INDEX, 0),
    atomics:put(AtomicsRef, ?MAX_INDEX, -1),
    AtomicsRef;
new(Max) when is_integer(Max), Max > 0 ->
    AtomicsRef = atomics:new(2, [{signed, true}]),
    atomics:put(AtomicsRef, ?COUNTER_INDEX, 0),
    atomics:put(AtomicsRef, ?MAX_INDEX, Max),
    AtomicsRef.

-doc "Release a connection slot.".
-spec release(counter()) -> ok.
release(AtomicsRef) ->
    release_cas(AtomicsRef).

-doc """
Reset the counter to zero.
Used after a catastrophic restart of the listener subtree where every
tracked connection has been killed.
""".
-spec reset(counter()) -> ok.
reset(AtomicsRef) ->
    atomics:put(AtomicsRef, ?COUNTER_INDEX, 0),
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec acquire_with_limit(atomics:atomics_ref(), pos_integer()) -> ok | nhttp_error:t().
acquire_with_limit(AtomicsRef, Max) ->
    Current = atomics:get(AtomicsRef, ?COUNTER_INDEX),
    case Current >= Max of
        true ->
            nhttp_error:at_capacity();
        false ->
            case atomics:compare_exchange(AtomicsRef, ?COUNTER_INDEX, Current, Current + 1) of
                ok ->
                    ok;
                _ActualValue ->
                    acquire_with_limit(AtomicsRef, Max)
            end
    end.

-spec release_cas(atomics:atomics_ref()) -> ok.
release_cas(AtomicsRef) ->
    Current = atomics:get(AtomicsRef, ?COUNTER_INDEX),
    case Current > 0 of
        true ->
            case atomics:compare_exchange(AtomicsRef, ?COUNTER_INDEX, Current, Current - 1) of
                ok ->
                    ok;
                _ActualValue ->
                    release_cas(AtomicsRef)
            end;
        false ->
            ok
    end.
