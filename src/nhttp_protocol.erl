-module(nhttp_protocol).

-moduledoc """
Behaviour for nhttp's proc_lib protocol loop modules.

A protocol module owns one phase of a connection process's lifetime:
`nhttp_conn` (pre-classification socket wait), `nhttp_conn_h1`,
`nhttp_conn_h2`, `nhttp_conn_h3`, and `nhttp_conn_ws_h1` (post-upgrade
WebSocket session). The connection process never changes pid across
phases; control is handed from one module's loop to the next by a tail
call into `loop/3`.

`loop/3` is the (re-)entry point of the module's receive loop:
`nhttp_conn` tail-calls it after protocol classification, and
`proc_lib:hibernate/3` wake-ups re-enter through it or a module wake
function that converges on it. The state term is owned by the module
that is currently looping; callers treat it as opaque.

The three `system_*` callbacks are the `sys` contract for a special
process (see `sys:handle_system_msg/6`, called with the looping module
as the callback module): `system_continue/3` must re-enter `loop/3`,
`system_terminate/4` must run the connection's termination path and
exit, and `system_code_change/4` returns the (unchanged) state via the
`?NHTTP_SYSTEM_CODE_CHANGE_NOOP` macro from `nhttp_proc_lib.hrl`.
""".

-callback loop(Parent :: pid(), Debug :: [sys:debug_option()], State :: term()) ->
    no_return().

-callback system_continue(Parent :: pid(), Debug :: [sys:debug_option()], State :: term()) ->
    no_return().

-callback system_terminate(
    Reason :: term(), Parent :: pid(), Debug :: [sys:debug_option()], State :: term()
) -> no_return().

-callback system_code_change(
    State :: term(), Module :: module(), OldVsn :: term(), Extra :: term()
) -> {ok, NewState :: term()}.
