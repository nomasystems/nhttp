-module(nhttp_conn_h2_push).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include("nhttp_status_codes.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    validate_h2_stream_push/2
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Decide whether a `stream_push` response is admissible for this request.

Returns:
- `ok` to proceed with HEADERS-nofin and start streaming chunks;
- `ok_head` to send HEADERS-fin (the request was HEAD, no body) and the
  caller must abort the producer worker;
- `{reject, Reason}` to send a 500 instead. `Reason` is logged via
  `nhttp_log:stream_push_rejected/4`.
""".
-spec validate_h2_stream_push(nhttp_lib:request(), nhttp_lib:status()) ->
    ok | ok_head | {reject, term()}.
validate_h2_stream_push(_Request, Status) when
    Status =:= ?HTTP_NO_CONTENT; Status =:= ?HTTP_NOT_MODIFIED
->
    {reject, {no_body_status, Status}};
validate_h2_stream_push(#{method := head}, _Status) ->
    ok_head;
validate_h2_stream_push(_Request, _Status) ->
    ok.
