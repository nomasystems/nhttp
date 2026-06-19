-module(nhttp_sse).

-moduledoc """
Server-Sent Events (SSE) helpers for nhttp.

Provides convenience functions for building SSE responses per the
W3C Server-Sent Events specification.

## Example Usage

```erlang
handle_request(#{path := <<"/events">>}, State) ->
    Headers = nhttp_sse:headers(),
    Producer = fun(SendChunk) ->
        loop(SendChunk)
    end,
    Spec = nhttp_stream:producer(200, Headers, Producer),
    {stream, Spec, State}.

loop(SendChunk) ->
    Event = nhttp_sse:event(<<"update">>, <<"new data arrived">>),
    case SendChunk(Event) of
        ok -> loop(SendChunk);
        {error, _} -> ok
    end.
```
""".

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-export([
    data/1,
    event/1,
    event/2,
    headers/0,
    headers/1,
    id/1,
    retry/1
]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Encode a data field, properly handling multi-line data.
Each line of data is prefixed with "data: " per the SSE spec.
""".
-spec data(iodata()) -> iolist().
data(Data) when is_binary(Data) ->
    encode_data_lines(Data);
data(Data) ->
    encode_data_lines(iolist_to_binary(Data)).

-doc """
Encode a data-only event (no event type).
The data field may contain newlines. They will be properly encoded.
""".
-spec event(iodata()) -> iolist().
event(Data) ->
    data(Data).

-doc """
Encode a named event with data.
The event type should not contain newlines.
""".
-spec event(iodata(), iodata()) -> iolist().
event(EventType, Data) ->
    [<<"event: ">>, EventType, <<"\n">>, data(Data)].

-doc "Returns the required headers for an SSE response.".
-spec headers() -> [{binary(), binary()}].
headers() ->
    [
        {<<"content-type">>, <<"text/event-stream">>},
        {<<"cache-control">>, <<"no-cache">>}
    ].

-doc "Returns SSE headers merged with additional headers.".
-spec headers([{binary(), binary()}]) -> [{binary(), binary()}].
headers(Extra) ->
    headers() ++ Extra.

-doc """
Encode an event ID field.
The id should not contain newlines.
""".
-spec id(iodata()) -> iolist().
id(Id) ->
    [<<"id: ">>, Id, <<"\n">>].

-doc "Encode a retry field (reconnection time in milliseconds).".
-spec retry(non_neg_integer()) -> iolist().
retry(Ms) when is_integer(Ms), Ms >= 0 ->
    [<<"retry: ">>, integer_to_binary(Ms), <<"\n">>].

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec encode_data_lines(binary()) -> iolist().
encode_data_lines(Data) ->
    Lines = binary:split(Data, <<"\n">>, [global]),
    encode_lines(Lines, []).

-spec encode_lines([binary()], iolist()) -> iolist().
encode_lines([Line], Acc) ->
    lists:reverse([<<"\n\n">>, Line, <<"data: ">> | Acc]);
encode_lines([Line | Rest], Acc) ->
    encode_lines(Rest, [<<"\n">>, Line, <<"data: ">> | Acc]).
