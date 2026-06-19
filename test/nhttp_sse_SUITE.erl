%%%-----------------------------------------------------------------------------
%%% @doc Test suite for nhttp_sse module.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_sse_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        headers_test,
        headers_with_extra_test,
        simple_event_test,
        named_event_test,
        multiline_data_test,
        id_test,
        retry_test,
        full_event_test,
        data_iolist_test
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------

headers_test(_Config) ->
    Headers = nhttp_sse:headers(),
    ?assertEqual(
        [
            {<<"content-type">>, <<"text/event-stream">>},
            {<<"cache-control">>, <<"no-cache">>}
        ],
        Headers
    ).

headers_with_extra_test(_Config) ->
    Extra = [{<<"x-custom">>, <<"value">>}],
    Headers = nhttp_sse:headers(Extra),
    ?assertEqual(
        [
            {<<"content-type">>, <<"text/event-stream">>},
            {<<"cache-control">>, <<"no-cache">>},
            {<<"x-custom">>, <<"value">>}
        ],
        Headers
    ).

simple_event_test(_Config) ->
    Event = nhttp_sse:event(<<"hello">>),
    Bin = iolist_to_binary(Event),
    ?assertEqual(<<"data: hello\n\n">>, Bin).

named_event_test(_Config) ->
    Event = nhttp_sse:event(<<"message">>, <<"hello">>),
    Bin = iolist_to_binary(Event),
    ?assertEqual(<<"event: message\ndata: hello\n\n">>, Bin).

multiline_data_test(_Config) ->
    Event = nhttp_sse:event(<<"line1\nline2\nline3">>),
    Bin = iolist_to_binary(Event),
    ?assertEqual(<<"data: line1\ndata: line2\ndata: line3\n\n">>, Bin).

id_test(_Config) ->
    Id = nhttp_sse:id(<<"12345">>),
    Bin = iolist_to_binary(Id),
    ?assertEqual(<<"id: 12345\n">>, Bin).

retry_test(_Config) ->
    Retry = nhttp_sse:retry(5000),
    Bin = iolist_to_binary(Retry),
    ?assertEqual(<<"retry: 5000\n">>, Bin).

full_event_test(_Config) ->
    Event = [
        nhttp_sse:id(<<"msg-001">>),
        nhttp_sse:event(<<"notification">>, <<"You have mail">>),
        nhttp_sse:retry(3000)
    ],
    Bin = iolist_to_binary(Event),
    Expected = <<"id: msg-001\nevent: notification\ndata: You have mail\n\nretry: 3000\n">>,
    ?assertEqual(Expected, Bin).

data_iolist_test(_Config) ->
    IOList = ["hello", <<" ">>, "world"],
    Event = nhttp_sse:data(IOList),
    Bin = iolist_to_binary(Event),
    ?assertEqual(<<"data: hello world\n\n">>, Bin).
