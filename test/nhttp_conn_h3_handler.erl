%%%-----------------------------------------------------------------------------
%%% @doc Test handler for nhttp_conn_h3_SUITE.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_conn_h3_handler).

-behaviour(nhttp_handler).

-export([
    init/1,
    handle_request/2,
    handle_request_body/3,
    handle_ws_frame/3,
    handle_ws_closed/3,
    terminate/2
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

init(fail) ->
    {error, init_failure};
init(Args) ->
    {ok, Args}.

handle_request(#{path := <<"/hello">>}, State) ->
    {reply, nhttp_resp:ok(<<"Hello!">>), State};
handle_request(#{path := <<"/echo">>}, State) ->
    {accept_body, [], State};
handle_request(#{path := <<"/error">>}, State) ->
    {abort, handler_error, State};
handle_request(#{path := <<"/crash-error">>}, _State) ->
    erlang:error(intentional_handler_error);
handle_request(#{path := <<"/crash-exit">>}, _State) ->
    exit(intentional_handler_exit);
handle_request(#{path := <<"/crash-throw">>}, _State) ->
    throw(intentional_handler_throw);
handle_request(#{path := <<"/empty">>}, State) ->
    {reply, #{status => 200, headers => [], body => <<>>}, State};
handle_request(#{path := <<"/large">>}, State) ->
    Body = binary:copy(<<"Hello World! This is a test message. ">>, 100),
    {reply,
        #{
            status => 200,
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body => Body
        },
        State};
handle_request(#{path := <<"/small">>}, State) ->
    {reply,
        #{
            status => 200,
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body => <<"tiny">>
        },
        State};
handle_request(#{path := <<"/no-ct">>}, State) ->
    Body = binary:copy(<<"no content type data ">>, 100),
    {reply, #{status => 200, headers => [], body => Body}, State};
handle_request(#{path := <<"/stream-iterator">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    Producer = stream_chunks_producer([<<"chunk1">>, <<"chunk2">>, <<"chunk3">>]),
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/stream-iterator-fin">>}, State) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    Producer = stream_chunks_producer([]),
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/conn-pid">>}, State) ->
    case whereis(h3_conn_pid_receiver) of
        undefined -> ok;
        Pid -> Pid ! {h3_conn_pid, self()}
    end,
    {reply, nhttp_resp:ok(<<"ok">>), State};
handle_request(#{method := Method}, State) ->
    MethodBin = method_to_binary(Method),
    {reply, nhttp_resp:ok(<<"Method: ", MethodBin/binary>>), State}.

handle_request_body({data, Chunk}, Acc, State) ->
    {accept_body, [Chunk | Acc], State};
handle_request_body({fin, _Trailers}, Acc, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    {reply, nhttp_resp:ok(Body), State};
handle_request_body({abort, Reason}, _Acc, State) ->
    {abort, Reason, State}.

handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, <<"echo: ", Data/binary>>}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State};
handle_ws_frame(_Frame, _Session, State) ->
    {ok, State}.

handle_ws_closed(_Reason, _Session, _State) ->
    ok.

terminate(_Reason, _State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------

stream_chunks_producer(Chunks) ->
    fun(SendChunk) ->
        lists:foreach(fun(Chunk) -> _ = SendChunk(Chunk) end, Chunks),
        ok
    end.

method_to_binary(Method) when is_atom(Method) -> atom_to_binary(Method);
method_to_binary(Method) when is_binary(Method) -> Method.
