-module(nhttp_conn_compress).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    config/4,
    maybe_compress/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    config/0
]).

-type config() :: #{
    enabled := boolean(),
    level := 1..9,
    threshold := non_neg_integer(),
    mime_types := [binary()]
}.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(LIST_SEPARATORS, [<<",">>, <<" ">>, <<"\t">>]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Build a `config()` from its four components.".
-spec config(boolean(), 1..9, non_neg_integer(), [binary()]) -> config().
config(Enabled, Level, Threshold, MimeTypes) ->
    #{
        enabled => Enabled,
        level => Level,
        threshold => Threshold,
        mime_types => MimeTypes
    }.

-doc """
Compress a response body with the coding that the request `Accept-Encoding`
selects, and add `vary: accept-encoding` when the selection depends on it.

The response is returned unchanged when compression is disabled, the status
has no content or holds a range (1xx, 204, 206, 304), the body is empty,
smaller than the threshold or of a type outside the compressible list, the
handler already set `content-encoding`, or `cache-control` holds
`no-transform`.
""".
-spec maybe_compress(nhttp_lib:response(), nhttp_lib:request(), config()) ->
    nhttp_lib:response().
maybe_compress(Response, _Request, #{enabled := false}) ->
    Response;
maybe_compress(#{status := Status} = Response, _Request, _Config) when
    Status < 200; Status =:= 204; Status =:= 206; Status =:= 304
->
    Response;
maybe_compress(#{body := <<>>} = Response, _Request, _Config) ->
    Response;
maybe_compress(#{headers := Headers} = Response, #{headers := ReqHeaders}, Config) ->
    Body = maps:get(body, Response, <<>>),
    case is_eligible(Headers, iolist_size(Body), Config) of
        false ->
            Response;
        true ->
            Encoding = nhttp_compress:negotiate_encoding(ReqHeaders),
            encode_response(Response, Body, Encoding, Config)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec add_compression_headers(nhttp_lib:headers(), binary(), non_neg_integer()) ->
    nhttp_lib:headers().
add_compression_headers(Headers, EncodingHeader, NewSize) ->
    Filtered = nhttp_headers:delete(<<"content-length">>, Headers),
    [
        {<<"content-encoding">>, EncodingHeader},
        {<<"content-length">>, integer_to_binary(NewSize)}
        | add_vary(Filtered)
    ].

-spec add_vary(nhttp_lib:headers()) -> nhttp_lib:headers().
add_vary(Headers) ->
    Members = list_members(<<"vary">>, Headers),
    case lists:member(<<"accept-encoding">>, Members) orelse lists:member(<<"*">>, Members) of
        true -> Headers;
        false -> [{<<"vary">>, <<"accept-encoding">>} | Headers]
    end.

-spec encode_response(nhttp_lib:response(), iodata(), nhttp_compress:encoding(), config()) ->
    nhttp_lib:response().
encode_response(#{headers := Headers} = Response, _Body, identity, _Config) ->
    Response#{headers := add_vary(Headers)};
encode_response(#{headers := Headers} = Response, Body, Encoding, #{level := Level}) ->
    case nhttp_compress:compress(Body, Encoding, Level) of
        {ok, Compressed} ->
            EncodingHeader = nhttp_compress:encoding_header(Encoding),
            NewHeaders = add_compression_headers(Headers, EncodingHeader, byte_size(Compressed)),
            Response#{body => Compressed, headers := NewHeaders};
        {error, _Reason} ->
            Response
    end.

-spec is_eligible(nhttp_lib:headers(), non_neg_integer(), config()) -> boolean().
is_eligible(Headers, Size, #{threshold := Threshold, mime_types := MimeTypes}) ->
    ContentType = nhttp_headers:get(<<"content-type">>, Headers),
    nhttp_compress:should_compress(ContentType, Size, MimeTypes, Threshold) andalso
        not nhttp_headers:has(<<"content-encoding">>, Headers) andalso
        not lists:member(<<"no-transform">>, list_members(<<"cache-control">>, Headers)).

-doc """
The lowercase members of every `Name` field, for a field whose value is a
comma-separated list (RFC 9110 §5.6.1).
""".
-spec list_members(binary(), nhttp_lib:headers()) -> [binary()].
list_members(Name, Headers) ->
    [
        Member
     || {Field, Value} <- Headers,
        nhttp_headers:name_eq(Field, Name),
        Member <- binary:split(nhttp_headers:to_lower(Value), ?LIST_SEPARATORS, [global, trim_all])
    ].
