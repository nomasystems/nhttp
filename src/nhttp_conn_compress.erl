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
-define(NO_SIZE_LIMIT, 16#FFFFFFFF).

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
Potentially compress a response body based on the request `Accept-Encoding`.

Returns the response unchanged when compression is disabled, no encoding
is negotiated, the body is empty or smaller than the threshold, or the
content type is not in the compressible list.
""".
-spec maybe_compress(nhttp_lib:response(), nhttp_lib:request(), config()) ->
    nhttp_lib:response().
maybe_compress(Response, _Request, #{enabled := false}) ->
    Response;
maybe_compress(#{body := <<>>} = Response, _Request, _Config) ->
    Response;
maybe_compress(
    #{headers := Headers} = Response,
    #{headers := ReqHeaders},
    #{
        level := Level,
        threshold := Threshold,
        mime_types := MimeTypes
    }
) ->
    Body = maps:get(body, Response, <<>>),
    Encoding = nhttp_compress:negotiate_encoding(ReqHeaders),
    case Encoding of
        identity ->
            Response;
        _ ->
            ContentType = nhttp_headers:get(<<"content-type">>, Headers),
            BodySize = iolist_size(Body),
            case should_compress(ContentType, BodySize, Threshold, MimeTypes) of
                false ->
                    Response;
                true ->
                    compress_response(Response, Body, Encoding, Level)
            end
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
        | Filtered
    ].

-spec compress_response(nhttp_lib:response(), iodata(), nhttp_compress:encoding(), 1..9) ->
    nhttp_lib:response().
compress_response(#{headers := Headers} = Response, Body, Encoding, Level) ->
    case nhttp_compress:compress(Body, Encoding, Level) of
        {ok, Compressed} ->
            EncodingHeader = nhttp_compress:encoding_header(Encoding),
            NewHeaders = add_compression_headers(Headers, EncodingHeader, byte_size(Compressed)),
            Response#{body => Compressed, headers := NewHeaders};
        {error, _Reason} ->
            Response
    end.

-spec should_compress(
    binary() | undefined,
    non_neg_integer(),
    non_neg_integer(),
    [binary()]
) -> boolean().
should_compress(undefined, _Size, _Threshold, _MimeTypes) ->
    false;
should_compress(_ContentType, Size, Threshold, _MimeTypes) when Size < Threshold ->
    false;
should_compress(ContentType, _Size, _Threshold, MimeTypes) ->
    nhttp_compress:should_compress(ContentType, ?NO_SIZE_LIMIT, MimeTypes).
