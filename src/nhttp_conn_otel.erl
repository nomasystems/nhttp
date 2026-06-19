-module(nhttp_conn_otel).

-moduledoc false.

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    classify_term_reason/1,
    connection_start/3,
    connection_stop/3,
    request_start/5,
    request_stop/3,
    request_stop_with_size/4,
    stream_start/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([conn_attrs/0]).

-type conn_attrs() :: #{
    transport := tcp | ssl | udp,
    version := nhttp_lib:version(),
    scheme := binary()
}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec classify_term_reason(term()) -> normal | timeout | error.
classify_term_reason(normal) -> normal;
classify_term_reason(idle_timeout) -> timeout;
classify_term_reason({socket_error, _}) -> error;
classify_term_reason({h2_connection_error, _}) -> error;
classify_term_reason({h2_error, _}) -> error;
classify_term_reason({h3_connection_error, _}) -> error;
classify_term_reason({protocol_error, _}) -> error;
classify_term_reason(_) -> error.

-spec connection_start(
    nhttp_otel:config(),
    {inet:ip_address(), inet:port_number()},
    conn_attrs()
) -> nhttp_otel:span_ctx().
connection_start(OtelConfig, {RemoteIP, RemotePort}, #{
    transport := Transport, version := Version
}) ->
    nhttp_otel:connection_span_start(OtelConfig, #{
        remote_ip => RemoteIP,
        remote_port => RemotePort,
        transport => Transport,
        version => Version
    }).

-spec connection_stop(nhttp_otel:span_ctx(), non_neg_integer(), term()) -> ok.
connection_stop(ConnSpan, RequestsCount, Reason) ->
    nhttp_otel:connection_span_end(ConnSpan, #{
        requests => RequestsCount,
        reason => classify_term_reason(Reason)
    }).

-spec request_start(
    nhttp_otel:config(),
    nhttp_otel:span_ctx(),
    nhttp_lib:stream_id(),
    nhttp_lib:request(),
    conn_attrs()
) -> {nhttp_otel:span_ctx(), integer()}.
request_start(
    OtelConfig, ConnSpan, StreamId, #{method := Method, path := Path}, #{scheme := Scheme}
) ->
    nhttp_otel:request_span_start(OtelConfig, ConnSpan, #{
        method => nhttp_lib:encode_method(Method),
        path => Path,
        scheme => Scheme,
        stream_id => StreamId
    }).

-spec request_stop(
    nhttp_otel:config(),
    nhttp_lib:status(),
    {nhttp_otel:span_ctx(), integer()}
) -> ok.
request_stop(OtelConfig, Status, ReqSpanCtx) ->
    nhttp_otel:request_span_end(ReqSpanCtx, #{status => Status}),
    {_, StartTime} = ReqSpanCtx,
    nhttp_otel:record_request_duration(OtelConfig, StartTime, #{status => Status}).

-spec request_stop_with_size(
    nhttp_otel:config(),
    nhttp_lib:status(),
    {nhttp_otel:span_ctx(), integer()},
    non_neg_integer()
) -> ok.
request_stop_with_size(OtelConfig, Status, ReqSpanCtx, Size) ->
    EndAttrs = #{status => Status, response_body_size => Size},
    nhttp_otel:request_span_end(ReqSpanCtx, EndAttrs),
    {_, StartTime} = ReqSpanCtx,
    nhttp_otel:record_request_duration(OtelConfig, StartTime, EndAttrs).

-spec stream_start(
    nhttp_otel:config(),
    {nhttp_otel:span_ctx(), integer()}
) -> ok.
stream_start(OtelConfig, ReqSpanCtx) ->
    nhttp_otel:stream_start(OtelConfig, ReqSpanCtx).
