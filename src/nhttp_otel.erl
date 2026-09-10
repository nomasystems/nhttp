-module(nhttp_otel).

-moduledoc """
OpenTelemetry integration for nhttp server.

Provides distributed tracing (spans) and metrics for connection and request
lifecycle events. Telemetry is configurable via the otel option in server
settings.

Configuration:
- `otel => #{traces => true, metrics => true}` - Enable both
- `otel => true` - Shorthand for both
- `otel => false` - Disabled (default)

Spans follow HTTP semantic conventions:
- Connection span: long-lived, parent of request spans
- Request spans: method, path, status, duration

Active requests counter:
Use `nhttp_otel:active_requests/0` to query the current count of
in-flight requests across all connections.
""".

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include("nhttp_status_codes.hrl").

%%%-----------------------------------------------------------------------------
%% CONFIGURATION API
%%%-----------------------------------------------------------------------------
-export([
    config/1,
    metrics_enabled/1,
    traces_enabled/1
]).

%%%-----------------------------------------------------------------------------
%% CONNECTION SPANS
%%%-----------------------------------------------------------------------------
-export([
    connection_rejected/2,
    connection_span_end/2,
    connection_span_start/2
]).

%%%-----------------------------------------------------------------------------
%% REQUEST SPANS
%%%-----------------------------------------------------------------------------
-export([
    request_span_end/2,
    request_span_error/3,
    request_span_start/3,
    stream_start/2
]).

%%%-----------------------------------------------------------------------------
%% METRICS
%%%-----------------------------------------------------------------------------
-export([
    record_request_duration/3
]).

%%%-----------------------------------------------------------------------------
%% ACTIVE REQUESTS COUNTER
%%%-----------------------------------------------------------------------------
-export([
    active_requests/0,
    decr_active_requests/1,
    incr_active_requests/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    config/0,
    conn_attrs/0,
    conn_end_attrs/0,

    req_attrs/0,
    req_end_attrs/0,
    span_ctx/0
]).

-type conn_attrs() :: #{
    version := nhttp_lib:version(),
    remote_ip := inet:ip_address(),
    remote_port := inet:port_number(),
    server_port => inet:port_number(),
    transport := tcp | ssl | udp
}.

-type conn_end_attrs() :: #{
    reason := atom(),
    requests := non_neg_integer()
}.

-type config() ::
    #{
        metrics := boolean(),
        traces := boolean()
    }
    | false.

-type req_attrs() :: #{
    method := binary(),
    path := binary(),
    scheme := binary(),
    stream_id := non_neg_integer()
}.

-type req_end_attrs() :: #{
    response_body_size => non_neg_integer(),
    status := 100..599
}.

-type span_ctx() :: opentelemetry:span_ctx() | undefined.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(ACTIVE_REQUESTS_KEY, nhttp_active_requests_counter).
-define(METER_NAME, nhttp).
-define(METRIC_ACTIVE_REQUESTS, 'nhttp.http.active_requests').
-define(METRIC_REQUEST_DURATION, 'nhttp.http.request.duration').
-define(TRACER_NAME, nhttp).

%%%-----------------------------------------------------------------------------
%% CONFIGURATION API
%%%-----------------------------------------------------------------------------
-doc "Parse otel configuration from server options.".
-spec config(map()) -> config().
config(Opts) ->
    case maps:get(otel, Opts, false) of
        true ->
            #{traces => true, metrics => true};
        false ->
            false;
        #{} = Config ->
            #{
                traces => maps:get(traces, Config, false),
                metrics => maps:get(metrics, Config, false)
            }
    end.

-doc false.
-spec metrics_enabled(config()) -> boolean().
metrics_enabled(#{metrics := true}) -> true;
metrics_enabled(_) -> false.

-doc false.
-spec traces_enabled(config()) -> boolean().
traces_enabled(#{traces := true}) -> true;
traces_enabled(_) -> false.

%%%-----------------------------------------------------------------------------
%% CONNECTION SPANS
%%%-----------------------------------------------------------------------------
-doc false.
-spec connection_rejected(config(), #{listener_name := term(), reason := atom()}) -> ok.
connection_rejected(Config, #{reason := Reason}) ->
    case traces_enabled(Config) of
        false ->
            ok;
        true ->
            Tracer = opentelemetry:get_tracer(?TRACER_NAME),
            SpanName = <<"nhttp.connection.rejected">>,
            SpanOpts = #{
                kind => ?SPAN_KIND_SERVER,
                attributes => #{
                    'nhttp.connection.rejected_reason' => Reason
                }
            },
            SpanCtx = otel_tracer:start_span(Tracer, SpanName, SpanOpts),
            otel_set_span_status(SpanCtx, ?OTEL_STATUS_ERROR, <<"connection rejected">>),
            otel_end_span(SpanCtx)
    end.

-doc false.
-spec connection_span_end(span_ctx(), conn_end_attrs()) -> ok.
connection_span_end(undefined, _Attrs) ->
    ok;
connection_span_end(SpanCtx, #{reason := Reason, requests := Requests}) ->
    otel_set_span_attributes(SpanCtx, #{
        'nhttp.connection.requests' => Requests,
        'nhttp.connection.close_reason' => Reason
    }),
    case Reason of
        normal ->
            ok;
        timeout ->
            otel_set_span_status(SpanCtx, ?OTEL_STATUS_OK);
        _ ->
            otel_set_span_status(SpanCtx, ?OTEL_STATUS_ERROR, atom_to_binary(Reason))
    end,
    otel_end_span(SpanCtx).

-doc false.
-spec connection_span_start(config(), conn_attrs()) -> span_ctx().
connection_span_start(Config, Attrs) ->
    case traces_enabled(Config) of
        false ->
            undefined;
        true ->
            Tracer = opentelemetry:get_tracer(?TRACER_NAME),
            SpanName = <<"nhttp.connection">>,
            SpanOpts = #{
                kind => ?SPAN_KIND_SERVER,
                attributes => connection_attributes(Attrs)
            },
            otel_tracer:start_span(Tracer, SpanName, SpanOpts)
    end.

%%%-----------------------------------------------------------------------------
%% REQUEST SPANS
%%%-----------------------------------------------------------------------------
-doc false.
-spec request_span_end({span_ctx(), integer()}, req_end_attrs()) -> ok.
request_span_end({undefined, _StartTime}, _Attrs) ->
    ok;
request_span_end({SpanCtx, _StartTime}, Attrs) ->
    #{status := Status} = Attrs,
    ResponseAttrs = #{
        'http.response.status_code' => Status
    },
    ResponseAttrs1 =
        case maps:get(response_body_size, Attrs, undefined) of
            undefined -> ResponseAttrs;
            Size -> ResponseAttrs#{'http.response.body.size' => Size}
        end,
    otel_set_span_attributes(SpanCtx, ResponseAttrs1),
    case Status of
        S when S >= ?HTTP_INTERNAL_SERVER_ERROR ->
            otel_set_span_status(SpanCtx, ?OTEL_STATUS_ERROR);
        _ ->
            ok
    end,
    otel_end_span(SpanCtx).

-doc false.
-spec request_span_error(span_ctx() | {span_ctx(), integer()}, atom(), term()) -> ok.
request_span_error({SpanCtx, _StartTime}, Kind, Reason) ->
    request_span_error(SpanCtx, Kind, Reason);
request_span_error(undefined, _Kind, _Reason) ->
    ok;
request_span_error(SpanCtx, Kind, Reason) ->
    _ = otel_span:record_exception(SpanCtx, Kind, Reason, [], #{}),
    otel_set_span_status(SpanCtx, ?OTEL_STATUS_ERROR),
    otel_end_span(SpanCtx).

-doc false.
-spec request_span_start(config(), span_ctx(), req_attrs()) ->
    {span_ctx(), integer()}.
request_span_start(Config, ParentSpanCtx, Attrs) ->
    StartTime = erlang:monotonic_time(),
    case traces_enabled(Config) of
        false ->
            {undefined, StartTime};
        true ->
            Tracer = opentelemetry:get_tracer(?TRACER_NAME),
            #{method := Method, path := Path} = Attrs,
            SpanName = <<Method/binary, " ", Path/binary>>,
            Ctx =
                case ParentSpanCtx of
                    undefined ->
                        otel_ctx:get_current();
                    _ ->
                        _ = otel_tracer:set_current_span(ParentSpanCtx),
                        otel_ctx:get_current()
                end,
            SpanOpts = #{
                kind => ?SPAN_KIND_SERVER,
                attributes => request_attributes(Attrs)
            },
            SpanCtx = otel_tracer:start_span(Ctx, Tracer, SpanName, SpanOpts),
            {SpanCtx, StartTime}
    end.

-doc false.
-spec stream_start(config(), {span_ctx(), integer()}) -> ok.
stream_start(_Config, {undefined, _StartTime}) ->
    ok;
stream_start(Config, {SpanCtx, _StartTime}) ->
    case traces_enabled(Config) of
        false ->
            ok;
        true ->
            _ = otel_span:add_event(SpanCtx, <<"nhttp.stream.start">>, #{}),
            ok
    end.

%%%-----------------------------------------------------------------------------
%% METRICS
%%%-----------------------------------------------------------------------------
-doc false.
-spec record_request_duration(config(), integer(), req_end_attrs()) -> ok.
record_request_duration(Config, StartTime, Attrs) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            Duration = erlang:monotonic_time() - StartTime,
            Ctx = otel_ctx:get_current(),
            Meter = opentelemetry_experimental:get_meter(?METER_NAME),
            MetricAttrs = request_duration_attributes(Attrs),
            otel_record_histogram(Ctx, Meter, ?METRIC_REQUEST_DURATION, Duration, MetricAttrs)
    end.

%%%-----------------------------------------------------------------------------
%% ACTIVE REQUESTS COUNTER
%%%-----------------------------------------------------------------------------
-doc """
Get the current count of active (in-flight) requests.
Returns 0 if the counter hasn't been initialized.
""".
-spec active_requests() -> non_neg_integer().
active_requests() ->
    case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
        undefined ->
            0;
        AtomicsRef ->
            max(0, atomics:get(AtomicsRef, 1))
    end.

-doc false.
-spec decr_active_requests(config()) -> ok.
decr_active_requests(false) ->
    ok;
decr_active_requests(#{metrics := false}) ->
    ok;
decr_active_requests(_Config) ->
    case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
        undefined ->
            ok;
        AtomicsRef ->
            atomics:sub(AtomicsRef, 1, 1)
    end,
    Ctx = otel_ctx:get_current(),
    Meter = opentelemetry_experimental:get_meter(?METER_NAME),
    otel_record_updown_counter(Ctx, Meter, ?METRIC_ACTIVE_REQUESTS, -1, #{}).

-doc false.
-spec incr_active_requests(config()) -> ok.
incr_active_requests(false) ->
    ok;
incr_active_requests(#{metrics := false}) ->
    ok;
incr_active_requests(_Config) ->
    AtomicsRef = ensure_active_requests_counter(),
    atomics:add(AtomicsRef, 1, 1),
    Ctx = otel_ctx:get_current(),
    Meter = opentelemetry_experimental:get_meter(?METER_NAME),
    otel_record_updown_counter(Ctx, Meter, ?METRIC_ACTIVE_REQUESTS, 1, #{}).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec connection_attributes(conn_attrs()) -> opentelemetry:attributes_map().
connection_attributes(
    #{
        remote_ip := RemoteIP,
        remote_port := RemotePort,
        transport := Transport,
        version := Version
    } = Attrs
) ->
    BaseAttrs = #{
        'client.address' => format_ip(RemoteIP),
        'client.port' => RemotePort,
        'network.transport' => transport_to_binary(Transport),
        'network.protocol.name' => <<"http">>,
        'network.protocol.version' => version_to_binary(Version)
    },
    case maps:get(server_port, Attrs, undefined) of
        undefined -> BaseAttrs;
        Port -> BaseAttrs#{'server.port' => Port}
    end.

-spec ensure_active_requests_counter() -> atomics:atomics_ref().
ensure_active_requests_counter() ->
    case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
        undefined ->
            AtomicsRef = atomics:new(1, [{signed, true}]),
            atomics:put(AtomicsRef, 1, 0),
            persistent_term:put(?ACTIVE_REQUESTS_KEY, AtomicsRef),
            AtomicsRef;
        AtomicsRef ->
            AtomicsRef
    end.

-spec format_ip(inet:ip_address()) -> binary().
format_ip({A, B, C, D}) ->
    iolist_to_binary(io_lib:format("~B.~B.~B.~B", [A, B, C, D]));
format_ip({A, B, C, D, E, F, G, H}) ->
    iolist_to_binary(
        io_lib:format(
            "~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B",
            [A, B, C, D, E, F, G, H]
        )
    ).

-spec otel_end_span(span_ctx()) -> ok.
otel_end_span(SpanCtx) ->
    _ = otel_span:end_span(SpanCtx),
    ok.

-spec otel_record_histogram(
    otel_ctx:t(),
    opentelemetry_experimental:meter(),
    otel_instrument:name(),
    number(),
    opentelemetry:attributes_map()
) -> ok.
otel_record_histogram(Ctx, Meter, Name, Number, Attrs) ->
    try
        _ = otel_histogram:record(Ctx, Meter, Name, Number, Attrs),
        ok
    catch
        error:undef -> ok
    end.

-spec otel_record_updown_counter(
    otel_ctx:t(),
    opentelemetry_experimental:meter(),
    otel_instrument:name(),
    number(),
    opentelemetry:attributes_map()
) -> ok.
otel_record_updown_counter(Ctx, Meter, Name, Number, Attrs) ->
    try
        _ = otel_updown_counter:add(Ctx, Meter, Name, Number, Attrs),
        ok
    catch
        error:undef -> ok
    end.

-spec otel_set_span_attributes(span_ctx(), opentelemetry:attributes_map()) -> ok.
otel_set_span_attributes(SpanCtx, Attrs) ->
    _ = otel_span:set_attributes(SpanCtx, Attrs),
    ok.

-spec otel_set_span_status(span_ctx(), atom()) -> ok.
otel_set_span_status(SpanCtx, Status) ->
    _ = otel_span:set_status(SpanCtx, Status),
    ok.

-spec otel_set_span_status(span_ctx(), atom(), binary()) -> ok.
otel_set_span_status(SpanCtx, Status, Description) ->
    _ = otel_span:set_status(SpanCtx, Status, Description),
    ok.

-spec request_attributes(req_attrs()) -> opentelemetry:attributes_map().
request_attributes(#{
    method := Method,
    path := Path,
    scheme := Scheme,
    stream_id := StreamId
}) ->
    #{
        'http.request.method' => Method,
        'url.path' => Path,
        'url.scheme' => Scheme,
        'nhttp.stream_id' => StreamId
    }.

-spec request_duration_attributes(req_end_attrs()) -> opentelemetry:attributes_map().
request_duration_attributes(#{status := Status} = Attrs) ->
    BaseAttrs = #{'http.response.status_code' => Status},
    case maps:get(response_body_size, Attrs, undefined) of
        undefined -> BaseAttrs;
        Size -> BaseAttrs#{'http.response.body.size' => Size}
    end.

-spec transport_to_binary(tcp | ssl | udp) -> binary().
transport_to_binary(tcp) -> <<"tcp">>;
transport_to_binary(ssl) -> <<"tcp">>;
transport_to_binary(udp) -> <<"udp">>.

-spec version_to_binary(nhttp_lib:version()) -> binary().
version_to_binary(http1_0) -> <<"1.0">>;
version_to_binary(http1_1) -> <<"1.1">>;
version_to_binary(http2) -> <<"2">>;
version_to_binary(http3) -> <<"3">>.
