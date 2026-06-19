%%%-----------------------------------------------------------------------------
%%% HTTP status codes (RFC 9110) used outside the canonical tables in
%%% nhttp_resp.
%%%-----------------------------------------------------------------------------

-ifndef(NHTTP_STATUS_CODES_HRL).
-define(NHTTP_STATUS_CODES_HRL, true).

-define(HTTP_SWITCHING_PROTOCOLS, 101).
-define(HTTP_NO_CONTENT, 204).
-define(HTTP_NOT_MODIFIED, 304).
-define(HTTP_BAD_REQUEST, 400).
-define(HTTP_PAYLOAD_TOO_LARGE, 413).
-define(HTTP_REQUEST_TIMEOUT, 408).
-define(HTTP_URI_TOO_LONG, 414).
-define(HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE, 431).
-define(HTTP_CLIENT_CLOSED_REQUEST, 499).
-define(HTTP_INTERNAL_SERVER_ERROR, 500).

-endif.
