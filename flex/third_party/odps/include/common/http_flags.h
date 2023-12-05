#ifndef APSARA_ODPS_SDK_HTTP_FLAGS_H
#define APSARA_ODPS_SDK_HTTP_FLAGS_H

namespace apsara {
namespace odps {
namespace sdk {

/* HTTP standard headers */
#define AUTHORIZATION "Authorization"
#define CACHE_CONTROL "Cache-Control"
#define CONTENT_DISPOSITION "Content-Disposition"
#define CONTENT_ENCODING "Content-Encoding"
#define CONTENT_LENGTH "Content-Length"
#define CONTENT_MD5 "Content-MD5"
#define CONTENT_TYPE "Content-Type"
#define TRANSFER_ENCODING "Transfer-Encoding"
#define CHUNKED "chunked"
#define DATE "Date"
#define ETAG "ETag"
#define EXPIRES "Expires"
#define LAST_MODIFIED "Last-Modified"
#define RANGE "Range"
#define LOCATION "Location"
#define USER_AGENT "USER-AGENT"

/* HTTP extended headers */
#define APP_AUTHENTICATION "application-authentication"
#define HEADER_ODPS_CREATION_TIME "x-odps-creation-time"
#define HEADER_ODPS_OWNER "x-odps-owner"
#define HEADER_ODPS_START_TIME "x-odps-start-time"
#define HEADER_ODPS_END_TIME "x-odps-end-time"
#define HEADER_ODPS_COPY_TABLE_SOURCE "x-odps-copy-table-source"
#define HEADER_ODPS_COMMENT "x-odps-comment"
#define HEADER_ODPS_RESOURCE_NAME "x-odps-resource-name"
#define HEADER_ODPS_RESOURCE_TYPE "x-odps-resource-type"
#define HEADER_ODPS_AUTHORIZATION "Authorization"
#define HEADER_ODPS_PREFIX "x-odps-"
#define HEADER_ODPS_REQUEST_ID "x-odps-request-id"
#define HEADER_ALI_DATA_PREFIX "x-ali-data-"
#define HEADER_ALI_DATA_SERVICE "x-ali-data-service"
#define HEADER_ALI_DATA_AUTH_METHOD "x-ali-data-auth-method"
#define HEADER_ALI_DATA_AUTH_SIGNATURE_TYPE "x-ali-data-auth-signature-type"
#define HEADER_ODPS_BEARER_TOKEN "x-odps-bearer-token"
#define HEADER_ODPS_ROUTED_SERVER "odps-tunnel-routed-server"
#define HEADER_ODPS_SLOT_NUM "odps-tunnel-slot-num"
#define HEADER_ODPS_APPLICATION_AUTHENTICATION "application-authentication"
#define HEADER_ODPS_TUNNEL_FILE_NAME "odps-tunnel-file_name"
#define HEADER_ODPS_TUNNEL_FILE_OFFSET "odps-tunnel-file-offset"
#define HEADER_ODPS_TUNNEL_RECORD_OFFSET "odps-tunnel-record-offset"

#define HEADER_ODPS_TUNNEL_VERSION "x-odps-tunnel-version"
#define HEADER_STREAM_VERSION "x-odps-tunnel-stream-version"
#define HEADER_ODPS_PACK_NUM "x-odps-pack-num"
#define HEADER_ODPS_CURRENT_PACKID "x-odps-current-packid"
#define HEADER_ODPS_NEXT_PACKID "x-odps-next-packid"
#define VERSION_1 "1"
#define VERSION_2 "2"
#define VERSION_3 "3"
#define VERSION_4 "4"
#define VERSION_5 "5"
#define CONTENT_ENCODING "Content-Encoding"

#define HTTP_METHOD_GET "GET"
#define HTTP_METHOD_POST "POST"
#define HTTP_METHOD_PUT "PUT"
#define HTTP_METHOD_DELETE "DELETE"

#define PARAM_ARROW "arrow"
#define PARAM_DOWNLOADS "downloads"
#define PARAM_DOWNLOAD_ID "downloadid"
#define PARAM_UPLOADS "uploads"
#define PARAM_UPLOAD_ID "uploadid"
#define PARAM_DATA "data"
#define PARAM_BLOCK_ID "blockid"
#define PARAM_PARTITION "partition"
#define PARAM_ROWRANGE "rowrange"
#define PARAM_ASYNC_MODE "asyncmode"
#define PARAM_OVERWRITE_MODE "overwrite"
#define PARAM_COLUMNS "columns"
#define PARAM_CURR_PROJECT "curr_project"
#define PARAM_QUERY "query"
#define PARAM_TYPE "type"
#define PARAM_SHARD_NUMBER "shardnumber"
#define PARAM_PACKID "packid"
#define PARAM_PACKNUM "packnum"
#define PARAM_ITERATE_MODE "iteratemode"
#define PARAM_MODE_AT_PACKID "AT_PACKID"
#define PARAM_MODE_AFTER_PACKID "AFTER_PACKID"
#define PARAM_MODE_FIRST_PACK "FIRST_PACK"
#define PARAM_MODE_LAST_PACK "LAST_PACK"
#define PARAM_SHARD_STATUS "shardstatus"
#define PARAM_RECORD_COUNT "recordcount"
#define PARAM_ASYNC_MODE "asyncmode"
#define PARAM_REGION_ID "region_id"
#define PARAM_SLOT_ID "slotid"
#define PARAM_CREATE_PARTITION "create_partition"
#define PARAM_ZORDER_COLUMNS "zorder_columns"
#define PARAM_ENABLE_OFFSET "enable_offset"
#define PARAM_STORAGE_COMPRESS_MODE "storage_compress_mode"

enum {
  HTTP_CONTINUE = 100,
  HTTP_SWITCHING_PROTOCOLS = 101,
  HTTP_PROCESSING = 102,
  HTTP_OK = 200,
  HTTP_CREATED = 201,
  HTTP_ACCEPTED = 202,
  HTTP_NON_AUTHORITATIVE = 203,
  HTTP_NO_CONTENT = 204,
  HTTP_RESET_CONTENT = 205,
  HTTP_PARTIAL_CONTENT = 206,
  HTTP_MULTI_STATUS = 207,
  HTTP_MULTIPLE_CHOICES = 300,
  HTTP_MOVED_PERMANENTLY = 301,
  HTTP_MOVED_TEMPORARILY = 302,
  HTTP_SEE_OTHER = 303,
  HTTP_NOT_MODIFIED = 304,
  HTTP_USE_PROXY = 305,
  HTTP_TEMPORARY_REDIRECT = 307,
  HTTP_BAD_REQUEST = 400,
  HTTP_UNAUTHORIZED = 401,
  HTTP_PAYMENT_REQUIRED = 402,
  HTTP_FORBIDDEN = 403,
  HTTP_NOT_FOUND = 404,
  HTTP_METHOD_NOT_ALLOWED = 405,
  HTTP_NOT_ACCEPTABLE = 406,
  HTTP_PROXY_AUTHENTICATION_REQUIRED = 407,
  HTTP_REQUEST_TIME_OUT = 408,
  HTTP_CONFLICT = 409,
  HTTP_GONE = 410,
  HTTP_LENGTH_REQUIRED = 411,
  HTTP_PRECONDITION_FAILED = 412,
  HTTP_REQUEST_ENTITY_TOO_LARGE = 413,
  HTTP_REQUEST_URI_TOO_LARGE = 414,
  HTTP_UNSUPPORTED_MEDIA_TYPE = 415,
  HTTP_RANGE_NOT_SATISFIABLE = 416,
  HTTP_EXPECTATION_FAILED = 417,
  HTTP_UNPROCESSABLE_ENTITY = 422,
  HTTP_LOCKED = 423,
  HTTP_FAILED_DEPENDENCY = 424,
  HTTP_UPGRADE_REQUIRED = 426,
  HTTP_INTERNAL_SERVER_ERROR = 500,
  HTTP_NOT_IMPLEMENTED = 501,
  HTTP_BAD_GATEWAY = 502,
  HTTP_SERVICE_UNAVAILABLE = 503,
  HTTP_GATEWAY_TIME_OUT = 504,
  HTTP_VERSION_NOT_SUPPORTED = 505,
  HTTP_VARIANT_ALSO_VARIES = 506,
  HTTP_INSUFFICIENT_STORAGE = 507,
  HTTP_NOT_EXTENDED = 510,
};

}  // namespace sdk
}  // namespace odps
}  // namespace apsara
#endif
