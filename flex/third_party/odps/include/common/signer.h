#pragma once

#include <map>
#include <sstream>
#include "common/base64.h"
#include "common/configuration.h"
#include "common/hmac.h"
#include "common/http_flags.h"
#include "common/string_tools.h"
#include "flex/third_party/httplib.h"

using namespace apsara;
using namespace apsara::security;

namespace apsara {
namespace odps {
namespace sdk {
namespace storage_api {

#define NEW_LINE "\n"

inline std::string buildCanonicalizedResource(
    const std::string& resource_path,
    const std::map<std::string, std::string>& parameters) {
  std::ostringstream builder;
  builder << resource_path;

  char separater = '?';
  auto it = parameters.begin();
  for (; it != parameters.end(); ++it) {
    builder << separater;
    builder << it->first;
    if (it->second != "") {
      builder << "=" << it->second;
    }
    separater = '&';
  }

  return builder.str();
}

inline std::string buildCanonicalString(
    const std::string& method, const std::string& resource_path,
    std::map<std::string, std::string> headers,
    std::map<std::string, std::string> params, const std::string& prefix) {
  std::ostringstream builder;
  builder << method << NEW_LINE;

  std::map<std::string, std::string> headers_to_sign;

  auto it = headers.begin();
  for (; it != headers.end(); ++it) {
    std::string lower_key = ToLowerCaseString(it->first);

    if (lower_key == ToLowerCaseString(CONTENT_TYPE) ||
        lower_key == ToLowerCaseString(CONTENT_MD5) ||
        lower_key == ToLowerCaseString(DATE) || StartWith(lower_key, prefix)) {
      headers_to_sign[lower_key] = it->second;
    }
  }

  if (headers_to_sign.find(ToLowerCaseString(CONTENT_TYPE)) ==
      headers_to_sign.end()) {
    headers_to_sign[ToLowerCaseString(CONTENT_TYPE)] = "";
  }
  if (headers_to_sign.find(ToLowerCaseString(CONTENT_MD5)) ==
      headers_to_sign.end()) {
    headers_to_sign[ToLowerCaseString(CONTENT_MD5)] = "";
  }

  // Add params that have the prefix "x-oss-"
  it = params.begin();
  for (; it != params.end(); ++it) {
    if (StartWith(it->first, prefix)) {
      headers_to_sign[it->first] = it->second;
    }
  }

  // Add all headers to sign to the builder
  it = headers_to_sign.begin();
  for (; it != headers_to_sign.end(); ++it) {
    std::string key = it->first;
    std::string value = it->second;
    // if (StartWith(key, prefix)){
    if (StartWith(key, prefix)) {
      builder << key << ":" << value;
    } else {
      builder << value;
    }

    builder << "\n";
  }

  // Add canonical resource
  builder << buildCanonicalizedResource(resource_path, params);

  return builder.str();
}

inline void AliyunAccountSign(std::string method, std::string url,
                              Account account, httplib::Params& req_params,
                              httplib::Headers& req_headers) {
  time_t timer;
  char buffer[50];
  memset(buffer, 0, 50);
  struct tm* tm_info;
  struct tm result;
  time(&timer);
  tm_info = gmtime_r(&timer, &result);
  strftime(buffer, 50, "%a, %d %b %Y %H:%M:%S GMT", tm_info);
  std::string date(buffer, strlen(buffer));
  req_headers.insert({DATE, date});

  // reqHeaders.insert({"Content-Type", "application/json"});
  req_headers.insert({"odps-tunnel-date-transform", "v1"});
  req_headers.insert({"x-odps-tunnel-version", "5"});

  std::map<std::string, std::string> headers;
  for (auto it = req_headers.begin(); it != req_headers.end(); it++) {
    headers[it->first] = it->second;
  }
  std::map<std::string, std::string> params;
  for (auto it = req_params.begin(); it != req_params.end(); it++) {
    params[it->first] = it->second;
  }

  if (account.GetId().length() > 0 && account.GetKey().length() > 0) {
    std::string string_to_sign =
        buildCanonicalString(method, url, headers, params, HEADER_ODPS_PREFIX);

    HMAC hmac(reinterpret_cast<const uint8_t*>(account.GetKey().data()),
              account.GetKey().size());
    hmac.add(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
             string_to_sign.size());

    std::istringstream iss(std::string(
        reinterpret_cast<const char*>(hmac.result()), SHA1_DIGEST_BYTES));
    std::ostringstream oss;
    Base64Encoding(iss, oss);

    auto signature = oss.str();
    signature = "ODPS " + account.GetId() + ":" + signature;

    req_headers.insert({AUTHORIZATION, signature});
  } else if (account.GetId().length() > 0) {
    req_headers.insert({AUTHORIZATION, account.GetId()});
  }
}

inline void AppAccountSign(std::string method, std::string url, Account account,
                           httplib::Params& req_params,
                           httplib::Headers& req_headers) {
  if (account.GetId().length() > 0 && account.GetKey().length() > 0) {
    std::string string_to_sign = req_headers.find(AUTHORIZATION)->second;
    if (string_to_sign.empty()) {
      std::cerr << "String to sign cannot be empty. Please contact developer"
                << std::endl;
      throw std::exception();
    }

    HMAC hmac(reinterpret_cast<const uint8_t*>(account.GetKey().data()),
              account.GetKey().size());
    hmac.add(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
             string_to_sign.size());

    std::istringstream iss(std::string(
        reinterpret_cast<const char*>(hmac.result()), SHA1_DIGEST_BYTES));
    std::ostringstream oss;
    Base64Encoding(iss, oss);

    auto signature = oss.str();
    std::ostringstream formatted_signature;
    formatted_signature << "account_provider:"
                        << ToLowerCaseString(account.GetType());
    formatted_signature << ",signature_method:"
                        << "hmac-sha1";
    formatted_signature << ",access_id:" << account.GetId();
    formatted_signature << ",signature:" << signature;

    req_headers.insert({APP_AUTHENTICATION, formatted_signature.str()});
  } else {
    std::cerr << "App Account's accessId and accessKey cannot be empty"
              << std::endl;
    throw std::exception();
  }
}

inline void AliAccountSign(std::string method, std::string url, Account account,
                           httplib::Params& req_params,
                           httplib::Headers& req_headers) {
  if (!account.GetToken().empty()) {
    req_headers.insert({HEADER_ALI_DATA_SERVICE, "ODPS"});
    req_headers.insert({AUTHORIZATION, "Bearer " + account.GetToken()});
    return;
  } else {
    time_t timer;
    char buffer[50];
    memset(buffer, 0, 50);
    struct tm* tm_info;
    struct tm result;
    time(&timer);
    tm_info = gmtime_r(&timer, &result);
    strftime(buffer, 50, "%a, %d %b %Y %H:%M:%S GMT", tm_info);
    std::string date(buffer, strlen(buffer));
    req_headers.insert({DATE, date});

    // reqHeaders.insert({"Content-Type", "application/json"});
    req_headers.insert({"odps-tunnel-date-transform", "v1"});
    req_headers.insert({"x-odps-tunnel-version", "5"});

    std::map<std::string, std::string> headers;
    for (auto it = req_headers.begin(); it != req_headers.end(); it++) {
      headers[it->first] = it->second;
    }
    std::map<std::string, std::string> params;
    for (auto it = req_params.begin(); it != req_params.end(); it++) {
      params[it->first] = it->second;
    }

    std::string string_to_sign = buildCanonicalString(
        method, url, headers, params, HEADER_ALI_DATA_PREFIX);

    std::string sign_type = account.GetAlgorithm();
    std::string signature = "";

    if (sign_type == "hmac-sha1") {
      HMAC hmac(reinterpret_cast<const uint8_t*>(account.GetKey().data()),
                account.GetKey().size());
      hmac.add(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
               string_to_sign.size());

      std::istringstream iss(std::string(
          reinterpret_cast<const char*>(hmac.result()), SHA1_DIGEST_BYTES));
      std::ostringstream oss;
      Base64Encoding(iss, oss);

      signature = oss.str();
    } else {
      std::cerr << "Sign algorithm not support" << std::endl;
      throw std::exception();
    }

    req_headers.insert({AUTHORIZATION, account.GetId() + ":" + signature});
  }
}

inline void Sign(Configuration configuration, std::string method,
                 std::string url, httplib::Params& req_params,
                 httplib::Headers& req_headers) {
  Account account = configuration.GetAccount();
  std::string type = ToLowerCaseString(account.GetType());

  AppAccount app_account = configuration.GetAppAccount();
  StsToken sts_token = configuration.GetStsToken();
  if (type == "" || type == ACCOUNT_ALIYUN) {
    AliyunAccountSign(method, url, account, req_params, req_headers);
    if (app_account.IsValid()) {
      AppAccountSign(method, url, account, req_params, req_headers);
    }
    if (sts_token.IsValid()) {
      req_headers.insert(
          {"authorization-sts-token", configuration.stsToken.GetToken()});
    }
  } else if (type == ACCOUNT_DOMAIN) {
    if (!account.GetToken().empty()) {
      std::string authorization = "account_provider:" + account.GetType() +
                                  ",access_token:" + account.GetToken();
      req_headers.insert({AUTHORIZATION, authorization});
    }
  } else if (type == ACCOUNT_TOKEN) {
    req_headers.insert({HEADER_ODPS_BEARER_TOKEN, account.GetToken()});
    req_headers.insert({AUTHORIZATION, "ODPS bearer token, no need to sign"});
  } else if (type == ACCOUNT_TAOBAO) {  // havana
    AliAccountSign(method, url, account, req_params, req_headers);
  } else {
    std::cerr << "Unsupported authorization type in C++ SDK" << std::endl;
    throw std::exception();
  }

  if (!account.GetApplicationSignature().empty()) {
    req_headers.insert(
        {"application-authentication", account.GetApplicationSignature()});
  }
}
}  // namespace storage_api
}  // namespace sdk
}  // namespace odps
}  // namespace apsara