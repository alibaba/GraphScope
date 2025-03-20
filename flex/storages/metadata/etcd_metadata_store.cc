/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/storages/metadata/etcd_metadata_store.h"
#include "flex/utils/result.h"

namespace gs {

#define TRT_PUT_ETCD_KEY_VALUE_TXN(client, key, value)         \
  etcdv3::Transaction txn;                                     \
  txn.setup_put(key, value);                                   \
  auto txn_resp = client->txn(txn);                            \
  if (!txn_resp.is_ok()) {                                     \
    return Status(etcdCodeToStatusCode(txn_resp.error_code()), \
                  txn_resp.error_message());                   \
  }

#define FAILS_IF_KEY_EXISTS(client, key)                             \
  auto __res = client->get(key);                                     \
  if (__res.is_ok()) {                                               \
    return Status(StatusCode::META_KEY_ALREADY_EXIST, "Key exists"); \
  }                                                                  \
  if (__res.error_code() != 100) {                                   \
    return Status(etcdCodeToStatusCode(__res.error_code()),          \
                  __res.error_message());                            \
  }

#define FAILS_IF_KEY_NOT_EXISTS(client, key)                        \
  auto __res = client->get(key);                                    \
  if (!__res.is_ok()) {                                             \
    return Status(etcdCodeToStatusCode(__res.error_code()),         \
                  __res.error_message());                           \
  }                                                                 \
  if (__res.value().as_string().empty()) {                          \
    return Status(StatusCode::META_KEY_NOT_FOUND, "Key not found"); \
  }

std::pair<std::string, std::string> extractBaseUrlAndMetaRootUri(
    const std::string& uri) {
  VLOG(10) << "Extracting base URL and meta root URI from: " << uri;
  // Find the position of the scheme (http:// or https://)
  size_t schemeEnd = uri.find("://");
  if (schemeEnd == std::string::npos) {
    return std::make_pair("", "");  // Invalid URI
  }
  // The scheme should be http or https
  if (uri.compare(0, schemeEnd, "http") != 0 &&
      uri.compare(0, schemeEnd, "https") != 0) {
    LOG(ERROR) << "The scheme should be http or https: "
               << uri.substr(0, schemeEnd);
    return std::make_pair("", "");
  }

  // Find the position of the next slash after the scheme
  size_t pathStart = uri.find('/', schemeEnd + 3);  // +3 to move past "://"

  if (pathStart == std::string::npos) {
    // No path found, return the full URI up to the scheme
    return std::make_pair(uri, "");
  }

  // Extract the base URL
  return std::make_pair(uri.substr(0, pathStart), uri.substr(pathStart));
}

Result<bool> ETCDMetadataStore::Open() { return true; }

Result<bool> ETCDMetadataStore::Close() { return true; }

// Insert the value without specifying the key, so we need to generate the key
// by ourself. Suppose we are using prefx_/{meta_key}/cur_id to store the
// current id of the meta.
Result<ETCDMetadataStore::meta_key_t> ETCDMetadataStore::CreateMeta(
    const meta_kind_t& meta_kind, const meta_value_t& value) {
  meta_key_t meta_key;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(meta_key, get_next_meta_key(meta_kind));
  VLOG(10) << "got next meta key: " << meta_key;
  auto real_key = get_full_meta_key(meta_kind, meta_key);
  FAILS_IF_KEY_EXISTS(client_, real_key);
  TRT_PUT_ETCD_KEY_VALUE_TXN(client_, real_key, value);
  return meta_key;
}

Result<ETCDMetadataStore::meta_key_t> ETCDMetadataStore::CreateMeta(
    const meta_kind_t& meta_kind, const meta_key_t& key,
    const meta_value_t& value) {
  auto real_key = get_full_meta_key(meta_kind, key);
  FAILS_IF_KEY_EXISTS(client_, real_key);
  TRT_PUT_ETCD_KEY_VALUE_TXN(client_, real_key, value);
  return key;
}

Result<ETCDMetadataStore::meta_value_t> ETCDMetadataStore::GetMeta(
    const meta_kind_t& meta_kind, const meta_key_t& key) {
  auto real_key = get_full_meta_key(meta_kind, key);
  auto res = client_->get(real_key);
  if (!res.is_ok()) {
    return Result<meta_value_t>(
        Status(etcdCodeToStatusCode(res.error_code()), res.error_message()));
  }
  return res.value().as_string();
}

Result<std::vector<
    std::pair<ETCDMetadataStore::meta_key_t, ETCDMetadataStore::meta_value_t>>>
ETCDMetadataStore::GetAllMeta(const meta_kind_t& meta_kind) {
  // List all key-value pair under directory preifx_ + "/" + meta_kind
  auto res = client_->ls(get_full_meta_key(meta_kind, ""));
  if (!res.is_ok()) {
    return Result<std::vector<std::pair<meta_key_t, meta_value_t>>>(
        Status(etcdCodeToStatusCode(res.error_code()), res.error_message()));
  }
  std::vector<std::pair<meta_key_t, meta_value_t>> result;
  for (size_t i = 0; i < res.keys().size(); ++i) {
    result.push_back(
        std::make_pair(res.keys()[i], res.values()[i].as_string()));
  }
  return result;
}

Result<bool> ETCDMetadataStore::DeleteMeta(const meta_kind_t& meta_kind,
                                           const meta_key_t& key) {
  etcdv3::Transaction txn;
  txn.setup_delete(get_full_meta_key(meta_kind, key));
  auto res = client_->txn(txn);
  if (!res.is_ok()) {
    return Result<bool>(
        Status(etcdCodeToStatusCode(res.error_code()), res.error_message()));
  }
  return true;
}

Result<bool> ETCDMetadataStore::DeleteAllMeta(const meta_kind_t& meta_kind) {
  // auto res = client_->rmdir(get_full_meta_key(meta_kind, ""), true);
  etcdv3::Transaction txn;
  txn.setup_delete(get_full_meta_key(meta_kind, ""), "", true);
  auto res = client_->txn(txn);
  if (!res.is_ok()) {
    if (res.error_code() == 100) {  // Key not found
      return true;
    }
    return Result<bool>(
        Status(etcdCodeToStatusCode(res.error_code()), res.error_message()));
  }
  return true;
}

Result<bool> ETCDMetadataStore::UpdateMeta(const meta_kind_t& meta_kind,
                                           const meta_key_t& key,
                                           const meta_value_t& value) {
  auto real_key = get_full_meta_key(meta_kind, key);
  FAILS_IF_KEY_NOT_EXISTS(client_, real_key);
  auto result = initOrUpdateValue(
      real_key, value, [&value](const std::string& input) { return value; });
  if (!result.ok()) {
    return result.status();
  }
  return true;
}

Result<bool> ETCDMetadataStore::UpdateMeta(
    const meta_kind_t& meta_kind, const meta_key_t& key,
    ETCDMetadataStore::update_func_t update_func) {
  auto real_key = get_full_meta_key(meta_kind, key);
  FAILS_IF_KEY_NOT_EXISTS(client_, real_key);
  auto result =
      initOrUpdateValue(real_key, "", [update_func](const std::string& input) {
        auto res = update_func(input);
        if (!res.ok()) {
          LOG(ERROR) << "Failed to update meta: "
                     << res.status().error_message();
          return input;
        } else {
          return res.value();
        }
      });
  if (!result.ok()) {
    return result.status();
  }
  return true;
}

Result<std::string> ETCDMetadataStore::initOrUpdateValue(
    const std::string& key, const std::string& initial_value,
    std::function<std::string(const std::string&)> update_func) {
  auto value = client_->get(key);
  etcdv3::Transaction txn;
  std::string real_value;

  if (value.is_ok()) {
    // do fetch_add
    real_value = value.value().as_string();
    int32_t max_retry = 10;
    while (true && max_retry--) {
      txn.setup_compare_and_swap(key, real_value, update_func(real_value));
      etcd::Response resp = client_->txn(txn);
      if (resp.is_ok()) {
        break;
      }
      if (max_retry == 0) {
        return Status(etcdCodeToStatusCode(resp.error_code()),
                      "Failed to update key:" + resp.error_message());
      }
      real_value = stoi(resp.value().as_string());
    }
  } else {
    // do put
    if (initial_value.empty()) {
      return Status(StatusCode::ILLEGAL_OPERATION, "Initial value is empty");
    }
    txn.setup_put(key, initial_value);
    auto txn_resp = client_->txn(txn);
    if (!txn_resp.is_ok()) {
      return Status(etcdCodeToStatusCode(txn_resp.error_code()),
                    "Failed to initialize key:" + txn_resp.error_message());
    }
  }
  return client_->get(key).value().as_string();
}

/**
 * @brief Get the next meta key for the given meta kind.
 * We store a current id for each meta kind, and the next meta key is the
 * current id plus one.
 *
 * There is no synchronization in this function, so it should be called in a
 * synchronized context.
 */
Result<std::string> ETCDMetadataStore::get_next_meta_key(
    const std::string& meta_kind) {
  std::string cur_id_key = get_full_meta_key("META_" + meta_kind, CUR_ID_KEY);
  return initOrUpdateValue(cur_id_key, "1", [](const std::string& value) {
    return std::to_string(std::stoi(value) + 1);
  });
}

std::string ETCDMetadataStore::get_full_meta_key(const std::string& meta_kind,
                                                 const std::string& key) {
  return prefix_ + "/" + meta_kind + "/" + key;
}
}  // namespace gs
