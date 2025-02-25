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

#ifndef FLEX_STORAGES_METADATA_ETCD_METADATA_STORE_H_
#define FLEX_STORAGES_METADATA_ETCD_METADATA_STORE_H_

#include "flex/storages/metadata/i_meta_store.h"
#include "flex/third_party/etcd-cpp-apiv3/etcd/Client.hpp"
#include "flex/third_party/etcd-cpp-apiv3/etcd/v3/Transaction.hpp"
#include "flex/utils/service_utils.h"

#include <boost/format.hpp>

namespace gs {

std::pair<std::string, std::string> extractBaseUrlAndMetaRootUri(
    const std::string& uri);
/**
 * @brief ETCDMetadataStore is a concrete implementation of MetadataStore,
 * which stores metadata via ETCD
 */
class ETCDMetadataStore : public IMetaStore {
 public:
  using meta_key_t = IMetaStore::meta_key_t;
  using meta_value_t = IMetaStore::meta_value_t;
  using meta_kind_t = IMetaStore::meta_kind_t;

  static constexpr const char* CUR_ID_KEY = "CUR_ID";

  ETCDMetadataStore(const std::string& path) {
    // Expect the path is like http://ip:port/uri/path, extract the part after
    // http://ip:port/
    std::string base_uri;
    std::tie(base_uri, prefix_) = extractBaseUrlAndMetaRootUri(path);
    VLOG(10) << "ETCD base URI: " << base_uri
             << ", meta base path: " << prefix_;
    client_ = std::make_unique<etcd::SyncClient>(base_uri);
  }

  ~ETCDMetadataStore() { Close(); }

  Result<bool> Open() override;

  Result<bool> Close() override;

  /*
   * @brief Create a meta with a new key.
   * @param meta_kind The kind of meta.
   * @param value The value of the meta.
   * @return The key of the meta.
   */
  Result<meta_key_t> CreateMeta(const meta_kind_t& meta_kind,
                                const meta_value_t& value) override;

  /*
   * @brief Create a meta with a specific key.
   * @param meta_kind The kind of meta.
   * @param key The key of the meta.
   * @param value The value of the meta.
   * @return If the meta is created successfully.
   */
  Result<meta_key_t> CreateMeta(const meta_kind_t& meta_kind,
                                const meta_key_t& key,
                                const meta_value_t& value) override;

  Result<meta_value_t> GetMeta(const meta_kind_t& meta_kind,
                               const meta_key_t& key) override;

  Result<std::vector<std::pair<meta_key_t, meta_value_t>>> GetAllMeta(
      const meta_kind_t& meta_kind) override;

  Result<bool> DeleteMeta(const meta_kind_t& meta_kind,
                          const meta_key_t& key) override;

  Result<bool> DeleteAllMeta(const meta_kind_t& meta_kind) override;

  /*
   * @brief Update the meta with a specific key, regardless of the original
   * value.
   * @param meta_kind The kind of meta.
   * @param key The key of the meta.
   * @param value The new value of the meta.
   * @return If the meta is updated successfully.
   */
  Result<bool> UpdateMeta(const meta_kind_t& meta_kind, const meta_key_t& key,
                          const meta_value_t& value) override;

  /**
   * @brief Update the meta with a specific key, based on the original value.
   * @param meta_kind The kind of meta.
   * @param key The key of the meta.
   * @param update_func The function to update the meta.
   * @return If the meta is updated successfully.
   */
  Result<bool> UpdateMeta(const meta_kind_t& meta_kind, const meta_key_t& key,
                          update_func_t update_func) override;

 private:
  Result<std::string> get_next_meta_key(const std::string& meta_kind);
  std::string get_full_meta_key(const std::string& meta_kind,
                                const std::string& key);
  Result<std::string> initOrUpdateValue(
      const std::string& key, const std::string& initial_value,
      std::function<std::string(const std::string&)> update_func);

  std::unique_ptr<etcd::SyncClient> client_;
  std::string prefix_;
  // std::mutex meta_mutex_;
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_ETCD_METADATA_STORE_H_