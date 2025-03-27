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

#ifndef FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_
#define FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_

#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "flex/storages/metadata/i_meta_store.h"
#include "flex/utils/service_utils.h"

#include <boost/format.hpp>

namespace gs {

/**
 * @brief LocalFileMetadataStore is a concrete implementation of MetadataStore,
 * which stores metadata via local files.
 *
 * We store the graph meta and procedure meta in to files under workspace.
 * ├── META_CLASS1
 * │   ├── KEY1
 * │   └── KEY2
 * └── META_CLASS2
 *     ├── KEY1
 *     └── KEY2
 */
class LocalFileMetadataStore : public IMetaStore {
 public:
  using meta_key_t = IMetaStore::meta_key_t;
  using meta_value_t = IMetaStore::meta_value_t;
  using meta_kind_t = IMetaStore::meta_kind_t;

  static constexpr const char* META_FILE_PREFIX = "META_";
  static constexpr const char* CUR_ID_FILE_NAME = "CUR_ID";

  LocalFileMetadataStore(const std::string& path);

  ~LocalFileMetadataStore();

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
  Result<meta_key_t> get_next_meta_key(const meta_kind_t& meta_kind);
  std::string get_root_meta_dir() const;
  std::string get_meta_kind_dir(const meta_kind_t& meta_kind) const;
  std::string get_meta_file(const meta_kind_t& meta_kind,
                            const meta_key_t& meta_key) const;
  /**
   * For the specified meta_kind, increase the id and return the new id.
   */
  int32_t increase_and_get_id(const meta_kind_t& meta_kind);
  bool is_key_exist(const meta_kind_t& meta_kind,
                    const meta_key_t& meta_key) const;

  Result<bool> dump_file(const std::string& file_path,
                         const std::string& content) const;
  Result<meta_value_t> read_file(const std::string& file_path) const;

  Result<bool> create_directory(const std::string& dir) const;

  std::mutex meta_mutex_;

  std::string root_dir_;
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_