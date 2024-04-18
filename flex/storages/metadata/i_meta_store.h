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

#ifndef FLEX_STORAGES_METADATA_I_META_STORE_H_
#define FLEX_STORAGES_METADATA_I_META_STORE_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flex/utils/result.h"

namespace gs {

/**
 * A general kv-base metadata store interface.
 * The multi-thread safety should be guaranteed.
 */
class IMetaStore {
 public:
  using meta_kind_t = std::string;
  using meta_key_t = std::string;
  using meta_value_t = std::string;
  using update_func_t =
      std::function<Result<meta_value_t>(const meta_value_t&)>;
  virtual ~IMetaStore() = default;

  virtual Result<bool> Open() = 0;
  virtual Result<bool> Close() = 0;

  virtual Result<meta_key_t> CreateMeta(const meta_kind_t& meta_kind,
                                        const meta_value_t& value) = 0;

  virtual Result<meta_value_t> CreateMeta(const meta_kind_t& meta_kind,
                                          const meta_key_t& key,
                                          const meta_value_t& value) = 0;

  virtual Result<meta_value_t> GetMeta(const meta_kind_t& meta_kind,
                                       const meta_key_t& key) = 0;

  virtual Result<std::vector<std::pair<meta_key_t, meta_value_t>>> GetAllMeta(
      const meta_kind_t& meta_kind) = 0;

  virtual Result<bool> DeleteMeta(const meta_kind_t& meta_kind,
                                  const meta_key_t& key) = 0;

  virtual Result<bool> DeleteAllMeta(const meta_kind_t& meta_kind) = 0;

  virtual Result<bool> UpdateMeta(const meta_kind_t& meta_kind,
                                  const meta_key_t& key,
                                  const meta_value_t& value) = 0;

  virtual Result<bool> UpdateMeta(const meta_kind_t& meta_kind,
                                  const meta_key_t& key,
                                  update_func_t update_func) = 0;
};

}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_I_META_STORE_H_