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

#ifndef FLEX_STORAGES_METADATA_METADATA_STORE_FACTORY_H_
#define FLEX_STORAGES_METADATA_METADATA_STORE_FACTORY_H_

#include <string>
#include "flex/storages/metadata/default_graph_meta_store.h"
#include "flex/storages/metadata/graph_meta_store.h"
#include "flex/storages/metadata/local_file_metadata_store.h"

namespace gs {

enum class MetadataStoreType {
  kLocalFile,
};
/**
 * @brief LoaderFactory is a factory class to create IFragmentLoader.
 * Support Using dynamically built library as plugin.
 */
class MetadataStoreFactory {
 public:
  static std::shared_ptr<IGraphMetaStore> Create(MetadataStoreType type,
                                                 const std::string& path);
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_METADATA_STORE_FACTORY_H_