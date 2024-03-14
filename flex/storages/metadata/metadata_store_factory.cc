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

#include "flex/storages/metadata/metadata_store_factory.h"

namespace gs {
std::shared_ptr<IMetaDataStore> MetadataStoreFactory::Create(
    MetadataStoreType type, const std::string& path) {
  switch (type) {
  case MetadataStoreType::kLocalFile:
#ifdef BUILD_FILE_META_STORE
    return std::make_shared<LocalFileMetadataStore>(path);
#else
    LOG(FATAL)
        << "Local file metadata store is not supported in current build.";
#endif
  default:
    LOG(FATAL) << "Unsupported metadata store type: " << static_cast<int>(type);
  }
  return nullptr;
}
}  // namespace gs