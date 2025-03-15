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
std::shared_ptr<IGraphMetaStore> MetadataStoreFactory::Create(
    const std::string& metadata_store_uri) {
  auto scheme = get_uri_scheme(metadata_store_uri);
  if (scheme == "file") {
    return std::make_shared<DefaultGraphMetaStore>(
        std::make_unique<LocalFileMetadataStore>(
            get_uri_path(metadata_store_uri)));
  }
#ifdef BUILD_ETCD_METASTORE
  else if (scheme == "http") {  // assume http uri must be etcd
    return std::make_shared<DefaultGraphMetaStore>(
        std::make_unique<ETCDMetadataStore>(metadata_store_uri));
  }
#endif
  else {
    LOG(FATAL) << "Unsupported metadata store type: " << scheme;
  }
  return nullptr;
}
}  // namespace gs