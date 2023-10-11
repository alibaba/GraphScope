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

#include "flex/storages/rt_mutable_graph/loader/loader_factory.h"

namespace gs {

void LoaderFactory::Init() {}

void LoaderFactory::Finalize() {}

std::shared_ptr<IFragmentLoader> LoaderFactory::CreateFragmentLoader(
    const Schema& schema, const LoadingConfig& loading_config, int thread_num) {
  auto format = loading_config.GetFormat();
  auto& known_loaders_ = getKnownLoaders();
  auto iter = known_loaders_.find(format);
  if (iter != known_loaders_.end()) {
    return iter->second(schema, loading_config, thread_num);
  } else {
    LOG(FATAL) << "Unsupported format: " << format;
  }
  // if (loading_config.GetFormat() == "csv") {
  //   return std::make_shared<CSVFragmentLoader>(schema, loading_config,
  //                                              thread_num);
  // } else {
  //   LOG(FATAL) << "Unsupported format: " << loading_config.GetFormat();
  // }
}

// the key of map should be scheme + format.
bool LoaderFactory::Register(const std::string& scheme_type,
                             const std::string& format,
                             LoaderFactory::loader_initializer_t initializer) {
  LOG(INFO) << "Registering loader: " << scheme_type << ", format:" << format;
  auto& known_loaders_ = getKnownLoaders();
  auto key = scheme_type + format;
  known_loaders_.emplace(key, initializer);
  return true;
}

std::unordered_map<std::string, LoaderFactory::loader_initializer_t>&
LoaderFactory::getKnownLoaders() {
  static std::unordered_map<std::string, LoaderFactory::loader_initializer_t>*
      known_loaders_ =
          new std::unordered_map<std::string, loader_initializer_t>();
  return *known_loaders_;
}

}  // namespace gs