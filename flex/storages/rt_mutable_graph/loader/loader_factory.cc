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
#include <dlfcn.h>
#include <memory>
#include <utility>

namespace gs {

void LoaderFactory::Init() {
  // get env FLEX_OTHER_LOADERS
  if (getenv("FLEX_OTHER_LOADERS")) {
    auto other_loaders = getenv("FLEX_OTHER_LOADERS");
    std::vector<std::string> adaptors;
    ::boost::split(adaptors, other_loaders,
                   ::boost::is_any_of(std::string(1, ':')));
    for (auto const& adaptor : adaptors) {
      if (!adaptor.empty()) {
        if (dlopen(adaptor.c_str(), RTLD_GLOBAL | RTLD_NOW) == nullptr) {
          LOG(WARNING) << "Failed to load io adaptors " << adaptor
                       << ", reason = " << dlerror();
        } else {
          LOG(INFO) << "Loaded io adaptors " << adaptor;
        }
      }
    }
  } else {
    LOG(INFO) << "No extra loaders provided";
  }
}

void LoaderFactory::Finalize() {}

std::shared_ptr<IFragmentLoader> LoaderFactory::CreateFragmentLoader(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config) {
  auto scheme = loading_config.GetScheme();
  auto format = loading_config.GetFormat();
  auto key = scheme + format;
  auto& known_loaders_ = getKnownLoaders();
  auto iter = known_loaders_.find(key);
  if (iter != known_loaders_.end()) {
    return iter->second(work_dir, schema, loading_config);
  } else {
    LOG(FATAL) << "Unsupported format: " << format;
  }
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
