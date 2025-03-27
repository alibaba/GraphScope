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

#include "flex/engines/graph_db/database/wal/wal.h"
#include <dlfcn.h>
#include <memory>
#include <utility>

#include <boost/algorithm/string.hpp>

namespace gs {

std::string get_wal_uri_scheme(const std::string& uri) {
  std::string scheme;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    scheme = uri.substr(0, pos);
  }
  if (scheme.empty()) {
    VLOG(1) << "No scheme found in wal uri: " << uri
            << ", using default scheme: file";
    scheme = "file";
  }
  return scheme;
}

std::string get_wal_uri_path(const std::string& uri) {
  std::string path;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    path = uri.substr(pos + 3);
  } else {
    path = uri;
  }
  return path;
}

void WalWriterFactory::Init() {}

void WalWriterFactory::Finalize() {}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateWalWriter(
    const std::string& wal_uri) {
  auto& known_writers_ = getKnownWalWriters();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = known_writers_.find(scheme);
  if (iter != known_writers_.end()) {
    return iter->second();
  } else {
    LOG(FATAL) << "Unknown wal writer: " << scheme << " for uri: " << wal_uri;
  }
}

bool WalWriterFactory::RegisterWalWriter(
    const std::string& wal_writer_type,
    WalWriterFactory::wal_writer_initializer_t initializer) {
  LOG(INFO) << "Registering wal writer of type: " << wal_writer_type;
  auto& known_writers_ = getKnownWalWriters();
  known_writers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalWriterFactory::wal_writer_initializer_t>&
WalWriterFactory::getKnownWalWriters() {
  static std::unordered_map<
      std::string, WalWriterFactory::wal_writer_initializer_t>* known_writers_ =
      new std::unordered_map<std::string, wal_writer_initializer_t>();
  return *known_writers_;
}

////////////////////////// WalParserFactory //////////////////////////

void WalParserFactory::Init() {}

void WalParserFactory::Finalize() {}

std::unique_ptr<IWalParser> WalParserFactory::CreateWalParser(
    const std::string& wal_uri) {
  auto& know_parsers_ = getKnownWalParsers();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = know_parsers_.find(scheme);
  if (iter != know_parsers_.end()) {
    return iter->second(wal_uri);
  } else {
    LOG(FATAL) << "Unknown wal parser: " << scheme << " for uri: " << wal_uri;
  }
}

bool WalParserFactory::RegisterWalParser(
    const std::string& wal_writer_type,
    WalParserFactory::wal_parser_initializer_t initializer) {
  LOG(INFO) << "Registering wal writer of type: " << wal_writer_type;
  auto& known_parsers_ = getKnownWalParsers();
  known_parsers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalParserFactory::wal_parser_initializer_t>&
WalParserFactory::getKnownWalParsers() {
  static std::unordered_map<
      std::string, WalParserFactory::wal_parser_initializer_t>* known_parsers_ =
      new std::unordered_map<std::string, wal_parser_initializer_t>();
  return *known_parsers_;
}

}  // namespace gs
