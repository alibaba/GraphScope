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

#include "flex/storages/metadata/local_file_metadata_store.h"

namespace gs {

LocalFileMetadataStore::LocalFileMetadataStore(const std::string& path)
    : root_dir_(path) {
  VLOG(10) << "Root dir: " << root_dir_;
}

LocalFileMetadataStore::~LocalFileMetadataStore() { Close(); }

Result<bool> LocalFileMetadataStore::Open() {
  // open directories.
  RETURN_IF_NOT_OK(create_directory(root_dir_));
  LOG(INFO) << "Successfully open metadata store";
  return true;
}

Result<bool> LocalFileMetadataStore::Close() { return true; }

Result<LocalFileMetadataStore::meta_key_t> LocalFileMetadataStore::CreateMeta(
    const meta_kind_t& meta_kind, const meta_value_t& value) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  meta_key_t meta_key;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(meta_key, get_next_meta_key(meta_kind));
  VLOG(10) << "got next meta key: " << meta_key;
  if (is_key_exist(meta_kind, meta_key)) {
    return Status(StatusCode::INTERNAL_ERROR,
                  "When creating meta, got an existing key");
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  auto res = dump_file(meta_file, value);
  if (!res.ok()) {
    return res.status();
  }
  return meta_key;
}

Result<LocalFileMetadataStore::meta_key_t> LocalFileMetadataStore::CreateMeta(
    const meta_kind_t& meta_kind, const meta_key_t& meta_key,
    const meta_value_t& value) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  if (is_key_exist(meta_kind, meta_key)) {
    LOG(ERROR) << "Can not insert meta, key already exists: " << meta_kind
               << ", meta_key: " << meta_key;
    return Status(StatusCode::INTERNAL_ERROR,
                  "key " + meta_key + " already exits for meta: " + meta_kind);
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  auto res = dump_file(meta_file, value);
  if (!res.ok()) {
    return res.status();
  }
  return meta_key;
}

Result<LocalFileMetadataStore::meta_value_t> LocalFileMetadataStore::GetMeta(
    const meta_key_t& meta_kind, const meta_key_t& meta_key) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  if (!is_key_exist(meta_kind, meta_key)) {
    return Status(StatusCode::NOT_FOUND,
                  "key " + meta_key + " not found for :" + meta_kind);
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  meta_value_t meta_value;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(meta_value, read_file(meta_file));
  return meta_value;
}

Result<std::vector<std::pair<LocalFileMetadataStore::meta_key_t,
                             LocalFileMetadataStore::meta_value_t>>>
LocalFileMetadataStore::GetAllMeta(const meta_kind_t& meta_kind) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  std::vector<std::pair<meta_key_t, meta_value_t>> meta_values;
  auto meta_dir = get_meta_kind_dir(meta_kind);
  for (auto& p : std::filesystem::directory_iterator(meta_dir)) {
    if (!std::filesystem::is_regular_file(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(META_FILE_PREFIX) != std::string::npos) {
      if (file_name.find(META_FILE_PREFIX) == std::string::npos) {
        VLOG(10) << "Skip invalid file: " << file_name;
        continue;
      }
      auto id_str = file_name.substr(strlen(META_FILE_PREFIX));
      auto meta_file = get_meta_file(meta_kind, id_str);
      auto meta_value_res = read_file(meta_file);
      if (meta_value_res.ok()) {
        meta_values.push_back(
            std::make_pair(id_str, meta_value_res.move_value()));
      } else {
        LOG(ERROR) << "Error when reading meta file: " << meta_file;
      }
    } else {
      LOG(WARNING) << "Invalid file: " << file_name;
    }
  }
  return meta_values;
}

Result<bool> LocalFileMetadataStore::DeleteMeta(const meta_kind_t& meta_kind,
                                                const meta_key_t& meta_key) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  if (!is_key_exist(meta_kind, meta_key)) {
    return Status(StatusCode::NOT_FOUND,
                  "key " + meta_key + " not found for :" + meta_kind);
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  if (!std::filesystem::remove(meta_file)) {
    return Status(StatusCode::IO_ERROR, "Failed to delete meta");
  }
  return true;
}

Result<bool> LocalFileMetadataStore::DeleteAllMeta(
    const meta_kind_t& meta_kind) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  auto meta_dir = get_meta_kind_dir(meta_kind);
  if (!std::filesystem::remove_all(meta_dir)) {
    return Status(StatusCode::IO_ERROR, "Failed to delete meta");
  }
  VLOG(10) << "Remove all meta for " << meta_kind;
  return true;
}

Result<bool> LocalFileMetadataStore::UpdateMeta(
    const meta_kind_t& meta_kind, const meta_key_t& meta_key,
    const meta_value_t& meta_value) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  if (!is_key_exist(meta_kind, meta_key)) {
    return Status(StatusCode::NOT_FOUND,
                  "key " + meta_key + " not found for :" + meta_kind);
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  auto res = dump_file(meta_file, meta_value);
  if (!res.ok()) {
    return res.status();
  }
  return true;
}

Result<bool> LocalFileMetadataStore::UpdateMeta(const meta_kind_t& meta_kind,
                                                const meta_key_t& meta_key,
                                                update_func_t update_func) {
  std::unique_lock<std::mutex> lock(meta_mutex_);
  if (!is_key_exist(meta_kind, meta_key)) {
    return Status(StatusCode::NOT_FOUND,
                  "key " + meta_key + " not found for :" + meta_kind);
  }
  auto meta_file = get_meta_file(meta_kind, meta_key);
  auto meta_value_res = read_file(meta_file);
  if (!meta_value_res.ok()) {
    return meta_value_res.status();
  }
  auto new_meta_value = update_func(meta_value_res.value());
  if (!new_meta_value.ok()) {
    return new_meta_value.status();
  }
  auto res = dump_file(meta_file, new_meta_value.value());
  if (!res.ok()) {
    return res.status();
  }
  return true;
}

Result<LocalFileMetadataStore::meta_key_t>
LocalFileMetadataStore::get_next_meta_key(
    const LocalFileMetadataStore::meta_kind_t& meta_kind) const {
  return std::to_string(get_max_id(meta_kind) + 1);
}

std::string LocalFileMetadataStore::get_root_meta_dir() const {
  auto ret = root_dir_ + "/" + METADATA_DIR;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_meta_kind_dir(
    const meta_kind_t& meta_kind) const {
  auto ret = get_root_meta_dir() + "/" + meta_kind;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_meta_file(const meta_kind_t& meta_kind,
                                                  const meta_key_t& key) const {
  auto ret = get_meta_kind_dir(meta_kind) + "/" + META_FILE_PREFIX + key;
  return ret;
}

int32_t LocalFileMetadataStore::get_max_id(const meta_kind_t& meta_kind) const {
  // iterate all files in the directory, get the max id.
  int max_id_ = 0;
  auto dir = get_meta_kind_dir(meta_kind);
  for (auto& p : std::filesystem::directory_iterator(dir)) {
    if (std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(META_FILE_PREFIX));
      int32_t id;
      try {
        id = std::stoi(id_str);
      } catch (std::invalid_argument& e) {
        LOG(ERROR) << "Invalid id: " << id_str;
        continue;
      }
      if (id > max_id_) {
        max_id_ = id;
      }
    }
  }
  return max_id_;
}

bool LocalFileMetadataStore::is_key_exist(
    const LocalFileMetadataStore::meta_kind_t& meta_kind,
    const LocalFileMetadataStore::meta_key_t& meta_key) const {
  auto meta_file = get_meta_file(meta_kind, meta_key);
  return std::filesystem::exists(meta_file);
}

Result<bool> LocalFileMetadataStore::dump_file(
    const std::string& file_path, const std::string& content) const {
  std::ofstream out_file(file_path);
  if (!out_file.is_open()) {
    return Result<bool>(gs::StatusCode::IO_ERROR, false);
  }
  out_file << content;
  out_file.close();
  return Result<bool>(true);
}

Result<LocalFileMetadataStore::meta_value_t> LocalFileMetadataStore::read_file(
    const std::string& file_path) const {
  std::ifstream in_file(file_path);
  if (!in_file.is_open()) {
    return Result<LocalFileMetadataStore::meta_value_t>(
        gs::StatusCode::IO_ERROR, "Failed to open file");
  }
  std::string content((std::istreambuf_iterator<char>(in_file)),
                      std::istreambuf_iterator<char>());
  in_file.close();
  return Result<LocalFileMetadataStore::meta_value_t>(content);
}

Result<bool> LocalFileMetadataStore::create_directory(
    const std::string& dir) const {
  if (!std::filesystem::exists(dir)) {
    if (!std::filesystem::create_directory(dir)) {
      return Result<bool>(gs::StatusCode::IO_ERROR,
                          "Failed to create directory");
    }
  }
  return Result<bool>(true);
}

}  // namespace gs