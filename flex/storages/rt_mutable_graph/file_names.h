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
#ifndef STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_
#define STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <filesystem>

#include "glog/logging.h"

namespace gs {
// clang-format off
/*
    ├── schema
    ├── runtime
    │   ├── allocator                                // allocator dir
    │   ├── tails                                    // tails (mutable parts) of tables 
    │   │   ├── vertex_table_PERSON.col_0 
    │   │   ├── vertex_table_PERSON.col_1.data 
    │   │   └── vertex_table_PERSON.col_1.items
    │   └── tmp                                      // tmp dir, used for touched vertex maps, vertex tables and adjlists of csrs 
    │       ├── ie_PERSON_KNOWS_PERSON.adj 
    │       ├── oe_PERSON_KNOWS_PERSON.adj 
    │       ├── vertex_map_PERSON.indices 
    │       ├── vertex_map_PERSON.keys 
    │       ├── vertex_table_PERSON.col_0 
    │       ├── vertex_table_PERSON.col_1.data
    │       └── vertex_table_PERSON.col_1.items 
            └── bulk_load_progress.log               // bulk load progress file
    ├── snapshots // snapshots dir 
    │   ├── 0 
    │   │   ├── ie_PERSON_KNOWS_PERSON.deg 
    │   │   ├── ie_PERSON_KNOWS_PERSON.nbr 
    │   │   ├── oe_PERSON_KNOWS_PERSON.deg 
    │   │   ├── oe_PERSON_KNOWS_PERSON.nbr 
    │   │   ├── vertex_map_PERSON.indices 
    │   │   ├── vertex_map_PERSON.keys 
    │   │   ├── vertex_map_PERSON.meta 
    │   │   ├── vertex_table_PERSON.col_0 
    │   │   ├── vertex_table_PERSON.col_1.data 
    │   │   └── vertex_table_PERSON.col_1.items
    │   ├── 1234567
    │   │   ├── ie_PERSON_KNOWS_PERSON.deg
    │   │   ├── ie_PERSON_KNOWS_PERSON.nbr
    │   │   ├── oe_PERSON_KNOWS_PERSON.deg
    │   │   ├── oe_PERSON_KNOWS_PERSON.nbr
    │   │   ├── vertex_map_PERSON.indices
    │   │   ├── vertex_map_PERSON.keys
    │   │   ├── vertex_map_PERSON.meta
    │   │   ├── vertex_table_PERSON.col_0
    │   │   ├── vertex_table_PERSON.col_1.data
    │   │   └── vertex_table_PERSON.col_1.items
    │   ├── ...
    │   └── VERSION
    └── wal                                         // wal dir
        ├── log_0
        ├── log_1
        └── ...
*/
// clang-format on

inline void copy_file(const std::string& src, const std::string& dst) {
  if (!std::filesystem::exists(src)) {
    LOG(ERROR) << "file not exists: " << src;
    return;
  }
#if USE_COPY_FILE_RANGE
  size_t len = std::filesystem::file_size(src);
  int src_fd = open(src.c_str(), O_RDONLY, 0777);
  bool creat = false;
  if (!std::filesystem::exists(dst)) {
    creat = true;
  }
  int dst_fd = open(dst.c_str(), O_WRONLY | O_CREAT, 0777);
  if (creat) {
    std::filesystem::perms readWritePermission =
        std::filesystem::perms::owner_read |
        std::filesystem::perms::owner_write;
    std::error_code errorCode;
    std::filesystem::permissions(dst, readWritePermission,
                                 std::filesystem::perm_options::add, errorCode);
    if (errorCode) {
      LOG(ERROR) << "Failed to set read/write permission for file: " << dst
                 << " " << errorCode.message() << std::endl;
    }

    // For a newly created file, you may need to close and then reopen it,
    // otherwise you may encounter a copy_file_range "Invalid cross-device link"
    // error, one possible cause of the error could be that the
    // file's metadata has not yet been flushed to the file system.
    close(dst_fd);
    dst_fd = open(dst.c_str(), O_WRONLY, 0777);
  }
  ssize_t ret;
  do {
    ret = copy_file_range(src_fd, NULL, dst_fd, NULL, len, 0);
    if (ret == -1) {
      perror("copy_file_range");
      return;
    }
    len -= ret;
  } while (len > 0 && ret > 0);
  close(src_fd);
  close(dst_fd);
#else
  bool creat = false;
  if (!std::filesystem::exists(dst)) {
    creat = true;
  }
  std::error_code errorCode;
  std::filesystem::copy_file(
      src, dst, std::filesystem::copy_options::overwrite_existing, errorCode);
  if (errorCode) {
    LOG(ERROR) << "Failed to copy file from " << src << " to " << dst << " "
               << errorCode.message() << std::endl;
  }
  if (creat) {
    std::filesystem::perms readWritePermission =
        std::filesystem::perms::owner_read |
        std::filesystem::perms::owner_write;
    std::error_code errorCode;
    std::filesystem::permissions(dst, readWritePermission,
                                 std::filesystem::perm_options::add, errorCode);
    if (errorCode) {
      LOG(INFO) << "Failed to set read/write permission for file: " << dst
                << " " << errorCode.message() << std::endl;
    }
  }

#endif
}

inline std::string schema_path(const std::string& work_dir) {
  return work_dir + "/schema";
}

inline std::string snapshots_dir(const std::string& work_dir) {
  return work_dir + "/snapshots/";
}

inline std::string snapshot_version_path(const std::string& work_dir) {
  return snapshots_dir(work_dir) + "/VERSION";
}

inline std::string get_latest_snapshot(const std::string& work_dir) {
  std::string snapshots_dir = work_dir + "/snapshots";
  uint32_t version;
  {
    FILE* fin = fopen((snapshots_dir + "/VERSION").c_str(), "r");
    CHECK_EQ(fread(&version, sizeof(uint32_t), 1, fin), 1);
    fclose(fin);
  }
  return snapshots_dir + "/" + std::to_string(version);
}

inline uint32_t get_snapshot_version(const std::string& work_dir) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "rb");
  uint32_t version = 0;
  CHECK_EQ(fread(&version, sizeof(uint32_t), 1, version_file), 1);
  fclose(version_file);
  return version;
}

inline void set_snapshot_version(const std::string& work_dir,
                                 uint32_t version) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "wb");
  CHECK_EQ(fwrite(&version, sizeof(uint32_t), 1, version_file), 1);
  fflush(version_file);
  fclose(version_file);
}

inline std::string snapshot_dir(const std::string& work_dir, uint32_t version) {
  return snapshots_dir(work_dir) + std::to_string(version) + "/";
}

inline std::string wal_dir(const std::string& work_dir) {
  return work_dir + "/wal/";
}

inline std::string runtime_dir(const std::string& work_dir) {
  return work_dir + "/runtime/";
}

inline std::string update_txn_dir(const std::string& work_dir,
                                  uint32_t version) {
  return runtime_dir(work_dir) + "update_txn_" + std::to_string(version) + "/";
}

inline std::string allocator_dir(const std::string& work_dir) {
  return runtime_dir(work_dir) + "allocator/";
}

inline std::string tmp_dir(const std::string& work_dir) {
  return runtime_dir(work_dir) + "tmp/";
}

inline std::string bulk_load_progress_file(const std::string& work_dir) {
  return tmp_dir(work_dir) + "bulk_load_progress.log";
}

inline void clear_tmp(const std::string& work_dir) {
  std::string tmp_dir_str = tmp_dir(work_dir);
  if (std::filesystem::exists(tmp_dir_str)) {
    assert(std::filesystem::is_directory(tmp_dir_str));
    if (std::filesystem::directory_iterator(tmp_dir_str) !=
        std::filesystem::directory_iterator()) {
      for (const auto& entry :
           std::filesystem::directory_iterator(tmp_dir_str)) {
        std::filesystem::remove_all(entry.path());
      }
    }
  }
}

inline std::string vertex_map_prefix(const std::string& label) {
  return "vertex_map_" + label;
}

inline std::string ie_prefix(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string edge_label) {
  return "ie_" + src_label + "_" + edge_label + "_" + dst_label;
}

inline std::string oe_prefix(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string edge_label) {
  return "oe_" + src_label + "_" + edge_label + "_" + dst_label;
}

inline std::string edata_prefix(const std::string& src_label,
                                const std::string& dst_label,
                                const std::string& edge_label) {
  return "e_" + src_label + "_" + edge_label + "_" + dst_label + "_data";
}
inline std::string vertex_table_prefix(const std::string& label) {
  return "vertex_table_" + label;
}

inline std::string thread_local_allocator_prefix(const std::string& work_dir,
                                                 int thread_id) {
  return allocator_dir(work_dir) + "allocator_" + std::to_string(thread_id) +
         "_";
}

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_
