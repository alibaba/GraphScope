#ifndef GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_
#define GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_

#include <string>

namespace gs {

/*
    ├── schema
    ├── runtime
    │   ├── allocator                                // allocator dir
    │   ├── tails                                    // tails (mutable parts) of
   tables │   │   ├── vertex_table_PERSON.col_0 │   │   ├──
   vertex_table_PERSON.col_1.data │   │   └── vertex_table_PERSON.col_1.items
    │   └── tmp                                      // tmp dir, used for
   touched vertex maps, vertex tables and adjlists of csrs │       ├──
   ie_PERSON_KNOWS_PERSON.adj │       ├── oe_PERSON_KNOWS_PERSON.adj │       ├──
   vertex_map_PERSON.indices │       ├── vertex_map_PERSON.keys │       ├──
   vertex_table_PERSON.col_0 │       ├── vertex_table_PERSON.col_1.data │   └──
   vertex_table_PERSON.col_1.items ├── snapshots // snapshots dir │   ├── 0 │  
   │   ├── ie_PERSON_KNOWS_PERSON.deg │   │   ├── ie_PERSON_KNOWS_PERSON.nbr │  
   │   ├── oe_PERSON_KNOWS_PERSON.deg │   │   ├── oe_PERSON_KNOWS_PERSON.nbr │  
   │   ├── vertex_map_PERSON.indices │   │   ├── vertex_map_PERSON.keys │   │  
   ├── vertex_map_PERSON.meta │   │   ├── vertex_table_PERSON.col_0 │   │   ├──
   vertex_table_PERSON.col_1.data │   │   └── vertex_table_PERSON.col_1.items
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

inline std::string schema_path(const std::string& work_dir) {
  return work_dir + "/schema";
}

inline std::string snapshots_dir(const std::string& work_dir) {
  return work_dir + "/snapshots/";
}

inline std::string snapshot_version_path(const std::string& work_dir) {
  return snapshots_dir(work_dir) + "/VERSION";
}

inline uint32_t get_snapshot_version(const std::string& work_dir) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "rb");
  uint32_t version = 0;
  fread(&version, sizeof(uint32_t), 1, version_file);
  fclose(version_file);
  return version;
}

inline void set_snapshot_version(const std::string& work_dir,
                                 uint32_t version) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "wb");
  fwrite(&version, sizeof(uint32_t), 1, version_file);
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

inline std::string vertex_table_prefix(const std::string& label) {
  return "vertex_table_" + label;
}

inline std::string thread_local_allocator_prefix(const std::string& work_dir,
                                                 int thread_id) {
  return allocator_dir(work_dir) + "allocator_" + std::to_string(thread_id) +
         "_";
}

}  // namespace gs

#endif  // GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_