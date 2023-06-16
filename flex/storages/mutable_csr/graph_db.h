#ifndef GRAPHSCOPE_MUTABLE_CSR_GRAPH_DB_H_
#define GRAPHSCOPE_MUTABLE_CSR_GRAPH_DB_H_

#include <dlfcn.h>

#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "flex/utils/app_utils.h"
#include "grape/grape.h"
#include "flex/storages/mutable_csr/fragment/ts_property_fragment.h"
#include "flex/storages/mutable_csr/read_transaction.h"
#include "flex/storages/mutable_csr/version_manager.h"

namespace gs {

class StaticIndex;

class GraphDB {
 public:
  GraphDB();
  ~GraphDB();

  void Init(const std::string& graph_dir, const std::string& data_dir,
            int thread_num);
  ReadTransaction GetReadTransaction();

  const TSPropertyFragment& graph() const;
  TSPropertyFragment& graph();

  const Schema& schema() const;

  const std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column_x(
      uint8_t label, const std::string& col_name) const;

 private:
  void loadFromRawFiles(const std::string& graph_dir, int thread_num);

  TSPropertyFragment graph_;
  VersionManager version_manager_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_MUTABLE_CSR_GRAPH_DB_H_
