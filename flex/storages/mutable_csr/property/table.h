#ifndef GRAPHSCOPE_PROPERTY_TABLE_H_
#define GRAPHSCOPE_PROPERTY_TABLE_H_

#include <map>
#include <memory>
#include <string_view>

#include "flex/storages/mutable_csr/graph/id_indexer.h"
#include "grape/io/local_io_adaptor.h"
#include "flex/storages/mutable_csr/property/column.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class Table {
 public:
  Table();
  ~Table();

  void init(const std::vector<std::string>& col_name,
            const std::vector<PropertyType>& types,
            const std::vector<StorageStrategy>& strategies_,
            size_t max_row_num);

  std::vector<std::string> column_names() const;

  std::vector<PropertyType> column_types() const;

  std::shared_ptr<ColumnBase> get_column(const std::string& name);

  const std::shared_ptr<ColumnBase> get_column(const std::string& name) const;

  std::vector<Any> get_row(size_t row_id) const;

  std::shared_ptr<ColumnBase> get_column_by_id(size_t index);

  const std::shared_ptr<ColumnBase> get_column_by_id(size_t index) const;

  size_t col_num() const;
  std::vector<std::shared_ptr<ColumnBase>>& columns();

  void insert(size_t index, const std::vector<Any>& values);

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer,
                 const std::string& prefix, size_t row_num);

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader,
                   const std::string& prefix);

  Any at(size_t row_id, size_t col_id);

  Any at(size_t row_id, size_t col_id) const;

  void ingest(uint32_t index, grape::OutArchive& arc);

 private:
  void buildColumnPtrs();

  std::vector<std::shared_ptr<ColumnBase>> columns_;
  IdIndexer<std::string, int> col_id_indexer_;

  std::vector<ColumnBase*> column_ptrs_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_TABLE_H_
