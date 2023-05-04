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

#ifndef GRAPHSCOPE_PROPERTY_TABLE_H_
#define GRAPHSCOPE_PROPERTY_TABLE_H_

#include <map>
#include <memory>
#include <string_view>

#include "flex/utils/id_indexer.h"
#include "flex/utils/property/column.h"
#include "grape/io/local_io_adaptor.h"
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

  void reset_header(const std::vector<std::string>& col_name);

  std::vector<std::string> column_names() const;

  std::vector<PropertyType> column_types() const;

  std::shared_ptr<ColumnBase> get_column(const std::string& name);

  const std::shared_ptr<ColumnBase> get_column(const std::string& name) const;

  Property get_row(size_t row_id) const;

  std::vector<Property> get_row_as_vec(size_t row_id) const;

  std::shared_ptr<ColumnBase> get_column_by_id(size_t index);

  const std::shared_ptr<ColumnBase> get_column_by_id(size_t index) const;

  size_t col_num() const;

  std::vector<std::shared_ptr<ColumnBase>>& columns();

  const std::vector<std::shared_ptr<ColumnBase>>& columns() const;

  void insert(size_t index, const std::vector<Property>& values);

  void insert(size_t index, const Property& value);

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer,
                 const std::string& prefix, size_t row_num);

  void Serialize(const std::string& prefix, size_t row_num);

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader,
                   const std::string& prefix);

  void Deserialize(const std::string& prefix);

  Property at(size_t row_id, size_t col_id) const;

  void ingest(uint32_t index, grape::OutArchive& arc);

 private:
  void buildColumnPtrs();

  std::vector<std::shared_ptr<ColumnBase>> columns_;
  IdIndexer<std::string, int> col_id_indexer_;

  std::vector<ColumnBase*> column_ptrs_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_TABLE_H_
