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

#include "flex/utils/property/table.h"

namespace gs {

Table::Table() {}
Table::~Table() {}

void Table::init(const std::vector<std::string>& col_name,
                 const std::vector<PropertyType>& types,
                 const std::vector<StorageStrategy>& strategies_,
                 size_t max_row_num) {
  size_t col_num = col_name.size();
  columns_.resize(col_num);
  auto strategies = strategies_;
  strategies.resize(col_num, StorageStrategy::kMem);

  for (size_t i = 0; i < col_num; ++i) {
    int col_id;
    col_id_indexer_.add(col_name[i], col_id);
    columns_[col_id] = CreateColumn(types[i], strategies[i]);
    columns_[col_id]->init(max_row_num);
  }
  columns_.resize(col_id_indexer_.size());

  buildColumnPtrs();
}

void Table::reset_header(const std::vector<std::string>& col_name) {
  IdIndexer<std::string, int> new_col_id_indexer;
  size_t col_num = col_name.size();
  for (size_t i = 0; i < col_num; ++i) {
    int tmp;
    new_col_id_indexer.add(col_name[i], tmp);
  }
  CHECK_EQ(col_num, new_col_id_indexer.size());
  col_id_indexer_.swap(new_col_id_indexer);
}

std::vector<std::string> Table::column_names() const {
  size_t col_num = col_id_indexer_.size();
  std::vector<std::string> names(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    CHECK(col_id_indexer_.get_key(col_i, names[col_i]));
  }
  return names;
}

std::vector<PropertyType> Table::column_types() const {
  size_t col_num = col_id_indexer_.size();
  std::vector<PropertyType> types(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    types[col_i] = columns_[col_i]->type();
  }
  return types;
}

std::shared_ptr<ColumnBase> Table::get_column(const std::string& name) {
  int col_id;
  if (col_id_indexer_.get_index(name, col_id)) {
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id];
    }
  }

  return nullptr;
}

const std::shared_ptr<ColumnBase> Table::get_column(
    const std::string& name) const {
  int col_id;
  if (col_id_indexer_.get_index(name, col_id)) {
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id];
    }
  }

  return nullptr;
}

std::vector<Property> Table::get_row_as_vec(size_t row_id) const {
  std::vector<Property> ret;
  for (auto ptr : columns_) {
    ret.push_back(ptr->get(row_id));
  }
  return ret;
}

Property Table::get_row(size_t row_id) const {
  auto vec = get_row_as_vec(row_id);
  Property ret;
  ret.set_value<std::vector<Property>>(vec);
  return ret;
}

std::shared_ptr<ColumnBase> Table::get_column_by_id(size_t index) {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index];
  }
}

const std::shared_ptr<ColumnBase> Table::get_column_by_id(size_t index) const {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index];
  }
}

size_t Table::col_num() const { return columns_.size(); }
std::vector<std::shared_ptr<ColumnBase>>& Table::columns() { return columns_; }
const std::vector<std::shared_ptr<ColumnBase>>& Table::columns() const { return columns_; }

void Table::insert(size_t index, const Property& value) {
  if (value.type() == PropertyType::kEmpty) {
    CHECK_EQ(columns_.size(), 0);
  } else if (value.type() == PropertyType::kList) {
    auto list = value.get_value<std::vector<Property>>();
    insert(index, list);
  } else {
    CHECK_EQ(columns_.size(), 1);
    columns_[0]->set(index, value);
  }
}

void Table::insert(size_t index, const std::vector<Property>& values) {
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set(index, values[i]);
  }
}

void Table::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer,
                      const std::string& prefix, size_t row_num) {
  col_id_indexer_.Serialize(writer);
  std::vector<PropertyType> types;
  std::vector<StorageStrategy> stategies;
  for (auto col : columns_) {
    types.push_back(col->type());
    stategies.push_back(col->storage_strategy());
  }
  CHECK_EQ(types.size(), col_id_indexer_.size());
  if (types.empty()) {
    return;
  }
  CHECK(writer->Write(types.data(), sizeof(PropertyType) * types.size()));
  CHECK(writer->Write(stategies.data(),
                      sizeof(StorageStrategy) * stategies.size()));
  size_t col_id = 0;
  for (auto col : columns_) {
    col->Serialize(prefix + ".col_" + std::to_string(col_id), row_num);
    ++col_id;
  }
}

void Table::Serialize(const std::string& prefix, size_t row_num) {
  grape::InArchive arc;
  arc << col_id_indexer_;
  CHECK_EQ(col_id_indexer_.size(), columns_.size())
      << "col_id_indexer_.size() = " << col_id_indexer_.size()
      << ", columns_.size() = " << columns_.size();
  for (auto col : columns_) {
    arc << col->type() << col->storage_strategy();
  }
  std::string meta_file_name = prefix + ".meta";
  FILE* fmeta = fopen(meta_file_name.c_str(), "wb");
  fwrite(arc.GetBuffer(), 1, arc.GetSize(), fmeta);
  fflush(fmeta);
  fclose(fmeta);

  size_t col_id = 0;
  for (auto col : columns_) {
    col->Serialize(prefix + ".col_" + std::to_string(col_id), row_num);
    ++col_id;
  }
}

void Table::Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader,
                        const std::string& prefix) {
  col_id_indexer_.Deserialize(reader);
  std::vector<PropertyType> types(col_id_indexer_.size());
  std::vector<StorageStrategy> strategies(col_id_indexer_.size());
  if (types.empty()) {
    return;
  }
  CHECK(reader->Read(types.data(), sizeof(PropertyType) * types.size()));
  CHECK(reader->Read(strategies.data(),
                     sizeof(StorageStrategy) * strategies.size()));
  columns_.resize(types.size());
  for (size_t i = 0; i < types.size(); ++i) {
    auto type = types[i];
    auto strategy = strategies[i];
    auto ptr = CreateColumn(type, strategy);
    ptr->Deserialize(prefix + ".col_" + std::to_string(i));
    columns_[i] = ptr;
  }

  buildColumnPtrs();
}

void Table::Deserialize(const std::string& prefix) {
  std::string meta_file_name = prefix + ".meta";
  FILE* fmeta = fopen(meta_file_name.c_str(), "rb");
  fseek(fmeta, 0, SEEK_END);
  size_t meta_size = ftell(fmeta);
  fseek(fmeta, 0, SEEK_SET);
  grape::OutArchive arc(meta_size);
  fread(arc.GetBuffer(), 1, meta_size, fmeta);
  fclose(fmeta);

  IdIndexer<std::string, int> new_col_id_indexer;
  arc >> new_col_id_indexer;
  col_id_indexer_.swap(new_col_id_indexer);
  size_t col_num = col_id_indexer_.size();

  columns_.clear();
  columns_.resize(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    PropertyType type;
    StorageStrategy strategy;
    arc >> type >> strategy;
    auto ptr = CreateColumn(type, strategy);
    ptr->Deserialize(prefix + ".col_" + std::to_string(col_i));
    columns_[col_i] = ptr;
  }

  buildColumnPtrs();
}

Property Table::at(size_t row_id, size_t col_id) const {
  return columns_[col_id]->get(row_id);
}

void Table::ingest(uint32_t index, grape::OutArchive& arc) {
  for (auto col : column_ptrs_) {
    col->ingest(index, arc);
  }
}

void Table::buildColumnPtrs() {
  size_t col_num = columns_.size();
  column_ptrs_.clear();
  column_ptrs_.resize(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    column_ptrs_[col_i] = columns_[col_i].get();
  }
}

}  // namespace gs
