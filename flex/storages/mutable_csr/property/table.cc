#include "flex/storages/mutable_csr/property/table.h"

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

std::vector<Any> Table::get_row(size_t row_id) const {
  std::vector<Any> ret;
  for (auto ptr : columns_) {
    ret.push_back(ptr->get(row_id));
  }
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

void Table::insert(size_t index, const std::vector<Any>& values) {
  assert(values.size() == columns_.size());
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set_any(index, values[i]);
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
  LOG(INFO) << "----------------------------------------";
  for (size_t i = 0; i < types.size(); ++i) {
    auto type = types[i];
    auto strategy = strategies[i];
    if (type == PropertyType::kInt32) {
      LOG(INFO) << "\tcol-" << i << " int";
    } else if (type == PropertyType::kDate) {
      LOG(INFO) << "\tcol-" << i << " Date";
    } else if (type == PropertyType::kString) {
      LOG(INFO) << "\tcol-" << i << " String";
    } else if (type == PropertyType::kBrowser) {
      LOG(INFO) << "\tcol-" << i << " Browser";
    } else if (type == PropertyType::kIpAddr) {
      LOG(INFO) << "\tcol-" << i << " IpAddr";
    } else if (type == PropertyType::kGender) {
      LOG(INFO) << "\tcol-" << i << " Gender";
    } else if (type == PropertyType::kInt64) {
      LOG(INFO) << "\tcol-" << i << " int64";
    } else {
      LOG(INFO) << "\t unexpected col-" << i << ": " << static_cast<int>(type);
    }
    auto ptr = CreateColumn(type, strategy);
    ptr->Deserialize(prefix + ".col_" + std::to_string(i));
    columns_[i] = ptr;
  }

  buildColumnPtrs();
}

Any Table::at(size_t row_id, size_t col_id) {
  return columns_[col_id]->get(row_id);
}

Any Table::at(size_t row_id, size_t col_id) const {
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
