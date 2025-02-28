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

#ifndef RUNTIME_COMMON_CONTEXT_H_
#define RUNTIME_COMMON_CONTEXT_H_

#include <set>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

namespace gs {

namespace runtime {

class Context {
 public:
  static Context InitContext() { return Context(); }
  Context newContext() const;

  ~Context() = default;

  void clear();

  void set(int alias, std::shared_ptr<IContextColumn> col);

  void set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                          const std::vector<size_t>& offsets);

  void reshuffle(const std::vector<size_t>& offsets);
  void optional_reshuffle(const std::vector<size_t>& offsets);

  std::shared_ptr<IContextColumn> get(int alias);

  const std::shared_ptr<IContextColumn> get(int alias) const;

  void remove(int alias);

  size_t row_num() const;

  bool exist(int alias) const;

  void desc(const std::string& info = "") const;

  void show(const GraphReadInterface& graph) const;

  size_t col_num() const;

  void gen_offset();

  Context union_ctx(const Context& ctx) const;

  std::vector<std::shared_ptr<IContextColumn>> columns;
  std::shared_ptr<IContextColumn> head;

  // for intersect
  const ValueColumn<size_t>& get_offsets() const;
  std::shared_ptr<ValueColumn<size_t>> offset_ptr;
  std::vector<int> tag_ids;

  mutable std::shared_ptr<ValueCollection> value_collection;

 private:
  Context();
};

class ContextMeta {
 public:
  ContextMeta() = default;
  ~ContextMeta() = default;

  bool exist(int alias) const {
    if (alias == -1) {
      return head_exists_;
    }
    return alias_set_.find(alias) != alias_set_.end();
  }

  void set(int alias) {
    if (alias >= 0) {
      head_ = alias;
      head_exists_ = true;
      alias_set_.insert(alias);
    }
  }

  const std::set<int>& columns() const { return alias_set_; }

  void desc() const {
    std::cout << "===============================================" << std::endl;
    for (auto col : alias_set_) {
      std::cout << "col - " << col << std::endl;
    }
    if (head_exists_) {
      std::cout << "head - " << head_ << std::endl;
    }
  }

 private:
  std::set<int> alias_set_;
  int head_ = -1;
  bool head_exists_ = false;
};

class WriteContext {
 public:
  struct WriteParams {
    WriteParams() = default;
    WriteParams(const std::string& val) : value(val) {}
    WriteParams(std::string_view val) : value(val) {}
    bool operator<(const WriteParams& rhs) const { return value < rhs.value; }
    bool operator==(const WriteParams& rhs) const { return value == rhs.value; }
    std::vector<WriteParams> unfold() const {
      std::vector<WriteParams> res;
      size_t start = 0;
      for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == ';') {
          res.emplace_back(value.substr(start, i - start));
          start = i + 1;
        }
      }
      if (start < value.size()) {
        res.emplace_back(value.substr(start));
      }
      return res;
    }

    std::pair<WriteParams, WriteParams> pairs() const {
      size_t pos = value.find_first_of(',');
      if (pos == std::string::npos) {
        LOG(FATAL) << "Invalid pair value: " << value;
      }
      return std::make_pair(WriteParams(value.substr(0, pos)),
                            WriteParams(value.substr(pos + 1)));
    }
    Any to_any(PropertyType type) const {
      if (type == PropertyType::kInt32) {
        return Any(std::stoi(std::string(value)));
      } else if (type == PropertyType::kInt64) {
        return Any(static_cast<int64_t>(std::stoll(std::string(value))));
      } else if (type == PropertyType::kDouble) {
        return Any(std::stod(std::string(value)));
      } else if (type == PropertyType::kString) {
        return Any(value);
      } else if (type == PropertyType::kBool) {
        return Any(value == "true");
      } else if (type == PropertyType::kDate) {
        return Any(Date(std::stoll(std::string(value))));
      } else if (type == PropertyType::kDay) {
        return Any(Day(std::stoll(std::string(value))));
      } else {
        LOG(FATAL) << "Unsupported type: " << type;
        return Any();
      }
    }

    std::string_view value;
  };

  struct WriteParamsColumn {
    WriteParamsColumn() : is_set(false) {}
    WriteParamsColumn(std::vector<WriteParams>&& col)
        : values(std::move(col)), is_set(true) {}
    int size() const { return values.size(); }
    const WriteParams& get(int idx) const { return values[idx]; }
    std::pair<WriteParamsColumn, std::vector<size_t>> unfold() {
      std::vector<WriteParams> res;
      std::vector<size_t> offsets;
      for (size_t i = 0; i < values.size(); ++i) {
        auto unfolded = values[i].unfold();
        for (size_t j = 0; j < unfolded.size(); ++j) {
          res.push_back(unfolded[j]);
          offsets.push_back(i);
        }
      }
      return std::make_pair(WriteParamsColumn(std::move(res)),
                            std::move(offsets));
    }

    std::pair<WriteParamsColumn, WriteParamsColumn> pairs() {
      WriteParamsColumn res;
      WriteParamsColumn res2;
      for (size_t i = 0; i < values.size(); ++i) {
        auto [left, right] = values[i].pairs();
        res.push_back(left);
        res2.push_back(right);
      }
      return std::make_pair(WriteParamsColumn(std::move(res)), std::move(res2));
    }
    void push_back(WriteParams&& val) {
      is_set = true;
      values.push_back(std::move(val));
    }
    void push_back(const WriteParams& val) {
      is_set = true;
      values.push_back(val);
    }
    void clear() {
      is_set = false;
      values.clear();
    }
    std::vector<WriteParams> values;
    bool is_set;
  };

  WriteContext() = default;
  ~WriteContext() = default;

  int col_num() const { return vals.size(); }

  void set(int alias, WriteParamsColumn&& col) {
    if (alias >= static_cast<int>(vals.size())) {
      vals.resize(alias + 1);
    }
    vals[alias] = std::move(col);
  }
  void reshuffle(const std::vector<size_t>& offsets) {
    for (size_t i = 0; i < vals.size(); ++i) {
      if (vals[i].is_set) {
        WriteParamsColumn new_col;
        for (size_t j = 0; j < offsets.size(); ++j) {
          new_col.push_back(vals[i].get(offsets[j]));
        }
        vals[i] = std::move(new_col);
      }
    }
  }

  void set_with_reshuffle(int alias, WriteParamsColumn&& col,
                          const std::vector<size_t>& offsets) {
    if (alias >= static_cast<int>(vals.size())) {
      vals.resize(alias + 1);
    }
    if (vals[alias].is_set) {
      vals[alias].clear();
    }
    reshuffle(offsets);
    vals[alias] = std::move(col);
  }

  const WriteParamsColumn& get(int alias) const {
    if (alias >= static_cast<int>(vals.size())) {
      LOG(FATAL) << "Alias " << alias << " not found in WriteContext";
    }
    return vals[alias];
  }

  WriteParamsColumn& get(int alias) {
    if (alias >= static_cast<int>(vals.size())) {
      LOG(FATAL) << "Alias " << alias << " not found in WriteContext";
    }
    return vals[alias];
  }

  void clear() { vals.clear(); }

  int row_num() const {
    if (vals.empty()) {
      return 0;
    }
    for (size_t i = 0; i < vals.size(); ++i) {
      if (vals[i].is_set) {
        return vals[i].size();
      }
    }
    return 0;
  }

 private:
  std::vector<WriteParamsColumn> vals;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_CONTEXT_H_