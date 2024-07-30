#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

namespace gs {

namespace runtime {

std::shared_ptr<IContextColumn> ValueColumn<std::string_view>::shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<std::string_view> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> ValueColumn<std::string_view>::dup() const {
  ValueColumnBuilder<std::string_view> builder;
  for (auto v : data_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

template class ValueColumn<int>;
template class ValueColumn<std::set<std::string>>;
template class ValueColumn<std::vector<vid_t>>;

}  // namespace runtime

}  // namespace gs
