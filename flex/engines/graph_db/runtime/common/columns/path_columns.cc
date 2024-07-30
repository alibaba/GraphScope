#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"

namespace gs {
namespace runtime {
std::shared_ptr<IContextColumn> GeneralPathColumn::dup() const {
  GeneralPathColumnBuilder builder;
  for (const auto& path : data_) {
    builder.push_back_opt(path);
  }
  builder.set_path_impls(path_impls_);
  return builder.finish();
}

std::shared_ptr<IContextColumn> GeneralPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  GeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  builder.set_path_impls(path_impls_);
  return builder.finish();
}
std::shared_ptr<IContextColumnBuilder> GeneralPathColumn::builder() const {
  auto builder = std::make_shared<GeneralPathColumnBuilder>();
  builder->set_path_impls(path_impls_);
  return std::dynamic_pointer_cast<IContextColumnBuilder>(builder);
}

}  // namespace runtime
}  // namespace gs