#include "grin/src/predefine.h"

GRIN_DATATYPE _get_data_type(const gs::PropertyType& type) {
  if (type == gs::PropertyType::kBool) {
    return GRIN_DATATYPE::Bool;
  } else if (type == gs::PropertyType::kInt32) {
    return GRIN_DATATYPE::Int32;
  } else if (type == gs::PropertyType::kUInt32) {
    return GRIN_DATATYPE::UInt32;
  } else if (type == gs::PropertyType::kInt64) {
    return GRIN_DATATYPE::Int64;
  } else if (type == gs::PropertyType::kUInt64) {
    return GRIN_DATATYPE::UInt64;
  } else if (type == gs::PropertyType::kStringView) {
    return GRIN_DATATYPE::StringView;
  } else if (type == gs::PropertyType::kDate) {
    return GRIN_DATATYPE::Timestamp64;
  } else if (type == gs::PropertyType::kDouble) {
    return GRIN_DATATYPE::Double;
  } else if (type == gs::PropertyType::kFloat) {
    return GRIN_DATATYPE::Float;
  } else {
    return GRIN_DATATYPE::Undefined;
  }
}

void init_cache(GRIN_GRAPH_T* g) {
  auto v_label_num = g->g.vertex_label_num_;
  for (size_t i = 0; i < v_label_num; ++i) {
    std::vector<const void*> tmp;
    const auto& vec = g->g.schema().get_vertex_properties(i);
    const auto& table = g->g.get_vertex_table(i);
    for (size_t idx = 0; idx < vec.size(); ++idx) {
      const auto& type = vec[idx];
      if (type == gs::PropertyType::kInt32) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::IntColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kInt64) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::LongColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kUInt32) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::UIntColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kUInt64) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::ULongColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kBool) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::BoolColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kStringView) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::StringColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kDate) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::DateColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kDouble) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::DoubleColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else if (type == gs::PropertyType::kFloat) {
        tmp.emplace_back(std::dynamic_pointer_cast<gs::FloatColumn>(
                             table.get_column_by_id(idx))
                             .get());
      } else {
        tmp.emplace_back((const void*) NULL);
      }
    }
    g->vproperties.emplace_back(tmp);
  }
}