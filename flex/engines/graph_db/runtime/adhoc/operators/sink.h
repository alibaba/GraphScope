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

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/proto_generated_gie/results.pb.h"

namespace gs {

namespace runtime {

void sink_impl(const RTAny& any, common::Value* value) {
  const auto& type_ = any.type_;
  const auto& value_ = any.value_;
  if (type_ == RTAnyType::kI64Value) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    value->set_str(value_.str_val.data(), value_.str_val.size());
  } else if (type_ == RTAnyType::kI32Value) {
    value->set_i32(value_.i32_val);
  } else if (type_ == RTAnyType::kStringSetValue) {
    LOG(FATAL) << "not support string set sink";
  } else if (type_ == RTAnyType::kDate32) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kBoolValue) {
    value->set_boolean(value_.b_val);
  } else if (type_ == RTAnyType::kF64Value) {
    value->set_f64(value_.f64_val);
  } else if (type_ == RTAnyType::kList) {
    LOG(FATAL) << "not support list sink";
  } else if (type_ == RTAnyType::kTuple) {
    auto tup = value_.t;
    for (size_t i = 0; i < value_.t.size(); ++i) {
      std::string s = tup.get(i).to_string();
      value->mutable_str_array()->add_item(s.data(), s.size());
    }
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_.type_enum_);
  }
}

static void sink_any(const Any& any, common::Value* value) {
  if (any.type == PropertyType::Int64()) {
    value->set_i64(any.AsInt64());
  } else if (any.type == PropertyType::StringView()) {
    auto str = any.AsStringView();
    value->set_str(str.data(), str.size());
  } else if (any.type == PropertyType::Date()) {
    value->set_i64(any.AsDate().milli_second);
  } else if (any.type == PropertyType::Int32()) {
    value->set_i32(any.AsInt32());
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.AsDouble());
  } else if (any.type == PropertyType::Bool()) {
    value->set_boolean(any.AsBool());
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.AsDouble());
  } else {
    LOG(FATAL) << "Any value: " << any.to_string()
               << ", type = " << any.type.type_enum;
  }
}

template <typename GRAPH_IMPL>
void sink_vertex(const GraphInterface<GRAPH_IMPL>& txn,
                 const std::pair<label_t, vid_t>& vertex, results::Vertex* v) {
  v->mutable_label()->set_id(vertex.first);
  v->set_id(encode_unique_vertex_id(vertex.first, vertex.second));
  const auto& vertex_prop_names = txn.GetVertexPropertyNames(vertex.first);
  for (size_t i = 0; i < vertex_prop_names.size(); ++i) {
    auto prop = v->add_properties();
    prop->mutable_key()->set_name(vertex_prop_names[i]);
    sink_any(txn.GetVertexProperty(vertex.first, i, vertex.second),
             prop->mutable_value());
  }
}

template <typename GRAPH_IMPL>
void sink(const RTAny& any, const GraphInterface<GRAPH_IMPL>& txn, int id,
          results::Column* col) {
  const auto& type_ = any.type_;
  const auto& value_ = any.value_;
  col->mutable_name_or_id()->set_id(id);
  if (type_ == RTAnyType::kList) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.list.size(); ++i) {
      sink_impl(value_.list.get(i),
                collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kStringSetValue) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (auto& s : *value_.str_set) {
      collection->add_collection()->mutable_object()->set_str(s);
    }
  } else if (type_ == RTAnyType::kTuple) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.t.size(); ++i) {
      sink_impl(value_.t.get(i),
                collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kVertex) {
    auto v = col->mutable_entry()->mutable_element()->mutable_vertex();
    sink_vertex(txn, value_.vertex, v);

  } else if (type_ == RTAnyType::kMap) {
    auto mp = col->mutable_entry()->mutable_map();
    auto [keys_ptr, vals_ptr] = value_.map.key_vals();
    auto& keys = *keys_ptr;
    auto& vals = *vals_ptr;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (vals[i].is_null()) {
        continue;
      }
      auto ret = mp->add_key_values();
      ret->mutable_key()->set_str(keys[i]);
      if (vals[i].type_ == RTAnyType::kVertex) {
        auto v = ret->mutable_value()->mutable_element()->mutable_vertex();
        sink_vertex(txn, vals[i].as_vertex(), v);
      } else {
        sink_impl(vals[i],
                  ret->mutable_value()->mutable_element()->mutable_object());
      }
    }

  } else if (type_ == RTAnyType::kEdge) {
    auto e = col->mutable_entry()->mutable_element()->mutable_edge();
    auto [label, src, dst, prop, dir] = any.as_edge();
    e->mutable_src_label()->set_id(label.src_label);
    e->mutable_dst_label()->set_id(label.dst_label);
    auto edge_label = generate_edge_label_id(label.src_label, label.dst_label,
                                             label.edge_label);
    e->mutable_label()->set_id(label.edge_label);
    e->set_src_id(encode_unique_vertex_id(label.src_label, src));
    e->set_dst_id(encode_unique_vertex_id(label.dst_label, dst));
    e->set_id(encode_unique_edge_id(edge_label, src, dst));
    const auto& edge_props_names = txn.GetEdgePropertyNames(
        label.src_label, label.dst_label, label.edge_label);
    if (edge_props_names.size() == 1) {
      auto props = e->add_properties();
      props->mutable_key()->set_name(edge_props_names[0]);
      sink_any(prop, e->mutable_properties(0)->mutable_value());
    } else if (edge_props_names.size() > 1) {
      auto rv = prop.AsRecordView();
      if (rv.size() != edge_props_names.size()) {
        LOG(ERROR) << "record view size not match with prop names";
      }
      for (size_t i = 0; i < edge_props_names.size(); ++i) {
        auto props = e->add_properties();
        props->mutable_key()->set_name(edge_props_names[i]);
        sink_any(rv[i], props->mutable_value());
      }
    }
  } else if (type_ == RTAnyType::kPath) {
    LOG(FATAL) << "not support path sink";

  } else {
    sink_impl(any, col->mutable_entry()->mutable_element()->mutable_object());
  }
}

template <typename GRAPH_IMPL>
void eval_sink(const Context& ctx, const GraphInterface<GRAPH_IMPL>& txn,
               Encoder& output) {
  size_t row_num = ctx.row_num();
  results::CollectiveResults results;
  for (size_t i = 0; i < row_num; ++i) {
    auto result = results.add_results();
    for (size_t j : ctx.tag_ids) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto column = result->mutable_record()->add_columns();
      auto val = col->get_elem(i);
      sink(val, txn, j, column);
    }
  }
  // LOG(INFO) << "sink: " << results.DebugString();
  auto res = results.SerializeAsString();
  output.put_bytes(res.data(), res.size());
}

}  // namespace runtime

}  // namespace gs