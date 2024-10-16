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
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {

enum class AggrKind {
  kSum,
  kMin,
  kMax,
  kCount,
  kCountDistinct,
  kToSet,
  kFirst,
  kToList,
  kAvg,
};

AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v) {
  if (v == physical::GroupBy_AggFunc::SUM) {
    return AggrKind::kSum;
  } else if (v == physical::GroupBy_AggFunc::MIN) {
    return AggrKind::kMin;
  } else if (v == physical::GroupBy_AggFunc::MAX) {
    return AggrKind::kMax;
  } else if (v == physical::GroupBy_AggFunc::COUNT) {
    return AggrKind::kCount;
  } else if (v == physical::GroupBy_AggFunc::COUNT_DISTINCT) {
    return AggrKind::kCountDistinct;
  } else if (v == physical::GroupBy_AggFunc::TO_SET) {
    return AggrKind::kToSet;
  } else if (v == physical::GroupBy_AggFunc::FIRST) {
    return AggrKind::kFirst;
  } else if (v == physical::GroupBy_AggFunc::TO_LIST) {
    return AggrKind::kToList;
  } else if (v == physical::GroupBy_AggFunc::AVG) {
    return AggrKind::kAvg;
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(v);
    return AggrKind::kSum;
  }
}

struct AggFunc {
  AggFunc(const physical::GroupBy_AggFunc& opr, const ReadTransaction& txn,
          const Context& ctx)
      : aggregate(parse_aggregate(opr.aggregate())), alias(-1) {
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    int var_num = opr.vars_size();
    for (int i = 0; i < var_num; ++i) {
      vars.emplace_back(txn, ctx, opr.vars(i), VarType::kPathVar);
    }
  }

  std::vector<Var> vars;
  AggrKind aggregate;
  int alias;
};

struct AggKey {
  AggKey(const physical::GroupBy_KeyAlias& opr, const ReadTransaction& txn,
         const Context& ctx)
      : key(txn, ctx, opr.key(), VarType::kPathVar), alias(-1) {
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
  }

  Var key;
  int alias;
  std::shared_ptr<IContextColumnBuilder> column_builder;
};

std::pair<std::vector<std::vector<size_t>>, Context> generate_aggregate_indices(
    const std::vector<AggKey>& keys, size_t row_num,
    const std::vector<AggFunc>& functions) {
  std::unordered_map<std::string_view, size_t> sig_to_root;
  std::vector<std::vector<char>> root_list;
  std::vector<std::vector<size_t>> ret;

  size_t keys_num = keys.size();
  std::vector<std::shared_ptr<IContextColumnBuilder>> keys_columns;
  std::vector<RTAny> keys_row(keys_num);
  for (size_t k_i = 0; k_i < keys_num; ++k_i) {
    auto type = keys[k_i].key.type();
    std::shared_ptr<IContextColumnBuilder> builder;
    if (type == RTAnyType::kList) {
      builder = keys[k_i].key.builder();
    } else {
      builder = create_column_builder(type);
    }
    keys_columns.push_back(builder);
  }

  for (size_t r_i = 0; r_i < row_num; ++r_i) {
    bool has_null{false};
    for (auto func : functions) {
      for (size_t v_i = 0; v_i < func.vars.size(); ++v_i) {
        if (func.vars[v_i].is_optional()) {
          if (func.vars[v_i].get(r_i, 0).is_null()) {
            has_null = true;
            break;
          }
        }
      }
      if (has_null) {
        break;
      }
    }

    std::vector<char> buf;
    ::gs::Encoder encoder(buf);
    for (size_t k_i = 0; k_i < keys_num; ++k_i) {
      keys_row[k_i] = keys[k_i].key.get(r_i);
      keys_row[k_i].encode_sig(keys[k_i].key.type(), encoder);
    }

    std::string_view sv(buf.data(), buf.size());
    auto iter = sig_to_root.find(sv);
    if (iter != sig_to_root.end()) {
      if (!has_null) {
        ret[iter->second].push_back(r_i);
      }
    } else {
      sig_to_root.emplace(sv, ret.size());
      root_list.emplace_back(std::move(buf));

      for (size_t k_i = 0; k_i < keys_num; ++k_i) {
        keys_columns[k_i]->push_back_elem(keys_row[k_i]);
      }

      std::vector<size_t> ret_elem;
      if (!has_null) {
        ret_elem.push_back(r_i);
      }
      ret.emplace_back(std::move(ret_elem));
    }
  }

  Context ret_ctx;
  for (size_t k_i = 0; k_i < keys_num; ++k_i) {
    ret_ctx.set(keys[k_i].alias, keys_columns[k_i]->finish());
    ret_ctx.append_tag_id(keys[k_i].alias);
  }

  return std::make_pair(std::move(ret), std::move(ret_ctx));
}

template <typename NT>
std::shared_ptr<IContextColumn> numeric_sum(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    NT s = 0;
    for (auto idx : vec) {
      s += TypedConverter<NT>::to_typed(var.get(idx));
    }
    builder.push_back_opt(s);
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> numeric_count_distinct(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::unordered_set<NT> s;
    for (auto idx : vec) {
      s.insert(TypedConverter<NT>::to_typed(var.get(idx)));
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> vertex_count_distinct(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<std::pair<label_t, vid_t>> s;
    for (auto idx : vec) {
      s.insert(var.get(idx).as_vertex());
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> general_count_distinct(
    const std::vector<Var>& vars,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<std::string> s;
    std::vector<char> bytes;
    for (auto idx : vec) {
      bytes.clear();
      Encoder encoder(bytes);
      for (auto& var : vars) {
        var.get(idx).encode_sig(var.get(idx).type(), encoder);
        encoder.put_byte('#');
      }
      std::string str(bytes.begin(), bytes.end());
      s.insert(str);
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> general_count(
    const std::vector<Var>& vars,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  if (vars.size() == 1) {
    if (vars[0].is_optional()) {
      size_t col_size = to_aggregate.size();
      builder.reserve(col_size);
      for (size_t k = 0; k < col_size; ++k) {
        auto& vec = to_aggregate[k];
        int64_t s = 0;
        for (auto idx : vec) {
          if (vars[0].get(idx, 0).is_null()) {
            continue;
          }
          s += 1;
        }
        builder.push_back_opt(s);
      }
      return builder.finish();
    }
  }
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    builder.push_back_opt(vec.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> vertex_first(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  MLVertexColumnBuilder builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    for (auto idx : vec) {
      builder.push_back_elem(var.get(idx));
      break;
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_first(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    for (auto idx : vec) {
      builder.push_back_elem(var.get(idx));
      break;
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_min(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    if (vec.size() == 0) {
      continue;
    }
    NT s = TypedConverter<NT>::to_typed(var.get(vec[0]));
    for (auto idx : vec) {
      s = std::min(s, TypedConverter<NT>::to_typed(var.get(idx)));
    }
    if constexpr (std::is_same<NT, std::string_view>::value) {
      builder.push_back_opt(std::string(s));
    } else {
      builder.push_back_opt(s);
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_max(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    if (vec.size() == 0) {
      continue;
    }
    NT s = TypedConverter<NT>::to_typed(var.get(vec[0]));
    for (auto idx : vec) {
      s = std::max(s, TypedConverter<NT>::to_typed(var.get(idx)));
    }
    if constexpr (std::is_same<NT, std::string_view>::value) {
      builder.push_back_opt(std::string(s));
    } else {
      builder.push_back_opt(s);
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> string_to_set(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<std::set<std::string>> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<std::string> elem;
    for (auto idx : vec) {
      elem.insert(std::string(var.get(idx).as_string()));
    }
    builder.push_back_opt(std::move(elem));
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> tuple_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<Tuple> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<RTAny> elem;
    for (auto idx : vec) {
      elem.push_back(var.get(idx));
    }
    auto impl = ListImpl<Tuple>::make_list_impl(elem);
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

std::shared_ptr<IContextColumn> string_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<std::string> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<std::string> elem;
    for (auto idx : vec) {
      elem.push_back(std::string(var.get(idx).as_string()));
    }
    auto impl = ListImpl<std::string_view>::make_list_impl(std::move(elem));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

bl::result<std::shared_ptr<IContextColumn>> apply_reduce(
    const AggFunc& func, const std::vector<std::vector<size_t>>& to_aggregate) {
  if (func.aggregate == AggrKind::kSum) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to sum is allowed";
    }
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      return numeric_sum<int>(var, to_aggregate);
    } else {
      LOG(ERROR) << "reduce on " << static_cast<int>(var.type().type_enum_)
                 << " is not supported...";
      RETURN_UNSUPPORTED_ERROR(
          "reduce on " +
          std::to_string(static_cast<int>(var.type().type_enum_)) +
          " is not supported...");
    }
  } else if (func.aggregate == AggrKind::kToSet) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to to_set is allowed";
    }
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kStringValue) {
      return string_to_set(var, to_aggregate);
    } else {
      LOG(ERROR) << "reduce on " << static_cast<int>(var.type().type_enum_)
                 << " is not supported...";
      RETURN_UNSUPPORTED_ERROR(
          "reduce on " +
          std::to_string(static_cast<int>(var.type().type_enum_)) +
          " is not supported...");
    }
  } else if (func.aggregate == AggrKind::kCountDistinct) {
    if (func.vars.size() == 1 && func.vars[0].type() == RTAnyType::kVertex) {
      const Var& var = func.vars[0];
      return vertex_count_distinct(var, to_aggregate);
    } else {
      return general_count_distinct(func.vars, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kCount) {
    // return general_count(to_aggregate);
    return general_count(func.vars, to_aggregate);
  } else if (func.aggregate == AggrKind::kFirst) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to first is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kVertex) {
      return vertex_first(var, to_aggregate);
    } else if (var.type() == RTAnyType::kI64Value) {
      return general_first<int64_t>(var, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kMin) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to min is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      return general_min<int>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return general_min<std::string_view>(var, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kMax) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to max is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      return general_max<int>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return general_max<std::string_view>(var, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kToList) {
    const Var& var = func.vars[0];
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to to_list is allowed";
    }
    if (var.type() == RTAnyType::kTuple) {
      return tuple_to_list(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return string_to_list(var, to_aggregate);
    } else {
      LOG(FATAL) << "not support" << static_cast<int>(var.type().type_enum_);
    }
  } else if (func.aggregate == AggrKind::kAvg) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to avg is allowed";
    }
    // LOG(FATAL) << "not support";
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      ValueColumnBuilder<int32_t> builder;
      size_t col_size = to_aggregate.size();
      builder.reserve(col_size);
      for (size_t k = 0; k < col_size; ++k) {
        auto& vec = to_aggregate[k];
        int32_t s = 0;

        for (auto idx : vec) {
          s += TypedConverter<int32_t>::to_typed(var.get(idx));
        }
        builder.push_back_opt(s / vec.size());
      }
      return builder.finish();
    }
  }

  LOG(ERROR) << "Unsupported aggregate function "
             << static_cast<int>(func.aggregate);
  RETURN_UNSUPPORTED_ERROR("Unsupported aggregate function " +
                           std::to_string(static_cast<int>(func.aggregate)));
  return nullptr;
}

bl::result<Context> eval_group_by(const physical::GroupBy& opr,
                                  const ReadTransaction& txn, Context&& ctx) {
  std::vector<AggFunc> functions;
  std::vector<AggKey> mappings;
  int func_num = opr.functions_size();
  for (int i = 0; i < func_num; ++i) {
    functions.emplace_back(opr.functions(i), txn, ctx);
  }

  int mappings_num = opr.mappings_size();
  // return ctx;
  if (mappings_num == 0) {
    Context ret;
    for (int i = 0; i < func_num; ++i) {
      std::vector<size_t> tmp;
      for (size_t _i = 0; _i < ctx.row_num(); ++_i) {
        tmp.emplace_back(_i);
      }
      BOOST_LEAF_AUTO(new_col, apply_reduce(functions[i], {tmp}));
      ret.set(functions[i].alias, new_col);
      ret.append_tag_id(functions[i].alias);
    }

    return ret;
  } else {
    for (int i = 0; i < mappings_num; ++i) {
      mappings.emplace_back(opr.mappings(i), txn, ctx);
    }

    auto keys_ret =
        generate_aggregate_indices(mappings, ctx.row_num(), functions);
    std::vector<std::vector<size_t>>& to_aggregate = keys_ret.first;

    Context& ret = keys_ret.second;

    // exclude null values
    if (func_num == 1 && functions[0].aggregate != AggrKind::kCount &&
        functions[0].aggregate != AggrKind::kCountDistinct) {
      std::vector<size_t> tmp;
      std::vector<std::vector<size_t>> tmp_to_aggregate;
      for (size_t i = 0; i < to_aggregate.size(); ++i) {
        if (to_aggregate[i].size() == 0) {
          continue;
        }
        tmp_to_aggregate.emplace_back(to_aggregate[i]);
        tmp.emplace_back(i);
      }
      ret.reshuffle(tmp);
      std::swap(to_aggregate, tmp_to_aggregate);
    }

    for (int i = 0; i < func_num; ++i) {
      BOOST_LEAF_AUTO(new_col, apply_reduce(functions[i], to_aggregate));
      ret.set(functions[i].alias, new_col);
      ret.append_tag_id(functions[i].alias);
    }
    return ret;
  }
}

}  // namespace runtime

}  // namespace gs