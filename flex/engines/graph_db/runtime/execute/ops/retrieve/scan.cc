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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/scan.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/scan.h"
#include "flex/engines/graph_db/runtime/utils/expr_impl.h"

namespace gs {
namespace runtime {
namespace ops {

static bool check_idx_predicate(const physical::Scan& scan_opr,
                                bool& scan_oid) {
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    return false;
  }

  if (!scan_opr.has_params()) {
    return false;
  }
  /**
  const algebra::QueryParams& p = scan_opr.params();
  if (p.has_predicate()) {
    return false;
  }*/

  if (!scan_opr.has_idx_predicate()) {
    return false;
  }
  const algebra::IndexPredicate& predicate = scan_opr.idx_predicate();
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }
  auto key = triplet.key();
  if (key.has_key()) {
    scan_oid = true;
  } else if (key.has_id()) {
    scan_oid = false;
  } else {
    LOG(FATAL) << "unexpected key case";
  }

  if (triplet.cmp() != common::Logical::EQ &&
      triplet.cmp() != common::Logical::WITHIN) {
    return false;
  }

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
  } break;
  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
  } break;
  default: {
    return false;
  } break;
  }

  return true;
}

typedef const std::map<std::string, std::string>& ParamsType;
// numeric type
template <typename T>
void parse_ids_from_idx_predicate(
    const algebra::IndexPredicate& predicate,
    std::function<std::vector<Any>(ParamsType)>& ids) {
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    std::vector<Any> ret;
    if (triplet.const_().item_case() == common::Value::kI32) {
      ret.emplace_back(static_cast<T>(triplet.const_().i32()));
    } else if (triplet.const_().item_case() == common::Value::kI64) {
      ret.emplace_back(static_cast<T>(triplet.const_().i64()));
    } else if (triplet.const_().item_case() == common::Value::kI64Array) {
      const auto& arr = triplet.const_().i64_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(static_cast<T>(arr.item(i)));
      }
    } else if (triplet.const_().item_case() == common::Value::kI32Array) {
      const auto& arr = triplet.const_().i32_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(static_cast<T>(arr.item(i)));
      }
    }
    ids = [ret = std::move(ret)](ParamsType) { return ret; };
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type == RTAnyType::kI32Value) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{
            static_cast<T>(std::stoi(params.at(triplet.param().name())))};
      };
    } else if (param_type == RTAnyType::kI64Value) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{
            static_cast<T>(std::stoll(params.at(triplet.param().name())))};
      };
    }
  }
  default:
    break;
  }
}

void parse_ids_from_idx_predicate(
    const algebra::IndexPredicate& predicate,
    std::function<std::vector<Any>(ParamsType)>& ids) {
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  std::vector<Any> ret;
  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    if (triplet.const_().item_case() == common::Value::kStr) {
      ret.emplace_back(triplet.const_().str());
      ids = [ret = std::move(ret)](ParamsType) { return ret; };

    } else if (triplet.const_().item_case() == common::Value::kStrArray) {
      const auto& arr = triplet.const_().str_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(arr.item(i));
      }
      ids = [ret = std::move(ret)](ParamsType) { return ret; };
    }
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type == RTAnyType::kStringValue) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{params.at(triplet.param().name())};
      };
    }
  }
  default:
    break;
  }
}

class FilterOidsWithoutPredOpr : public IReadOperator {
 public:
  FilterOidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Any>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    std::vector<Any> oids = oids_(params);
    if (params_.tables.size() == 1 && oids.size() == 1) {
      return Scan::find_vertex_with_oid(graph, params_.tables[0], oids[0],
                                        params_.alias);
    }
    return Scan::filter_oids(
        graph, params_, [](label_t, vid_t) { return true; }, oids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
};

class FilterMultiTypeOidsWithoutPredOpr : public IReadOperator {
 public:
  FilterMultiTypeOidsWithoutPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids)
      : params_(params), oids_(oids) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    std::vector<Any> oids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        oids.push_back(o);
      }
    }
    return Scan::filter_oids(
        graph, params_, [](label_t, vid_t) { return true; }, oids);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
};

class FilterGidsWithoutPredOpr : public IReadOperator {
 public:
  FilterGidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Any>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }
    if (params_.tables.size() == 1 && gids.size() == 1) {
      return Scan::find_vertex_with_gid(graph, params_.tables[0], gids[0],
                                        params_.alias);
    }
    return Scan::filter_gids(
        graph, params_, [](label_t, vid_t) { return true; }, gids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
};

class FilterOidsSPredOpr : public IReadOperator {
 public:
  FilterOidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const std::function<std::unique_ptr<SPVertexPredicate>(
                         const gs::runtime::GraphReadInterface&,
                         const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ids = oids_(params);
    auto pred = pred_(graph, params);
    return Scan::filter_oids_with_special_vertex_predicate(graph, params_,
                                                           *pred, ids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterOidsGPredOpr : public IReadOperator {
 public:
  FilterOidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ids = oids_(params);
    Context tmp;
    auto expr =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      return Scan::filter_oids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0, 0).as_bool();
          },
          ids);
    } else {
      return Scan::filter_oids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0).as_bool();
          },
          ids);
    }
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  common::Expression pred_;
};

class FilterOidsMultiTypeSPredOpr : public IReadOperator {
 public:
  FilterOidsMultiTypeSPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids,
      const std::function<std::unique_ptr<SPVertexPredicate>(
          const gs::runtime::GraphReadInterface&,
          const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(oids), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    std::vector<Any> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    auto pred = pred_(graph, params);
    return Scan::filter_oids_with_special_vertex_predicate(graph, params_,
                                                           *pred, all_ids);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterOidsMultiTypeGPredOpr : public IReadOperator {
 public:
  FilterOidsMultiTypeGPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids,
      const common::Expression& pred)
      : params_(params), oids_(oids), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    std::vector<Any> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    Context tmp;
    auto expr =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      return Scan::filter_oids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0, 0).as_bool();
          },
          all_ids);
    } else {
      return Scan::filter_oids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0).as_bool();
          },
          all_ids);
    }
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
  common::Expression pred_;
};

class FilterGidsSPredOpr : public IReadOperator {
 public:
  FilterGidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const std::function<std::unique_ptr<SPVertexPredicate>(
                         const gs::runtime::GraphReadInterface&,
                         const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }
    auto pred = pred_(graph, params);
    return Scan::filter_gids_with_special_vertex_predicate(graph, params_,
                                                           *pred, gids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterGidsGPredOpr : public IReadOperator {
 public:
  FilterGidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            ParamsType params, gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }
    Context tmp;
    auto expr =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      return Scan::filter_gids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0, 0).as_bool();
          },
          gids);
    } else {
      return Scan::filter_gids(
          graph, params_,
          [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0).as_bool();
          },
          gids);
    }
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  common::Expression pred_;
};

class ScanWithSPredOpr : public IReadOperator {
 public:
  ScanWithSPredOpr(const ScanParams& scan_params,
                   std::function<std::unique_ptr<SPVertexPredicate>(
                       const gs::runtime::GraphReadInterface&,
                       const std::map<std::string, std::string>&)>
                       pred)
      : scan_params_(scan_params), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto pred = pred_(graph, params);
    return Scan::scan_vertex_with_special_vertex_predicate(graph, scan_params_,
                                                           *pred);
  }

 private:
  ScanParams scan_params_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class ScanWithGPredOpr : public IReadOperator {
 public:
  ScanWithGPredOpr(const ScanParams& scan_params,
                   const common::Expression& pred)
      : scan_params_(scan_params), pred_(pred) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    Context tmp;
    auto expr =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      auto ret = Scan::scan_vertex(
          graph, scan_params_, [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0, 0).as_bool();
          });
      return ret;
    } else {
      auto ret = Scan::scan_vertex(
          graph, scan_params_, [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0).as_bool();
          });
      return ret;
    }
  }

 private:
  ScanParams scan_params_;
  common::Expression pred_;
};

class ScanWithoutPredOpr : public IReadOperator {
 public:
  ScanWithoutPredOpr(const ScanParams& scan_params)
      : scan_params_(scan_params) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return Scan::scan_vertex(graph, scan_params_,
                             [](label_t, vid_t) { return true; });
  }

 private:
  ScanParams scan_params_;
};

auto parse_ids_with_type(PropertyType type,
                         const algebra::IndexPredicate& triplet) {
  std::function<std::vector<Any>(ParamsType)> ids;
  switch (type.type_enum) {
  case impl::PropertyTypeImpl::kInt64: {
    parse_ids_from_idx_predicate<int64_t>(triplet, ids);
  } break;
  case impl::PropertyTypeImpl::kInt32: {
    parse_ids_from_idx_predicate<int32_t>(triplet, ids);
  } break;
  case impl::PropertyTypeImpl::kStringView: {
    parse_ids_from_idx_predicate(triplet, ids);
  } break;
  default:
    LOG(FATAL) << "unsupported type" << static_cast<int>(type.type_enum);
    break;
  }
  return ids;
}

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> ScanOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  int alias = -1;
  if (plan.plan(op_idx).opr().scan().has_alias()) {
    alias = plan.plan(op_idx).opr().scan().alias().value();
  }
  ret_meta.set(alias);
  auto scan_opr = plan.plan(op_idx).opr().scan();
  CHECK(scan_opr.scan_opt() == physical::Scan::VERTEX);
  CHECK(scan_opr.has_params());

  ScanParams scan_params;
  scan_params.alias = scan_opr.has_alias() ? scan_opr.alias().value() : -1;
  scan_params.limit = std::numeric_limits<int32_t>::max();
  if (scan_opr.params().has_limit()) {
    auto& limit_range = scan_opr.params().limit();
    if (limit_range.lower() != 0) {
      LOG(FATAL) << "Scan with lower limit expect 0, but got "
                 << limit_range.lower();
    }
    if (limit_range.upper() > 0) {
      scan_params.limit = limit_range.upper();
    }
  }
  for (auto& table : scan_opr.params().tables()) {
    // bug here, exclude invalid vertex label id
    if (schema.vertex_label_num() <= table.id()) {
      continue;
    }
    scan_params.tables.emplace_back(table.id());
  }
  if (scan_opr.has_idx_predicate()) {
    bool scan_oid = false;
    CHECK(check_idx_predicate(scan_opr, scan_oid));
    // only one label and without predicate
    if (scan_params.tables.size() == 1 && scan_oid &&
        (!scan_opr.params().has_predicate())) {
      const auto& pks = schema.get_vertex_primary_key(scan_params.tables[0]);
      const auto& [type, _, __] = pks[0];
      auto oids = parse_ids_with_type(type, scan_opr.idx_predicate());
      return std::make_pair(
          std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids),
          ret_meta);
    }

    // without predicate
    if (!scan_opr.params().has_predicate()) {
      if (!scan_oid) {
        auto gids =
            parse_ids_with_type(PropertyType::kInt64, scan_opr.idx_predicate());
        return std::make_pair(
            std::make_unique<FilterGidsWithoutPredOpr>(scan_params, gids),
            ret_meta);
      } else {
        std::vector<std::function<std::vector<Any>(ParamsType)>> oids;
        std::set<int> types;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          int type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            types.insert(type_impl);
            const auto& oid =
                parse_ids_with_type(type, scan_opr.idx_predicate());
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          return std::make_pair(
              std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids[0]),
              ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterMultiTypeOidsWithoutPredOpr>(scan_params,
                                                                  oids),
              ret_meta);
        }
      }
    } else {
      auto sp_vertex_pred =
          parse_special_vertex_predicate(scan_opr.params().predicate());
      if (scan_oid) {
        std::set<int> types;
        std::vector<std::function<std::vector<Any>(ParamsType)>> oids;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          auto type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            auto oid = parse_ids_with_type(type, scan_opr.idx_predicate());
            types.insert(type_impl);
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          if (sp_vertex_pred.has_value()) {
            return std::make_pair(std::make_unique<FilterOidsSPredOpr>(
                                      scan_params, oids[0], *sp_vertex_pred),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsGPredOpr>(
                    scan_params, oids[0], scan_opr.params().predicate()),
                ret_meta);
          }
        } else {
          if (sp_vertex_pred.has_value()) {
            return std::make_pair(std::make_unique<FilterOidsMultiTypeSPredOpr>(
                                      scan_params, oids, *sp_vertex_pred),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsMultiTypeGPredOpr>(
                    scan_params, oids, scan_opr.params().predicate()),
                ret_meta);
          }
        }

      } else {
        auto gids =
            parse_ids_with_type(PropertyType::kInt64, scan_opr.idx_predicate());
        if (sp_vertex_pred.has_value()) {
          return std::make_pair(std::make_unique<FilterGidsSPredOpr>(
                                    scan_params, gids, *sp_vertex_pred),
                                ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterGidsGPredOpr>(
                  scan_params, gids, scan_opr.params().predicate()),
              ret_meta);
        }
      }
    }

  } else {
    if (scan_opr.params().has_predicate()) {
      auto sp_vertex_pred =
          parse_special_vertex_predicate(scan_opr.params().predicate());
      if (sp_vertex_pred.has_value()) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, *sp_vertex_pred),
            ret_meta);
      } else {
        return std::make_pair(std::make_unique<ScanWithGPredOpr>(
                                  scan_params, scan_opr.params().predicate()),
                              ret_meta);
      }
    } else {
      return std::make_pair(std::make_unique<ScanWithoutPredOpr>(scan_params),
                            ret_meta);
    }
  }
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs