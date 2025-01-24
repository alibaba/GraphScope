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
#include "flex/engines/graph_db/runtime/execute/ops/update/project.h"
#include "flex/engines/graph_db/runtime/common/operators/update/project.h"

namespace gs {
namespace runtime {
namespace ops {
class ProjectInsertOpr : public IInsertOperator {
 public:
  ProjectInsertOpr(
      const std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
          const std::map<std::string, std::string>&)>>& exprs)
      : exprs_(exprs) {}

  std::string get_operator_name() const override { return "ProjectInsertOpr"; }

  bl::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    std::vector<std::unique_ptr<WriteProjectExprBase>> exprs;
    for (auto& expr : exprs_) {
      exprs.push_back(expr(params));
    }
    return Project::project(std::move(ctx), exprs);
  }

 private:
  std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
      const std::map<std::string, std::string>&)>>
      exprs_;
};

std::unique_ptr<IInsertOperator> ProjectInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  auto opr = plan.plan(op_id).opr().project();
  int mappings_size = opr.mappings_size();
  std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
      const std::map<std::string, std::string>&)>>
      exprs;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = opr.mappings(i);
    if (!m.has_alias()) {
      LOG(ERROR) << "project mapping should have alias";
      return nullptr;
    }
    if ((!m.has_expr()) || m.expr().operators_size() != 1) {
      LOG(ERROR) << "project mapping should have one expr";
      return nullptr;
    }
    if (m.expr().operators(0).item_case() == common::ExprOpr::kParam) {
      auto param = m.expr().operators(0).param();
      auto name = param.name();
      int alias = m.alias().value();
      exprs.emplace_back(
          [name, alias](const std::map<std::string, std::string>& params) {
            CHECK(params.find(name) != params.end());
            return std::make_unique<ParamsGetter>(params.at(name), alias);
          });
    } else if (m.expr().operators(0).item_case() == common::ExprOpr::kVar) {
      auto var = m.expr().operators(0).var();
      if (!var.has_tag()) {
        LOG(ERROR) << "project mapping should have tag";
        return nullptr;
      }
      if (var.has_property()) {
        LOG(ERROR) << "project mapping should not have property";
        return nullptr;
      }
      int tag = var.tag().id();
      int alias = m.alias().value();
      exprs.emplace_back(
          [tag, alias](const std::map<std::string, std::string>&) {
            return std::make_unique<DummyWGetter>(tag, alias);
          });
    } else if (m.expr().operators(0).has_udf_func()) {
      auto udf_func = m.expr().operators(0).udf_func();

      if (udf_func.name() == "gs.function.first") {
        if (udf_func.parameters_size() != 1 ||
            udf_func.parameters(0).operators_size() != 1) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto param = udf_func.parameters(0).operators(0);

        if (param.item_case() != common::ExprOpr::kVar) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto var = param.var();
        if (!var.has_tag()) {
          LOG(ERROR) << "project mapping should have tag";
          return nullptr;
        }
        if (var.has_property()) {
          LOG(ERROR) << "project mapping should not have property";
          return nullptr;
        }
        int tag = var.tag().id();
        int alias = m.alias().value();

        if (i + 1 < mappings_size) {
          const physical::Project_ExprAlias& next = opr.mappings(i + 1);
          if (!next.has_alias()) {
            LOG(ERROR) << "project mapping should have alias";
            return nullptr;
          }
          if (!next.has_expr()) {
            LOG(ERROR) << "project mapping should have expr";
            return nullptr;
          }
          if (next.expr().operators_size() != 1) {
            LOG(ERROR) << "project mapping should have one expr";
            return nullptr;
          }
          if (next.expr().operators(0).has_udf_func()) {
            auto next_udf_func = next.expr().operators(0).udf_func();
            if (next_udf_func.name() == "gs.function.second") {
              auto param = udf_func.parameters(0).operators(0);
              if (param.item_case() != common::ExprOpr::kVar) {
                LOG(ERROR) << "not support for " << m.expr().DebugString();
                return nullptr;
              }
              auto var = param.var();
              if (!var.has_tag()) {
                LOG(ERROR) << "project mapping should have tag";
                return nullptr;
              }
              if (var.has_property()) {
                LOG(ERROR) << "project mapping should not have property";
                return nullptr;
              }
              int next_tag = var.tag().id();
              int next_alias = next.alias().value();
              if (next_tag == tag) {
                exprs.emplace_back(
                    [tag, alias,
                     next_alias](const std::map<std::string, std::string>&) {
                      return std::make_unique<PairsGetter>(tag, alias,
                                                           next_alias);
                    });
                ++i;
                continue;
              }
            }
          }
        }
        exprs.emplace_back(
            [tag, alias](const std::map<std::string, std::string>&) {
              return std::make_unique<PairsFstGetter>(tag, alias);
            });

      } else if (udf_func.name() == "gs.function.second") {
        if (udf_func.parameters_size() != 1 ||
            udf_func.parameters(0).operators_size() != 1) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto param = udf_func.parameters(0).operators(0);

        if (param.item_case() != common::ExprOpr::kVar) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto var = param.var();
        if (!var.has_tag()) {
          LOG(ERROR) << "project mapping should have tag";
          return nullptr;
        }
        if (var.has_property()) {
          LOG(ERROR) << "project mapping should not have property";
          return nullptr;
        }
        int tag = var.tag().id();
        int alias = m.alias().value();
        exprs.emplace_back(
            [tag, alias](const std::map<std::string, std::string>&) {
              return std::make_unique<PairsSndGetter>(tag, alias);
            });
      } else {
        LOG(ERROR) << "not support for " << m.expr().DebugString();
        return nullptr;
      }
    } else {
      LOG(ERROR) << "not support for " << m.expr().DebugString();
      return nullptr;
    }
  }

  return std::make_unique<ProjectInsertOpr>(exprs);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs