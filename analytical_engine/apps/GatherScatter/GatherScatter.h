#ifndef ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_GATHER_SCATTER_H_
#define ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_GATHER_SCATTER_H_

#include <vector>

#include "grape/grape.h"

#include "apps/GatherScatter/IVertexProgram.h"
#include "apps/GatherScatter/Vertex.h"
#include "core/app/property_app_base.h"
#include "core/context/labeled_vertex_property_context.h"

namespace gs {

template <typename VP_T>
class GatherScatterContext
    : public LabeledVertexPropertyContext<
          vineyard::ArrowFragment<typename VP_T::id_type, uint64_t>> {
  using id_type = typename VP_T::id_type;
  using fragment_t = vineyard::ArrowFragment<id_type, uint64_t>;
  using label_id_t = typename fragment_t::label_id_t;
  using active_array_t = typename fragment_t::template vertex_array_t<bool>;
  using vp_array_t = typename fragment_t::template vertex_array_t<VP_T>;
  using gather_type = typename VP_T::gather_type;

  using base_t = LabeledVertexPropertyContext<fragment_t>;

 public:
  explicit GatherScatterContext(const fragment_t& fragment)
      : LabeledVertexPropertyContext<fragment_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages) {
    auto& frag = this->fragment();
    label_id_t v_label_num = frag.vertex_label_num();

    active_arrays.clear();
    active_arrays.resize(v_label_num);
    next_active_arrays.clear();
    next_active_arrays.resize(v_label_num);
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto inner_vertices = frag.InnerVertices(i);
      active_arrays[i].Init(inner_vertices, true);
      next_active_arrays[i].Init(inner_vertices, false);
    }

    step = 0;
    context.set_procid(frag.fid());
    context.set_num_procs(frag.fnum());
    context.set_num_vertices(frag.GetTotalVerticesNum());
  }

  void createGatherColumns(const std::string& name) {
    gather_column_name = name;

    auto& frag = this->fragment();
    label_id_t v_label_num = frag.vertex_label_num();

    next_gather.clear();
    next_gather.resize(v_label_num);
    gather_column_index.clear();
    gather_column_index.resize(v_label_num);

    ContextDataType gather_type_value = ContextTypeToEnum<gather_type>::value;
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto inner_vertices = frag.InnerVertices(i);
      next_gather[i] =
          std::dynamic_pointer_cast<Column<fragment_t, gather_type>>(
              CreateColumn<fragment_t>(name, inner_vertices,
                                       gather_type_value));

      gather_column_index[i] = base_t::add_column(i, name, gather_type_value);
    }
  }

  void initNextGatherColumns(const gather_type& value) {
    for (auto column : next_gather) {
      column->data().SetValue(value);
    }
  }

  void swapGatherColumns() {
    auto& frag = this->fragment();
    label_id_t v_label_num = frag.vertex_label_num();
    for (label_id_t i = 0; i < v_label_num; ++i) {
      std::shared_ptr<Column<fragment_t, gather_type>> tmp = next_gather[i];
      next_gather[i] = this->template get_typed_column<gather_type>(
          i, gather_column_index[i]);

      this->vertex_properties()[i][gather_column_index[i]] = tmp;
      this->properties_map()[i][gather_column_name] = tmp;
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    label_id_t v_label_num = frag.vertex_label_num();

    for (label_id_t i = 0; i < v_label_num; ++i) {
      os << "label-" << i << ":\n";
      for (auto& pair : this->properties_map()[i]) {
        os << "\t"
           << "column_name: " << pair.first
           << ", column_type: " << static_cast<int>(pair.second->type())
           << "\n";
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      std::shared_ptr<Column<fragment_t, double>> column =
          base_t::template get_typed_column<double>(i, "$pr");
      auto inner_vertices = frag.InnerVertices(i);
      for (auto v : inner_vertices) {
        os << frag.GetId(v) << "\t" << column->at(v) << "\n";
      }
    }
  }

  std::vector<gather_scatter::Vertex<id_type>> vertices;
  std::vector<active_array_t> active_arrays;
  std::vector<active_array_t> next_active_arrays;

  std::vector<int> gather_column_index;
  std::string gather_column_name;
  std::vector<std::shared_ptr<Column<fragment_t, gather_type>>> next_gather;

  int step;
  gather_scatter::Context context;
};

template <typename VP_T>
class GatherScatter
    : public PropertyAppBase<
          vineyard::ArrowFragment<typename VP_T::id_type, uint64_t>,
          GatherScatterContext<VP_T>> {
  using vertex_program_t = VP_T;
  using oid_t = typename vertex_program_t::id_type;
  using gather_type = typename vertex_program_t::gather_type;
  using message_type = typename vertex_program_t::message_type;

 public:
  using fragment_t = vineyard::ArrowFragment<typename VP_T::id_type, uint64_t>;
  using context_t = GatherScatterContext<vertex_program_t>;
  using message_manager_t = PropertyMessageManager;
  using worker_t = DefaultPropertyWorker<GatherScatter<vertex_program_t>>;
  static std::shared_ptr<worker_t> CreateWorker(
      std::shared_ptr<GatherScatter<vertex_program_t>> app,
      std::shared_ptr<fragment_t> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  using label_id_t = typename fragment_t::label_id_t;
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto& context = ctx.context;
    label_id_t v_label_num = frag.vertex_label_num();

    VP_T vp;
    vp.Setup(context);
    ctx.createGatherColumns(vp.GatherIndex());

    for (auto& pair : context.columns()) {
      ContextDataType type;
      if (pair.second == "double") {
        type = ContextDataType::kDouble;
      } else {
        type = ContextDataType::kUndefined;
        LOG(FATAL) << "Unsupport column type: " << pair.second;
      }
      for (label_id_t i = 0; i < v_label_num; ++i) {
        ctx.add_column(i, pair.first, type);
      }
    }

    ctx.vertices.clear();
    for (label_id_t i = 0; i < v_label_num; ++i) {
      ctx.vertices.emplace_back(frag, ctx, i);
    }

    InitStep(frag, ctx);

    if (ctx.step >= vp.MaxIterations()) {
      return;
    }

    ScatterStep(frag, ctx, messages);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    size_t active_vnum = GatherStep(frag, ctx, messages);
    VP_T vp;
    if (active_vnum == 0 || ctx.step >= vp.MaxIterations()) {
      return;
    }
    ++ctx.step;

    ScatterStep(frag, ctx, messages);
  }

  void InitStep(const fragment_t& frag, context_t& ctx) {
    label_id_t v_label_num = frag.vertex_label_num();
    auto& context = ctx.context;
    VP_T vp;
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      auto& vertex = ctx.vertices[i];
      vertex.setActiveArray(ctx.active_arrays[i]);
      vertex.resetVertex();
      for (auto v : iv) {
        vp.Init(context, vertex);
        vertex.nextVertex();
      }
    }
  }

  void ScatterStep(const fragment_t& frag, context_t& ctx,
                   message_manager_t& messages) {
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    VP_T vp;
    ctx.initNextGatherColumns(vp.GatherInit());

    auto& context = ctx.context;

    for (label_id_t i = 0; i < v_label_num; ++i) {
      ctx.next_active_arrays[i].SetValue(false);
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      auto& vertex = ctx.vertices[i];
      vertex.setActiveArray(ctx.active_arrays[i]);
      vertex.resetVertex();
      for (auto v : iv) {
        vp.PreProcess(context, vertex);

        if (vertex.IsActive()) {
          gather_scatter::EdgeDir dir = vp.ScatterEdges(context, vertex);
          if (dir == gather_scatter::EdgeDir::IN_EDGES) {
            auto msg = vp.ScatterValueSupplier(context, vertex);
            for (label_id_t j = 0; j < e_label_num; ++j) {
              auto ies = frag.GetIncomingAdjList(v, j);
              for (auto& e : ies) {
                auto v = e.neighbor();
                auto v_label = frag.vertex_label(v);
                if (frag.IsInnerVertex(v)) {
                  ctx.next_active_arrays[v_label][v] = true;
                  vp.Aggregate(ctx.next_gather[v_label]->at(v), msg);
                } else {
                  messages.SyncStateOnOuterVertex(frag, v, msg);
                }
              }
            }
          } else if (dir == gather_scatter::EdgeDir::OUT_EDGES) {
            auto msg = vp.ScatterValueSupplier(context, vertex);
            for (label_id_t j = 0; j < e_label_num; ++j) {
              auto oes = frag.GetOutgoingAdjList(v, j);
              for (auto& e : oes) {
                auto v = e.neighbor();
                auto v_label = frag.vertex_label(v);
                if (frag.IsInnerVertex(v)) {
                  ctx.next_active_arrays[v_label][v] = true;
                  vp.Aggregate(ctx.next_gather[v_label]->at(v), msg);
                } else {
                  messages.SyncStateOnOuterVertex(frag, v, msg);
                }
              }
            }
          } else if (dir == gather_scatter::EdgeDir::BOTH_EDGES) {
            auto msg = vp.ScatterValueSupplier(context, vertex);
            for (label_id_t j = 0; j < e_label_num; ++j) {
              auto ies = frag.GetIncomingAdjList(v, j);
              for (auto& e : ies) {
                auto v = e.neighbor();
                auto v_label = frag.vertex_label(v);
                if (frag.IsInnerVertex(v)) {
                  ctx.active_arrays[v_label][v] = true;
                  vp.Aggregate(ctx.next_gather[v_label]->at(v), msg);
                } else {
                  messages.SyncStateOnOuterVertex(frag, v, msg);
                }
              }
              auto oes = frag.GetOutgoingAdjList(v, j);
              for (auto& e : oes) {
                auto v = e.neighbor();
                auto v_label = frag.vertex_label(v);
                if (frag.IsInnerVertex(v)) {
                  ctx.next_active_arrays[v_label][v] = true;
                  vp.Aggregate(ctx.next_gather[v_label]->at(v), msg);
                } else {
                  messages.SyncStateOnOuterVertex(frag, v, msg);
                }
              }
            }
          }
        }

        vertex.nextVertex();
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto inner_vertices = frag.InnerVertices(i);
      for (auto v : inner_vertices) {
        if (ctx.next_active_arrays[i][v] == true) {
          messages.ForceContinue();
          return;
        }
      }
    }
  }

  size_t GatherStep(const fragment_t& frag, context_t& ctx,
                    message_manager_t& messages) {
    vertex_t v(0);
    message_type msg;
    VP_T vp;
    while (messages.GetMessage(frag, v, msg)) {
      auto v_label = frag.vertex_label(v);
      ctx.next_active_arrays[v_label][v] = true;
      vp.Aggregate(ctx.next_gather[v_label]->at(v), msg);
    }

    ctx.active_arrays.swap(ctx.next_active_arrays);

    label_id_t v_label_num = frag.vertex_label_num();
    auto& context = ctx.context;
    size_t active_num = 0;
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      auto& vertex = ctx.vertices[i];
      vertex.setActiveArray(ctx.active_arrays[i]);
      vertex.resetVertex();
      for (auto v : iv) {
        if (vertex.IsActive()) {
          vp.PostProcess(context, vertex);
          if (vertex.IsActive()) {
            ++active_num;
          }
        }
        vertex.nextVertex();
      }
    }

    return active_num;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_GATHER_SCATTER_H_
