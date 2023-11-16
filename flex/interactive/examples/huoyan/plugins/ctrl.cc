#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include <queue>

namespace gs
{
    // Update vertex and edges.
    class Query4 : public AppBase
    {
    public:
        Query4(GraphDBSession &graph)
            : comp_label_id_(graph.schema().get_vertex_label_id("company")),
              invest_label_id_(graph.schema().get_edge_label_id("invest")),
              graph_(graph)
        {
            auto v_num = graph_.graph().vertex_num(comp_label_id_);
            invests_.resize(v_num, 0.0);
        }

        bool Query(Decoder &input, Encoder &output)
        {
            int64_t comp_id = input.get_long();
            int32_t hop_limit = input.get_int();
            double thresh_hold = input.get_double();
            auto txn = graph_.GetReadTransaction();

            vid_t vid;
            if (!txn.GetVertexIndex(comp_label_id_, Any::From(comp_id), vid))
            {
                LOG(ERROR) << "Vertex not found: " << comp_id;
                return false;
            }
            invests_[vid] = 1.0;

            auto outgoin_view = txn.GetOutgoingGraphView<double>(
                comp_label_id_, comp_label_id_, invest_label_id_);
            std::queue<vid_t> q;
            q.push(vid);
            std::unordered_set<vid_t> st;
            while (!q.empty() && hop_limit > 0)
            {
                auto size = q.size();
                for (int i = 0; i < size; ++i)
                {
                    auto cur = q.front();
                    q.pop();
                    auto cur_invest = invests_[cur];
                    const auto &edges = outgoin_view.get_edges(cur);
                    for (auto &edge : edges)
                    {
                        auto &dst = edge.neighbor;
                        invests_[dst] += cur_invest * edge.data;
                        q.push(dst);
                        st.insert(dst);
                    }
                }
                hop_limit -= 1;
            }
            VLOG(10) << st.size() << " size " << invests_.size();
            //  return all companys has invests[vid] >  thresh_hold
            size_t cnt = 0;
            std::vector<int64_t> results;
            for (vid_t i : st)
            {

                if (i != vid && invests_[i] >= thresh_hold)
                {
                    results.emplace_back(txn.GetVertexId(comp_label_id_, i).AsInt64());
                    cnt += 1;
                }
                invests_[i] = 0;
            }
            output.put_long(cnt);
            for (auto &result : results)
            {
		VLOG(10) << "put: " << result;
                output.put_long(result);
            }
            txn.Commit();
            return true;
        }

    private:
        GraphDBSession &graph_;
        label_t comp_label_id_;
        label_t invest_label_id_;
        std::vector<double> invests_;
    };

} // namespace gs

extern "C"
{
    void *CreateApp(gs::GraphDBSession &db)
    {
        gs::Query4 *app = new gs::Query4(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app)
    {
        gs::Query4 *casted = static_cast<gs::Query4 *>(app);
        delete casted;
    }
}
// 136326251
