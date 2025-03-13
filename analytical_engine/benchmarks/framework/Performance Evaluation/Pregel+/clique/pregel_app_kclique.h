#include "basic/pregel-dev.h"
#include <set>
using namespace std;
const int K = 3;
// int all_tri = 0;
struct TCValue_pregel {
    set<VertexID> neighbors;
    int triangle_count;

    // 序列化操作符
    friend ibinstream & operator<<(ibinstream & m, const TCValue_pregel & v) {
        m << v.neighbors;
        m << v.triangle_count;
        return m;
    }

    // 反序列化操作符
    friend obinstream & operator>>(obinstream & m, TCValue_pregel & v) {
        m >> v.neighbors;
        m >> v.triangle_count;
        return m;
    }
};
struct TCMsg_pregel {
    VertexID id;
    set<VertexID> neighbors;

    // 序列化操作符
    friend ibinstream & operator<<(ibinstream & m, const TCMsg_pregel & v) {
        m << v.id;
        m << v.neighbors;
        return m;
    }

    // 反序列化操作符
    friend obinstream & operator>>(obinstream & m, TCMsg_pregel & v) {
        m >> v.id;
        m >> v.neighbors;
        return m;
    }
};
class TCVertex_pregel : public Vertex<VertexID, TCValue_pregel, TCMsg_pregel> {
public:

    int KCliqueCounting(map<int, vector<int>> & subgraph, set<int> & cand, int lev) {
        if (lev == K - 1) {
            return cand.size();
        }
        int t = 0;
        for (auto &u : cand) {
            set<int> next_cand;
            for (auto &v : subgraph[u]) { 
                if (cand.count(v)) {
                    next_cand.insert(v);
                }
            }
            if (next_cand.size() >= K - lev - 1) {
                t = t + KCliqueCounting(subgraph, next_cand, lev + 1);
            }
            next_cand.clear();
        }
        return t;
    }

    virtual void compute(MessageContainer & messages) {
        if (step_num() == 1) {
            // std::cout << "compute at " << id << std::endl;
            // 第一步：将邻居列表发送给所有邻居
            TCMsg_pregel msg = {id, {}};
            for (auto& neighbor : value().neighbors) {
                if (neighbor > id) {
                    msg.neighbors.insert(neighbor);
                }
            }

            for (const VertexID &nb : value().neighbors) {
                if (nb < id) {
                    send_message(nb, msg);
                }
            }

            // for (const VertexID &nb : value().neighbors) {
            //     if (nb <= id) continue;

            //     TCMsg_pregel msg = {id, {}};
            //     for (auto& neighbor : value().neighbors) {
            //         if (neighbor < id) {
            //             msg.neighbors.insert(neighbor);
            //         }
            //     }
            //     send_message(nb, msg);
            // }

        } else {
            // 第二步：接收来自邻居的消息并计算共同邻居
            map<int, vector<int>> subgraph;
            set<int> cand;
            for (const TCMsg_pregel &msg : messages) {
                subgraph[id].push_back(msg.id);
                cand.insert(msg.id);
                for (const VertexID &nb : msg.neighbors) {
                    if (value().neighbors.count(nb)) {
                        subgraph[msg.id].push_back(nb);
                    }
                }
            }
            // output subgraph
            // for (auto &u : cand) {
            //     cout << u << " ";
            // }
            // cout << endl;
            // cout << "subgraph of " << id << endl;
            // for (auto &u : subgraph) {
            //     for (auto &v : u.second) {
            //         cout << u.first << " " << v << endl;
            //     }
            // }
            value().triangle_count = KCliqueCounting(subgraph, cand, 1);
            // cout << id << " has " << value().triangle_count << endl;
        }
        vote_to_halt();
    }
};
class TCWorker_pregel : public Worker<TCVertex_pregel> {
    char buf[1000];
    // int all_tri = 0;

public:
    // 输入行格式: vertexID \t neighbor1 neighbor2 ...
    virtual TCVertex_pregel* toVertex(char* line) {
        char * pch;
        pch = strtok(line, "\t");
        TCVertex_pregel* v = new TCVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        while (pch = strtok(NULL, " ")) {
            v->value().neighbors.insert(atoi(pch));
        }
        v->value().triangle_count = 0;
        return v;
    }

    // 输出行格式: vertexID \t triangle_count
    
    virtual void toline(TCVertex_pregel* v, BufferedWriter & writer) {
        // return;
        // all_tri+=v->value().triangle_count;
        sprintf(buf, "%d\t%d\n", v->id, v->value().triangle_count);
        // cout<<v->id<<" "<< v->value().triangle_count<<endl;
        writer.write(buf);
    }
    
};
void pregel_triangle_counting(string in_path, string out_path) {
    // cout << "start1" << endl;
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    TCWorker_pregel worker;
    // cout << "start2" << endl;
    worker.run(param);

    // cout<<"all_tri:"<<all_tri/3<<endl;
}