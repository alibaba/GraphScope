#include <set>
#include "basic/pregel-dev.h"
using namespace std;

int all_tri = 0;
struct TCValue_pregel {
  set<VertexID> neighbors;
  int triangle_count;

  // 序列化操作符
  friend ibinstream& operator<<(ibinstream& m, const TCValue_pregel& v) {
    m << v.neighbors;
    m << v.triangle_count;
    return m;
  }

  // 反序列化操作符
  friend obinstream& operator>>(obinstream& m, TCValue_pregel& v) {
    m >> v.neighbors;
    m >> v.triangle_count;
    return m;
  }
};
struct TCMsg_pregel {
  VertexID id;
  set<VertexID> neighbors;

  // 序列化操作符
  friend ibinstream& operator<<(ibinstream& m, const TCMsg_pregel& v) {
    m << v.id;
    m << v.neighbors;
    return m;
  }

  // 反序列化操作符
  friend obinstream& operator>>(obinstream& m, TCMsg_pregel& v) {
    m >> v.id;
    m >> v.neighbors;
    return m;
  }
};
class TCVertex_pregel : public Vertex<VertexID, TCValue_pregel, TCMsg_pregel> {
 public:
  virtual void compute(MessageContainer& messages) {
    if (step_num() == 1) {
      // 第一步：将邻居列表发送给所有邻居
      // if(id%1000 == 0) cout<<id<<endl;
      for (const VertexID& nb : value().neighbors) {
        if (nb <= id)
          continue;

        TCMsg_pregel msg = {id, {}};
        for (auto& neighbor : value().neighbors) {
          if (neighbor < id) {
            msg.neighbors.insert(neighbor);
          }
        }
        send_message(nb, msg);
      }
    } else if (step_num() == 2) {
      // 第二步：接收来自邻居的消息并计算共同邻居
      // set<VertexID> received_neighbors;
      int triangle_count = 0;
      // if(id%1000 == 0) cout<<id<<endl;
      for (const TCMsg_pregel& msg : messages) {
        if (msg.id >= id)
          continue;
        for (const VertexID& nb : msg.neighbors) {
          if (nb < id && nb < msg.id && value().neighbors.count(nb)) {
            // received_neighbors.insert(nb);
            // cout<<id<<" "<<nb<<" "<<msg.id<<endl;
            triangle_count++;
          }
        }
      }

      // 计算三角形数量

      // for (const VertexID &nb : received_neighbors) {
      //     if (value().neighbors.count(nb)) {

      //     }
      // }
      value().triangle_count = triangle_count;  // 每个三角形被计算了两次
      all_tri += triangle_count;
      vote_to_halt();
    } else {
      vote_to_halt();
    }
  }
};
class TCWorker_pregel : public Worker<TCVertex_pregel> {
  char buf[1000];
  // int all_tri = 0;

 public:
  // 输入行格式: vertexID \t neighbor1 neighbor2 ...
  virtual TCVertex_pregel* toVertex(char* line) {
    char* pch;
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

  virtual void toline(TCVertex_pregel* v, BufferedWriter& writer) {
    return;
    // all_tri+=v->value().triangle_count;
    // sprintf(buf, "%d\t%d\n", v->id, v->value().triangle_count);
    // cout<<v->id<<" "<< v->value().triangle_count<<endl;
    // writer.write(buf);
  }
};
void pregel_triangle_counting(string in_path, string out_path) {
  WorkerParams param;
  param.input_path = in_path;
  param.output_path = out_path;
  param.force_write = true;
  param.native_dispatcher = false;
  TCWorker_pregel worker;
  worker.run(param);

  cout << "all_tri:" << all_tri << endl;
}