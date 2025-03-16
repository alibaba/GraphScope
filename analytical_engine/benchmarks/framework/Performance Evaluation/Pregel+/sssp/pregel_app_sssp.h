#include <float.h>
#include "basic/pregel-dev.h"
using namespace std;

// input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
// edge lengths are assumed to be 1

// output line format: v \t shortest_path_length previous_vertex_on_shorest_path
// previous_vertex_on_shorest_path=-1 for source vertex

int src = 0;

struct SPEdge_pregel {
  double len;
  int nb;
};

ibinstream& operator<<(ibinstream& m, const SPEdge_pregel& v) {
  m << v.len;
  m << v.nb;
  return m;
}

obinstream& operator>>(obinstream& m, SPEdge_pregel& v) {
  m >> v.len;
  m >> v.nb;
  return m;
}

//====================================

struct SPValue_pregel {
  double dist;
  int from;
  vector<SPEdge_pregel> edges;
};

ibinstream& operator<<(ibinstream& m, const SPValue_pregel& v) {
  m << v.dist;
  m << v.from;
  m << v.edges;
  return m;
}

obinstream& operator>>(obinstream& m, SPValue_pregel& v) {
  m >> v.dist;
  m >> v.from;
  m >> v.edges;
  return m;
}

//====================================

struct SPMsg_pregel {
  double dist;
  int from;
};

ibinstream& operator<<(ibinstream& m, const SPMsg_pregel& v) {
  m << v.dist;
  m << v.from;
  return m;
}

obinstream& operator>>(obinstream& m, SPMsg_pregel& v) {
  m >> v.dist;
  m >> v.from;
  return m;
}

//====================================

class SPVertex_pregel : public Vertex<VertexID, SPValue_pregel, SPMsg_pregel> {
 public:
  void broadcast() {
    vector<SPEdge_pregel>& nbs = value().edges;
    for (int i = 0; i < nbs.size(); i++) {
      SPMsg_pregel msg;
      msg.dist = value().dist + nbs[i].len;
      msg.from = id;
      // cout << "------------------------------- id " << id << "  --->  " <<
      // "target " << nbs[i].nb << endl;
      send_message(nbs[i].nb, msg);
    }
  }

  virtual void compute(MessageContainer& messages) {
    if (step_num() == 1) {
      if (id == src) {
        value().dist = 0;
        value().from = -1;
        broadcast();
      } else {
        value().dist = DBL_MAX;
        value().from = -1;
      }
    } else {
      SPMsg_pregel min;
      min.dist = DBL_MAX;
      for (int i = 0; i < messages.size(); i++) {
        SPMsg_pregel msg = messages[i];
        if (min.dist > msg.dist) {
          min = msg;
        }
      }
      if (min.dist < value().dist) {
        value().dist = min.dist;
        value().from = min.from;
        broadcast();
      }
    }
    vote_to_halt();
  }

  virtual void print() {}
};

class SPWorker_pregel : public Worker<SPVertex_pregel> {
  char buf[1000];

 public:
  virtual SPVertex_pregel* toVertex(char* line) {
    char* pch;
    char* saveptr;
    pch = strtok_r(line, "\t", &saveptr);
    SPVertex_pregel* v = new SPVertex_pregel;
    int id = atoi(pch);
    v->id = id;
    v->value().from = -1;
    if (id == src)
      v->value().dist = 0;
    else {
      v->value().dist = DBL_MAX;
      v->vote_to_halt();
    }
    pch = strtok_r(NULL, " ", &saveptr);
    while (pch = strtok_r(NULL, " ", &saveptr)) {
      int nb = atoi(pch);
      double len = 1;
      SPEdge_pregel edge = {len, nb};
      v->value().edges.push_back(edge);
    }
    return v;
  }

  // output line:
  // vid \t dist from
  virtual void toline(SPVertex_pregel* v, BufferedWriter& writer) {
    if (v->value().dist != DBL_MAX)
      sprintf(buf, "%d\t%f %d\n", v->id, v->value().dist, v->value().from);
    else
      sprintf(buf, "%d\tunreachable\n", v->id);
    writer.write(buf);
  }
};

class SPCombiner_pregel : public Combiner<SPMsg_pregel> {
 public:
  virtual void combine(SPMsg_pregel& old, const SPMsg_pregel& new_msg) {
    if (old.dist > new_msg.dist)
      old = new_msg;
  }
};

void pregel_sssp(int srcID, string in_path, string out_path,
                 bool use_combiner) {
  src = srcID;  // set the src first

  WorkerParams param;
  param.input_path = in_path;
  param.output_path = out_path;
  param.force_write = true;
  param.native_dispatcher = false;
  SPWorker_pregel worker;
  SPCombiner_pregel combiner;
  if (use_combiner)
    worker.setCombiner(&combiner);
  worker.run(param);
}
