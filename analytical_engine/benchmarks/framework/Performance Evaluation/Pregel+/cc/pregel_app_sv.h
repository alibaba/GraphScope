#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

// input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
// output line format: v \t min_vertexID(v's connected component)

// naming rules:
// G(get): receive messages
// D(do): process vertex
// S(send): send messages
// R(respond): including GDS

//<V>=<D[v], star[v]>
// initially, D[v]=v, star[v]=false
struct SVValue_pregel {
  int D;
  bool star;
  vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const SVValue_pregel& v) {
  m << v.D;
  m << v.star;
  m << v.edges;
  return m;
}

obinstream& operator>>(obinstream& m, SVValue_pregel& v) {
  m >> v.D;
  m >> v.star;
  m >> v.edges;
  return m;
}

//====================================

class SVVertex_pregel : public Vertex<int, SVValue_pregel, int> {
  void treeInit_D() {
    // set D[u]=min{v} to allow fastest convergence, though any v is ok
    // (assuming (u, v) is accessed last)
    vector<VertexID>& edges = value().edges;
    for (int i = 0; i < edges.size(); i++) {
      int nb = edges[i];
      if (nb < value().D)
        value().D = nb;
    }
  }

  // ========================================

  // w = Du

  void rtHook_1S()  // = shortcut's request to w
  {                 // request to w
    int Du = value().D;
    send_message(Du, id);
  }

  void rtHook_2R(MessageContainer& msgs)  // = shortcut's respond by w
  {                                       // respond by w
    int Dw = value().D;
    for (int i = 0; i < msgs.size(); i++) {
      int requester = msgs[i];
      send_message(requester, Dw);
    }
  }

  void rtHook_2S()  // = starhook's send D[v]
  {                 // send negated D[v]
    int Dv = value().D;
    vector<VertexID>& edges = value().edges;
    for (int i = 0; i < edges.size(); i++) {
      int nb = edges[i];
      send_message(
          nb, -Dv - 1);  // negate Dv to differentiate it from other msg types
    }
  }  // in fact, a combiner with MIN operator can be used here

  bool rtHook_3GDS(MessageContainer& msgs)  // return whether a msg is sent
  {  // set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok
     // (assuming (u, v) is accessed last)
    int Dw = -1;
    int Du = value().D;
    int Dv = -1;  // pick the min
    for (int i = 0; i < msgs.size(); i++) {
      int msg = msgs[i];
      if (msg >= 0)
        Dw = msg;
      else  // type==rtHook_2R_v
      {
        int cur = -msg - 1;
        if (Dv == -1 || cur < Dv)
          Dv = cur;
      }
    }
    if (Dw == Du && Dv != -1 && Dv < Du)  // condition checking
    {
      send_message(Du, Dv);
      return true;
    }
    return false;
  }

  void rtHook_4GD(MessageContainer& msgs)  // = starhook's write D[D[u]]
  {  // set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok
     // (assuming (u, v) is accessed last)
    int Dv = -1;
    for (int i = 0; i < msgs.size(); i++) {
      int cur = msgs[i];
      if (Dv == -1 || cur < Dv)
        Dv = cur;
    }
    if (Dv != -1)
      value().D = Dv;
  }

  // ========================================

  // call rtHook_2S()

  void starHook_3GDS(vector<int>& msgs)  // set star[u] first
  {  // set D[w]=min_v{D[v]} to allow fastest convergence
    if (value().star) {
      int Du = value().D;
      int Dv = -1;
      for (int i = 0; i < msgs.size(); i++) {
        int cur = msgs[i];
        if (Dv == -1 || cur < Dv)
          Dv = cur;
      }
      if (Dv != -1 && Dv < Du)  // condition checking
      {
        send_message(Du, Dv);
      }
    }
  }

  // call rtHook_4GD

  // ========================================

  // call rtHook_1S
  // call rtHook_2R

  void shortcut_3GD(MessageContainer& msgs) {  // D[u]=D[D[u]]
    value().D = msgs[0];
  }

  // ========================================

  void setStar_1S() {
    value().star = true;
    int Du = value().D;
    send_message(Du, id);
  }

  void setStar_2R(MessageContainer& msgs) {
    int Dw = value().D;
    for (int i = 0; i < msgs.size(); i++) {
      int requester = msgs[i];
      send_message(requester, Dw);
    }
  }

  void setStar_3GDS(MessageContainer& msgs) {
    int Du = value().D;
    int Dw = msgs[0];
    if (Du != Dw) {
      value().star = false;
      // notify Du
      send_message(Du, -1);  //-1 means star_notify
      // notify Dw
      send_message(Dw, -1);
    }
    send_message(Du, id);
  }

  void setStar_4GDS(MessageContainer& msgs) {
    vector<int> requesters;
    for (int i = 0; i < msgs.size(); i++) {
      int msg = msgs[i];
      if (msg == -1)
        value().star = false;
      else
        requesters.push_back(msg);  // star_request
    }
    bool star = value().star;
    for (int i = 0; i < requesters.size(); i++) {
      send_message(requesters[i], star);
    }
  }

  void setStar_5GD(MessageContainer& msgs) {
    for (int i = 0; i < msgs.size(); i++)  // at most one
    {
      value().star = msgs[i];
    }
  }

  void setStar_5GD_starhook(MessageContainer& messages, vector<int>& msgs) {
    for (int i = 0; i < messages.size(); i++) {
      int msg = messages[i];
      if (msg >= 0)
        value().star = msg;
      else
        msgs.push_back(-msg - 1);
    }
  }

  //==================================================

 public:
  virtual void compute(MessageContainer& messages) {
    int cycle = 14;
    if (step_num() == 1) {
      treeInit_D();
      rtHook_1S();
    } else if (step_num() % cycle == 2) {
      rtHook_2R(messages);
      rtHook_2S();
    } else if (step_num() % cycle == 3) {
      if (rtHook_3GDS(messages))
        wakeAll();
      else {
        //============== end condition ==============
        bool* agg = (bool*) getAgg();
        if (*agg) {
          vote_to_halt();
          return;
        }
        //===========================================
      }

    } else if (step_num() % cycle == 4) {
      rtHook_4GD(messages);
      setStar_1S();
    } else if (step_num() % cycle == 5) {
      setStar_2R(messages);
    } else if (step_num() % cycle == 6) {
      setStar_3GDS(messages);
    } else if (step_num() % cycle == 7) {
      setStar_4GDS(messages);
      rtHook_2S();
    } else if (step_num() % cycle == 8) {
      vector<int> msgs;
      setStar_5GD_starhook(messages, msgs);
      starHook_3GDS(msgs);  // set star[v] first
    } else if (step_num() % cycle == 9) {
      rtHook_4GD(messages);
      rtHook_1S();
    } else if (step_num() % cycle == 10) {
      rtHook_2R(messages);
    } else if (step_num() % cycle == 11) {
      shortcut_3GD(messages);
      setStar_1S();
    } else if (step_num() % cycle == 12) {
      setStar_2R(messages);
    } else if (step_num() % cycle == 13) {
      setStar_3GDS(messages);
    } else if (step_num() % cycle == 0) {
      setStar_4GDS(messages);
    } else if (step_num() % cycle == 1) {
      setStar_5GD(messages);
      rtHook_1S();
    }
  }
};

//====================================

class SVAgg_pregel : public Aggregator<SVVertex_pregel, bool, bool> {
 private:
  bool AND;

 public:
  virtual void init() { AND = true; }

  virtual void stepPartial(SVVertex_pregel* v) {
    if (v->value().star == false)
      AND = false;
  }

  virtual void stepFinal(bool* part) {
    if (*part == false)
      AND = false;
  }

  virtual bool* finishPartial() { return &AND; }
  virtual bool* finishFinal() { return &AND; }
};

//====================================

class SVWorker_pregel : public Worker<SVVertex_pregel, SVAgg_pregel> {
  char buf[100];

 public:
  virtual SVVertex_pregel* toVertex(char* line) {
    char* pch;
    char* saveptr;
    pch = strtok_r(line, "\t", &saveptr);
    SVVertex_pregel* v = new SVVertex_pregel;
    v->id = atoi(pch);
    pch = strtok_r(NULL, " ", &saveptr);
    int num = atoi(pch);
    for (int i = 0; i < num; i++) {
      pch = strtok_r(NULL, " ", &saveptr);
      v->value().edges.push_back(atoi(pch));
    }
    v->value().D = v->id;
    v->value().star = false;  // strictly speaking, this should be true
    // after treeInit_D(), should do star-checking
    // however, this is time-consuming, and it's very unlikely that treeInit_D()
    // gives stars therefore, set false here to save the first star-checking
    return v;
  }

  virtual void toline(SVVertex_pregel* v, BufferedWriter& writer) {
    // sprintf(buf, "%d\t%d\n", v->id, v->value().D);
    // writer.write(buf);
  }
};

void pregel_sv(string in_path, string out_path) {
  WorkerParams param;
  param.input_path = in_path;
  param.output_path = out_path;
  param.force_write = true;
  param.native_dispatcher = false;
  SVWorker_pregel worker;
  SVAgg_pregel agg;
  worker.setAggregator(&agg);
  worker.run(param);
}