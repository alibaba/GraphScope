#include "../core/api.h"

int main(int argc, char* argv[]) {
  VertexType(int, c, vector<int>, s, int, cc, ONE + TWO);
  SetDataset(argv[1], argv[2]);

  DefineMapV(init) {
    v.c = id(v);
    v.cc = -1;
    v.s.clear();
    return v;
  };

  DefineMapE(update) { d.s.push_back(s.c); };

  vector<int> cnt(n_vertex, 0);
  DefineMapV(local1) {
    int max_cnt = 0;
    for (auto& i : v.s) {
      cnt[i]++;
      if (cnt[i] > max_cnt) {
        max_cnt = cnt[i];
        v.cc = i;
      }
    }
    v.s.clear();
    return v;
  };

  DefineFV(filter) { return v.cc != v.c; };
  DefineMapV(local2) { v.c = v.cc; };

  vertexSubset A = vertexMap(All, CTrueV, init);
  for (int i = 0; i < 10 && Size(A) > 0; i++) {
    print("Round %d: size=%d\n", i, Size(A));
    A = edgeMapDense(All, EU, CTrueE, update, CTrueV);
    A = vertexMap(All, CTrueV, local1);
    A = vertexMap(All, filter, local2);
  }

  print("total time=%0.3lf secs\n", GetTime());
  return 0;
}
