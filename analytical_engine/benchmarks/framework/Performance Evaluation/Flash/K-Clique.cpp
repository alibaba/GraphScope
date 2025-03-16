#include <set>
#include "../core/api.h"

int main(int argc, char* argv[]) {
  VertexType(int, deg, int, id, vector<int>, out);
  SetDataset(argv[1], argv[2]);
  int k = argc > 3 ? atoi(argv[3]) : 4;
  long long cnt = 0, cnt_loc = 0;

  DefineMapV(init) {
    v.deg = deg(v);
    v.id = id(v);
    return v;
  };

  DefineFE(check) {
    return (s.deg > d.deg) || ((s.deg == d.deg) && (s.id > d.id));
  };
  DefineMapE(update) { d.out.push_back(s.id); };

  DefineFV(filter) { return v.out.size() >= k - 1; };
  function<void(vector<int>&, vector<int>&, int)> compute =
      [&](vector<int>& result, vector<int>& cand, int nowk) {
        if (nowk == k) {
          ++cnt_loc;
          return;
        }
        vector<int> c(cand.size());
        for (auto& u : cand) {
          result[nowk] = u;
          c.resize(cand.size());
          int len = set_intersect(cand, GetV(u).out, c);
          if (len < k - nowk - 1)
            continue;
          c.resize(len);
          compute(result, c, nowk + 1);
        }
      };
  DefineMapV(local) {
    vector<int> res(k);
    res[0] = v.id;
    compute(res, v.out, 1);
  };

  print("Loading...\n");
  vertexSubset A = vertexMap(All, CTrueV, init);
  edgeMapDense(All, EU, check, update, CTrueV);

  print("Computing...\n");
  A = vertexMap(All, filter, local);

  cnt = Sum(cnt_loc);
  print("Number of %d-cliques=%lld\ntotal time=%0.3lf secs\n", k, cnt,
        GetTime());
  return 0;
}
