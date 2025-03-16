#include "../core/api.h"

int main(int argc, char* argv[]) {
  VertexType(char, d, float, c, float, b);
  SetDataset(argv[1], argv[2]);
  int s = atoi(argv[3]);
  int curLevel;

  DefineMapV(init) {
    if (id(v) == s) {
      v.d = 0;
      v.c = 1;
      v.b = 0;
    } else {
      v.d = -1;
      v.c = 0;
      v.b = 0;
    }
    return v;
  };
  DefineFV(filter) { return id(v) == s; };

  // DefineFE(check1) {return s.d == curLevel - 1;};
  DefineMapE(update1) { d.c += s.c; };
  DefineCond(cond) { return v.d == -1; };
  DefineReduce(reduce1) {
    d.c += s.c;
    return d;
  };
  DefineMapV(local) { v.d = curLevel; };

  DefineFE(check2) { return (d.d == s.d - 1); };
  DefineMapE(update2) {
    d.b += d.c / s.c * (1 + s.b);
    return d;
  };
  DefineReduce(reduce2) {
    d.b += s.b;
    return d;
  };

  function<void(vertexSubset&, int)> bn = [&](vertexSubset& S, int h) {
    curLevel = h;
    int sz = Size(S);
    if (sz == 0)
      return;
    else
      print("size=%d\n", sz);
    vertexSubset T = edgeMap(S, ED, CTrueE, update1, cond, reduce1);
    T = vertexMap(T, CTrueV, local);
    bn(T, h + 1);
    print("-size=%d\n", sz);
    curLevel = h;
    edgeMap(T, EjoinV(ER, S), check2, update2, CTrueV, reduce2);
  };

  vertexSubset S = vertexMap(All, CTrueV, init);
  S = vertexMap(S, filter);

  bn(S, 1);

  print("total time=%0.3lf secs\n", GetTime());
  return 0;
}