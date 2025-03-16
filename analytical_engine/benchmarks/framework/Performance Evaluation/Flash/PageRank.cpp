#include "../core/api.h"

int main(int argc, char* argv[]) {
  VertexType(float, val, float, next, int, deg);
  SetDataset(argv[1], argv[2]);

  DefineMapV(init) {
    v.val = 1.0 / n_vertex;
    v.next = 0.0;
    v.deg = deg(v);
  };
  DefineMapE(update) { d.next += 0.85 * s.val / s.deg; };
  DefineMapV(local) {
    v.val = v.next + 0.15 / n_vertex;
    v.next = 0;
  };

  vertexMap(All, CTrueV, init);

  for (int i = 0; i < 10; ++i) {
    print("Round %d\n", i);
    edgeMapDense(All, EU, CTrueE, update, CTrueV);
    vertexMap(All, CTrueV, local);
  }

  float max_val = -1;
  double tt = 0, t = GetTime();
  All.Gather(if (v.val > max_val) max_val = v.val; tt += v.val);

  print("max_val=%0.5f, t_val=%0.5lf\ntotal time=%0.3lf secs\n", max_val, tt,
        t);

  return 0;
}
