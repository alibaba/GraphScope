// This code is part of the project "Ligra: A Lightweight Graph Processing
// Framework for Shared Memory", presented at Principles and Practice of
// Parallel Programming, 2013.
// Copyright (c) 2013 Julian Shun and Guy Blelloch
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// Parallel implementation of K-Core decomposition of a symmetric
// graph.
#include "ligra.h"

struct Update_Deg {
  intE* Degrees;
  Update_Deg(intE* _Degrees) : Degrees(_Degrees) {}
  inline bool update(uintE s, uintE d) {
    Degrees[d]--;
    return 1;
  }
  inline bool updateAtomic(uintE s, uintE d) {
    writeAdd(&Degrees[d], -1);
    return 1;
  }
  inline bool cond(uintE d) { return Degrees[d] > 0; }
};

template <class vertex>
struct Deg_LessThan_K {
  vertex* V;
  uintE* coreNumbers;
  intE* Degrees;
  uintE k;
  Deg_LessThan_K(vertex* _V, intE* _Degrees, uintE* _coreNumbers, uintE _k)
      : V(_V), k(_k), Degrees(_Degrees), coreNumbers(_coreNumbers) {}
  inline bool operator()(uintE i) {
    if (Degrees[i] < k) {
      coreNumbers[i] = k - 1;
      Degrees[i] = 0;
      return true;
    } else
      return false;
  }
};

template <class vertex>
struct Deg_AtLeast_K {
  vertex* V;
  intE* Degrees;
  uintE k;
  Deg_AtLeast_K(vertex* _V, intE* _Degrees, uintE _k)
      : V(_V), k(_k), Degrees(_Degrees) {}
  inline bool operator()(uintE i) { return Degrees[i] >= k; }
};

// assumes symmetric graph
//  1) iterate over all remaining active vertices
//  2) for each active vertex, remove if induced degree < k. Any vertex removed
//  has
//     core-number (k-1) (part of (k-1)-core, but not k-core)
//  3) stop once no vertices are removed. Vertices remaining are in the k-core.
template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);
  const long n = GA.n;
  bool* active = newA(bool, n);
  { parallel_for(long i = 0; i < n; i++) active[i] = 1; }
  vertexSubset Frontier(n, n, active);
  uintE* coreNumbers = newA(uintE, n);
  intE* Degrees = newA(intE, n);
  {
    parallel_for(long i = 0; i < n; i++) {
      coreNumbers[i] = 0;
      Degrees[i] = GA.V[i].getOutDegree();
    }
  }
  long largestCore = -1;
  for (long k = 1; k <= n; k++) {
    while (true) {
      vertexSubset toRemove = vertexFilter(
          Frontier, Deg_LessThan_K<vertex>(GA.V, Degrees, coreNumbers, k));
      vertexSubset remaining =
          vertexFilter(Frontier, Deg_AtLeast_K<vertex>(GA.V, Degrees, k));
      Frontier.del();
      Frontier = remaining;
      if (0 == toRemove.numNonzeros()) {  // fixed point. found k-core
        toRemove.del();
        break;
      } else {
        edgeMap(GA, toRemove, Update_Deg(Degrees), -1, no_output);
        toRemove.del();
      }
    }
    if (Frontier.numNonzeros() == 0) {
      largestCore = k - 1;
      break;
    }
  }
  cout << "largestCore was " << largestCore << endl;
  Frontier.del();
  free(coreNumbers);
  free(Degrees);
}
