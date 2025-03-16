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

// Triangle counting code (assumes a symmetric graph, so pass the "-s"
// flag). This is not optimized (no ordering heuristic is used)--for
// optimized code, see "Multicore Triangle Computations Without
// Tuning", ICDE 2015. Currently only works with uncompressed graphs,
// and not with compressed graphs.
#include "ligra.h"
#include "quickSort.h"

// assumes sorted neighbor lists
template <class vertex>
long countCommon(vertex& A, vertex& B, uintE a, uintE b) {
  uintT i = 0, j = 0, nA = A.getOutDegree(), nB = B.getOutDegree();
  uintE *nghA = (uintE*) A.getOutNeighbors(),
        *nghB = (uintE*) B.getOutNeighbors();
  long ans = 0;
  while (i < nA && j < nB && nghA[i] < a &&
         nghB[j] < b) {  // count "directed" triangles
    if (nghA[i] == nghB[j])
      i++, j++, ans++;
    else if (nghA[i] < nghB[j])
      i++;
    else
      j++;
  }
  return ans;
}

template <class vertex>
struct countF {  // for edgeMap
  vertex* V;
  long* counts;
  countF(vertex* _V, long* _counts) : V(_V), counts(_counts) {}
  inline bool update(uintE s, uintE d) {
    if (s > d)  // only count "directed" triangles
      writeAdd(&counts[s], countCommon<vertex>(V[s], V[d], s, d));
    return 1;
  }
  inline bool updateAtomic(uintE s, uintE d) {
    if (s > d)  // only count "directed" triangles
      writeAdd(&counts[s], countCommon<vertex>(V[s], V[d], s, d));
    return 1;
  }
  inline bool cond(uintE d) { return cond_true(d); }  // does nothing
};

struct intLT {
  bool operator()(uintT a, uintT b) { return a < b; };
};

template <class vertex>
struct initF {  // for vertexMap to initial counts and sort neighbors for
                // merging
  vertex* V;
  long* counts;
  initF(vertex* _V, long* _counts) : V(_V), counts(_counts) {}
  inline bool operator()(uintE i) {
    counts[i] = 0;
    quickSort(V[i].getOutNeighbors(), V[i].getOutDegree(), intLT());
    return 1;
  }
};

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);
  uintT n = GA.n;
  long* counts = newA(long, n);
  bool* frontier = newA(bool, n);
  { parallel_for(long i = 0; i < n; i++) frontier[i] = 1; }
  vertexSubset Frontier(n, n, frontier);  // frontier contains all vertices

  vertexMap(Frontier, initF<vertex>(GA.V, counts));
  edgeMap(GA, Frontier, countF<vertex>(GA.V, counts), -1, no_output);
  long count = sequence::plusReduce(counts, n);
  cout << "triangle count = " << count << endl;
  Frontier.del();
  free(counts);
}
