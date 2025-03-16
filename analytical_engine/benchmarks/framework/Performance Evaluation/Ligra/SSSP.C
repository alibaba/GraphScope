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
#define WEIGHTED 1
#include "ligra.h"

struct BF_F {
  intE* ShortestPathLen;
  int* Visited;
  BF_F(intE* _ShortestPathLen, int* _Visited)
      : ShortestPathLen(_ShortestPathLen), Visited(_Visited) {}
  inline bool update(
      uintE s, uintE d,
      intE edgeLen) {  // Update ShortestPathLen if found a shorter path
    intE newDist = ShortestPathLen[s] + edgeLen;
    if (ShortestPathLen[d] > newDist) {
      ShortestPathLen[d] = newDist;
      if (Visited[d] == 0) {
        Visited[d] = 1;
        return 1;
      }
    }
    return 0;
  }
  inline bool updateAtomic(uintE s, uintE d, intE edgeLen) {  // atomic Update
    intE newDist = ShortestPathLen[s] + edgeLen;
    return (writeMin(&ShortestPathLen[d], newDist) && CAS(&Visited[d], 0, 1));
  }
  inline bool cond(uintE d) { return cond_true(d); }
};

// reset visited vertices
struct BF_Vertex_F {
  int* Visited;
  BF_Vertex_F(int* _Visited) : Visited(_Visited) {}
  inline bool operator()(uintE i) {
    Visited[i] = 0;
    return 1;
  }
};

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);

  long start = P.getOptionLongValue("-r", 0);
  long n = GA.n;
  // initialize ShortestPathLen to "infinity"
  intE* ShortestPathLen = newA(intE, n);
  { parallel_for(long i = 0; i < n; i++) ShortestPathLen[i] = INT_MAX / 2; }
  ShortestPathLen[start] = 0;

  int* Visited = newA(int, n);
  { parallel_for(long i = 0; i < n; i++) Visited[i] = 0; }

  vertexSubset Frontier(n, start);  // initial frontier

  long round = 0;
  while (!Frontier.isEmpty()) {
    // std::cout << round << std::endl;
    if (round == n) {
      // negative weight cycle
      {
        parallel_for(long i = 0; i < n; i++) ShortestPathLen[i] =
            -(INT_E_MAX / 2);
      }
      break;
    }
    vertexSubset output = edgeMap(GA, Frontier, BF_F(ShortestPathLen, Visited),
                                  GA.m / 20, dense_forward);
    vertexMap(output, BF_Vertex_F(Visited));
    Frontier.del();
    Frontier = output;
    round++;
  }
  std::cout << "iteration round:" << round << std::endl;
  // for (int i = 0;i < n; i++) std::cout << "vertex " << i << " distance " <<
  // ShortestPathLen[i] << std::endl;
  Frontier.del();
  free(Visited);
  free(ShortestPathLen);
}
