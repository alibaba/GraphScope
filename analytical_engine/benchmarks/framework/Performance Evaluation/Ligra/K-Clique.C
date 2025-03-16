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
#include <map>
#include <set>
#include <vector>
#include "ligra.h"
#include "math.h"

#define K 3

template <class vertex>
struct GetClique {
  vertex* V;
  long* clique_num;
  GetClique(vertex* _V, long* _clique_num) : V(_V), clique_num(_clique_num) {}

  int KCliqueCounting(map<int, vector<int>>& subgraph, set<int>& cand,
                      int lev) {
    if (lev == K - 1) {
      return cand.size();
    }
    int t = 0;
    for (auto& u : cand) {
      set<int> next_cand;
      for (auto& v : subgraph[u]) {
        if (cand.count(v)) {
          next_cand.insert(v);
        }
      }
      if (next_cand.size() >= K - lev - 1) {
        t = t + KCliqueCounting(subgraph, next_cand, lev + 1);
      }
    }
    return t;
  }

  inline bool operator()(uintE i) {
    map<int, vector<int>> subgraph;
    set<int> cand;
    set<int> nb;

    uintE* neighbors = (uintE*) V[i].getOutNeighbors();
    for (uintT j = 0; j < V[i].getOutDegree(); j++) {
      long neighbor = neighbors[j];
      if (neighbor <= i)
        continue;

      nb.insert(neighbor);
    }

    // uintE* neighbors = (uintE*) V[i].getOutNeighbors();
    for (uintT j = 0; j < V[i].getOutDegree(); j++) {
      long neighbor = neighbors[j];
      if (neighbor <= i)
        continue;

      subgraph[i].push_back(neighbor);
      cand.insert(neighbor);

      uintE* second_neighbors = (uintE*) V[neighbor].getOutNeighbors();
      for (uintT k = 0; k < V[neighbor].getOutDegree(); k++) {
        long second_neighbor = second_neighbors[k];
        if (second_neighbor <= neighbor)
          continue;

        if (nb.count(second_neighbor)) {
          subgraph[neighbor].push_back(second_neighbor);
        }
      }
    }
    // output subgraph
    // for (auto &u : cand) {
    //     cout << u << " ";
    // }
    // cout << endl;
    // cout << "subgraph of " << i << endl;
    // for (auto &u : subgraph) {
    //     for (auto &v : u.second) {
    //         cout << u.first << " " << v << endl;
    //     }
    // }
    clique_num[i] = KCliqueCounting(subgraph, cand, 1);
    // cout << "k-clique num: " << clique_num[i] << endl;
    return 1;
  }
};

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);
  const intE n = GA.n;

  long* clique_num = newA(long, n);
  { parallel_for(long i = 0; i < n; i++) clique_num[i] = i; }

  bool* frontier = newA(bool, n);
  { parallel_for(long i = 0; i < n; i++) frontier[i] = 1; }
  vertexSubset Frontier(n, n, frontier);

  vertexMap(Frontier, GetClique<vertex>(GA.V, clique_num));

  long long ans = 0;
  for (long i = 0; i < n; i++)
    ans += clique_num[i];
  cout << "---------   start k-clique   ---------" << endl;
  cout << "K-Clique num: " << ans << endl;
  cout << "---------   finish k-clique   ---------" << endl;

  Frontier.del();
  free(clique_num);
}
