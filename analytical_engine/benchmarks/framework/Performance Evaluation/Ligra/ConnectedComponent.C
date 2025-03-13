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
#include "ligra.h"

struct CC_F {
  uintE* IDs, *prevIDs;
  CC_F(uintE* _IDs, uintE* _prevIDs) : 
    IDs(_IDs), prevIDs(_prevIDs) {}
  inline bool update(uintE s, uintE d){ //Update function writes min ID
    uintE origID = IDs[d];
    if(IDs[s] < origID) {
      IDs[d] = min(origID,IDs[s]);
      if(origID == prevIDs[d]) return 1;
    } return 0; }
  inline bool updateAtomic (uintE s, uintE d) { //atomic Update
    uintE origID = IDs[d];
    return (writeMin(&IDs[d],IDs[s]) && origID == prevIDs[d]);
  }
  inline bool cond (uintE d) { return cond_true(d); } //does nothing
};

//function used by vertex map to sync prevIDs with IDs
struct CC_Vertex_F {
  uintE* IDs, *prevIDs;
  CC_Vertex_F(uintE* _IDs, uintE* _prevIDs) :
    IDs(_IDs), prevIDs(_prevIDs) {}
  inline bool operator () (uintE i) {
    prevIDs[i] = IDs[i];
    return 1; }};

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);
  long n = GA.n;
  uintE* IDs = newA(uintE,n), *prevIDs = newA(uintE,n);
  {parallel_for(long i=0;i<n;i++) IDs[i] = i;} //initialize unique IDs

  bool* frontier = newA(bool,n);
  {parallel_for(long i=0;i<n;i++) frontier[i] = 1;} 
  vertexSubset Frontier(n,n,frontier); //initial frontier contains all vertices

  while(!Frontier.isEmpty()){ //iterate until IDS converge
    vertexMap(Frontier,CC_Vertex_F(IDs,prevIDs));
    vertexSubset output = edgeMap(GA, Frontier, CC_F(IDs,prevIDs));
    Frontier.del();
    Frontier = output;
  }
  Frontier.del(); free(IDs); free(prevIDs);
}
