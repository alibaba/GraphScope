#include "../core/api.h"
#include <set>
#include <algorithm>

int main(int argc, char *argv[]) {
	VertexType(short,core, int, cnt, vector<short>, s, ONE);
	SetDataset(argv[1], argv[2]);

	DefineMapV(init) {v.core = min(SHRT_MAX,deg(v));};
	DefineMapV(local1) {v.cnt = 0; v.s.clear();};

	DefineFE(check1) {return s.core >= d.core;};
	DefineMapE(update1) {d.cnt++; return d;};

	DefineFV(filter) {return v.cnt < v.core;};
	
	DefineMapE(update2) {d.s.push_back(s.core); return d;};

	vector<int> cnt(SHRT_MAX);
	DefineMapV(local2) {
		memset(cnt.data(),0,sizeof(int)*(v.core+1));
		for (auto &i: v.s) {
			++cnt[min(v.core, i)];
		}
		for(int s=0; s+cnt[v.core]<v.core; --v.core) s += cnt[v.core];
	};

	vertexSubset A = vertexMap(All, CTrueV, init);
	for(int len = Size(A), i = 0; len > 0; len = Size(A),++i) {
		print("Round %d: size=%d\n", i, len);
		A = vertexMap(All, CTrueV, local1); 
		edgeMapDense(All, EU, check1, update1, CTrueV); 
		A = vertexMap(All, filter); 
		edgeMapDense(All, EjoinV(EU, A), CTrueE, update2, CTrueV);
		A = vertexMap(A, CTrueV, local2);
	}

	int max_core = 0;

	long long sum_core = 0; double t = GetTime();
	All.Gather(sum_core += v.core);
	All.Gather(if (v.core > max_core) max_core = v.core);
	print( "sum_core=%lld\ntotal time=%0.3lf secs\n", sum_core, t);
	print( "max_core=%lld\ntotal time=%0.3lf secs\n", max_core, t);
	return 0;
}
