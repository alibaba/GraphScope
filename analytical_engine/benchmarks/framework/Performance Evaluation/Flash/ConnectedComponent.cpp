#include "../core/api.h"

int main(int argc, char *argv[]) {
	VertexType(long long,cid);
	SetDataset(argv[1], argv[2]);

	DefineMapV(init) { 
		v.cid = deg(v) * (long long) n_vertex + id(v);
		return v;
	};

	//DefineFE(check) {return s.cid > d.cid;};
	DefineMapE(update) {d.cid = max(d.cid, s.cid); return d;};

	vertexSubset A = vertexMap(All, CTrueV, init);

	for(int len = A.size(), i = 0; len > 0; len = A.size(),++i) {
		print("Round %d: size=%d\n", i, len);
		A = edgeMap(A, EU, CTrueE, update, CTrueV, update);
	}

	double t = GetTime();
	vector<int> cnt(n_vertex,0);
	int nc = 0, lc = 0;
	All.Gather(if (cnt[v.cid%n_vertex] == 0) ++nc; ++cnt[v.cid%n_vertex]; lc = max(lc, cnt[v.cid%n_vertex]));

	print( "num_cc=%d, max_cc=%d\ntotal time=%0.3lf secs\n", nc, lc, t);
	return 0;
}
