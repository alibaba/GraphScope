#include "../core/api.h"

int main(int argc, char *argv[]) {
	VertexType(int,deg, int,id, vector<int>,out, int,count, ONE+TWO+THREE);
	SetDataset(argv[1], argv[2]);

	DefineMapV(init) {
		v.id = id(v); v.deg = deg(v); v.count = 0; v.out.clear();
		return v;
	};

	DefineFE(check) {return (s.deg > d.deg) || (s.deg == d.deg && s.id > d.id);};
	DefineMapE(update) {d.out.push_back(s.id); return d;};

	vector<int> res(n_vertex);
	DefineMapE(update2) {
		d.count += set_intersect(s.out, d.out, res);
	};
	
	
	vertexMap(All, CTrueV, init);
	edgeMapDense(All, EU, check, update, CTrueV);
	edgeMapDense(All, EU, check, update2, CTrueV, false);


    long long cnt = 0, cnt_all = 0; double t = GetTime();

	DefineMapV(count) { cnt += v.count;};
	vertexMap(All, CTrueV, count);
    cnt_all = Sum(cnt);

    print( "number of triangles=%lld\ntotal time=%0.3lf secs\n", cnt_all, t);
	return 0;
}
