//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

#include "subg-dev.h"

//note for changing to triangle enumeration
//for triangle v1-v2-v3 with v1<v2<v3, we can maintain v1 in task.context if we output triangles

const int K = 5;

typedef vector<VertexID> TriangleValue;
typedef Vertex<VertexID, TriangleValue> TriangleVertex;
typedef Subgraph<TriangleVertex> TriangleSubgraph;
typedef Task<TriangleVertex> TriangleTask;
typedef unordered_set<VertexID> VSet;

class TriangleTrimmer:public Trimmer<TriangleVertex>
{
    virtual void trim(TriangleVertex & v) {
    	TriangleValue & val = v.value;
    	TriangleValue newval;
        for (int i = 0; i < val.size(); i++) {
            if (v.id < val[i])
            	newval.push_back(val[i]);
        }
        val.swap(newval);
        sort(val.begin(), val.end());
    }
};

class TriangleAgg:public Aggregator<size_t, size_t, size_t>  //all args are counts
{
private:
	size_t count;
	size_t sum;

public:

    virtual void init()
    {
    	sum = count = 0;
    }

    virtual void init_udf(size_t & prev) {
    	sum = 0;
    }

    virtual void aggregate_udf(size_t & task_count)
    {
    	count += task_count;
    }

    virtual void stepFinal_udf(size_t & partial_count)
    {
    	sum += partial_count; //add all other machines' counts (not master's)
    }

    virtual void finishPartial_udf(size_t & collector)
    {
    	collector = count;
    }

    virtual void finishFinal_udf(size_t & collector)
    {
    	sum += count; //add master itself's count
    	if(_my_rank == MASTER_RANK) cout<<"K-Clique Count = "<<sum<<endl;
    	collector = sum;
    }
};

class TriangleComper:public Comper<TriangleTask, TriangleAgg>
{
public:
    virtual void task_spawn(VertexT * v)
    {
    	if(v->value.size() < K - 1) return;
    	// cout<<v->id<<": in task_spawn"<<endl;//@@@@@@@@@@@@@
    	TriangleTask * t = new TriangleTask;
        for(int i=0; i<v->value.size(); i++)
        {
            VertexID nb = v->value[i];
            t->pull(nb);
        }
        add_task(t);
    }

	size_t KCliqueCounting(SubgraphT & g, VSet & cand, int lev) {
        if (lev == K - 1) {
            return cand.size();
        }
        int t = 0;
        for (auto &u : cand) {
            VSet next_cand;
            TriangleValue & u_neighbor = g.getVertex(u)->value;
            for (int j = 0;j < u_neighbor.size(); j++) {
                if (cand.find(u_neighbor[j]) != cand.end()) {
                    next_cand.insert(u_neighbor[j]);
                }
            }
            if (next_cand.size() >= K - lev - 1) {
                t = t + KCliqueCounting(g, next_cand, lev + 1);
            }
        }
        return t;
    }

    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
        VSet cand;
        for(int i = 0; i < frontier.size(); i++) {
            TriangleVertex v;
            v.id = frontier[i]->id;
            g.addVertex(v);
            cand.insert(v.id);
        }
        for(int i = 0; i < frontier.size(); i++) {
        	TriangleVertex *v = g.getVertex(frontier[i]->id);
        	TriangleValue &fval = frontier[i]->value;
            for (int j = 0; j < fval.size(); j++) {
                TriangleVertex *v1 = g.getVertex(fval[j]);
                if (v1 != NULL) {
                    v->value.push_back(fval[j]);
                    // v1->value.push_back(v->id);
                }
            }
        }
        // now g is a reduced neighboring subgraph
        //@@@@@@ report graph g @@@@@@
        // cout<<"********** g *******   ";
        // for(int i=0; i<g.vertexes.size(); i++)
        // {
        // 	VertexT & v = g.vertexes[i];
        // 	cout<<"v"<<v.id<<": ";
        // 	for(int j=0; j<v.value.size(); j++) cout<<v.value[j]<<" ";
        // 	cout<<"       ";
        // }
        // cout << endl;
        //@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		//------
        size_t count = KCliqueCounting(g, cand, 1);
        TriangleAgg* agg = get_aggregator();
        agg->aggregate(count);
        //cout<<rootID<<": done"<<endl;//@@@@@@@@@@@@@
        return false;
    }
};

class TriangleWorker:public Worker<TriangleComper>
{
public:
	TriangleWorker(int num_compers) : Worker<TriangleComper>(num_compers){}

    virtual VertexT* toVertex(char* line)
    {
        VertexT* v = new VertexT;
        char * pch;
        pch=strtok(line, " \t");
        v->id=atoi(pch);
        strtok(NULL," \t");
        TriangleValue & val = v->value;
        while((pch=strtok(NULL, " ")) != NULL)
        {
            val.push_back(atoi(pch));
        }
        return v;
    }

    virtual void task_spawn(VertexT * v, vector<TriangleTask> & tcollector)
	{
    	if(v->value.size() < K - 1) return;
    	TriangleTask t;
    	tcollector.push_back(t);
    	TriangleTask & task = tcollector.back();
		for(int i=0; i<v->value.size(); i++)
		{
			VertexID nb = v->value[i];
			task.pull(nb);
		}
	}
};

int main(int argc, char* argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    param.input_path = argv[1];  //input path in HDFS
    int thread_num = atoi(argv[2]);  //number of threads per process
    param.force_write=true;
    param.native_dispatcher=false;
    //------
    TriangleTrimmer trimmer;
    TriangleAgg aggregator;
    TriangleWorker worker(thread_num);
    worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    worker.run(param);
    worker_finalize();
    return 0;
}
