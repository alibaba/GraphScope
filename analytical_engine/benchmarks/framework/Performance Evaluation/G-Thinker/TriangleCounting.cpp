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

typedef vector<VertexID> TriangleValue;
typedef Vertex<VertexID, TriangleValue> TriangleVertex;
typedef Subgraph<TriangleVertex> TriangleSubgraph;
typedef Task<TriangleVertex, VertexID> TriangleTask; //VertexID is the largest vertex among v1's neighbors

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
    	if(_my_rank == MASTER_RANK) cout<<"Triangle Count = "<<sum<<endl;
    	collector = sum;
    }
};

class TriangleComper:public Comper<TriangleTask, TriangleAgg>
{
public:
    virtual void task_spawn(VertexT * v)
    {
    	if(v->value.size() < 2) return;
    	//cout<<v->id<<": in task_spawn"<<endl;//@@@@@@@@@@@@@
    	TriangleTask * t = new TriangleTask;
        t->subG.addVertex(*v);
        for(int i=0; i<v->value.size() - 1; i++) //-1 since we do not need to pull the largest vertex
        {
            VertexID nb = v->value[i];
            t->pull(nb);
        }
        t->context = v->value.back();
        add_task(t);
    }

	//input adj-list
	//(1) must be sorted !!! toVertex(v) did it
	//(2) must remove all IDs less than vid !!!
	//trimmer guarantees them
	size_t triangle_count(vector<VertexT *> & frontier, VertexID last)
	{
		size_t count = 0;
		TriangleValue vlist;
		for(int j=0; j<frontier.size(); j++) vlist.push_back(frontier[j]->id);
		vlist.push_back(last);
		//------
		for(int j=0; j<vlist.size() - 1; j++)
		{
			VertexID u = vlist[j]; //u is the next smallest neighbor of v
			int m = j+1; //m is vlist's starting position to check
			TriangleValue & ulist = frontier[j]->value;
			int k = 0; //k is ulist's starting position to check
			while(k<ulist.size() && m<vlist.size())
			{
				if(ulist[k] == vlist[m])
				{
					count++;
					m++;
					k++;
				}
				else if(ulist[k] > vlist[m]) m++;
				else k++;
			}
		}
		return count;
	}

    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
        //run single-threaded mining code
        size_t count = triangle_count(frontier, context);
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
    	if(v->value.size() < 2) return;
    	TriangleTask t;
    	tcollector.push_back(t);
    	TriangleTask & task = tcollector.back();
		task.subG.addVertex(*v);
		for(int i=0; i<v->value.size() - 1; i++) //-1 since we do not need to pull the largest vertex
		{
			VertexID nb = v->value[i];
			task.pull(nb);
		}
		task.context = v->value.back();
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
