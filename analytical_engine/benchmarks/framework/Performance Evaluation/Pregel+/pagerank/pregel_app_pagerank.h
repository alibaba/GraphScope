#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t PageRank(v) ...

//an aggregator collects PageRank(v) for all dangling vertices, which is then redistributed to all vertices in the next superstep
//equivalent to adding edges from a dangling vertex to all vertices in the graph

struct PRValue_pregel
{
	double pr;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const PRValue_pregel & v){
	m<<v.pr;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, PRValue_pregel & v){
	m>>v.pr;
	m>>v.edges;
	return m;
}

//====================================

class PRVertex_pregel:public Vertex<VertexID, PRValue_pregel, double>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				value().pr=1.0/get_vnum();
			}
			else
			{
				double sum=0;
				for(MessageIter it=messages.begin(); it!=messages.end(); it++)
				{
					sum+=*it;
				}
				double* agg=(double*)getAgg();
				double residual=*agg/get_vnum();
				value().pr=0.15/get_vnum()+0.85*(sum+residual);
			}
			if(step_num()<ROUND)
			{
				double msg=value().pr/value().edges.size();
				for(vector<VertexID>::iterator it=value().edges.begin(); it!=value().edges.end(); it++)
				{
					send_message(*it, msg);
				}
			}
			else vote_to_halt();
		}

};

//====================================

class PRAgg_pregel:public Aggregator<PRVertex_pregel, double, double>
{
	private:
		double sum;
	public:
		virtual void init(){
			sum=0;
		}

		virtual void stepPartial(PRVertex_pregel* v)
		{
			if(v->value().edges.size()==0) sum+=v->value().pr;
		}

		virtual void stepFinal(double* part)
		{
			sum+=*part;
		}

		virtual double* finishPartial(){ return &sum; }
		virtual double* finishFinal(){ return &sum; }
};

class PRWorker_pregel:public Worker<PRVertex_pregel, PRAgg_pregel>
{
	char buf[100];
	public:

		virtual PRVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			PRVertex_pregel* v=new PRVertex_pregel;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(PRVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
			writer.write(buf);
		}
};

class PRCombiner_pregel:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			old+=new_msg;
		}
};

void pregel_pagerank(string in_path, string out_path, bool use_combiner){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	PRWorker_pregel worker;
	PRCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	PRAgg_pregel agg;
	worker.setAggregator(&agg);
	worker.run(param);
}

void pregel_pagerank_report(string in_path, string out_path, string report_path, bool use_combiner){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	PRWorker_pregel worker;
	PRCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	PRAgg_pregel agg;
	worker.setAggregator(&agg);
	worker.run_report(param, report_path);
}
