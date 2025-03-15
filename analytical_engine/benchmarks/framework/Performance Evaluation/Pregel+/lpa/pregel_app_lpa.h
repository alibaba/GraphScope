#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t Label(v) ...

struct LpaValue_pregel
{
	VertexID label;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const LpaValue_pregel & v){
	m<<v.label;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, LpaValue_pregel & v){
	m>>v.label;
	m>>v.edges;
	return m;
}

//====================================

class LpaVertex_pregel:public Vertex<VertexID, LpaValue_pregel, VertexID>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				value().label = id;
				// cout << "new vertex " << id << endl;
			}
			else
			{
				map<int, int> labels;
				labels[value().label] = 1;
				VertexID max_label = value().label;
				int max_label_num = 1;
				for(auto &label : messages)
				{
					labels[label] = labels[label] + 1;
					if (labels[label] > max_label_num || labels[label] == max_label_num && label < max_label) {
						max_label_num = labels[label];
						max_label = label;
					}
				}
				value().label = max_label;
			}

			if(step_num()<ROUND)
			{
				for(vector<VertexID>::iterator it=value().edges.begin(); it!=value().edges.end(); it++)
				{
					send_message(*it, value().label);
				}
			}
			else vote_to_halt();
		}

};

//====================================

class LpaWorker_pregel:public Worker<LpaVertex_pregel>
{
	char buf[100];
	public:

		virtual LpaVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			LpaVertex_pregel* v=new LpaVertex_pregel;
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

		virtual void toline(LpaVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d\n", v->id, v->value().label);
			writer.write(buf);
		}
};

void pregel_lpa(string in_path, string out_path){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	LpaWorker_pregel worker;
	worker.run(param);
}
