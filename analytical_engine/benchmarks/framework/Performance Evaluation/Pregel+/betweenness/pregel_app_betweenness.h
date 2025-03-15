#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t BC(v) ...

struct BcValue
{
	int level=0; // 0: not visited   other: level
	int sigma;
	double delta;
	int trigger;
	map<VertexID, int> prev;
	vector<VertexID> edges;
};

struct MessageType {
	int level; // source level
	int source;
	int sigma;
	double delta;
};

ibinstream & operator<<(ibinstream & m, const BcValue & v){
	m<<v.level;
	m<<v.sigma;
	m<<v.delta;
	m<<v.trigger;
	m<<v.prev;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, BcValue & v){
	m>>v.level;
	m>>v.sigma;
	m>>v.delta;
	m>>v.trigger;
	m>>v.prev;
	m>>v.edges;
	return m;
}

ibinstream & operator<<(ibinstream & m, const MessageType & v){
	m<<v.level;
	m<<v.source;
	m<<v.sigma;
	m<<v.delta;
	return m;
}

obinstream & operator>>(obinstream & m, MessageType & v){
	m>>v.level;
	m>>v.source;
	m>>v.sigma;
	m>>v.delta;
	return m;
}

//====================================

class BcVertex:public Vertex<VertexID, BcValue, MessageType>
{
	public:
		void PushDown() {
			MessageType m;
			m.level = value().level;
			m.source = id;
			m.sigma = value().sigma;
			m.delta = 0; // useless
			value().trigger = value().edges.size() - value().prev.size();
			for (auto & target : value().edges) {
				if (value().prev[target] == 0) {
					// cout << "----------------- id " << id << "  target " << target << "  push down level = " << value().level << endl;
					// cout << id << "  -->> " << target << endl;
					send_message(target, m);
				}
			}
		}

		void PushUp() {
			MessageType m;
			m.level = value().level;
			m.source = id;
			m.sigma = value().sigma;
			m.delta = value().delta;
			for (auto & target : value().edges) {
				if (value().prev[target] == 1) {
					// cout << "----------------- id " << id << "  target " << target << "  push up level = " << value().level << endl;
					// cout << id << "  -->> " << target << endl;
					send_message(target, m);
				}
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if (step_num() == 1) {
				if (id == 0) {
					value().level = 1;
					value().sigma = 1;
					PushDown();
				}
				vote_to_halt();
				return;
			}

			if (value().level == 0) {	// first visited
				for(auto &m : messages) {
					// if (m.level + 1 != value().level && value().level != 0) cout << "1value().level: " << value().level << "    m.level: " << m.level << endl;
					assert(m.level + 1 == value().level || value().level == 0);
					value().level = m.level + 1;
					value().sigma += m.sigma;
					value().prev[m.source] = 1;
				}

				if (value().prev.size() == value().edges.size()) {
					PushUp();	// leaf node
				}	else {
					PushDown();
				}

			}	else {	// receive pushup value
				for(auto &m : messages) {
					value().trigger -= 1;
					if (value().level == m.level) continue;	// ignore same-level message
					// if (value().level + 1 != m.level) { cout << "value().level: " << value().level << "    m.level: " << m.level << endl; }
					assert(value().level + 1 == m.level);
					value().delta += (1.0 * value().sigma / m.sigma) * (1 + m.delta);
				}
				if (value().trigger == 0) {
					PushUp();
				}
			}

			vote_to_halt();
		}

};

//====================================

class BcWorker:public Worker<BcVertex>
{
	char buf[1000];
	public:

		virtual BcVertex* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			BcVertex* v=new BcVertex;
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

		virtual void toline(BcVertex* v, BufferedWriter & writer)
		{
			sprintf(buf, "vertex: %d,  sigma = %d,   delta = %.2f\n", v->id, v->value().sigma, v->value().delta);
			writer.write(buf);
		}
};

void pregel_betweenness(string in_path, string out_path){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	BcWorker worker;
	worker.run(param);
}
