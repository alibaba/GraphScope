#include "pregel_app_tc.h"

int main(int argc, char* argv[]){
	string str = argv[1];
	init_workers(); 
	pregel_triangle_counting(str, "/pregel+_data/pregel+-adj-3600000_output5");                 
	worker_finalize();       
	return 0;     
}   