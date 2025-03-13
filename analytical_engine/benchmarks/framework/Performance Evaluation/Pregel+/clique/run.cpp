#include "pregel_app_kclique.h"

int main(int argc, char* argv[]){
	string str = argv[1];
	init_workers(); 
	pregel_triangle_counting(str, "/pregel+_data/toy_output");                  
	worker_finalize();        
	return 0;     
}   