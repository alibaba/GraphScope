#!/bin/bash
. /home/graphscope/.odps_env
cmd="python3 /home/graphscope/work/k8s-test/gs/charts/graphscope-interactive/script/create_graph.py --endpoint http://33.37.43.163:7777"
#cmd="python3 /home/graphscope/work/k8s-test/gs/charts/graphscope-interactive/script/get_service_status.py --endpoint http://33.37.43.163:7777" 
eval $cmd  >> /home/graphscope/daily_graph_import.log 2>&1
