#!/bin/bash
. /home/graphscope/.odps_env
cmd="python3 /home/graphscope/work/k8s-test/gs/charts/graphscope-interactive/script/create_graph.py --endpoint http://33.37.43.163:7777"
#cmd="python3 /home/graphscope/work/k8s-test/gs/charts/graphscope-interactive/script/get_service_status.py --endpoint http://33.37.43.163:7777" 
echo "date is $(date)" >> /home/graphscope/daily_graph_import.log
echo "--------------------------------" >> /home/graphscope/daily_graph_import.log
eval $cmd  >> /home/graphscope/daily_graph_import.log 2>&1
