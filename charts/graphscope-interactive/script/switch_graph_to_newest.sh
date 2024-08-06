#!/bin/bash
cmd="python3 /home/graphscope/work/k8s-test/gs/charts/graphscope-interactive/script/switch_graph.py --endpoint http://33.37.43.163:7777"
echo "date is $(date)" >> /home/graphscope/switch_graph.log
echo "--------------------------------" >> /home/graphscope/switch_graph.log
eval $cmd  >> /home/graphscope/switch_graph.log 2>&1
