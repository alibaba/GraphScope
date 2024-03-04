export NUM_SERVER_NODES=3
export NUM_CLIENT_NODES=3
export NUM_WORKER_REPLICAS=$(($NUM_CLIENT_NODES-1))
export MASTER_ADDR="10.244.0.150"
envsubst '${NUM_SERVER_NODES},${NUM_CLIENT_NODES},${NUM_WORKER_REPLICAS},${MASTER_ADDR}' < client.yaml | kubectl create -f -
# kubectl exec -it pytorch-simple-master-0 -- "python3 client.py --node_rank 0 --master_addr ${MASTER_ADDR} --num_server_nodes ${NUM_SERVER_NODES} --num_client_nodes ${NUM_CLIENT_NODES}"
# for ((i=1;i<${NUM_WORKER_REPLICAS};i++))
# do
#     kubectl exec -it pytorch-simple-worker-$(($i-1)) -- "python3 client.py --node_rank $i --master_addr ${MASTER_ADDR} --group_master \${GROUP_MASTER} --num_server_nodes ${NUM_SERVER_NODES} --num_client_nodes ${NUM_CLIENT_NODES}"
# done
