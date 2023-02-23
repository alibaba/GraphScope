#!/bin/bash

WORKER_ID=$1

cmd="./target/release/single_laser /home/admin/nas/neng/sf3000/initial_snapshot/ /home/admin/nas/luoxiaojian/csr_3k_p8 ./schema.json -p 8 -i $WORKER_ID"
echo $cmd
eval $cmd
