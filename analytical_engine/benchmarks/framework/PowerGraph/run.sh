#!/bin/bash

# === Argument Check ===
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <ALGORITHM> <HOST_PATH>"
    exit 1
fi

# === Input Arguments ===
ALGORITHM=$1         # Algorithm name (e.g., bfs, pagerank)
HOST_PATH=$2         # Working directory for running experiments

THREAD_LIST=(1 2 4 8 16 32)
MACHINE_LIST=(2 4 8 16)
DATASETS=(Standard Diameter Density)
MEMORY=10Gi
MPI_TEMPLATE="graphlab-mpijob-template.yaml"

# 新增：命名空间 & 实际 MPIJob 名称（模板里写的是这个名字）
NAMESPACE="hadoop"
JOB_NAME="graphlab-mpijob"

DATASET_NAME=""
ALGORITHM_PARAMETER_=0

# 修改：避免已存在报错
mkdir -p output

export CPU=32
export MEMORY=100Gi
export HOST_PATH=$HOST_PATH
export ALGORITHM=$ALGORITHM

if [ "$ALGORITHM" = "k-core-search" ]; then
    ALGORITHM_PARAMETER_=3
elif [ "$ALGORITHM" = "clique" ]; then
    ALGORITHM_PARAMETER_=5
else
    ALGORITHM_PARAMETER_=0
fi

# === Single-machine Multi-thread Testing ===
echo "[INFO] ====== SINGLE MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do

    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="graphlab-adj-8-${dataset}.txt"
    else
        DATASET_NAME="graphlab-adj-8-${dataset}.txt"
    fi

    for thread in "${THREAD_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=$thread
        export REPLICAS=1
        export MPIRUN_NP=$thread
        export ALGORITHM_PARAMETER=$ALGORITHM_PARAMETER_
        export SINGLE_MACHINE=1

        # 修改：machines 未定义，用 REPLICAS 代替
        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${REPLICAS}-p${SLOTS_PER_WORKER}.log"

        # 生成与提交
        envsubst  '${DATASET} ${ALGORITHM} ${MPIRUN_NP} ${CPU} ${MEMORY} ${HOST_PATH} ${SLOTS_PER_WORKER} ${REPLICAS}' < graphlab-mpijob-template.yaml > graphlab-mpijob.yaml
     
        echo "[INFO] Submitting MPIJob: $ALGORITHM with 1 machines..."
        kubectl -n "$NAMESPACE" apply -f graphlab-mpijob.yaml

        # 修改：等待的对象名与模板一致
        kubectl -n "$NAMESPACE" wait --for=condition=Succeeded mpijob/${JOB_NAME} --timeout=10m

        # 修改：launcher Job 名也要与 MPIJob 名一致
        kubectl -n "$NAMESPACE" logs job/${JOB_NAME}-launcher > "$LOG_FILE"

        # 清理
        kubectl -n "$NAMESPACE" delete -f graphlab-mpijob.yaml
    done
done

# === Multi-machine Distributed Testing using Kubeflow MPIJob ===
echo "[INFO] ====== MULTI-MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do
    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="graphlab-adj-9-${dataset}.txt"
    else
        DATASET_NAME="graphlab-adj-9-${dataset}.txt"
    fi

    for machines in "${MACHINE_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=32
        export REPLICAS=$machines
        export MPIRUN_NP=$((machines * 32))
        export ALGORITHM_PARAMETER=$ALGORITHM_PARAMETER_
        export SINGLE_MACHINE=0

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log"

        # 生成与提交
        envsubst  '${DATASET} ${ALGORITHM} ${MPIRUN_NP} ${CPU} ${MEMORY} ${HOST_PATH} ${SLOTS_PER_WORKER} ${REPLICAS}' < graphlab-mpijob-template.yaml > graphlab-mpijob.yaml
     
        echo "[INFO] Submitting MPIJob: $ALGORITHM with $machines machines..."
        kubectl -n "$NAMESPACE" apply -f graphlab-mpijob.yaml

        # 修改：等待的对象名与模板一致
        kubectl -n "$NAMESPACE" wait --for=condition=Succeeded mpijob/${JOB_NAME} --timeout=10m

        # 修改：launcher Job 名也要与 MPIJob 名一致
        kubectl -n "$NAMESPACE" logs job/${JOB_NAME}-launcher > "$LOG_FILE"

        # 清理
        kubectl -n "$NAMESPACE" delete -f graphlab-mpijob.yaml
    done
done

echo "[INFO] ✅ All experiments completed."
