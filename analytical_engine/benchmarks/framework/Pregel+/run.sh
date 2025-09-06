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
DATASETS=(Standard Density Diameter)
MEMORY=100Gi
MPI_TEMPLATE="pregel-mpijob-template.yaml"


NAMESPACE="hadoop"
JOB_NAME="pregel-mpijob"

DATASET_NAME=""
ALGORITHM_PARAMETER_=0

mkdir -p output

export CPU=32
export MEMORY=10Gi
export HOST_PATH=$HOST_PATH
export ALGORITHM=$ALGORITHM

# === Single-machine Multi-thread Testing ===
echo "[INFO] ====== SINGLE MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do

    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="pregel+-adj-8-${dataset}.txt"
    else
        DATASET_NAME="pregel+-adj-8-${dataset}.txt"
    fi

    for thread in "${THREAD_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=$thread
        export REPLICAS=1
        export MPIRUN_NP=$thread
        export ALGORITHM_PARAMETER=$ALGORITHM_PARAMETER_
        export SINGLE_MACHINE=1

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${REPLICAS}-p${SLOTS_PER_WORKER}.log"

        envsubst  '${DATASET} ${ALGORITHM} ${MPIRUN_NP} ${CPU} ${MEMORY} ${HOST_PATH} ${SLOTS_PER_WORKER} ${REPLICAS}' < pregel-mpijob-template.yaml > pregel-mpijob.yaml

        echo "[INFO] Submitting MPIJob: $ALGORITHM with 1 machines..."
        kubectl -n "$NAMESPACE" apply -f pregel-mpijob.yaml

        kubectl -n "$NAMESPACE" wait --for=condition=Succeeded mpijob/${JOB_NAME} --timeout=10m

        kubectl -n "$NAMESPACE" logs job/${JOB_NAME}-launcher > "$LOG_FILE"

        kubectl -n "$NAMESPACE" delete -f pregel-mpijob.yaml
    done
done

# === Multi-machine Distributed Testing using Kubeflow MPIJob ===
echo "[INFO] ====== MULTI-MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do
    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="pregel+-adj-9-${dataset}.txt"
    else
        DATASET_NAME="pregel+-adj-9-${dataset}.txt"
    fi

    for machines in "${MACHINE_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=32
        export REPLICAS=$machines
        export MPIRUN_NP=$((machines * 32))
        export ALGORITHM_PARAMETER=$ALGORITHM_PARAMETER_
        export SINGLE_MACHINE=0

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log"

        envsubst  '${DATASET} ${ALGORITHM} ${MPIRUN_NP} ${CPU} ${MEMORY} ${HOST_PATH} ${SLOTS_PER_WORKER} ${REPLICAS}' < pregel-mpijob-template.yaml > pregel-mpijob.yaml

        echo "[INFO] Submitting MPIJob: $ALGORITHM with $machines machines..."
        kubectl -n "$NAMESPACE" apply -f pregel-mpijob.yaml

        kubectl -n "$NAMESPACE" wait --for=condition=Succeeded mpijob/${JOB_NAME} --timeout=10m

        kubectl -n "$NAMESPACE" logs job/${JOB_NAME}-launcher > "$LOG_FILE"

        kubectl -n "$NAMESPACE" delete -f pregel-mpijob.yaml
    done
done

echo "[INFO] ✅ All experiments completed."
