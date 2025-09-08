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
MPI_TEMPLATE="grape-mpijob-template.yaml"

DATASET_NAME=""
ALGORITHM_PARAMETER_=0

mkdir output

export CPU=32
export MEMORY=100Gi
export HOST_PATH=$HOST_PATH
export ALGORITHM=$ALGORITHM

# === Single-machine Multi-thread Testing ===
echo "[INFO] ====== SINGLE MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do

    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="grape-sssp-edges-8-${dataset}"

    else
        DATASET_NAME="grape-edges-8-${dataset}"
    fi

    for thread in "${THREAD_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=$thread
        export REPLICAS=1
        export MPIRUN_NP=$thread
        export SINGLE_MACHINE=1

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log"

        # Generate and submit MPIJob YAML
        
        envsubst < "$MPI_TEMPLATE" > grape-mpijob.yaml
        echo "[INFO] Submitting MPIJob: $ALGORITHM with 1 machines..."
        kubectl apply -f grape-mpijob.yaml
        kubectl wait --for=condition=Succeeded mpijob/grape-mpijob --timeout=10m

        kubectl logs job/grape-mpijob-launcher > "$LOG_FILE"

        # Clean up the job
        kubectl delete -f grape-mpijob.yaml
    done
done

#=== Multi-machine Distributed Testing using Kubeflow MPIJob ===
echo "[INFO] ====== MULTI-MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do
    if [ "$ALGORITHM" = "sssp" ]; then
        DATASET_NAME="grape-sssp-edges-9-${dataset}"
    else
        DATASET_NAME="grape-edges-9-${dataset}"
    fi

    for machines in "${MACHINE_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=32
        export REPLICAS=$machines
        export MPIRUN_NP=$((machines * ${SLOTS_PER_WORKER}))
        export SINGLE_MACHINE=0

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log"

        # Generate and submit MPIJob YAML
        envsubst < "$MPI_TEMPLATE" > grape-mpijob.yaml
        echo "[INFO] Submitting MPIJob: $ALGORITHM with $machines machines..."
        kubectl apply -f grape-mpijob.yaml
        kubectl wait --for=condition=Succeeded mpijob/grape-mpijob --timeout=10m

        kubectl logs job/grape-mpijob-launcher > "$LOG_FILE"

        # Clean up the job
        kubectl delete -f grape-mpijob.yaml
    done
done

echo "[INFO] ✅ All experiments completed."
