#!/bin/bash

# === Argument Check ===
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <ALGORITHM> <PATH_TO_DATASET_FOLDER>"
    exit 1
fi

# === Input Arguments ===
ALGORITHM=$1         # Algorithm name (e.g., bfs, pagerank)
HOST_PATH=$2         # Working directory for running experiments

THREAD_LIST=(1 2 4 8 16 32)
DATASETS=(Standard Density Diameter)
MEMORY=100Gi
MPI_TEMPLATE="ligra-mpijob-template.yaml"

DATASET_NAME=""

mkdir -p output

export CPU=32
export MEMORY=100Gi
export HOST_PATH=$HOST_PATH

export ALGORITHM=$ALGORITHM

# === Single-machine Multi-thread Testing ===
echo "[INFO] ====== SINGLE MACHINE TESTING ======"
for dataset in "${DATASETS[@]}"; do

    if [ "$ALGORITHM" = "BellmanFord" ]; then
        DATASET_NAME="ligra-sssp-adj-8-${dataset}.txt"
    else
        DATASET_NAME="ligra-adj-8-${dataset}.txt"
    fi
    

    for thread in "${THREAD_LIST[@]}"; do
        export DATASET=$DATASET_NAME
        export SLOTS_PER_WORKER=$thread
        export REPLICAS=1
        export MPIRUN_NP=$thread
        export SINGLE_MACHINE=1

        LOG_FILE="output/${ALGORITHM}-${DATASET_NAME}-n1-p${SLOTS_PER_WORKER}.log"

        # Generate and submit MPIJob YAML
        envsubst < "$MPI_TEMPLATE" > ligra-mpijob.yaml
        echo "[INFO] Submitting MPIJob: $ALGORITHM with 1 machines..."
        kubectl apply -f ligra-mpijob.yaml
        kubectl wait --for=condition=Succeeded mpijob/ligra-mpijob --timeout=10m

        kubectl logs job/ligra-mpijob-launcher > "$LOG_FILE"

        # Clean up the job
        kubectl delete -f ligra-mpijob.yaml
    done
done


echo "[INFO] ✅ All experiments completed."
 
