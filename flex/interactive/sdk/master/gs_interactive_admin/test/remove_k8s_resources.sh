
# Expect 1 args
if [ $# -ne 1 ]; then
    echo "Usage: $0 <graph_id>"
    exit 1
fi
alias ktl="kubectl -n default"
graph_id=$1

ktl delete svc default-graph-${graph_id}-engine-headless 
ktl delete sts default-graph-${graph_id}-engine
ktl delete ConfigMap default-graph-${graph_id}-config
ktl delete pvc interactive-workspace-default-graph-${graph_id}-engine-0