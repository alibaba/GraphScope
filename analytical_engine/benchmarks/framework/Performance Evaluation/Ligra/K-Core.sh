export OPENMP=1
make

scale=8
threads=1

for dataset in Standard Density Diameter; do
    machines=1
    echo "K-Core $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
    ./K-Core -rounds 1 ligra-adj-${scale}-${dataset}.txt \
        2>&1 \
        | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
        > log_${dataset}_${machines}machines_${threads}threads.txt
done