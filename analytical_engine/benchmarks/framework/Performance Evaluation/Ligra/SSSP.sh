export OPENMP=1
make

scale=8
threads=1

for dataset in Standard Density Diameter; do
    machines=1
    echo "SSSP $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
    ./SSSP -rounds 1 -s ligra-sssp-adj-${scale}-${dataset}.txt \
        2>&1 \
        | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
        > log_${dataset}_${machines}machines_${threads}threads.txt
done