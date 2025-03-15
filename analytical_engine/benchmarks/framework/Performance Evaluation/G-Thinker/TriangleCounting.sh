rm ~/run

make clean
make
cp run ~
cp hosts ~
cat hosts

cd ~
for slave in GraphK8sMaster GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment5 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15;
do
    ssh admin@${slave} "rm ~/run"
    scp hosts ${slave}:~
    scp run ${slave}:~
done


cd ~
for type in "Standard";
do
    scale=9
    dataset=/pregel+_data/pregel+-adj-${scale}-${type}
    threads=32
    for machines in 1 2 4 8 16;
    do
        log=/home/admin/gthinker/G-thinker-master/app_triangle/aaalarge_Scale${scale}_${type}_Machines${machines}_Thread${threads}.txt
        echo ${log}
        echo -e "\n start \n" | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}
        mpiexec -n ${machines} --hostfile hosts ./run ${dataset} ${threads} 2>&1 | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}
    done
done

cd ~
scale=9
type="Density"
dataset=/pregel+_data/pregel+-adj-${scale}-${type}
machines=1
for threads in 32 16 8 4 2 1;
do
    log=/home/admin/gthinker/G-thinker-master/app_triangle/aa_Scale${scale}_${type}_Machines${machines}_Thread${threads}.txt
    echo ${log}
    echo -e "\n start \n" | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}
    mpiexec -n ${machines} --hostfile hosts ./run ${dataset} ${threads} 2>&1 | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}
done




# log=/home/admin/gthinker/G-thinker-master/app_triangle/aaaa2.txt
# echo ${log}
# echo -e "\n start \n" | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}
# mpiexec --mca btl_tcp_if_include bond0 -n 4 --hostfile hosts ./run /pregel+_data/orkut.txt 32 2>&1 | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0 }' >> ${log}

# for slave in GraphK8sMaster GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15;
# do
#     ssh admin@${slave} "echo 'export MPICH_TCP_IFACE=bond0' >> ~/.bashrc"
# done