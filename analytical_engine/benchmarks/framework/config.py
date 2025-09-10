from jinja2 import Template

# 平台参数配置
PLATFORM_CONFIG = {
    "flash": {
        "image": "flash-mpi:v0.4",
        "volume_name": "flash-data",
        "volume_mount": "/opt/data",
        "use_hadoop": False,
    },
    "ligra": {
        "image": "ligra-mpi:v0.1",
        "volume_name": "ligra-data",
        "volume_mount": "/opt/data",
        "use_hadoop": False,
    },
    "grape": {
        "image": "grape-mpi:v0.1",
        "volume_name": "grape-data",
        "volume_mount": "/opt/data",
        "use_hadoop": False,
    },
    "pregel+": {
        "image": "pregel-mpi:v0.1",
        "volume_name": "pregeldata",
        "volume_mount": "/opt/data",
        "use_hadoop": True,
    },
    "gthinker": {
        "image": "gthinker-mpi:v0.1",
        "volume_name": "gthinkerdata",
        "volume_mount": "/opt/data",
        "use_hadoop": True,
    },
    "powergraph": {
        "image": "graphlab-mpi:v0.1",
        "volume_name": "graphlabdata",
        "volume_mount": "/opt/data",
        "use_hadoop": True,
    },
}

# 不带hadoop的模板
TEMPLATE_NO_HADOOP = """
apiVersion: kubeflow.org/v2beta1
kind: MPIJob
metadata:
  name: {{ job_name }}
spec:
  slotsPerWorker: {{ SLOTS_PER_WORKER }}
  runPolicy:
    cleanPodPolicy: None
    ttlSecondsAfterFinished: 60
  sshAuthMountPath: /home/mpiuser/.ssh
  mpiReplicaSpecs:
    Launcher:
      replicas: 1
      template:
        spec:
          volumes:
          - name: {{ volume_name }}
            hostPath:
              path: {{ HOST_PATH }}
              type: Directory
          containers:
          - image: {{ image }}
            name: mpi-launcher
            securityContext:
              runAsUser: 1000
            volumeMounts:
            - name: {{ volume_name }}
              mountPath: {{ volume_mount }}
            command: {{ command }}
            args: {{ args }}
            resources:
              limits:
                cpu: {{ CPU }}
                memory: {{ MEMORY }}
    Worker:
      replicas: {{ REPLICAS }}
      template:
        spec:
          volumes:
          - name: {{ volume_name }}
            hostPath:
              path: {{ HOST_PATH }}
              type: Directory
          containers:
          - image: {{ image }}
            name: mpi-worker
            securityContext:
              runAsUser: 1000
            volumeMounts:
            - name: {{ volume_name }}
              mountPath: {{ volume_mount }}
            command:
            - /usr/sbin/sshd
            args:
            - -De
            - -f
            - /home/mpiuser/.sshd_config
            resources:
              limits:
                cpu: {{ CPU }}
                memory: {{ MEMORY }}
"""

# 带hadoop的模板
TEMPLATE_HADOOP = """
apiVersion: kubeflow.org/v2beta1
kind: MPIJob
metadata:
  name: {{ job_name }}
spec:
  slotsPerWorker: {{ SLOTS_PER_WORKER }}
  runPolicy:
    cleanPodPolicy: None
    ttlSecondsAfterFinished: 120
  sshAuthMountPath: /home/mpiuser/.ssh
  mpiReplicaSpecs:
    Launcher:
      replicas: 1
      template:
        spec:
          restartPolicy: OnFailure
          volumes:
            - name: hadoop-config
              configMap:
                name: my-hadoop-cluster-hadoop
            - name: scratch
              emptyDir: {}
            - name: {{ volume_name }}
              hostPath:
                path: {{ HOST_PATH }}
                type: Directory
            - name: shared-mpi-data
              emptyDir: {}
          containers:
            - name: mpi-launcher
              image: {{ image }}
              imagePullPolicy: IfNotPresent
              securityContext:
                runAsUser: 1000
              env:
                - name: HADOOP_CONF_DIR
                  value: "/etc/hadoop"
                - name: HDFS_NN_SERVICE
                  value: my-hadoop-cluster-hadoop-hdfs-nn
                - name: HDFS_NN_PORT
                  value: "9000"
                - name: CLEAN_OUTPUT
                  value: "1"
                - name: JAVA_HOME
                  value: /usr/lib/jvm/java-17-openjdk-amd64
                - name: PATH
                  value: "$(JAVA_HOME)/bin:/usr/local/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
              volumeMounts:
                - name: hadoop-config
                  mountPath: /etc/hadoop
                - name: scratch
                  mountPath: /opt/scratch
                - name: {{ volume_name }}
                  mountPath: {{ volume_mount }}
                - name: shared-mpi-data
                  mountPath: /mnt/mpi
              command: {{ command }}
              args: {{ args }}
              resources:
                limits:
                  cpu: {{ CPU }}
                  memory: {{ MEMORY }}
                requests:
                  cpu: {{ CPU }}
                  memory: {{ MEMORY }}
    Worker:
      replicas: {{ REPLICAS }}
      template:
        spec:
          restartPolicy: OnFailure
          volumes:
            - name: hadoop-config
              configMap:
                name: my-hadoop-cluster-hadoop
            - name: scratch
              emptyDir: {}
            - name: {{ volume_name }}
              hostPath:
                path: {{ HOST_PATH }}
                type: Directory
            - name: shared-mpi-data
              emptyDir: {}
          containers:
            - name: mpi-worker
              image: {{ image }}
              imagePullPolicy: IfNotPresent
              securityContext:
                runAsUser: 1000
              env:
                - name: JAVA_HOME
                  value: /usr/lib/jvm/java-17-openjdk-amd64
                - name: PATH
                  value: "$(JAVA_HOME)/bin:/usr/local/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                - name: HADOOP_CONF_DIR
                  value: "/etc/hadoop"
                - name: HDFS_NN_SERVICE
                  value: my-hadoop-cluster-hadoop-hdfs-nn
                - name: HDFS_NN_PORT
                  value: "9000"
              volumeMounts:
                - name: hadoop-config
                  mountPath: /etc/hadoop
                - name: scratch
                  mountPath: /opt/scratch
                - name: {{ volume_name }}
                  mountPath: {{ volume_mount }}
                - name: shared-mpi-data
                  mountPath: /mnt/mpi
              command: ["/usr/sbin/sshd"]
              args: ["-De","-f","/home/mpiuser/.sshd_config"]
              resources:
                limits:
                  cpu: {{ CPU }}
                  memory: {{ MEMORY }}
                requests:
                  cpu: {{ CPU }}
                  memory: {{ MEMORY }}
"""

def get_command_args(platform, params):
    """
    根据平台和参数生成 command 和 args。
    返回 (command, args) 两个字符串，直接用于 YAML 渲染。
    """
    if platform == "flash":
        command = '["/bin/bash", "-c"]'
        args = f'''
              - |
                set -e
                echo "[INFO] Waiting for DNS sync..."; sleep 5;

                if [ {params["SINGLE_MACHINE"]} = 1 ]; then
                  echo "Running in single-machine mode..."
                  mpirun -n {params["MPIRUN_NP"]} ./flash format /opt/data/{params["DATASET"]}/ /opt/scratch/gfs/ {params["DATASET"]}
                  echo "Running {params["ALGORITHM"]} in single-machine mode..."
                  mpirun -n {params["MPIRUN_NP"]} ./{params["ALGORITHM"]} /opt/scratch/gfs/ {params["DATASET"]} {params["ALGORITHM_PARAMETER"]}

                else
                  echo "Running in multi-machine mode..."
                  mpirun -n {params["MPIRUN_NP"]} -hostfile /etc/mpi/hostfile ./flash format /opt/data/{params["DATASET"]}/ /opt/scratch/gfs/ {params["DATASET"]}
                  mpirun -n {params["MPIRUN_NP"]} -hostfile /etc/mpi/hostfile ./{params["ALGORITHM"]} /opt/scratch/gfs/ {params["DATASET"]} {params["ALGORITHM_PARAMETER"]}
                fi
'''
        return command, args

    elif platform == "ligra":
        command = '["/bin/bash", "-c"]'
        args = f'''
                set -e
                echo "[INFO] Waiting for DNS sync..."; sleep 5;
                time ./{params["ALGORITHM"]}{params["MPIRUN_NP"]} -rounds 1 /opt/data/{params["DATASET"]}
                
'''
        return command, args

    elif platform == "grape":
        command = '["/bin/bash", "-c"]'
        args = f'''
              - |
                set -e
                echo "[INFO] Waiting for DNS sync..."; sleep 10;

                if [ {params["SINGLE_MACHINE"]} = 1 ]; then
                  echo "Running in single-machine mode..."
                  time mpirun -n 1 ./run_app --app_concurrency {params["MPIRUN_NP"]} --vfile /opt/data/{params["DATASET"]}.v --efile /opt/data/{params["DATASET"]}.e --application {params["ALGORITHM"]} --bfs_source 0 --sssp_source 0 --pr_d 0.85 --pr_mr 10 --cdlp_mr 10 --opt
                else
                  echo "Running in multi-machine mode..."
                  time mpirun -n {params["REPLICAS"]} --hostfile /etc/mpi/hostfile ./run_app --app_concurrency 32 --vfile /opt/data/{params["DATASET"]}.v --efile /opt/data/{params["DATASET"]}.e --application {params["ALGORITHM"]} --bfs_source 0 --sssp_source 0 --pr_d 0.85 --pr_mr 10 --cdlp_mr 10 --opt
                fi
'''
        return command, args

    elif platform == "pregel+":
        command = '["/bin/bash"]'
        args = f'''
                - "-c"
                - |

                    echo "[INFO] Waiting 10s for network/DNS to fully ready..."
                    sleep 10
                    echo "[INFO] Preparing HDFS data for dataset: {params["DATASET"]}"
                    DATASET_HDFS_PATH="/user/mpiuser/{params["DATASET"]}"

                    if hdfs dfs -test -e "${{DATASET_HDFS_PATH}}"; then
                       echo "[INFO] Dataset already exists on HDFS."
                    else
                        
                        echo "[INFO] Dataset not found on HDFS, uploading..."
                        HADOOP_USER_NAME=root hdfs dfs -mkdir -p /user/mpiuser
                        HADOOP_USER_NAME=root hdfs dfs -chown mpiuser:supergroup /user/mpiuser
                        HADOOP_USER_NAME=root hdfs dfs -chmod 775 /user/mpiuser
                        hdfs dfs -put "/opt/data/{params["DATASET"]}" "${{DATASET_HDFS_PATH}}"

                    fi
                    echo "[INFO] HDFS data is ready."

                    hdfs dfs -ls /user/mpiuser/

                    cat /etc/hadoop/core-site.xml

                    HADOOP_CP=$(cat /etc/hadoop_classpath)
                    
                    time mpiexec \\
                      -np {params["MPIRUN_NP"]} \\
                      -hostfile /etc/mpi/hostfile \\
                      -verbose \\
                      -x CLASSPATH="$(cat /etc/hadoop_classpath)" \\
                      -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \
                      -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                      -x HADOOP_CONF_DIR="/etc/hadoop" \\
                      /opt/pregel+/{params["ALGORITHM"]}/run "${{DATASET_HDFS_PATH}}"

'''
        return command, args


    elif platform == "gthinker":
        command = '["/bin/bash"]'
        args = f'''
                - "-c"
                - |
                    echo "[INFO] Waiting 10s for network/DNS to fully ready..."
                    sleep 10

                    echo "[INFO] Preparing HDFS data for dataset: {params["DATASET"]}"
                    DATASET_HDFS_PATH="/user/mpiuser/{params["DATASET"]}"

                    if hdfs dfs -test -e "${{DATASET_HDFS_PATH}}"; then
                       echo "[INFO] Dataset already exists on HDFS."
                    else
                        
                        echo "[INFO] Dataset not found on HDFS, uploading..."
                        HADOOP_USER_NAME=root hdfs dfs -mkdir -p /user/mpiuser
                        HADOOP_USER_NAME=root hdfs dfs -chown mpiuser:supergroup /user/mpiuser
                        HADOOP_USER_NAME=root hdfs dfs -chmod 775 /user/mpiuser
                        hdfs dfs -put "/opt/data/{params["DATASET"]}" "${{DATASET_HDFS_PATH}}"

                    fi
                    echo "[INFO] HDFS data is ready."

                    hdfs dfs -ls /user/mpiuser/

                    cat /etc/hadoop/core-site.xml

                    HADOOP_CP=$(cat /etc/hadoop_classpath)

                    CONVERTED_HOSTFILE="/mnt/mpi/hostfile" 
                    sed 's/ slots=/:/' /etc/mpi/hostfile > "${{CONVERTED_HOSTFILE}}"

                    time mpiexec \\
                      -np {params["REPLICAS"]} \\
                      --hostfile "${{CONVERTED_HOSTFILE}}" \\
                      -verbose \\
                      -env CLASSPATH "${{HADOOP_CP}}" \\
                      -env LD_LIBRARY_PATH "/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                      -env JAVA_HOME "/usr/lib/jvm/java-17-openjdk-amd64" \\
                      -env HADOOP_CONF_DIR "/etc/hadoop" \\
                      /opt/gthinker/{params["ALGORITHM"]}/run "${{DATASET_HDFS_PATH}}" {params["SLOTS_PER_WORKER"]}
   '''
        return command, args

    elif platform == f"powergraph":
        command = '["/bin/bash"]'
        args = f'''
                - "-c"
                - |
                

                    echo "[INFO] Waiting 10s for network/DNS to fully ready..."
                    sleep 10

                    HADOOP_CP=$(cat /etc/hadoop_classpath)

                    CONVERTED_HOSTFILE="/mnt/mpi/hostfile" 
                    sed 's/ slots=/:/' /etc/mpi/hostfile > "${{CONVERTED_HOSTFILE}}"

                    if [ "{params["ALGORITHM"]}" == "pagerank" ]; then
                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/{params["ALGORITHM"]} --iterations=10 --format=snap \\
                        --graph /opt/data/{params["DATASET"]}

                    elif [ "{params["ALGORITHM"]}" == "sssp" ]; then
                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/{params["ALGORITHM"]} --source=0 --format=snap \\
                        --graph /opt/data/{params["DATASET"]}

                    elif [ "{params["ALGORITHM"]}" == "triangle_count" ]; then

                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/undirected_triangle_count --format=snap \\
                        --graph /opt/data/{params["DATASET"]}


                    elif [ "{params["ALGORITHM"]}" == "lpa" ]; then

                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/label_propagation --format=snap \\
                        --graph /opt/data/{params["DATASET"]}


                    elif [ "{params["ALGORITHM"]}" == "kcore" ]; then

                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/kcore --format=snap --kmin 1 --kmax 3600000 \\
                        --graph /opt/data/{params["DATASET"]}


                    elif [ "{params["ALGORITHM"]}" == "connected_component" ]; then

                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/connected_component --format=snap \\
                        --graph /opt/data/{params["DATASET"]}

                    elif [ "{params["ALGORITHM"]}" == "bc" ]; then

                      time mpiexec \\
                        -n {params["REPLICAS"]} \\
                        --hostfile /etc/mpi/hostfile \\
                        -verbose \\
                        -x CLASSPATH="$HADOOP_CP" \\
                        -x LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk-amd64/lib/server:/usr/local/hadoop/lib/native:/usr/local/bin:/usr/local/lib:/opt/mpich/lib/:/usr/local/openmpi/bin:/usr/local/openmpi/lib" \\
                        -x JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \\
                        -x HADOOP_CONF_DIR="/etc/hadoop" \\
                        /opt/graphlab/release/toolkits/graph_analytics/betweeness --format=snap \\
                        --graph /opt/data/{params["DATASET"]}


                    else
                      echo "[ERROR] Unknown algorithm: {params["ALGORITHM"]}"
                    fi

        '''
        return command, args

    else:
        raise ValueError(f"Unknown platform: {platform}")

def render_mpijob_yaml(platform, params):
    
    plat = PLATFORM_CONFIG[platform]
    merged = {**plat, **params}
    command, args = get_command_args(platform, params)
    merged["command"] = command
    merged["args"] = args
    template = TEMPLATE_HADOOP if plat["use_hadoop"] else TEMPLATE_NO_HADOOP
    return Template(template).render(**merged)
