/*
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.cluster;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Properties;

/**
 * resource definition, graph configuration for instance
 *
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-05-08 上午9:57
 **/

public class InstanceConfig extends MaxGraphConfiguration {

    public final static String NAME_REGEX = "^\\w{1,128}$";
    public final static String CLOUD_NAME_REGEX = "^[a-zA-Z]\\w{3,63}$";

    public static final int SNAPSHOT_SPLIT_SIZE = 20000;

    public static final String CLUSTER_ID = "cluster.id";

    private static final String EXECUTOR = "executor";
    private static final String COORDINATOR = "coordinator";
    private static final String IDSERVICE = "idservice";
    private static final String FRONTEND = "frontend";
    private static final String INGESTNODE = "ingestnode";
    private static final String AM = "am";


    private static final String RESOURCE_COUNT = "resource.%s.count";
    private static final String RESOURCE_MEM = "resource.%s.heapmem.mb";
    private static final String RESOURCE_CPU = "resource.%s.cpu.cores";
    private static final String RESOURCE_DISK = "resource.%s.disk.mb";

    /**
     * pegasus config per query
     */
    public static final String PEGASUS_WORKER_NUM = "pegasus.worker.num";
    public static final String PEGASUS_TIMEOUT = "pegasus.timeout";
    public static final String PEGASUS_BATCH_SIZE = "pegasus.batch.size";
    public static final String PEGASUS_OUTPUT_CAPACITY = "pegasus.output.capacity";
    public static final String PEGASUS_MEMORY_LIMIT = "pegasus.memory.limit";

    /**
     * machine count
     */
    public static final String RESOURCE_EXECUTOR_COUNT = String.format(RESOURCE_COUNT, EXECUTOR);
    public static final String RESOURCE_COORDINATOR_COUNT = String.format(RESOURCE_COUNT, COORDINATOR);
    public static final String RESOURCE_IDSERVICE_COUNT = String.format(RESOURCE_COUNT, IDSERVICE);
    public static final String RESOURCE_FRONTEND_COUNT = String.format(RESOURCE_COUNT, FRONTEND);
    public static final String RESOURCE_INGESTNODE_COUNT = String.format(RESOURCE_COUNT, INGESTNODE);

    /**
     * memory used
     */
    public static final String RESOURCE_AM_HEAP_MEM_MB = String.format(RESOURCE_MEM, AM);
    public static final String RESOURCE_EXECUTOR_HEAP_MEM_MB = String.format(RESOURCE_MEM, EXECUTOR);
    public static final String RESOURCE_COORDINATOR_HEAP_MEM_MB = String.format(RESOURCE_MEM, COORDINATOR);
    public static final String RESOURCE_IDSERVICE_HEAP_MEM_MB = String.format(RESOURCE_MEM, IDSERVICE);
    public static final String RESOURCE_FRONTEND_HEAP_MEM_MB = String.format(RESOURCE_MEM, FRONTEND);
    public static final String RESOURCE_INGESTNODE_HEAP_MEM_MB = String.format(RESOURCE_MEM, INGESTNODE);

    /**
     * cpu cores used
     */
    public static final String RESOURCE_AM_CPU_CORES = String.format(RESOURCE_CPU, AM);
    public static final String RESOURCE_EXECUTOR_CPU_CORES = String.format(RESOURCE_CPU, EXECUTOR);
    public static final String RESOURCE_COORDINATOR_CPU_CORES = String.format(RESOURCE_CPU, COORDINATOR);
    public static final String RESOURCE_IDSERVICE_CPU_CORES = String.format(RESOURCE_CPU, IDSERVICE);
    public static final String RESOURCE_FRONTEND_CPU_CORES = String.format(RESOURCE_CPU, FRONTEND);
    public static final String RESOURCE_INGESTNODE_CPU_CORES = String.format(RESOURCE_CPU, INGESTNODE);

    /**
     * disk
     */
    public static final String RESOURCE_AM_DISK_MB = String.format(RESOURCE_DISK, AM);
    public static final String RESOURCE_EXECUTOR_DISK_MB = String.format(RESOURCE_DISK, EXECUTOR);
    public static final String RESOURCE_COORDINATOR_DISK_MB = String.format(RESOURCE_DISK, COORDINATOR);
    public static final String RESOURCE_IDSERVICE_DISK_MB = String.format(RESOURCE_DISK, IDSERVICE);
    public static final String RESOURCE_FRONTEND_DISK_MB = String.format(RESOURCE_DISK, FRONTEND);
    public static final String RESOURCE_INGESTNODE_DISK_MB = String.format(RESOURCE_DISK, INGESTNODE);

    /**
     * yarn
     */
    public static final String YARN_HDFS_PACKAGE_PATH = "yarn.hdfs.package.path";
    public static final String YARN_PACKAGE_DIR_NAME = "yarn.package.dir.name";
    public static final String YARN_AM_MAX_ATTEMPTS = "yarn.am.max.attempts";
    public static final String YARN_QUEUE = "yarn.queue";
    public static final String YARN_APPLICATION_NAME = "yarn.application.name";

    /**
     * fuxi
     */

    /**
     * single PC
     * if running on one single machine -> true
     */
    public static final String SINGLE_MODE_OPEN = "single.mode.open";

    public static final String GRAPH_NAME = "graph.name";
    public static final String COMPUTE_JOB_ID = "compute.job.id";

    public static final String ZK_CONNECT = "zookeeper.connect";

    public static final String ZK_AUTH_ENABLE = "zookeeper.auth.enable";
    public static final String ZK_AUTH_USER = "zookeeper.auth.user";
    public static final String ZK_AUTH_PASSWORD = "zookeeper.auth.password";

    public static final boolean ZK_AUTH_ENABLE_DEFAULT_VALUE = false;
    public static final String ZK_AUTH_USER_DEFAULT_VALUE = "graphcompute";
    public static final String ZK_AUTH_PASSWORD_DEFAULT_VALUE = "graphcompute";

    public static final String ZK_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";

    public static final String STUDIO_URL = "studio.url";

    public static final String PLATFORM_KIND = "platform.kind";
    public static final String HDFS_DEFAULT_FS = "hdfs.default.fs";

    public static final String JAVAROLE_MEMORY_USE_PERCENTAGE = "javarole.memory.use.percentage";

    public static final String FUXI_WORKER_MAX_RETRY_TIMES = "fuxi.worker.max.retry";
    public static final String FUXI_WORKER_RETRY_INTERVAL_SECONDS = "fuxi.worker.retry.interval.seconds";

    public static final String SCHEDULER_START_INTERVAL_MS = "scheduler.start.interval.ms";
    public static final String SCHEDULER_BLACKLIST_INTERVAL_MS = "scheduler.blacklist.interval.ms";

    /**
     * timeout config
     */
    public static final String REQUEST_RESOURCE_TIMEOUT_MS = "request.resource.timeout.ms";

    public static final String JAVA8_HOME = "java8.home";

    public static final String EXECUTOR_BINARY_NAME = "executor.binary.name";

    public static final String TIMELY_WORKER_PER_PROCESS = "timely.worker.per.process";

    public static final String COORDINATOR_PORT = "coordinator.port";

    public static final String COORDINATOR_SNAPSHOT_GC_TIME = "coordinator.snapshot.gc.time";

    public static final String COORDINATOR_SNAPSHOT_ONLINE_SIZE = "coordinator.snapshot.online.size";

    public static final String BLOCK_GRPC_ENABLE = "block.grpc.enable";

    public static final String GRAPH_TYPE = "graph.type";

    public static final String PARTITION_NUM = "partition.num";

    public static final String SERVER_DEBUG_ENABLED = "server.debug.enabled";

    public static final String GRAPH_LOADER_JAR = "graph.loader.jar";

    public static final String DATA_LOCAL_PATH = "data.local.path";

    public static final String DATA_SOURCE_ENDPOINT = "data.source.endpoint";

    public static final String DATA_SOURCE_SCHEDULER_ENDPOINT = "data.source.scheduler.endpoint";

    public static final String DATA_REMOTE_SOURCE = "data.remote.source";

    public static final String LOADDATA_HDFS_DEFAULT_FS = "loaddata.hdfs.default.fs";

    public static final int RPC_MAX_MESSAGE_DEFAULT_SIZE = 1024 * 1024 * 1024;

    public static final String RPC_MAX_MESSAGE_SIZE = "rpc.max.message.size";

    public static final String FRONTEND_SERVICE_MEMORY_THRESHOLD_PERCENT = "frontendservice.memory.threshold.percent";

    public static final String SERVER_ID = "server.id";

    public static final String ROLE_ORDER_ID = "role.order.id";

    public static final String KAFKA_PARTITION_NUM = "kafka.partition.num";

    public static final String FRONTEND_SERVICE_SESSION_MAX_NUM = "frontendservice.session.max.num";

    public static final String FRONTEND_SERVICE_SESSION_TIMEOUT_SECONDS = "frontendservice.session.timeout.seconds";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

    public static final String FRONTEND_AM_HB_INTERVAL_SECONDS = "frontend.am.hb.interval.seconds";

    public static final String DATA_PRODUCER_MODE = "data.producer.mode";

    public static final String DATA_DECIDER_THREAD_INTERVAL_SECONDS = "coordinator.data.decider.interval.seconds";
    public static final String DATA_SNAPSHOT_MODE = "coordinator.data.snapshot.mode";

    public static final String IDSERVICE_PARALLEL_NUM = "idservice.parallel.num";

    public static final String IDSERVICE_CAPACITY_NUM = "idservice.capacity.num";

    public static final String WORKER_LOCAL_RETRY_MAX_TIMES = "worker.local.retry.max.times";

    public static final String WORKER_OPERATION_THREAD_POOL_SIZE = "worker.operation.thread.pool.size";

    public static final String STORE_WRITE_BUFFER_SIZE = "store.write.buffer.size";
    public static final String STORE_PRECOMMIT_BUFFER_SIZE = "store.precommit.buffer.size";
    public static final String STORE_INSERT_THREAD_COUNT = "store.insert.thread.count";

    // hb related

    public static final String WORKER_ALIVEID = "worker.aliveid";

    public static final String CHECK_WORKER_ILLEGAL_ENABLED = "check.worker.illegal.enabled";

    public static final String WORKER_HB_SECONDS = "worker.hb.seconds";

    public static final String WORKER_HB_TIMEOUT_SECONDS = "worker.hb.timeout.seconds";

    public static final String WORKER_UNREGISTERED_TIMEOUT_SECONDS = "worker.unregistered.timeout.seconds";

    public static final String LOCAL_WORKER_PATH = "local.worker.path";

    public static final String REQUEST_EDGEID_INTERVAL = "request.edgeid.interval";

    public static final String REALTIME_INSERT_THREAD_NUM = "realtime.insert.thread.num";

    public static final String EXECUTOR_DOWNLOAD_DATA_THREAD_NUM = "executor.download.data.thread.num";

    public static final String EXECUTOR_LOAD_DATA_THREAD_NUM = "executor.load.data.thread.num";

    public static final String RETAINED_EVENT_NUM = "retained.event.num";

    // gremlin server related config

    public static final String GREMLIN_SERVICE_MODE = "gremlin.service.mode";

    public static final String GREMLIN_SERVER_VERTEX_CACHE_ENABLE = "gremlin.server.vertex.cache.enable";

    // compiler related config

    public static final String QUERY_LAMBDA_FLAG_ENABLE = "query.lambda.flag.enable";

    public static final String QUERY_VINEYARD_SCHEMA_PATH = "query.vineyard.schema.path";

    public static final String TIMELY_QUERY_CACHE_ENABLE = "timely.query.cache.enable";

    public static final String TIMELY_DAG_CHAIN_OPTIMIZE = "timely.dag.chain.optimize";

    public static final String TIMELY_DAG_CHAIN_BINARY = "timely.dag.chain.binary";

    public static final String TIMELY_DAG_CHAIN_GLOBAL_AGGREGATE = "timely.dag.chain.global.aggregate";

    public static final String TIMELY_QUERY_TIMEOUT_SEC = "timely.query.timeout.sec";

    public static final String TIMELY_PREPARE_TIMEOUT_SEC = "timely.prepare.timeout.sec";

    public static final String TIMELY_BATCH_QUERY_RESULT_SIZE = "timely.batch.query.result.size";

    public static final String TIMELY_GREMLIN_SERVER_PORT = "timely.gremlin.server.port";

    public static final String TIMELY_QUERY_RESULT_PARSE_FLAG = "timely.query.result.parse.flag";

    public static final String TIMELY_FETCH_PROP_FLAG = "timely.fetch.prop.flag";

    public static final String TIMELY_RESULT_ITERATION_BATCH_SIZE = "timely.result.iteration.batch.size";

    public static final String TIMELY_GLOBAL_PULL_GRAPH_FLAG = "timely.global.pull.graph.flag";

    public static final String REQUEST_CONTAINER_MAX_RETRY_TIMES = "request.container.max.retry.times";

    public static final String REQUEST_CONTAINER_RETRY_INTERVAL_SECONDS = "request.container.retry.interval.seconds";

    public static final String YARN_REQUEST_CONTAINER_TIME_INTERVAL_MS = "yarn.request.container.interval.ms";

    public static final String YARN_WAIT_CONTAINER_ALLOCATED_TIMEOUT_MS = "yarn.wait.container.allocated.timeout.ms";

    public static final String TIMELY_PREPARE_PERSIS_DIR = "timely.prepare.persist.directory";

    public static final String TIMELY_PREPARE_PERSIS_LOCK_DIR = "timely.prepare.persist.lock.directory";

    public static final String COMPILER_PREPARE_PERSIS_DIR = "compiler.prepare.persist.directory";

    public static final String COMPILER_PREPARE_PERSIS_LOCK_DIR = "compiler.prepare.persist.lock.directory";

    public static final String EXECUTOR_GRPC_THREAD_COUNT = "executor.grpc.thread.count";

    public static final String ODPS_ENDPOINT = "odps.endpoint";

    public static final String ODPS_ACCESSID = "odps.accessid";

    public static final String ODPS_ACCESSKEY = "odps.accesskey";

    public static final String ODPS_PROJECT = "odps.project";

    public static final String LOADER_ENABLE_MR_WITH_SQL = "loader.enable.mr.with.sql";

    public static final String MASTER_ADDRESS = "master.address";
    public static final String REPLICA_COUNT = "replica.count";
    public static final String INGEST_QUEUE_ID = "ingest.queue.id";
    public static final String LOGGER_STORE_HOME = "logger.store.home";
    public static final String SINK_BUFFER_SIZE = "sink.buffer.size";
    public static final String INGEST_BUFFER_SIZE = "ingest.buffer.size";
    public static final String INGEST_WRITER_THREAD_COUNT = "ingest.writer.thread.count";
    public static final String INGEST_FS_BUFFER_SIZE = "ingest.fs.buffer.size";
    public static final String INGEST_SOURCE_BUFFER_SIZE = "ingest.source.buffer.size";

    public static final String INSTANCE_AUTH_TYPE = "instance.auth.type";
    public static final String USER_AUTH_URL = "user.auth.url";

    public static final String DFS_ROOT_DIR = "dfs.root.dir";
    public static final String INGEST_CHECKPOINT_DIR = "ingest.checkpoint.dir";
    public static final String INGEST_PANGU_CAPABILITY = "ingest.pangu.capability";
    public static final String INGEST_CHECKPOINT_REPLICATION = "ingest.checkpoint.replication";

    public static final String INSTANCE_UNIQUE_ID = "instance.unique.id";
    public static final String TRY_ASSIGN_RESOURCE_MAX_MS = "try.assign.resource.max.ms";

    public static final String PANGU_DIR = "dfs.dir";

    public static final String COORDINATOR_NETWORK_THREAD_COUNT = "coordinator.network.thread.count";
    public static final String COORDINATOR_NETWORK_THREAD_QUEUE = "coordinator.network.thread.queue";
    public static final String CLIENT_RETRY_TIMES = "client.retry.times";


    public static final String EXECUTOR_WORKER_OVERLAP_ENABLED = "executor.machine.overlap.enabled";
    public static final String STORE_WRITE_BUFFER_MB = "store.write.buffer.mb";

    public static final String WRITE_BATCH_LIMIT_MB = "write.batch.limit.mb";
    public static final String REMAIN_SNAPSHOT_MAX = "remain.snapshot.max";

    public static final String ASYNC_GRPC_QUERY = "runtime.enable.async.grpc.query";
    public static final String QUEUE_TIME_BOUND_SECOND = "queue.time.bound.second";

    public static final String GRPC_THREAD_COUNT = "%s.grpc.thread.count";
    public static final int DEFAULT_GRPC_THREAD_COUNT = Math.min(Runtime.getRuntime().availableProcessors() * 2, 64);

    public static final String SNAPSHOT_PERSIST_SPLIT_SIZE = "snapshot.persist.split.size";
    // enable use user's odps project to sumbit bulk load job
    public static final String BULKLOAD_ENABLE_USER_ODPS_PROJECT = "bulkload.enable.user.odps.project";
    public static final String SHUFFLE_EVENT_THREAD_POOL_SIZE = "shuffle.event.thread.pool.size";

    public static final String VPC_ENDPOINT = "vpc.endpoint";
    public static final String VPC_ENDPOINT_SUFFIX = "vpc.endpoint.suffix";
    public static final String DNS_ZONE = "dns.zone";
    public static final String ALB_AK_ID = "alb.ak.id";
    public static final String ALB_AK_SECRET = "alb.ak.secret";
    public static final String ALB_URL = "alb.url";

    public static final String JUTE_MAXBUFFER = "jute.maxbuffer";
    public static final String STORE_ALLOW_MEMORY = "store.allow.memory";

    public static final String BULKLOAD_SUBMIT_BY_FRONTEND = "bulkload.submit.by.frontend";

    public static final String CONTAINER_REQUEST_WAITING_MS = "container.request.waiting.ms";

    public static final String ENGINE_PEGASUS = "pegasus";
    public static final String RUNTIME_ENGINE_NAME = "runtime.engine.name";
    public static final String RUNTIME_ENGINE_THREAD_POOL_SIZE = "runtime.engine.thread.pool.size";

    public InstanceConfig(Map<String, String> conf) {
        super(conf);
    }

    public InstanceConfig(Properties properties) {
        super(properties);
    }

    public int getPegasusTimeoutMS() {
        return getInt(PEGASUS_TIMEOUT, 240000);
    }

    public int getPegasusWorkerNum() {
        return getInt(PEGASUS_WORKER_NUM, 2);
    }

    public int getPegasusBatchSize() {
        return getInt(PEGASUS_BATCH_SIZE, 1024);
    }

    public int getPegasusOutputCapacity() {
        return getInt(PEGASUS_OUTPUT_CAPACITY, 16);
    }

    public int getPegasusMemoryLimit() {
        return getInt(PEGASUS_MEMORY_LIMIT, Integer.MAX_VALUE);
    }

    public String getJuteMaxbuffer() {
        return get(JUTE_MAXBUFFER, "8388608");
    }

    public boolean isStoreAllowMemory() {
        return getBoolean(STORE_ALLOW_MEMORY, false);
    }

    public int getSnapshotPersistSplitSize() {
        return getInt(SNAPSHOT_PERSIST_SPLIT_SIZE, SNAPSHOT_SPLIT_SIZE);
    }

    public int getCoordinatorGrpcThreadCount() {
        return getInt(String.format(GRPC_THREAD_COUNT, COORDINATOR), DEFAULT_GRPC_THREAD_COUNT);
    }

    public int getFrontendGrpcThreadCount() {
        return getInt(String.format(GRPC_THREAD_COUNT, FRONTEND), DEFAULT_GRPC_THREAD_COUNT);
    }

    public int getIngestCheckpointReplication() {
        return getInt(INGEST_CHECKPOINT_REPLICATION, 2);
    }

    public int getWriteBatchLimit() {
        return getInt(WRITE_BATCH_LIMIT_MB, 16);
    }

    public long getQueueTimeBoundSecond() {
        return getLong(QUEUE_TIME_BOUND_SECOND, 60);
    }

    public int getResourceExecutorCount() {
        return getInt(RESOURCE_EXECUTOR_COUNT, 0);
    }

    public int getReplicaCount() {
        return getInt(REPLICA_COUNT, 1);
    }

    public int getResourceCoordinatorCount() {
        return getInt(RESOURCE_COORDINATOR_COUNT, 1);
    }

    public int getResourceIdServiceCount() {
        return getInt(RESOURCE_IDSERVICE_COUNT, 0);
    }

    public int getResourceFrontendCount() {
        return getInt(RESOURCE_FRONTEND_COUNT, 1);
    }

    public int getResourceIngestNodeCount() {
        return getInt(RESOURCE_INGESTNODE_COUNT, 0);
    }

    public int getResourceExecutorHeapMemMb() {
        return getInt(RESOURCE_EXECUTOR_HEAP_MEM_MB, 4096);
    }

    public int getResourceCoordinatorHeapMemMb() {
        return getInt(RESOURCE_COORDINATOR_HEAP_MEM_MB, 1024);
    }

    public int getResourceIdServiceHeapMemMb() {
        return getInt(RESOURCE_IDSERVICE_HEAP_MEM_MB, 1024);
    }

    public int getResourceFrontendHeapMemMb() {
        return getInt(RESOURCE_FRONTEND_HEAP_MEM_MB, 1024);
    }

    public int getResourceAmHeapMemMb() {
        return getInt(RESOURCE_AM_HEAP_MEM_MB, 4096);
    }

    public int getResourceIngestNodeHeapMemMb() {
        return getInt(RESOURCE_INGESTNODE_HEAP_MEM_MB, 1024);
    }

    public int getResourceExecutorCpuCores() {
        return getInt(RESOURCE_EXECUTOR_CPU_CORES, 1);
    }

    public int getResourceCoordinatorCpuCores() {
        return getInt(RESOURCE_COORDINATOR_CPU_CORES, 1);
    }

    public int getResourceIdServiceCpuCores() {
        return getInt(RESOURCE_IDSERVICE_CPU_CORES, 1);
    }

    public int getResourceFrontendCpuCores() {
        return getInt(RESOURCE_FRONTEND_CPU_CORES, 1);
    }

    public int getResourceAmCpuCores() {
        return getInt(RESOURCE_AM_CPU_CORES, 1);
    }

    public int getResourceIngestNodeCpuCores() {
        return getInt(RESOURCE_INGESTNODE_CPU_CORES, 1);
    }

    public int getResourceExecutorDiskMb() {
        return getInt(RESOURCE_EXECUTOR_DISK_MB, 100);
    }

    public int getResourceCoordinatorDiskMb() {
        return getInt(RESOURCE_COORDINATOR_DISK_MB, 100);
    }

    public int getResourceIdServiceDiskMb() {
        return getInt(RESOURCE_IDSERVICE_DISK_MB, 100);
    }

    public int getResourceFrontendDiskMb() {
        return getInt(RESOURCE_FRONTEND_DISK_MB, 100);
    }

    public int getResourceIngestNodeDiskMb() {
        return getInt(RESOURCE_INGESTNODE_DISK_MB, 100);
    }

    public String getYarnHdfsPackagePath() {
        return getString(YARN_HDFS_PACKAGE_PATH);
    }

    public String getYarnPackageDirName() {
        return getString(YARN_PACKAGE_DIR_NAME, "maxgraph");
    }

    public int getYarnAmMaxAttempts() {
        return getInt(YARN_AM_MAX_ATTEMPTS, Integer.MAX_VALUE);
    }

    public String getYarnQueue() {
        return getString(YARN_QUEUE, "default");
    }

    public String getYarnApplicationName() {
        return getString(YARN_APPLICATION_NAME, "maxgraph");
    }

    public int getRequestResourceTimeoutMs() {
        return getInt(REQUEST_RESOURCE_TIMEOUT_MS, 300000);
    }

    public String getJava8Home() {
        return getString(JAVA8_HOME);
    }

    public String getGraphName() {
        return getString(GRAPH_NAME);
    }

    public String getComputeJobId() {
        return getString(COMPUTE_JOB_ID);
    }

    public String getZkConnect() {
        return getString(ZK_CONNECT);
    }

    public int getZkSessionTimeoutMs() {
        return getInt(ZK_SESSION_TIMEOUT_MS, 10000);
    }

    public int getZkConnectionTimeoutMs() {
        return getInt(ZK_CONNECTION_TIMEOUT_MS, 1000);
    }

    public boolean getZkAuthEnable() {
        return getBoolean(ZK_AUTH_ENABLE, ZK_AUTH_ENABLE_DEFAULT_VALUE);
    }

    public String getZkAuthUser() {
        return getString(ZK_AUTH_USER, ZK_AUTH_USER_DEFAULT_VALUE);
    }

    public String getZkAuthPassword() {
        return getString(ZK_AUTH_PASSWORD, ZK_AUTH_PASSWORD_DEFAULT_VALUE);
    }

    public String getCoordinatorPort() {
        return getString(COORDINATOR_PORT, "8388");
    }

    public Boolean isBlockGrpcEnable() {
        return getBoolean(BLOCK_GRPC_ENABLE, true);
    }

    public int getPartitionNum() {
        return getInt(PARTITION_NUM, 4);
    }

    public String getDataRemoteSource() {
        return getString(DATA_REMOTE_SOURCE, "hdfs://localhost:9000/tmp/demo");
    }

    public String getDataSourceEndpoint() {
        return getString(DATA_SOURCE_ENDPOINT);
    }

    public String getDataSourceSchedulerEndpoint() {
        return getString(DATA_SOURCE_SCHEDULER_ENDPOINT);
    }

    public String getDataLocalPath() {
        return getString(DATA_LOCAL_PATH, "./");
    }

    public Boolean isServerDebugEnabled() {
        return getBoolean(SERVER_DEBUG_ENABLED, true);
    }

    public String getGraphLoaderJar() {
        return getString(GRAPH_LOADER_JAR, "./");
    }

    public String getLoaddataHdfsDefaultFs() {
        return getString(LOADDATA_HDFS_DEFAULT_FS);
    }

    public int getMaxRPCMessageSize() {
        return getInt(RPC_MAX_MESSAGE_SIZE, RPC_MAX_MESSAGE_DEFAULT_SIZE);
    }

    public double getFrontendServiceMemoryThresholdPercent() {
        return getDouble(FRONTEND_SERVICE_MEMORY_THRESHOLD_PERCENT, 0.8);
    }

    public int getServerId() {
        return getInt(SERVER_ID);
    }

    public int getKafkaPartitionNum() {
        return getInt(KAFKA_PARTITION_NUM, 4);
    }

    public int getFrontendServiceSessionMaxNum() {
        return getInt(FRONTEND_SERVICE_SESSION_MAX_NUM, 1000);
    }

    public int getFrontendServiceTimeoutSeconds() {
        return getInt(FRONTEND_SERVICE_SESSION_TIMEOUT_SECONDS, 30);
    }

    public String getKafkaBootstrapServers() {
        return getString(KAFKA_BOOTSTRAP_SERVERS);
    }

    public int getFrontendAmHbIntervalSeconds() {
        return getInt(FRONTEND_AM_HB_INTERVAL_SECONDS, 3);
    }

    public int getWorkerHBSeconds() {
        return getInt(WORKER_HB_SECONDS, 5);
    }

    public int getWorkerHBTimeoutSeconds() {
        return getInt(WORKER_HB_TIMEOUT_SECONDS, 20);
    }

    public int getServerDataDeciderInterval() {
        return getInt(DATA_DECIDER_THREAD_INTERVAL_SECONDS, 5);
    }

    public String getInstanceZkConnect() {
        return getZkConnect() + "/" + getGraphName();
    }

    public int getWorkerUnregisteredTimeoutSeconds() {
        return getInt(WORKER_UNREGISTERED_TIMEOUT_SECONDS, 60);
    }

    public String getLocalWorkerPath() {
        return getString(LOCAL_WORKER_PATH, "/tmp");
    }

    public long getSnapshotGCTimeSeconds() {
        return getLong(COORDINATOR_SNAPSHOT_GC_TIME, 120);
    }

    public int getCoordinatorSnapshotOnlineSize() {
        return getInt(COORDINATOR_SNAPSHOT_ONLINE_SIZE, 5);
    }

    public int getRequestEdgeIdInterval() {
        return getInt(REQUEST_EDGEID_INTERVAL, 10000000);
    }

    public String getExecutorBinaryName() {
        return getString(EXECUTOR_BINARY_NAME, "executor");
    }

    public int getTimelyWorkerPerProcess() {
        return getInt(TIMELY_WORKER_PER_PROCESS, 1);
    }

    public int getRealtimeInsertThreadNum() {
        return getInt(REALTIME_INSERT_THREAD_NUM, 4);
    }

    public int getExecutorDownloadDataThreadNum() {
        return getInt(EXECUTOR_DOWNLOAD_DATA_THREAD_NUM, 8);
    }

    public int getExecutorLoadDataThreadNum() {
        return getInt(EXECUTOR_LOAD_DATA_THREAD_NUM, 16);
    }

    public int getRetainedEventNum() {
        return getInt(RETAINED_EVENT_NUM, 100);
    }

    public int getRequestContainerMaxRetryTimes() {
        return getInt(REQUEST_CONTAINER_MAX_RETRY_TIMES, 6);
    }

    public int getRequestContainerRetryIntervalSenonds() {
        return getInt(REQUEST_CONTAINER_RETRY_INTERVAL_SECONDS, 30);
    }

    public GremlinServerMode getGremlinServerMode() {
        String serverMode = getString(GREMLIN_SERVICE_MODE, GremlinServerMode.TIMELY.name());
        return GremlinServerMode.valueOf(StringUtils.upperCase(serverMode));
    }

    public boolean timelyQueryCacheEnable() {
        return getBoolean(TIMELY_QUERY_CACHE_ENABLE, false);
    }

    public boolean gremlinVertexCacheEnable() {
        return getBoolean(GREMLIN_SERVER_VERTEX_CACHE_ENABLE, true);
    }

    public boolean getChainOptimize() {
        return getBoolean(TIMELY_DAG_CHAIN_OPTIMIZE, false);
    }

    public boolean getChainBinary() {
        return getBoolean(TIMELY_DAG_CHAIN_BINARY, false);
    }

    public boolean getChainGlobalAggregate() {
        return getBoolean(TIMELY_DAG_CHAIN_GLOBAL_AGGREGATE, false);
    }

    public long getTimelyQueryTimeoutSec() {
        return getLong(TIMELY_QUERY_TIMEOUT_SEC, 6000L);
    }

    public boolean getGlobalPullGraphFlag() {
        return getBoolean(TIMELY_GLOBAL_PULL_GRAPH_FLAG, false);
    }

    public long getTimelyPrepareTimeoutSec() {
        return getLong(TIMELY_PREPARE_TIMEOUT_SEC, 10);
    }

    public int getBatchQuerySize() {
        return getInt(TIMELY_BATCH_QUERY_RESULT_SIZE, 1280);
    }

    public int getGremlinServerPort() {
        return getInt(TIMELY_GREMLIN_SERVER_PORT, -1);
    }

    public boolean timelyFetchPropFlag() {
        return getBoolean(TIMELY_FETCH_PROP_FLAG, true);
    }

    public int getTimelyResultIterationBatchSize() {
        return getInt(TIMELY_RESULT_ITERATION_BATCH_SIZE, 64);
    }

    public int getYarnRequestContaienrIntervalMs() {
        return getInt(YARN_REQUEST_CONTAINER_TIME_INTERVAL_MS, 1000);
    }

    public int getYarnWaitContainerAllocatedTimeoutMs() {
        return getInt(YARN_WAIT_CONTAINER_ALLOCATED_TIMEOUT_MS, 10000);
    }

    public String getYarnHdfsAddress() {
        return getString(HDFS_DEFAULT_FS, null);
    }

    public String getTimelyPrepareDir() {
        return getGraphName() + "/" + getString(TIMELY_PREPARE_PERSIS_DIR, "prepare");
    }

    public String getTimelyPrepareLockDir() {
        return getGraphName() + "/" + getString(TIMELY_PREPARE_PERSIS_LOCK_DIR, "prepare_lock");
    }

    public String getCompilerPreparePersisDir() {
        return getGraphName() + "/" + getString(COMPILER_PREPARE_PERSIS_DIR, "compiler");
    }

    public int getExecutorGrpcThreadCount() {
        return getInt(EXECUTOR_GRPC_THREAD_COUNT, 8);
    }

    public int getRoleOrderId() {
        return getInt(ROLE_ORDER_ID, 1);
    }

    public String getOdpsEndpoint() {
        return getString(ODPS_ENDPOINT, "null");
    }

    public String getOdpsAccessKey() {
        return getString(ODPS_ACCESSKEY, "null");
    }

    public String getOdpsAccessId() {
        return getString(ODPS_ACCESSID, "null");
    }

    public String getOdpsProject() {
        return getString(ODPS_PROJECT, "null");
    }

    public boolean getLoaderEnableMrWithSql() {
        return getBoolean(LOADER_ENABLE_MR_WITH_SQL, false);
    }

    public long getWorkerAliveId() {
        return getLong(WORKER_ALIVEID);
    }

    public int getInstanceAuthType() {
        return getInt(INSTANCE_AUTH_TYPE, 1);
    }

    public String getUserAuthUrl() {
        return getString(USER_AUTH_URL, "https://maxgraph/");
    }

    // 前端加上该配置后，将默认值设为true
    public boolean isCheckWorkerIllegalEnabled() {
        return getBoolean(CHECK_WORKER_ILLEGAL_ENABLED, false);
    }

    public double getJavaRoleMemoryUsePercentage() {
        return getDouble(JAVAROLE_MEMORY_USE_PERCENTAGE, 0.8);
    }

    public long getWorkerLocalRetryMaxTimes() {
        return getLong(WORKER_LOCAL_RETRY_MAX_TIMES, 10L);
    }

    public long getFuxiWorkerRetryMaxTimes() {
        return getLong(FUXI_WORKER_MAX_RETRY_TIMES, 1L);
    }

    public int getFuxiWorkerRetryIntervalSeconds() {
        return getInt(FUXI_WORKER_RETRY_INTERVAL_SECONDS, 1);
    }

    public long getSchedulerStartWorkersIntervalMS() {
        return getLong(SCHEDULER_START_INTERVAL_MS, 30 * 1000L);
    }

    public long getSchedulerBlackListIntervalMS() {
        return getLong(SCHEDULER_BLACKLIST_INTERVAL_MS, 0L);
    }

    public long getContainerRequestWaitingTimeMS() {
        return getLong(CONTAINER_REQUEST_WAITING_MS, 120 * 1000L);
    }


    public int getWorkerOperationThreadPoolSize() {
        return getInt(WORKER_OPERATION_THREAD_POOL_SIZE, 4);
    }

    public String getIngestPanguCapability() {
        return getString(INGEST_PANGU_CAPABILITY, null);
    }

    public String getInstanceUniqueId() {
        return getString(INSTANCE_UNIQUE_ID, "MAXGRAPH");
    }

    public long getTryAssignResourceMaxMS() {
        return getLong(TRY_ASSIGN_RESOURCE_MAX_MS, 3000L);
    }

    public String getClusterId() {
        return getString(CLUSTER_ID);
    }

    public String getDfsRootDir() {
        return String.format("%s/%s_%s", getString(DFS_ROOT_DIR, "."),
                this.getGraphName(),
                this.getInstanceUniqueId());
    }

    public String getComputeDfsRootDir() {
        return String.format("%s/compute", getString(DFS_ROOT_DIR, "."));
    }

    public String getComputeJobConfFileName(String jobName) {
        return String.format("%s/%s.dat", getComputeDfsRootDir(), jobName);
    }

    public String getUdfPath(String graphName, String udfName) {
        return String.format("%s/udf/%s/%s", getComputeDfsRootDir(), graphName, udfName);
    }

    public static final String COMPUTE_MASTER_STATUS = "/compute/master_status";

    public static String getComputeMasterStatusPath(String jobName) {
        return String.format("%s/%s", COMPUTE_MASTER_STATUS, jobName);
    }

    public boolean isExecutorWorkerOverlapEnabled() {
        return getBoolean(EXECUTOR_WORKER_OVERLAP_ENABLED, false);
    }

    public int getClientRetryTimes() {
        return getInt(CLIENT_RETRY_TIMES, 3);
    }

    public int getRemainSnapshotMax() {
        return getInt(REMAIN_SNAPSHOT_MAX, 500);
    }

    public boolean getAsyncGrpcQuery() {
        return getBoolean(ASYNC_GRPC_QUERY, true);
    }

    public String getRuntimeEngineName() {
        return getString(RUNTIME_ENGINE_NAME, ENGINE_PEGASUS);
    }

    public int getRuntimeEngineThreadPoolSize() {
        return getInt(RUNTIME_ENGINE_THREAD_POOL_SIZE, 16);
    }

    public boolean getSingleModeOpen() {
        return getBoolean(SINGLE_MODE_OPEN, false);
    }

    public boolean getBulkLoadEnableUserOdpsProject() {
        return getBoolean(BULKLOAD_ENABLE_USER_ODPS_PROJECT, true);
    }

    public String getVpcEndpoint() {
        return getString(VPC_ENDPOINT);
    }

    public String getVpcEndpointSuffix() {
        return getString(VPC_ENDPOINT_SUFFIX, null);
    }

    public String getDnsZone() {
        return getString(DNS_ZONE, null);
    }

    public String getAlbAkId() {
        return getString(ALB_AK_ID, null);
    }

    public String getAlbAkSecret() {
        return getString(ALB_AK_SECRET, null);
    }

    public String getAlbUrl() {
        return getString(ALB_URL, null);
    }

    public boolean getBulkLoadSubmitByFrontend() {
        return getBoolean(BULKLOAD_SUBMIT_BY_FRONTEND, false);
    }

    public boolean getLambdaEnableFlag() {
        return getBoolean(QUERY_LAMBDA_FLAG_ENABLE, false);
    }

    public String getVineyardSchemaPath() {
        return getString(QUERY_VINEYARD_SCHEMA_PATH, null);
    }
}
