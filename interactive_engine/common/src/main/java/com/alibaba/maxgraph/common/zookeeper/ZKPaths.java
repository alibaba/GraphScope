/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.zookeeper;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-11 下午5:19
 **/

public class ZKPaths {

    public static final String COORDINATOR = "/%s/coordinator";
    public static final String COORDINATOR_STATE = "/%s/cluster_state";
    public static final String LOGGER_STORE = "/%s/logger_store";

    public static final String SCHEMA_PATH = "/%s/schema";

    public static final String TYPE_SCHEMA_PATH = "/%s/schema/%s";

    public static final String TYPE_SCHEMA_ID_PATH = "/%s/schema/%s/%d";

    @Deprecated
    public static final String TYPE_CONFIG_PATH_SERVING = "/%s/typeconfig/all";
    @Deprecated
    public static final String TYPE_CONFIG_PATH_LOADING = "/%s/typeconfig/loading";

    public static final String TYPE_CONFIG_PATH_ONE = "/%s/typeconfig/one";

    public static final String INSTANCE_SNAPSHOT_PATH = "/%s/snapshot/serve";
    public static final String INSTANCE_OFFLINE_SNAPSHOT_PATH = "/%s/snapshot/offline";
    public static final String INSTANCE_SNAPSHOT_PATH_INDEX = "/%s/snapshot/serve/%s";

    public static final String INSTANCE_REPLICA_IN_SNAPSHOT_PATH = "/%s/snapshot/replica";

    public static final String INSTANCE_ASSIGN_PATH = "/%s/assignment";

    public static final String MASTER = "/%s/master";
    public static final String MASTER_WORKER_ROLE_INFO = MASTER + "/role_info";
    public static final String MASTER_WORKER_CONTAINER_INFO = MASTER + "/container_info";
    public static final String MASTER_WORKER_RESOURCE_INFO = MASTER + "/resource_info";
    public static final String MASTER_COMPLETED_CONTAINERS_INFO = MASTER + "/completed_info";

    public static final String RUNTIME_GROUP_INFO = "/%s/runtime_group_info";

    public static final String PREPARE_QUERY_INFO = "/%s/prepare_query_info";

    public static final String FRONTEND_ENDPOINT_INFO = "/%s/frontend_endpoint_info";

    public static String getPrepareQueryInfo(String graphName) {
        return String.format(PREPARE_QUERY_INFO, graphName);
    }

    public static String getFrontEndpointInfo(String graphName) {
        return String.format(FRONTEND_ENDPOINT_INFO, graphName);
    }

    public static String getRuntimeGroupInfoPath(String graphName) {
        return String.format(RUNTIME_GROUP_INFO, graphName);
    }

    public static String getCoordinatorStatePath(String graphName) {
        return String.format(COORDINATOR_STATE, graphName);
    }

    public static final String WORKER_ALIVEID_INFO = MASTER + "/alive_id";

    public static final String EXECUTOR_ADDRESS = MASTER + "/executor_address";

    public static String getCoordinatorPath(String graphName) {
        return String.format(COORDINATOR, graphName);
    }

    public static String getLoggerStorePath(String graphName) {
        return String.format(LOGGER_STORE, graphName);
    }

    public static String getMasterPath(String graphName) {
        return String.format(MASTER, graphName);
    }

    public static String getMasterWorkerRoleInfoPath(String graphName) {
        return String.format(MASTER_WORKER_ROLE_INFO, graphName);
    }

    public static String getMasterWorkerContainerInfoPath(String graphName) {
        return String.format(MASTER_WORKER_CONTAINER_INFO, graphName);
    }

    public static String getMasterCompletedContainersInfoPath(String graphName) {
        return String.format(MASTER_COMPLETED_CONTAINERS_INFO, graphName);
    }

    public static String getMasterWorkerResourceInfo(String graphName) {
        return String.format(MASTER_WORKER_RESOURCE_INFO, graphName);
    }

    public static String getWorkerAliveIdInfo(String graphName) {
        return String.format(WORKER_ALIVEID_INFO, graphName);
    }

    public static String getExecutorAddress(String graphName) {
        return String.format(EXECUTOR_ADDRESS, graphName);
    }

    public static String getSchemaPath(String graphName) {
        return String.format(SCHEMA_PATH, graphName);
    }

    public static String getTypeSchemaPath(String graphName, String label) {
        return String.format(TYPE_SCHEMA_PATH, graphName, label);
    }

    public static String getTypeSchemaIdPath(String graphName, String label, int id) {
        return String.format(TYPE_SCHEMA_ID_PATH, graphName, label, id);
    }

    public static String getTypeConfigPath(String graphName) {
        return String.format(TYPE_CONFIG_PATH_SERVING, graphName);
    }

    public static String getTypeConfigPathOne(String graphName) {
        return String.format(TYPE_CONFIG_PATH_ONE, graphName);
    }

    public static String getTypeLoadingConfigPath(String graphName) {
        return String.format(TYPE_CONFIG_PATH_LOADING, graphName);
    }

    public static String getServerAssignmentPath(String graphName) {
        return String.format(INSTANCE_ASSIGN_PATH, graphName);
    }

    public static String getSnapshotPath(String graphName) {
        return String.format(INSTANCE_SNAPSHOT_PATH, graphName);
    }

    public static String getSnapshotPath(String graphName, String index) {
        return String.format(INSTANCE_SNAPSHOT_PATH_INDEX, graphName, index);
    }

    public static String getReplicaInSnapshotPath(String graphName) {
        return String.format(INSTANCE_REPLICA_IN_SNAPSHOT_PATH, graphName);
    }

    public static String getSnapshotOfflinePath(String graphName) {
        return String.format(INSTANCE_OFFLINE_SNAPSHOT_PATH, graphName);
    }

    public static String getPath(String path, String graphName) {
        return String.format(path, graphName);
    }
}
