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

import com.alibaba.maxgraph.common.ServerAssignment;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-12 上午10:21
 **/

public class ZkNamingProxy {

    private static final Logger LOG = LoggerFactory.getLogger(ZkNamingProxy.class);

    private String graphName;
    private ZkUtils zkUtils;

    public ZkNamingProxy(String graphName, ZkUtils zkUtils) {
        this.graphName = graphName;
        this.zkUtils = zkUtils;
    }

    public void registerCoordinator(String hostName, int port) throws Exception {
        persistEndpointToZK(ZKPaths.getCoordinatorPath(graphName), hostName, port);
    }

    public void persistAppMasterEndpoint(String hostName, int port) throws Exception {
        persistEndpointToZK(ZKPaths.getMasterPath(graphName), hostName, port);
    }

    public void deleteCoordinatorInfo() {
        zkUtils.deletePath(ZKPaths.getCoordinatorPath(graphName));
    }

    private void persistEndpointToZK(String zkPath, String hostName, int port) throws Exception {
        Endpoint endpoint = new Endpoint(hostName, port);
        String serverInfo = JSON.toJson(endpoint);

        zkUtils.createOrUpdatePath(zkPath, serverInfo, CreateMode.EPHEMERAL);
        LOG.info("update coordinator endpoint in zk success:{}", endpoint);
    }

    private String readStringValueFromZK(String zkPath) {
        Pair<String, Stat> controllerInfo = zkUtils.readDataMaybeNull(zkPath);
        return controllerInfo.getLeft();
    }

    public Endpoint getCoordinatorEndpoint() {
        return getServerEndpoint(ZKPaths.getCoordinatorPath(graphName));
    }

    public Endpoint getAppMasterEndpoint() {
        return getServerEndpoint(ZKPaths.getMasterPath(graphName));
    }

    private Endpoint getServerEndpoint(String endpointPath) {
        try {
            String zkInfo = readStringValueFromZK(endpointPath);
            if (zkInfo != null) {
                return JSON.fromJson(zkInfo, Endpoint.class);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOG.error("", e);
            return null;
        }
    }

    public void createOrUpdateMasterRootNode() throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getMasterPath(graphName), "maxgraph master", CreateMode.PERSISTENT);
    }

    public void persistWorkerRoleInfo(Map<Integer, RoleType> roleTypeMap) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getMasterWorkerRoleInfoPath(graphName), JSON.toJson(roleTypeMap), CreateMode.PERSISTENT);
        LOG.debug("persist worker role info into zk: {}", roleTypeMap);
    }

    public Map<Integer, RoleType> getWorkerRoleInfo() {
        String workerRoleInfo = readStringValueFromZK(ZKPaths.getMasterWorkerRoleInfoPath(graphName));
        if (workerRoleInfo == null) {
            return Maps.newHashMap();
        }
        return JSON.fromJson(workerRoleInfo, new TypeReference<Map<Integer, RoleType>>() {
        });
    }

    public void persistWorkerAliveIdInfo(Map<Integer, Long> aliveIdMap) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getWorkerAliveIdInfo(graphName), JSON.toJson(aliveIdMap), CreateMode.PERSISTENT);
        LOG.debug("persist worker alive id into zk: {}", aliveIdMap);
    }

    public void persistExecutorAddress(Map<Integer, String> executorAddress) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getExecutorAddress(graphName), JSON.toJson(executorAddress), CreateMode.PERSISTENT);
    }

    public Map<Integer, String> getExecutorAddress() {
        String executorAddress = readStringValueFromZK(ZKPaths.getExecutorAddress(graphName));
        if (executorAddress == null) {
            return Maps.newHashMap();
        }
        return JSON.fromJson(executorAddress, new TypeReference<Map<Integer, String>>() {
        });
    }

    public void deleteAliveIdInfo() {
        zkUtils.deletePath(ZKPaths.getWorkerAliveIdInfo(graphName));
    }

    public Map<Integer, Long> getWorkerAliveIdInfo() {
        String workerAliveIdInfo = readStringValueFromZK(ZKPaths.getWorkerAliveIdInfo(graphName));
        if (workerAliveIdInfo == null) {
            return Maps.newHashMap();
        }
        return JSON.fromJson(workerAliveIdInfo, new TypeReference<Map<Integer, Long>>() {
        });
    }

    public void persistWorker2ContainerString(Map<Integer, String> roleContainerIdMap) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getMasterWorkerContainerInfoPath(graphName), JSON.toJson(roleContainerIdMap), CreateMode.PERSISTENT);
        LOG.info("persist worker container info into zk: {}", roleContainerIdMap);
    }

    public void persistCompletedContainers(List<String> completedContainerIdString) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getMasterCompletedContainersInfoPath(graphName), JSON.toJson(completedContainerIdString), CreateMode.PERSISTENT);
    }

    public List<String> getCompletedContainers() {
        String completedContainersInfo = readStringValueFromZK(ZKPaths.getMasterCompletedContainersInfoPath(graphName));
        List<String> completedContainerIdString = JSON.fromJson(completedContainersInfo, new TypeReference<List<String>>() {
        });
        if (completedContainerIdString == null) {
            return Lists.newArrayList();
        }
        return completedContainerIdString;
    }

    public void persistFrontEndEndpoints(String vpcEndpoint, String text) throws Exception {
        zkUtils.createOrUpdatePath(ZKPaths.getFrontEndpointInfo(graphName), text + " " + vpcEndpoint, CreateMode.EPHEMERAL);
        LOG.info("persist frontend ips into zk: {}", text);
    }

    public Map<Integer, String> getWorker2ContainerString() {
        String containerInfo = readStringValueFromZK(ZKPaths.getMasterWorkerContainerInfoPath(graphName));
        Map<Integer, String> infoMap = JSON.fromJson(containerInfo, new TypeReference<Map<Integer, String>>() {
        });
        if (infoMap == null) {
            return Maps.newHashMap();
        }
        return infoMap;
    }

    public void persistentServerAssignment(Map<Integer, ServerAssignment> serverAssignment) throws Exception {
        zkUtils.createPersistentPath(ZKPaths.getServerAssignmentPath(graphName), JSON.toJson(serverAssignment));
    }

    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    public CuratorFramework getZkClient() {
        return this.zkUtils.getZkClient();
    }

    public void close() throws IOException {
        this.zkUtils.close();
    }

}
