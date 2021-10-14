/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator.manager;

import com.alibaba.maxgraph.common.DataStatus;
import com.alibaba.maxgraph.common.ServerAssignment;
import com.alibaba.maxgraph.common.client.WorkerInfo;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.lock.ExtendedRWLock;
import com.alibaba.maxgraph.common.lock.LockWrapper;
import com.alibaba.maxgraph.sdkcommon.Protoable;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.logging.LogEvents;
import com.alibaba.maxgraph.logging.Logging;
import com.alibaba.maxgraph.proto.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.alibaba.maxgraph.proto.InstanceInfoProto.Status.ABNORMAL;
import static com.alibaba.maxgraph.proto.InstanceInfoProto.Status.NORMAL;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-07-09 下午7:29
 */
public class InstanceInfo implements Protoable<InstanceInfoProto> {

    private static final Logger LOG = LoggerFactory.getLogger(InstanceInfo.class);

    private final ExtendedRWLock lock = new ExtendedRWLock();

    // params
    private ServerDataManager serverDataManager;

    private InstanceConfig instanceConfig;

    // roleType -> WorkInfo
    public final HashMultimap<RoleType, WorkerInfo> workerInfoMap = HashMultimap.create();

    // serverId -> WorkInfo
    public final Map<Integer, WorkerInfo> server2WorkerInfo = Maps.newConcurrentMap();

    private ConcurrentMap<Integer, Long> workerId2ExistTime = Maps.newConcurrentMap();
    private ConcurrentMap<Integer, Long> workerLastRegisterTime = Maps.newConcurrentMap();

    // private ConcurrentMap<Integer, AtomicLong> workerId2AliveId = Maps.newConcurrentMap();
    private Map<Integer, Long> worker2AliveId = Maps.newHashMap();
    private static final long INITIAL_WORKER_ALIVE_ID = 0L;

    private int nodesPerGroup, replicaCount, executorCount;

    private Map<Integer, String> executorAddressMap = new HashMap<>();

    public int getGroupId(int serverId) {
        return (serverId - 1) / nodesPerGroup;
    }

    public int getNodesPerGroup() {
        return this.nodesPerGroup;
    }

    @VisibleForTesting
    public InstanceInfo() {}

    public InstanceInfo(ServerDataManager serverDataManager) {
        this.serverDataManager = serverDataManager;
        this.instanceConfig = serverDataManager.instanceConfig;

        this.executorCount = serverDataManager.instanceConfig.getResourceExecutorCount();
        this.replicaCount = instanceConfig.getReplicaCount();
        this.nodesPerGroup = executorCount / replicaCount;

        recoverWorkerId2AliveId();
        recoverExecutorAddress();
    }

    private void logSchedulerEvent(int workerId, LogEvents.ScheduleEvent event, String msg) {
        Logging.schedule(instanceConfig.getGraphName(), event, workerId, msg);
    }

    public void onServerHeartBeat(ServerHBResp.Builder builder, DataStatus serverStatus)
            throws Exception {
        // update status
        setServerStatus(serverStatus);
        updateWorkerExistTimestamp(serverStatus.serverId);
        updateWorkerRegisterTimestamp(serverStatus.serverId);
        ServerAssignment serverAssignment =
                serverDataManager.partitionManager.getServerAssignment(serverStatus.serverId);
        if (serverAssignment != null) {
            builder.addAllPartitions(serverAssignment.getPartitions());
            serverDataManager.runtimeManager.initRuntimeResp(builder, serverStatus);
            LOG.debug("executor id: {}, command is : {}", serverStatus.serverId, builder);
        }
    }

    public void onSimpleHeartBeat(
            RoleType roleType, int workerId, Endpoint endpoint, String logDir) {
        setWorkerInfo(roleType, workerId, endpoint, logDir);
        updateWorkerExistTimestamp(workerId);
        updateWorkerRegisterTimestamp(workerId);
    }

    public void initSimpleHBResponseForFrontend(WorkerInfoProtos.Builder workerInfoBuilder) {
        server2WorkerInfo
                .values()
                .forEach(workerInfo -> workerInfoBuilder.addInfos(workerInfo.toProto()));
    }

    public void getExecutorWorkerInfo(WorkerInfoProtos.Builder workerInfoBuilder) {
        server2WorkerInfo
                .values()
                .forEach(
                        workerInfo -> {
                            if (workerInfo.roleType == RoleType.EXECUTOR) {
                                workerInfoBuilder.addInfos(workerInfo.toProto());
                            }
                        });
    }

    public void updateWorkerExistTimestamp(int workerId) {
        workerId2ExistTime.put(workerId, System.currentTimeMillis());
    }

    public void updateWorkerRegisterTimestamp(int workerId) {
        workerLastRegisterTime.put(workerId, System.currentTimeMillis());
    }

    public Map<Integer, Long> getWorkerId2ExistTime() {
        return workerId2ExistTime;
    }

    public Map<Integer, Long> getWorkerLastRegisterTime() {
        return workerLastRegisterTime;
    }

    public Set<WorkerInfo> getWorkerInfo(RoleType roleType) {
        try (LockWrapper ignore = lock.openReadLock()) {
            return Sets.newHashSet(workerInfoMap.get(roleType));
        }
    }

    public boolean isAllWorkerRunning() {
        try (LockWrapper ignore = lock.openReadLock()) {
            long resourceExecutorCount = instanceConfig.getResourceExecutorCount();
            long resourceIngestNodeCount = instanceConfig.getResourceIngestNodeCount();
            long resourceFrontendCount = instanceConfig.getResourceFrontendCount();

            long eSize =
                    workerInfoMap.get(RoleType.EXECUTOR).stream()
                            .filter(r -> r.workerStatus == WorkerStatus.RUNNING)
                            .count();
            long iSize =
                    workerInfoMap.get(RoleType.INGEST_NODE).stream()
                            .filter(r -> r.workerStatus == WorkerStatus.RUNNING)
                            .count();
            long fSize =
                    workerInfoMap.get(RoleType.FRONTEND).stream()
                            .filter(r -> r.workerStatus == WorkerStatus.RUNNING)
                            .count();

            if (resourceExecutorCount == eSize
                    && resourceIngestNodeCount == iSize
                    && resourceFrontendCount == fSize) {
                return true;
            }

            return false;
        }
    }

    public List<Integer> getCurrentRouting() {
        List<Integer> routingServerIds = new ArrayList<>();

        Set<WorkerInfo> workerInfoSet = getWorkerInfo(RoleType.EXECUTOR);
        Map<Integer, Set<WorkerInfo>> groupWorkerInfo = new HashMap<>();
        for (WorkerInfo workerInfo : workerInfoSet) {
            int serverId = workerInfo.dataStatus.serverId;
            int groupId = getGroupId(serverId);
            Set<WorkerInfo> workerInfos =
                    groupWorkerInfo.computeIfAbsent(groupId, k -> new HashSet<>());
            workerInfos.add(workerInfo);
        }
        for (int i = 0; i < replicaCount; i++) {
            Set<WorkerInfo> workerInfos = groupWorkerInfo.get(i);
            if (workerInfos == null || workerInfos.size() != executorCount / replicaCount) {
                // bad group
                LOG.error(
                        "Invalid group: "
                                + i
                                + ". expected "
                                + executorCount / replicaCount
                                + ", but workers are: "
                                + workerInfos);
                continue;
            }
            boolean badGroup = false;
            for (WorkerInfo workerInfo : workerInfos) {
                if (workerInfo.workerStatus != WorkerStatus.RUNNING) {
                    // bad group
                    LOG.error("Invalid group: " + i + ". WorkerInfo: " + workerInfo);
                    badGroup = true;
                    break;
                }
            }
            if (badGroup) {
                continue;
            }
            routingServerIds.add(i * nodesPerGroup + 1);
        }
        return routingServerIds;
    }

    public InstanceInfoProto.Status getInstanceServingStatus() {
        if (!isAllWorkerRunning()) {
            return ABNORMAL;
        }
        return NORMAL;
    }

    @Override
    public void fromProto(byte[] data) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public InstanceInfoProto toProto() {
        InstanceInfoProto.Builder builder = toSimpleProto();
        return builder.build();
    }

    public InstanceInfoProto.Builder toSimpleProto() {
        InstanceInfoProto.Builder builder = InstanceInfoProto.newBuilder();
        PartitionProtos.Builder pBuilder = PartitionProtos.newBuilder();

        for (Map.Entry<Integer, ServerAssignment> assignmentEntry :
                serverDataManager.partitionManager.assignments.entrySet()) {
            pBuilder.clear();
            pBuilder.addAllPartitionId(assignmentEntry.getValue().getPartitions());
            builder.putAssignment(assignmentEntry.getKey(), pBuilder.build());
        }

        InstanceInfoProto.Status status = getInstanceServingStatus();
        builder.setStatus(status);
        try (LockWrapper ignore = lock.openReadLock()) {
            WorkerInfoProtos.Builder wBuilder = WorkerInfoProtos.newBuilder();
            for (Map.Entry<RoleType, Collection<WorkerInfo>> workerInfo :
                    workerInfoMap.asMap().entrySet()) {
                wBuilder.clear();
                for (WorkerInfo info : workerInfo.getValue()) {
                    wBuilder.addInfos(info.toProto());
                }

                RoleType roleType = workerInfo.getKey();
                builder.putWorkerInfos(roleType.getNumber(), wBuilder.build());
            }
        }
        return builder;
    }

    /**
     * can't modify runtime port when update executor hb
     *
     * @param serverStatus
     */
    public void setServerStatus(DataStatus serverStatus) {

        try (LockWrapper ignore = lock.openWriteLock()) {
            WorkerInfo newWorkerInfo = new WorkerInfo(serverStatus);
            int serverId = serverStatus.serverId;

            if (server2WorkerInfo.containsKey(serverId)) {
                WorkerInfo originWorkerInfo = server2WorkerInfo.get(serverId);
                workerInfoMap.remove(RoleType.EXECUTOR, originWorkerInfo);

                newWorkerInfo.endpoint.setRuntimePort(originWorkerInfo.endpoint.getRuntimePort());
                newWorkerInfo.endpoint.updateIp(serverStatus.endpoint.getIp());

                if (originWorkerInfo.workerStatus == WorkerStatus.LOST
                        || originWorkerInfo.workerStatus == WorkerStatus.RESTARTING) {
                    logSchedulerEvent(
                            serverId,
                            LogEvents.ScheduleEvent.WORKER_RESUME_RUNNING,
                            "worker "
                                    + serverId
                                    + " running again at "
                                    + CommonUtil.getCurrentDate());
                }
            }

            workerInfoMap.put(RoleType.EXECUTOR, newWorkerInfo);
            server2WorkerInfo.put(serverStatus.serverId, newWorkerInfo);
        }
    }

    public void setWorkerInfo(RoleType roleType, int workerId, Endpoint endpoint, String logDir) {
        WorkerInfo workerInfo =
                new WorkerInfo(
                        workerId,
                        endpoint,
                        roleType,
                        WorkerStatus.RUNNING,
                        logDir,
                        System.currentTimeMillis());
        try (LockWrapper ignore = lock.openWriteLock()) {
            workerInfoMap.remove(roleType, workerInfo);
            workerInfoMap.put(roleType, workerInfo);

            if (server2WorkerInfo.containsKey(workerId)) {
                WorkerInfo originWorkerInfo = server2WorkerInfo.get(workerId);
                if (originWorkerInfo.workerStatus == WorkerStatus.LOST
                        || originWorkerInfo.workerStatus == WorkerStatus.RESTARTING) {
                    logSchedulerEvent(
                            workerId,
                            LogEvents.ScheduleEvent.WORKER_RESUME_RUNNING,
                            "worker "
                                    + workerId
                                    + " running again at "
                                    + CommonUtil.getCurrentDate());
                }
            }

            server2WorkerInfo.put(workerId, workerInfo);
        }
    }

    public Pair<Boolean, Long> checkAndUpdateAliveId(
            RoleType roleType, int serverId, long workerAliveId, Endpoint endpoint) {
        synchronized (worker2AliveId) {
            try {
                Pair<Boolean, Long> result;
                if (workerAliveId == INITIAL_WORKER_ALIVE_ID) {
                    if (!roleType.equals(RoleType.EXECUTOR)) {
                        increaseWorkerAliveId(serverId);
                    } else {
                        String curAddress = executorAddressMap.get(serverId);
                        if (curAddress == null || !endpoint.getIp().equals(curAddress)) {
                            increaseWorkerAliveId(serverId);
                        }
                    }
                    Long curAliveId = worker2AliveId.get(serverId);
                    if (curAliveId == null) {
                        throw new NullPointerException("curAliveId in worker2AliveId is null");
                    }
                    result = ImmutablePair.of(true, curAliveId);
                } else {
                    Long curAliveId = worker2AliveId.get(serverId);
                    boolean isLegal = (curAliveId != null && workerAliveId >= curAliveId);
                    Long aliveId = isLegal ? curAliveId : INITIAL_WORKER_ALIVE_ID;
                    result = ImmutablePair.of(isLegal, aliveId);
                }
                return result;
            } catch (Exception e) {
                LOG.error("check and update aliveId fail {}", e);
                return ImmutablePair.of(false, INITIAL_WORKER_ALIVE_ID);
            } finally {
                executorAddressMap.put(serverId, endpoint.getIp());
                try {
                    persistWorkerId2AliveId();
                    persistExecutorAddress();
                } catch (Exception e) {
                    LOG.error("persiste workerId2Alive or executorAddress fail", e);
                }
            }
        }
    }

    private void persistWorkerId2AliveId() throws Exception {
        serverDataManager.namingProxy.persistWorkerAliveIdInfo(worker2AliveId);
    }

    private void recoverWorkerId2AliveId() {
        try {
            worker2AliveId = serverDataManager.namingProxy.getWorkerAliveIdInfo();
        } catch (Exception e) {
            LOG.error("recoverWorkerId2AliveId fail", e);
        }
    }

    private void persistExecutorAddress() throws Exception {
        serverDataManager.namingProxy.persistExecutorAddress(executorAddressMap);
    }

    private void recoverExecutorAddress() {
        try {
            executorAddressMap = serverDataManager.namingProxy.getExecutorAddress();
        } catch (Exception e) {
            LOG.error("recoverExecutorAddress fail", e);
        }
    }

    private void increaseWorkerAliveId(int workerId) throws Exception {
        Long curAliveId = worker2AliveId.get(workerId);
        curAliveId = (curAliveId == null) ? INITIAL_WORKER_ALIVE_ID : curAliveId;
        worker2AliveId.put(workerId, curAliveId + 1);
        LOG.debug("after increase worker2AliveId is {}", worker2AliveId);
    }

    public void updateWorkerStatus(int workerId, WorkerStatus newStatus) {
        try (LockWrapper ignore = lock.openWriteLock()) {
            if (server2WorkerInfo.containsKey(workerId)) {
                WorkerInfo originWorkerInfo = server2WorkerInfo.get(workerId);
                WorkerInfo newWorkerInfo;
                if (originWorkerInfo.roleType == RoleType.EXECUTOR
                        && originWorkerInfo.dataStatus != null) {
                    newWorkerInfo = new WorkerInfo(originWorkerInfo.dataStatus, newStatus);
                } else {
                    newWorkerInfo =
                            new WorkerInfo(
                                    workerId,
                                    originWorkerInfo.endpoint,
                                    originWorkerInfo.roleType,
                                    newStatus,
                                    originWorkerInfo.logDir,
                                    originWorkerInfo.lastReportTime);
                }

                workerInfoMap.remove(originWorkerInfo.roleType, originWorkerInfo);
                workerInfoMap.put(originWorkerInfo.roleType, newWorkerInfo);
                server2WorkerInfo.put(workerId, newWorkerInfo);
            }
        }
    }

    public List<String> updateExecutorRuntimeEnv(int serverId, String ip, int port) {
        // TODO : Avoid to use a mutex write lock to guard a concurrent hash map ???.
        try (LockWrapper ignore = lock.openWriteLock()) {
            if (server2WorkerInfo.containsKey(serverId)) {
                server2WorkerInfo.get(serverId).endpoint.setRuntimePort(port);
                server2WorkerInfo.get(serverId).endpoint.updateIp(ip);
            } else {
                Endpoint endpoint = new Endpoint(ip, -1);
                endpoint.setRuntimePort(port);
                server2WorkerInfo.put(
                        serverId,
                        new WorkerInfo(
                                serverId,
                                endpoint,
                                RoleType.EXECUTOR,
                                WorkerStatus.RUNNING,
                                StringUtils.EMPTY,
                                System.currentTimeMillis()));
            }
        }

        return getRuntimeEnv();
    }

    public void resetRuntimeEnv() {
        try (LockWrapper ignore = lock.openWriteLock()) {
            this.server2WorkerInfo.entrySet().stream()
                    .filter(e -> e.getValue().roleType == RoleType.EXECUTOR)
                    .forEach(e -> e.getValue().endpoint.setRuntimePort(-1));
        }
    }

    public List<String> getRuntimeEnv() {
        try (LockWrapper ignore = lock.openReadLock()) {
            return this.server2WorkerInfo.entrySet().stream()
                    .filter(
                            e ->
                                    e.getValue().roleType == RoleType.EXECUTOR
                                            && e.getValue().endpoint.getRuntimePort() != -1)
                    .map(
                            e -> {
                                Endpoint endpoint = e.getValue().endpoint;
                                String env =
                                        Joiner.on(':')
                                                .join(endpoint.getIp(), endpoint.getRuntimePort());
                                return Pair.of(e.getKey(), env);
                            })
                    .sorted(Comparator.comparing(Pair::getLeft))
                    .map(Pair::getRight)
                    .collect(Collectors.toList());
        }
    }

    public void setTimelyRuntimePortUnavailable() {
        try (LockWrapper ignore = lock.openWriteLock()) {
            for (Map.Entry<Integer, WorkerInfo> entry : server2WorkerInfo.entrySet()) {
                if (entry.getValue().roleType == RoleType.EXECUTOR) {
                    entry.getValue().endpoint.setRuntimePort(-1);
                }
            }
        }
    }

    public boolean isDataPathInUse(int serverId, long aliveId) {
        try (LockWrapper ignore = lock.openWriteLock()) {
            // return workerId2AliveId.containsKey(serverId) && workerId2AliveId.get(serverId).get()
            // == aliveId;
            Long curAliveId = worker2AliveId.get(serverId);
            return curAliveId != null && aliveId >= curAliveId;
        }
    }

    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    public void setInstanceConfig(InstanceConfig instanceConfig) {
        this.instanceConfig = instanceConfig;
    }
}
