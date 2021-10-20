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
package com.alibaba.maxgraph.coordinator.manager;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.logging.LogEvents;
import com.alibaba.maxgraph.logging.Logging;
import com.alibaba.maxgraph.proto.WorkerStatus;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerManager.class);

    private ScheduledExecutorService workerTimeoutMonitor;
    private ServerDataManager serverDataManager;

    private InstanceConfig instanceConfig;

    public WorkerManager(InstanceConfig instanceConfig, ServerDataManager serverDataManager) {
        this.instanceConfig = instanceConfig;
        this.serverDataManager = serverDataManager;
        startWorkerTimeoutMonitorThread();
    }

    private WorkerStatus getWorkerStatusById(int workerId) {
        return serverDataManager.instanceInfo.server2WorkerInfo.get(workerId).workerStatus;
    }

    private void updateWorkerStatusById(int workerId, WorkerStatus workerStatus) {
        serverDataManager.instanceInfo.updateWorkerStatus(workerId, workerStatus);
    }

    private void  checkAllWorkerExistTime() {
        serverDataManager.instanceInfo.getWorkerId2ExistTime().forEach((key, value) -> {
            Long workerExistTime = System.currentTimeMillis() - value;
            LOG.info("worker {} exist time:{}", key, workerExistTime);
            if (getWorkerStatusById(key) == WorkerStatus.RUNNING &&
                    workerExistTime > instanceConfig.getWorkerHBTimeoutSeconds() * 1000) {
                updateWorkerStatusById(key, WorkerStatus.LOST);

                Logging.schedule(instanceConfig.getGraphName(),
                        LogEvents.ScheduleEvent.WORKER_HB_TIMEOUT, key,
                        "worker has no hb " + workerExistTime + "ms");
            }
        });
    }

    private void startWorkerTimeoutMonitorThread() {
        workerTimeoutMonitor = new ScheduledThreadPoolExecutor(1,
                CommonUtil.createFactoryWithDefaultExceptionHandler("TIMEOUT_MONITOR", LOG));
        workerTimeoutMonitor.scheduleWithFixedDelay(this::checkAllWorkerExistTime, 1, 3, TimeUnit.SECONDS);
    }

    public Set<Integer> findWorkerIdSetByStatus(WorkerStatus workerStatus) {
        return serverDataManager.instanceInfo.server2WorkerInfo.entrySet().stream()
                .filter(entry -> entry.getValue().workerStatus == workerStatus)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public void setWorkerStatusRestart(int workerId) {
        serverDataManager.instanceInfo.updateWorkerStatus(workerId, WorkerStatus.RESTARTING);
        serverDataManager.instanceInfo.updateWorkerRegisterTimestamp(workerId);
    }

    public Set<Integer> getNeedRestartUnregisteredWorkerIdSet(Set<Integer> totalWorkerIdSet) {
        Set<Integer> unregisterWorkerIdSet = Sets.difference(totalWorkerIdSet, serverDataManager.instanceInfo.server2WorkerInfo.keySet());
        Set<Integer> restartingWorkerIdSet = serverDataManager.instanceInfo.server2WorkerInfo.entrySet().stream()
                .filter(entry -> entry.getValue().workerStatus == WorkerStatus.RESTARTING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        restartingWorkerIdSet.addAll(unregisterWorkerIdSet);

        Set<Integer> needRestartIdSet = Sets.newHashSet();
        restartingWorkerIdSet.forEach(id -> {
            if (serverDataManager.instanceInfo.getWorkerLastRegisterTime().containsKey(id)) {
                long timeCost = System.currentTimeMillis() - serverDataManager.instanceInfo.getWorkerLastRegisterTime().get(id);
                LOG.info("worker id {} not register time cost:{}", id, timeCost);
                if (timeCost > instanceConfig.getWorkerUnregisteredTimeoutSeconds() * 1000L) {
                    needRestartIdSet.add(id);
                    serverDataManager.instanceInfo.updateWorkerRegisterTimestamp(id);

                    Logging.schedule(instanceConfig.getGraphName(),
                            LogEvents.ScheduleEvent.WORKER_REG_TIMEOUT, id,
                            "worker not register in " + timeCost + "ms");
                }
            } else {
                serverDataManager.instanceInfo.updateWorkerRegisterTimestamp(id);
            }
        });

        return needRestartIdSet;
    }
}
