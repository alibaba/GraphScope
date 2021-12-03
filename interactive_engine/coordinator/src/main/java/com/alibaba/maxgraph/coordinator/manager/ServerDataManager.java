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

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.maxgraph.common.client.WorkerInfo;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.cluster.management.ClusterApplierService;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.common.zookeeper.ZkNamingProxy;
import com.alibaba.maxgraph.coordinator.manager.runtime.PegasusRuntimeManager;
import com.alibaba.maxgraph.coordinator.manager.runtime.RuntimeManager;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-07 下午3:22
 */
public class ServerDataManager {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDataManager.class);

    public final InstanceConfig instanceConfig;
    public final ZkNamingProxy namingProxy;
    public final PartitionManager partitionManager;
    public final InstanceInfo instanceInfo;
    public final RuntimeManager runtimeManager;
    private ScheduledThreadPoolExecutor frontEndUpdateSchedule;
    private Map<Integer, Endpoint> endpointMap = Maps.newHashMap();
    private final String vpcEndpoint;

    public ServerDataManager(
            InstanceConfig instanceConfig,
            ZkNamingProxy namingProxy,
            ClusterApplierService clusterApplierService) {
        this.instanceConfig = instanceConfig;
        this.namingProxy = namingProxy;
        this.partitionManager = new PartitionManager(this);
        this.instanceInfo = new InstanceInfo(this);
        this.runtimeManager = new PegasusRuntimeManager(this, clusterApplierService);
        this.vpcEndpoint = instanceConfig.getVpcEndpoint();
    }

    public void start() throws Exception {
        // start and recover
        this.partitionManager.start();
        this.runtimeManager.init();
        startUpdateThread();
    }

    public void stop() {
        // stop everything
        frontEndUpdateSchedule.shutdownNow();
        this.runtimeManager.close();
    }

    private void startUpdateThread() {
        frontEndUpdateSchedule =
                new ScheduledThreadPoolExecutor(
                        1,
                        CommonUtil.createFactoryWithDefaultExceptionHandler(
                                "FrontEndUpdate_THREAD", LOG));
        frontEndUpdateSchedule.scheduleWithFixedDelay(
                () -> {
                    Map<Integer, Endpoint> endpointMap = Maps.newHashMap();
                    for (WorkerInfo info : instanceInfo.getWorkerInfo(RoleType.FRONTEND)) {
                        endpointMap.put(info.id, info.endpoint);
                    }
                    if (!endpointMap.equals(this.endpointMap)) {
                        this.endpointMap = endpointMap;
                        StringBuilder sb = new StringBuilder();
                        for (Endpoint endpoint : this.endpointMap.values()) {
                            sb.append(endpoint.getIp())
                                    .append(":")
                                    .append(endpoint.getGremlinServerPort())
                                    .append(" ");
                        }
                        String text = StringUtils.removeEnd(sb.toString(), " ");
                        try {
                            namingProxy.persistFrontEndEndpoints(vpcEndpoint, text);
                        } catch (Exception e) {
                            LOG.error("persist frontend ips error", e);
                            LOG.error("front ips info: {}", text);
                        }
                    }
                },
                1,
                instanceConfig.getFrontendAmHbIntervalSeconds(),
                TimeUnit.SECONDS);
    }
}
