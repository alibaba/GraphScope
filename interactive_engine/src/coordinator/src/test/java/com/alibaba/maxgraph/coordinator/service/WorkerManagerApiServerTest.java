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
package com.alibaba.maxgraph.coordinator.service;

import java.util.List;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.coordinator.manager.InstanceInfo;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.proto.RuntimeEnvList;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

public class WorkerManagerApiServerTest {

    @Test
    public void testReadAndWriteRuntimeEnvs() {
        String logDir = "";
        InstanceInfo instanceInfo = new InstanceInfo();
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR, 0, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR, 1, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,2, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,3, new Endpoint("127.0.0.1", 1, 2), logDir);

        List<String> envs = instanceInfo.updateExecutorRuntimeEnv(0, "127.0.0.1", 3);
        Assert.assertEquals(envs, Lists.newArrayList("127.0.0.1:3", "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"));

        instanceInfo.updateExecutorRuntimeEnv(1, "127.0.0.1", 4);
        instanceInfo.updateExecutorRuntimeEnv(2, "127.0.0.1", 5);
        instanceInfo.updateExecutorRuntimeEnv(3, "127.0.0.1", 6);
        List<String> envs2 = instanceInfo.updateExecutorRuntimeEnv(4, "127.0.0.1", 7);

        Assert.assertEquals(envs2, Lists.newArrayList("127.0.0.1:3", "127.0.0.1:4", "127.0.0.1:5", "127.0.0.1:6", "127.0.0.1:7"));

        List<String> envs3 = instanceInfo.getRuntimeEnv();
        Assert.assertEquals(envs3, Lists.newArrayList("127.0.0.1:3", "127.0.0.1:4", "127.0.0.1:5", "127.0.0.1:6", "127.0.0.1:7"));
    }

    @Test
    public void testReadRuntimeEnvsFromBinary() throws InvalidProtocolBufferException {
        String logDir = "";
        InstanceInfo instanceInfo = new InstanceInfo();
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,0, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,1, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,2, new Endpoint("127.0.0.1", 1, 2), logDir);
        instanceInfo.setWorkerInfo(RoleType.EXECUTOR,3, new Endpoint("127.0.0.1", 1, 2), logDir);

        instanceInfo.updateExecutorRuntimeEnv(0, "127.0.0.1", 3);
        instanceInfo.updateExecutorRuntimeEnv(1, "127.0.0.1", 4);
        instanceInfo.updateExecutorRuntimeEnv(2, "127.0.0.1", 5);
        List<String> envs = instanceInfo.updateExecutorRuntimeEnv(3, "127.0.0.1", 6);

        RuntimeEnvList.Builder builder = RuntimeEnvList.newBuilder();
        envs.forEach(builder::addEnvs);

        byte[] binary = builder.build().toByteArray();

        RuntimeEnvList envList = RuntimeEnvList.parseFrom(binary);
        List<String> envs2 = Lists.newArrayList(envList.getEnvsList().iterator());
        Assert.assertEquals(envs2, Lists.newArrayList("127.0.0.1:3", "127.0.0.1:4", "127.0.0.1:5", "127.0.0.1:6"));
    }
}
