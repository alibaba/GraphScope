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
package com.alibaba.maxgraph.coordinator;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Author beimian
 * 2018/05/02
 */
public class CoordinatorMain {
    private static Logger LOG = LoggerFactory.getLogger(CoordinatorMain.class);
    /**
     * Start Coordinator service through arguments;
     *
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        InstanceConfig instanceConfig = CommonUtil.getInstanceConfig(args, 0);
        System.setProperty(InstanceConfig.JUTE_MAXBUFFER, instanceConfig.getJuteMaxbuffer());
        Coordinator coordinator = new Coordinator(instanceConfig);
        coordinator.start();

        CountDownLatch shutdown = new CountDownLatch(1);
        coordinator.waitShutdown(shutdown);
    }
}
