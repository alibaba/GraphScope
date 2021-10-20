/*
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
package com.alibaba.graphscope.gaia.vineyard.store;

import com.alibaba.maxgraph.common.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * the entrance for frontend service
 *
 */
public class FrontendServiceMain {
    private static final Logger LOG = LoggerFactory.getLogger(FrontendServiceMain.class);

    public static void main(String[] args) {
        try {
            LOG.info("start to run FrontendServiceMain.");
            Frontend frontend = new Frontend(CommonUtil.getInstanceConfig(args, 101));
            frontend.start();
            CountDownLatch shutdown = new CountDownLatch(1);
            shutdown.await();
        } catch (Throwable t) {
            LOG.error("Error in worker main:", t);
            System.exit(1);
        }

        System.exit(0);
    }
}
