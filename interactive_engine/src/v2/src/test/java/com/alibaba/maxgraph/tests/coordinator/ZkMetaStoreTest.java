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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.coordinator.ZkMetaStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ZkMetaStoreTest {

    @Test
    void testMetaStore() throws Exception {
        try (TestingServer testingServer = new TestingServer(-1)) {
            int zkPort = testingServer.getPort();
            Configs configs =
                    Configs.newBuilder()
                            .put(ZkConfig.ZK_CONNECT_STRING.getKey(), "localhost:" + zkPort)
                            .put(ZkConfig.ZK_BASE_PATH.getKey(), "test_meta_store")
                            .build();
            CuratorFramework curator = CuratorUtils.makeCurator(configs);
            curator.start();
            MetaStore metaStore = new ZkMetaStore(configs, curator);
            String path = "test_path";
            String data = "test_data";

            assertFalse(metaStore.exists(path));
            metaStore.write(path, data.getBytes());
            assertTrue(metaStore.exists(path));
            assertEquals(new String(metaStore.read(path)), data);
            metaStore.delete(path);
            assertFalse(metaStore.exists(path));
            curator.close();
        }
    }
}
