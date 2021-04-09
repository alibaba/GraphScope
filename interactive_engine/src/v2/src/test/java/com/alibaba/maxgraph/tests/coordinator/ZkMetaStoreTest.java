package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.ZkConfig;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.coordinator.MetaStore;
import com.alibaba.maxgraph.v2.coordinator.ZkMetaStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ZkMetaStoreTest {

    @Test
    void testMetaStore() throws Exception {
        try (TestingServer testingServer = new TestingServer(-1)) {
            int zkPort = testingServer.getPort();
            Configs configs = Configs.newBuilder()
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
