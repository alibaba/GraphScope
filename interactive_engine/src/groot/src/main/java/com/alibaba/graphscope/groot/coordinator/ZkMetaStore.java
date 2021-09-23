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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZkMetaStore implements MetaStore {
    private static final Logger logger = LoggerFactory.getLogger(ZkMetaStore.class);
    public static final String ROOT_NODE = "meta";

    private CuratorFramework curator;
    private String metaBasePath;

    public ZkMetaStore(Configs configs, CuratorFramework curator) {
        this(curator, ZkConfig.ZK_BASE_PATH.get(configs));
    }

    public ZkMetaStore(CuratorFramework curator, String basePath) {
        this.curator = curator;
        this.metaBasePath = ZKPaths.makePath(basePath, ROOT_NODE);
    }

    @Override
    public boolean exists(String path) {
        String fullPath = ZKPaths.makePath(this.metaBasePath, path);
        Stat stat;
        try {
            stat = this.curator.checkExists().forPath(fullPath);
        } catch (Exception e) {
            throw new MaxGraphException(e);
        }
        return stat != null;
    }

    @Override
    public byte[] read(String path) throws IOException {
        String fullPath = ZKPaths.makePath(this.metaBasePath, path);
        byte[] data;
        try {
            data = this.curator.getData().forPath(fullPath);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return data;
    }

    @Override
    public void write(String path, byte[] content) throws IOException {
        String fullPath = ZKPaths.makePath(this.metaBasePath, path);
        try {
            if (exists(path)) {
                this.curator.setData().forPath(fullPath, content);
            } else {
                this.curator.create().creatingParentsIfNeeded().forPath(fullPath, content);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        String fullPath = ZKPaths.makePath(this.metaBasePath, path);
        try {
            this.curator.delete().forPath(fullPath);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
