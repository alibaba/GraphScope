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
import com.alibaba.maxgraph.common.component.AbstractComponent;
import com.alibaba.maxgraph.common.zookeeper.ZKPaths;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import com.alibaba.maxgraph.coordinator.LoggerStore;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;

public class ZkLoggerStore extends AbstractComponent implements LoggerStore {

    private ZkUtils zkUtils;
    private String graphName;

    public ZkLoggerStore(InstanceConfig settings, ZkUtils zkUtils) {
        super(settings);
        this.zkUtils = zkUtils;
        this.graphName = settings.getGraphName();
    }

    @Override
    public byte[] read(String subPath) throws IOException {
        String path = ZKPaths.getLoggerStorePath(graphName) + "/" + subPath;
        try {
            return zkUtils.readBinaryData(path);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(String subPath, byte[] content) throws IOException {
        String path = ZKPaths.getLoggerStorePath(graphName) + "/" + subPath;
        try {
            zkUtils.createOrUpdatePath(path, content, CreateMode.PERSISTENT);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public boolean pathExists(String subPath) {
        String path = ZKPaths.getLoggerStorePath(graphName) + "/" + subPath;
        return this.zkUtils.pathExists(path);
    }
}
