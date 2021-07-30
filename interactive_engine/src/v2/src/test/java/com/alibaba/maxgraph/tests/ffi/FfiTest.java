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

package com.alibaba.maxgraph.tests.ffi;

import com.alibaba.maxgraph.tests.gremlin.MaxTestGraph;
import com.alibaba.maxgraph.tests.gremlin.MaxTestGraphProvider;
import com.alibaba.maxgraph.v2.MaxNode;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.store.Store;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.jna.GraphLibrary;
import com.alibaba.maxgraph.v2.store.jna.JnaGraphStore;
import com.sun.jna.Pointer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FfiTest {

    @Test
    public void testFfi() throws Exception {
        GraphProvider provider = new MaxTestGraphProvider();
        LoadGraphWith.GraphData modern = LoadGraphWith.GraphData.MODERN;
        Map<String, Object> conf = new HashMap<>();
        conf.put(CommonConfig.STORE_NODE_COUNT.getKey(), "1");
        conf.put(CommonConfig.PARTITION_COUNT.getKey(), "1");
        Configuration graphConf = provider.newGraphConfiguration("ffi-test", FfiTest.class, "testFfi", conf, modern);
        provider.clear(graphConf);
        Graph graph = provider.openTestGraph(graphConf);
        LoadGraphWith loadGraphWith = new LoadGraphWith() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }
            @Override
            public GraphData value() {
                return modern;
            }
        };
        provider.loadGraphData(graph, loadGraphWith, FfiTest.class, "testFfi");
        MaxNode maxNode = ((MaxTestGraph) graph).getMaxNode();
        List<NodeBase> storeNodes = maxNode.getStores();
        assertEquals(storeNodes.size(), 1);
        Store store = (Store) storeNodes.get(0);
        StoreService storeService = store.getStoreService();
        JnaGraphStore jnaGraphStore = (JnaGraphStore) storeService.getIdToPartition().get(0);
        Pointer wrapperPartitionGraph = GraphLibrary.INSTANCE.createWrapperPartitionGraph(jnaGraphStore.getPointer());
        GnnLibrary.INSTANCE.setPartitionGraph(wrapperPartitionGraph);
        GnnLibrary.INSTANCE.runLocalTests();
        provider.clear(graph, graphConf);
        maxNode.close();
    }
}
