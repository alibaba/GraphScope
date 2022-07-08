package com.alibaba.graphscope;

import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraphProvider;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(com.alibaba.graphscope.IrGremlinTestSuite.class)
@GraphProviderClass(provider = RemoteTestGraphProvider.class, graph = RemoteTestGraph.class)
public class IrGremlinTest {}
