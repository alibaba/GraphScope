package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraphProvider;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(IrGremlinTestSuite.class)
@GraphProviderClass(provider = RemoteTestGraphProvider.class, graph = RemoteTestGraph.class)
public class IrGremlinTest {}
