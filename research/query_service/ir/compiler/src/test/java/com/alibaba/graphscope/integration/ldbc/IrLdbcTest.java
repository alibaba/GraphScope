package com.alibaba.graphscope.integration.ldbc;

import com.alibaba.graphscope.integration.graph.RemoteTestGraph;
import com.alibaba.graphscope.integration.graph.RemoteTestGraphProvider;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(IrLdbcTestSuite.class)
@GraphProviderClass(provider = RemoteTestGraphProvider.class, graph = RemoteTestGraph.class)
public class IrLdbcTest {}
