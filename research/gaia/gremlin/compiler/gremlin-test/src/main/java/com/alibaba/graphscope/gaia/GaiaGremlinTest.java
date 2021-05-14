package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(GaiaGremlinTestSuite.class)
@GraphProviderClass(provider = RemoteTestGraphProvider.class, graph = RemoteTestGraph.class)
public class GaiaGremlinTest {
}
