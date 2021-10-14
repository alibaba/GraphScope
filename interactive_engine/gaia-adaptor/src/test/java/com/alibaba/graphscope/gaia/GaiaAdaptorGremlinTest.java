package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(GaiaAdaptorGremlinTestSuite.class)
@GraphProviderClass(provider = GaiaAdaptorTestGraphProvider.class, graph = GaiaAdaptorTestGraph.class)
public class GaiaAdaptorGremlinTest {
}
