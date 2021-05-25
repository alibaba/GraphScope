package com.alibaba.maxgraph.tests.gremlin;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(GremlinStandardTestSuite.class)
@GraphProviderClass(provider = MaxTestGraphProvider.class, graph = MaxTestGraph.class)
public class GremlinStandardTest {
}
