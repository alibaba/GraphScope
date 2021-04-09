package com.alibaba.maxgraph.tests.gremlin;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = MaxTestGraphProvider.class, graph = MaxTestGraph.class)
public class GremlinStandardTest {
}
