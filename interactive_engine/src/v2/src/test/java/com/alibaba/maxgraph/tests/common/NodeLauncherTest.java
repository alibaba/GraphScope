package com.alibaba.maxgraph.tests.common;

import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeLauncherTest {

    @Test
    void testNodeLauncher() {
        NodeBase nodeBase = mock(NodeBase.class);
        NodeLauncher nodeLauncher = new NodeLauncher(nodeBase);
        nodeLauncher.start();
        verify(nodeBase).start();
    }
}
