package com.alibaba.maxgraph.v2.frontend.compiler.tree.addition;

import java.util.Set;

public interface PropertyNode {
    Set<String> getPropKeyList();

    boolean edgePropFlag();
}
