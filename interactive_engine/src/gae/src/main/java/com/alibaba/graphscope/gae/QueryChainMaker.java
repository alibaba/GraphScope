package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gae.parser.Generator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

public interface QueryChainMaker extends Generator {
    boolean isValid(Traversal query);
}
