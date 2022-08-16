package com.alibaba.graphscope.gremlin.plugin.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.List;

public interface MultiByModulating {
    void modulateBy(final List<Traversal.Admin<?, ?>> kvTraversals);
}
