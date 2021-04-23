package com.alibaba.maxgraph.v2.frontend.server.loader;

import org.apache.tinkerpop.gremlin.server.Settings;

/**
 * Interface of maxgraph processor loader
 */
public interface ProcessorLoader {
    void loadProcessor(Settings settings);
}
