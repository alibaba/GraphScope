package com.alibaba.maxgraph.v2.frontend.server.gremlin.loader;

import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.server.OpProcessor;

import java.util.Map;
import java.util.Optional;

/**
 * Load maxgraph related processor
 */
public class MaxGraphOpLoader {
    private static Map<String, OpProcessor> processors = Maps.newHashMap();

    public static Optional<OpProcessor> getProcessor(String name) {
        return Optional.ofNullable(processors.get(name));
    }

    public static void addOpProcessor(String name, OpProcessor opProcessor) {
        processors.put(name, opProcessor);
    }
}
