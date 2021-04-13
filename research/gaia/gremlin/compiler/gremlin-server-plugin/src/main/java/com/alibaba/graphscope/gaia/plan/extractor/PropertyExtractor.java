package com.alibaba.graphscope.gaia.plan.extractor;

import org.apache.tinkerpop.gremlin.process.traversal.Step;

import java.util.List;

public interface PropertyExtractor {
    List<String> extractProperties(Step step);
}
