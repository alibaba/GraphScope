package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.graph.pattern.PatternCode;

public interface GlogueCardinalityEstimation {

    public double getCardinality(PatternCode patternCode);
    
}
