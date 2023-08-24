package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternOrdering;

public interface GlogueCardinalityEstimation {

    public double getCardinality(PatternOrdering patternCode);
    
}
