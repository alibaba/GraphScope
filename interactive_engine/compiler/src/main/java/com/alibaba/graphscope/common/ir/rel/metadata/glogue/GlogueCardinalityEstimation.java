package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

public interface GlogueCardinalityEstimation {

    public Double getCardinality(Pattern pattern);
}
