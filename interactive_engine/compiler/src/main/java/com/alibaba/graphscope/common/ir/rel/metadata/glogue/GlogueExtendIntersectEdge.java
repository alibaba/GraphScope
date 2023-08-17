package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.HashMap;
import java.util.Map;

public class GlogueExtendIntersectEdge extends GlogueEdge{
    Map<Integer, Integer> srcToTargetIdMapping; 
    ExtendStep extendStep;

    public GlogueExtendIntersectEdge(ExtendStep extendStep) {
        this.srcToTargetIdMapping = new HashMap<>();
        this.extendStep = extendStep;
    }

    @Override
    public String toString() {
        return extendStep.toString();
    }
}
