package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

public class GlogueExtendIntersectEdge extends GlogueEdge{
    // a mapping from src pattern vertex id to target pattern vertex id
    private Map<Integer, Integer> srcToTargetIdMapping; 
    private ExtendStep extendStep;
    private Pattern srcPattern;
    private Pattern dstPattern;

    public GlogueExtendIntersectEdge(Pattern srcPattern, Pattern dstPattern, ExtendStep extendStep, Map<Integer, Integer> srcToTargetIdMapping) {
        this.extendStep = extendStep;
        this.srcPattern = srcPattern;
        this.dstPattern = dstPattern;
        this.srcToTargetIdMapping = srcToTargetIdMapping;
    }

    public Pattern getSrcPattern() {
        return srcPattern;
    }

    public Pattern getDstPattern() {
        return dstPattern;
    }

    public ExtendStep getExtendStep() {
        return extendStep;
    }

    @Override
    public String toString() {
        return "ExtendIntersectEdge{" +
                "extendStep=" + extendStep +
                ", srcToTargetIdMapping=" + srcToTargetIdMapping +
                '}';
    }
}
