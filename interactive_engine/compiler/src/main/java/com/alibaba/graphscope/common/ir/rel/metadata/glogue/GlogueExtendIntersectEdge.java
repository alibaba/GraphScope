package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

import java.util.Map;

public class GlogueExtendIntersectEdge extends GlogueEdge {
    /// a mapping from src pattern vertex order to target pattern vertex order
    private Map<Integer, Integer> srcToTargetOrderMapping;
    /// a extend step records how to extend the src pattern to the target pattern
    private ExtendStep extendStep;
    /// the src pattern
    private Pattern srcPattern;
    /// the target pattern
    private Pattern dstPattern;

    public GlogueExtendIntersectEdge(
            Pattern srcPattern,
            Pattern dstPattern,
            ExtendStep extendStep,
            Map<Integer, Integer> srcToTargetOrderMapping) {
        this.extendStep = extendStep;
        this.srcPattern = srcPattern;
        this.dstPattern = dstPattern;
        this.srcToTargetOrderMapping = srcToTargetOrderMapping;
    }

    public Map<Integer, Integer> getSrcToTargetOrderMapping() {
        return srcToTargetOrderMapping;
    }

    @Override
    public Pattern getSrcPattern() {
        return srcPattern;
    }

    @Override
    public Pattern getDstPattern() {
        return dstPattern;
    }

    public ExtendStep getExtendStep() {
        return extendStep;
    }

    @Override
    public String toString() {
        return "ExtendIntersectEdge{"
                + "srcPattern="
                + srcPattern.getPatternId()
                + ", dstPattern="
                + dstPattern.getPatternId()
                + ", extendStep="
                + extendStep
                + ", srcToTargetIdMapping="
                + srcToTargetOrderMapping
                + '}';
    }
}
