package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;

import java.util.Set;

public class GlogueQuery {
    private Glogue glogue;

    public GlogueQuery(Glogue glogue) {
        this.glogue = glogue;
    }

    /**
     * Get in edges for the given pattern
     * @param pattern
     * @return
     */
    public Set<GlogueEdge> getInEdges(Pattern pattern) {
        return glogue.getInEdges(pattern);
    }

    /**
     * Get out edges for the given pattern
     * @param pattern
     * @return
     */
    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        return glogue.getOutEdges(pattern);
    }

    /**
     * get pattern count
     * @param pattern
     * @return
     */
    public Double getRowCount(Pattern pattern) {
        return glogue.getRowCount(pattern);
    }

    /**
     * get the max size of the preserved pattern
     * @return
     */
    public int getMaxPatternSize() {
        return glogue.getMaxPatternSize();
    }

    @Override
    public String toString() {
        return glogue.toString();
    }
}
