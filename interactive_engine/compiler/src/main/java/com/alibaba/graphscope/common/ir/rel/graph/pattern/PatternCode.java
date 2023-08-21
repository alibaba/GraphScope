package com.alibaba.graphscope.common.ir.rel.graph.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;


public class PatternCode {
    // TODO: designed to be a canonical form of patternGraph
    private Pattern pattern;

    public PatternCode(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public String toString() {
        return pattern.toString();
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PatternCode) {
            PatternCode other = (PatternCode) obj;
            return this.pattern.equals(other.pattern);
        }
        return false;
    }
}
