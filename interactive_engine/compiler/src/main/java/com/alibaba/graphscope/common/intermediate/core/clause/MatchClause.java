package com.alibaba.graphscope.common.intermediate.core.clause;

import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

public class MatchClause extends AbstractClause {
    private List<MatchSentence> sentences;

    /**
     * @param sentence
     * @return
     */
    public MatchClause addMatchSentence(MatchSentence sentence) {
        throw new NotImplementedException("");
    }
}
