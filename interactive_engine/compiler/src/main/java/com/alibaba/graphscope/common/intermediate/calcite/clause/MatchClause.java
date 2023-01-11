package com.alibaba.graphscope.common.intermediate.calcite.clause;

import org.apache.calcite.sql.AbstractClause;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
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

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}
}
