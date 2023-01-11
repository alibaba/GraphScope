package com.alibaba.graphscope.common.intermediate.calcite.clause;

import com.alibaba.graphscope.common.intermediate.calcite.IrExpandOperator;
import com.alibaba.graphscope.common.intermediate.calcite.clause.type.ExpandConfig;
import com.alibaba.graphscope.common.intermediate.calcite.clause.type.GetVConfig;
import com.alibaba.graphscope.common.intermediate.calcite.clause.type.ScanConfig;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.NotImplementedException;

/**
 * similar with {@code From} clause in Sql, maintain a group of scan tables, each scan table can be an entity or a relation,
 * i.e. for gremlin query {@code g.V().hasLabel("person").outE("knows")}, table("person") is an entity while table("knows") is a relation.
 * we use {@link IrExpandOperator} to denote relationships between each pair of entity and relation,
 * i.e. for the query above, we have the following tree structure:
 *             IrExpandOperator(OUT)
 *             /                   \
 *  IrIdentifier(["person"])   IrIdentifier(["knows"])
 * thus, all scan entities and relationships can be maintained in {@link #sentence} which is a tree structure.
 */
public class MatchSentence {
    private SqlNode sentence;
    /**
     * create a new {@code SqlIdentifier} for the table and set it as {@link #sentence}
     * @param config
     * @return
     */
    public MatchClause addScanTable(ScanConfig config) {
        throw new NotImplementedException("");
    }

    /**
     * create a new {@code SqlIdentifier} for the table and add it to {@link #sentence}
     * @param config
     * @return
     */
    public MatchClause addExpandTable(ExpandConfig config) {
        throw new NotImplementedException("");
    }

    /**
     * create a new {@code SqlIdentifier} for the table and add it to {@link #sentence}
     * @param config
     * @return
     */
    public MatchClause addGetVTable(GetVConfig config) {
        throw new NotImplementedException("");
    }
}
