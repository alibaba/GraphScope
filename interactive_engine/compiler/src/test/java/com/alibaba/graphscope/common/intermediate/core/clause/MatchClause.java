package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.IrNode;
import com.alibaba.graphscope.common.intermediate.core.IrOperator;
import com.alibaba.graphscope.common.intermediate.core.IrOperatorKind;
import com.alibaba.graphscope.common.intermediate.core.clause.type.ExtendConfig;
import com.alibaba.graphscope.common.intermediate.core.clause.type.SourceConfig;
import com.alibaba.graphscope.common.intermediate.core.validate.IrValidatorScope;
import com.alibaba.graphscope.common.intermediate.core.validate.TableNameSpace;

import org.apache.commons.lang.NotImplementedException;

/**
 * similar with {@code From} clause in Sql, maintain a group of scan tables, each scan table can be an entity or a relation,
 * i.e. for gremlin query {@code g.V().hasLabel("person").outE("knows")}, table("person") is an entity while table("knows") is a relation.
 * we use {@link IrOperator} with the kind of {@link IrOperatorKind#EXTEND_JOIN} to denote relationships between each pair of entity and relation,
 * i.e. for the query above, we have the following tree structure:
 *  IrOperator(EXTEND_JOIN, V_E, OUT)
 *        /              \
 *  table("person")   table("knows")
 * thus, all scan tables and relationships can be maintained in {@link #tables} which is a tree structure.
 */
public class MatchClause extends AbstractClause {
    private IrNode tables;
    private IrValidatorScope scope;

    /**
     * create a new {@code IrIdentifier} for the table and set it as {@link #tables},
     * register this table with alias by invoking {@link IrValidatorScope#registerNameSpace(String, TableNameSpace)}.
     * @param config
     * @return
     */
    public MatchClause addSourceTable(SourceConfig config) {
        throw new NotImplementedException();
    }

    /**
     * create a new {@code IrIdentifier} for the table and add it to {@link #tables},
     * register this table with alias by invoking {@link IrValidatorScope#registerNameSpace(String, TableNameSpace)}.
     * @param config
     * @return
     */
    public MatchClause addExtendTable(ExtendConfig config) {
        throw new NotImplementedException();
    }
}
