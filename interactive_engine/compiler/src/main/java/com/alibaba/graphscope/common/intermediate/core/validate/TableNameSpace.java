package com.alibaba.graphscope.common.intermediate.core.validate;

import com.alibaba.graphscope.common.intermediate.core.IrNode;
import com.alibaba.graphscope.common.intermediate.core.type.IrSchemaType;

/**
 * maintain meta for a specific table or a group of tables.
 *
 * {@link #identifier} is the identifier to get tables from a global view of all tables,
 * i.e. Identifier(["PERSON"]), IrNodeList(IrIdentifier(["PERSON"]), IrIdentifier(["SOFTWARE"])),
 * especially, Identifier(["*"]) represents all tables.
 *
 * {@link #schemaType} having schema for each table in {@link #identifier}.
 */
public class TableNameSpace {
    private IrNode identifier;
    private IrSchemaType schemaType;
}
