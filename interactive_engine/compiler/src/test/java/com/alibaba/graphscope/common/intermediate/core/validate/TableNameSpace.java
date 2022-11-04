package com.alibaba.graphscope.common.intermediate.core.validate;

import com.alibaba.graphscope.common.intermediate.core.IrIdentifier;

import org.apache.calcite.rel.type.RelRecordType;

/**
 * maintain meta of a specific scan table.
 */
public class TableNameSpace {
    private IrIdentifier tableName;
    private TableOpt tableOpt; // indicate Entity(Vertex) or Relation(Edge)
    // consisting a list of RelDataTypeField<name, RelDataType>:
    // for label, we have a RelDataTypeField<labelName, IrLabelType extends RelDataType>
    // for each attribute, we have a RelDataTypeField<attributeName, IrBasicType extends
    // RelDataType>
    private RelRecordType tableMeta;
}
