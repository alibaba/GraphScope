package com.alibaba.graphscope.common.intermediate.core.type;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.util.List;

/**
 * maintain schema meta.
 */
public class IrSchemaType extends RelRecordType {
    private TableOpt tableOpt;
    private IrLabelType labelType;
    // attribute fields, each field consists of name and type
    private RelRecordType attributesType;

    public IrSchemaType(StructKind kind, List<RelDataTypeField> fields, boolean nullable) {
        super(kind, fields, nullable);
    }
}
