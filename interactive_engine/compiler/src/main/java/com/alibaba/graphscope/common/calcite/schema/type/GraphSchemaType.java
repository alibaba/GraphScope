package com.alibaba.graphscope.common.calcite.schema.type;

import com.alibaba.graphscope.common.calcite.rel.builder.config.ScanOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.util.List;

/**
 * denote DataType of an entity or a relation, including opt, label and attributes
 */
public class GraphSchemaType extends RelRecordType {
    protected ScanOpt scanOpt;
    protected LabelType labelType;

    protected GraphSchemaType(ScanOpt scanOpt) {
        super(StructKind.NONE, ImmutableList.of(), false);
        this.scanOpt = scanOpt;
    }

    /**
     * @param scanOpt   entity or relation
     * @param labelType
     * @param fields    attribute fields, each field denoted by {@link RelDataTypeField} which consist of property name, property id and type
     */
    public GraphSchemaType(ScanOpt scanOpt, LabelType labelType, List<RelDataTypeField> fields) {
        super(StructKind.NONE, fields, false);
        this.scanOpt = scanOpt;
        this.labelType = labelType;
    }

    public ScanOpt getScanOpt() {
        return scanOpt;
    }

    public LabelType getLabelType() {
        return labelType;
    }
}
