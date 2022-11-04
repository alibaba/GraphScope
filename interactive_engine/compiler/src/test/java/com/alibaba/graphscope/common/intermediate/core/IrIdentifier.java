package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.validate.TableNameSpace;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * create an identifier from a simple variable name, including:
 * {@code IrIdentifier(alias)},
 * {@code IrIdentifier(alias.property)},
 * {@code IrIdentifier(table)} for table unique id in {@link TableNameSpace}.
 */
public class IrIdentifier extends IrNode {
    private List<String> nameList;

    // infer different return types according to different Identifier:
    // for alias -> return (RelRecordType tableMeta) (consisting of label and attributes)
    // for alias.label -> return IrLabelType
    // for alias.property -> return IrBasicType
    @Override
    public RelDataType inferReturnType() {
        return null;
    }

    // validate according to different Identifier:
    // for alias -> validate whether the table exists or not
    // for alias.property -> validate whether the property exists in the table inferred from the
    // alias
    @Override
    public void validate() {}
}
