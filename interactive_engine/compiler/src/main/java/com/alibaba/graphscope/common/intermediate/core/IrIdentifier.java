package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.validate.IrValidatorScope;
import com.alibaba.graphscope.common.intermediate.core.validate.TableNameSpace;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

/**
 * create an identifier from a simple variable name, including:
 * {@code IrIdentifier(alias)},
 * {@code IrIdentifier(alias.property)},
 * {@code IrIdentifier(table_id)} for table unique id in {@link TableNameSpace}.
 */
public class IrIdentifier extends IrNode {
    private List<String> nameList;

    public IrIdentifier(List<String> nameList) {}

    /**
     * infer different return types according to different {@code Identifier} types:
     * for table_id, get table or tables from a global view and return as {@code IrSchemaType}
     * for alias, get inferred type from {@code nodeToTypeMap} of {@link IrValidatorScope}
     * for alias#property, get property field from {@code IrSchemaType}
     * @return
     */
    @Override
    public RelDataType inferReturnType() {
        throw new NotImplementedException();
    }

    /**
     * validate according to different {@code Identifier} types:
     * for table_id, check whether the table id exists in a global view
     * for alias, check whether the alias exists in the {@code nodeToTypeMap}
     * for alias#property, check whether the property field or the table id exists
     */
    @Override
    public void validate() {}
}
