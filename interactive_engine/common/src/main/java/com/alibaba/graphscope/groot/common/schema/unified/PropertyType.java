package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.exception.InvalidDataTypeException;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;

public class PropertyType {
    public String primitiveType;
    public Str string;

    @Override
    public String toString() {
        return "PropertyType{"
                + "primitiveType='"
                + primitiveType
                + '\''
                + ", string="
                + string
                + '}';
    }

    public DataType toImpl() {
        if (string != null) {
            //            if (string.longText != null) {
            return DataType.STRING;
            //            }
        }
        if (primitiveType != null) {
            switch (primitiveType) {
                case "DT_SIGNED_INT32":
                case "DT_UNSIGNED_INT32":
                    return DataType.INT;
                case "DT_SIGNED_INT64":
                case "DT_UNSIGNED_INT64":
                    return DataType.LONG;
                case "DT_BOOL":
                    return DataType.BOOL;
                case "DT_FLOAT":
                    return DataType.FLOAT;
                case "DT_DOUBLE":
                    return DataType.DOUBLE;
            }
        }
        throw new InvalidDataTypeException("Cannot parse propertyType " + this);
    }
}
