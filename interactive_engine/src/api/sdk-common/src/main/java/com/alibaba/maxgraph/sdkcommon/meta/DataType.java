/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.sdkcommon.meta;


import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * @author beimian
 * 2018/05/06
 */
@JsonSerialize(using = DataTypeSerializer.class)
@JsonDeserialize(using = DataTypeDeserializer.class)
public class DataType {

    // for compatible
    public final static DataType BOOL = new DataType(InternalDataType.BOOL);
    public final static DataType CHAR = new DataType(InternalDataType.CHAR);
    public final static DataType SHORT = new DataType(InternalDataType.SHORT);
    public final static DataType INT = new DataType(InternalDataType.INT);
    public final static DataType LONG = new DataType(InternalDataType.LONG);
    public final static DataType FLOAT = new DataType(InternalDataType.FLOAT);
    public final static DataType DOUBLE = new DataType(InternalDataType.DOUBLE);
    public final static DataType BYTES = new DataType(InternalDataType.BYTES);
    public final static DataType STRING = new DataType(InternalDataType.STRING);
    public final static DataType DATE = new DataType(InternalDataType.DATE);

    @JsonProperty
    private String expression;
    @JsonProperty
    private InternalDataType type;

    public DataType(InternalDataType internalDataType) {
        this.type = internalDataType;
    }

    public static DataType toDataType(int i) {
        return new DataType(InternalDataType.values()[i]);
    }

    public static DataType valueOf(String typeName) {
        return new DataType(InternalDataType.valueOf(typeName));
    }

    public boolean isInt() {
        return this.type == InternalDataType.SHORT || this.type == InternalDataType.INT || this.type == InternalDataType.LONG;
    }

    public void setExpression(String expression) throws MaxGraphException {
        if (isPrimitiveType()) {
            this.expression = null;
            return;
        }

        if (!isValid(expression)) {
            throw new MaxGraphException(ExceptionUtils.ErrorCode.DataTypeNotValid, "expression is not valid, subType " +
                    "must be primitiveTypes: " + InternalDataType.primitiveTypes.toString());
        }
        this.expression = expression;
    }

    public String getExpression() {
        return this.expression;
    }

    public InternalDataType getType() {
        return type;
    }

    @JsonIgnore
    public String name() {
        return this.getType().name();
    }

    @JsonValue
    public String getJson(){
      return this.type.name() + (StringUtils.isEmpty(this.expression) ? "" : "<" + this.expression + ">");
    }

    public boolean isValid(String expression) {
        if (this.type == InternalDataType.SET || this.type == InternalDataType.LIST) {
            return validSubTypes(expression);
        } else if (this.type == InternalDataType.MAP) {
            String s = expression.replaceAll("[ ]*,[ ]*", ",");
            String[] split = s.split(",");
            if (split.length < 2) {
                return false;
            } else {
                return validSubTypes(split[0]) && validSubTypes(split[1]);
            }
        }
        return true;
    }

    public boolean validSubTypes(String expression) {
        if (!InternalDataType.primitiveTypes.contains(expression.trim().toUpperCase())) {
            return false;
        }

        return true;
    }

    public boolean isPrimitiveType() {
        return !(this.type == InternalDataType.LIST || this.type == InternalDataType.MAP || this.type ==
                InternalDataType.SET);
    }

    public boolean isFixedLen() {
        switch (this.type) {
            case BOOL:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public int getFixedSize() {
        switch (this.type) {
            case BOOL:
            case CHAR:
                return 1;
            case SHORT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case LONG:
            case DOUBLE:
                return 8;
            default:
                throw new RuntimeException("unreachable!");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataType)) return false;
        DataType dataType = (DataType) o;
        return Objects.equal(getExpression(), dataType.getExpression()) &&
                getType() == dataType.getType();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getExpression(), getType());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("expression", expression)
                .add("type", type)
                .toString();
    }
}
