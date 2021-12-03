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
package com.alibaba.maxgraph.compiler.prepare.store;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.CompilerConfig;
import com.alibaba.maxgraph.compiler.utils.MaxGraphUtils;
import com.google.common.collect.Lists;

import java.util.List;

import static com.alibaba.maxgraph.Message.LogicalCompare;
import static com.google.common.base.Preconditions.checkArgument;

public class PrepareCompareEntity extends PrepareEntity<LogicalCompare> {
    private static final long serialVersionUID = 7532546567820419296L;
    private int keyIndex;
    private int valueIndex;
    private boolean negate;
    private Message.CompareType compareType;

    private int compareKey;
    private Object compareValue;

    public PrepareCompareEntity(int argumentIndex, int keyIndex, int valueIndex, Message.CompareType compareType) {
        super(argumentIndex);
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.compareType = compareType;
    }

    @Override
    public List<Integer> getParamIndexList() {
        List<Integer> paramIndexList = Lists.newArrayList();
        if (isPrepareKey()) {
            paramIndexList.add(keyIndex);
        }
        if (isPrepareValue()) {
            paramIndexList.add(valueIndex);
        }

        return paramIndexList;
    }

    @Override
    public LogicalCompare prepareParam(List<List<Object>> paramList, GraphSchema schema, CompilerConfig compilerConfig) {
        checkArgument(!isPrepareKey() || keyIndex <= paramList.size(), "Can't get key[" + keyIndex + "] from paramList=>" + paramList);
        checkArgument(!isPrepareValue() || valueIndex <= paramList.size(), "Can't get value[" + valueIndex + "] from paramList=>" + paramList);

        LogicalCompare.Builder builder = LogicalCompare.newBuilder()
                .setIndex(getArgumentIndex())
                .setCompare(compareType);
        if (isPrepareKey()) {
            List<Object> param = paramList.get(keyIndex - 1);
            builder.setPropId(MaxGraphUtils.parsePropIdByKey(param.get(0).toString(), schema));
        } else {
            builder.setPropId(compareKey);
        }

        Message.Value.Builder valueBuilder;
        if (isPrepareValue()) {
            Object currentValue = paramList.get(valueIndex - 1).get(0);
            Message.VariantType variantType = MaxGraphUtils.parseVariantType(currentValue.getClass(), currentValue);
            builder.setType(variantType);
            valueBuilder = MaxGraphUtils.createValueFromType(currentValue, variantType, compilerConfig);
            builder.setType(variantType).setValue(MaxGraphUtils.createValueFromType(currentValue, variantType, compilerConfig));
        } else {
            Message.VariantType variantType = MaxGraphUtils.parseVariantType(compareValue.getClass(), compareValue);
            builder.setType(variantType);
            valueBuilder = MaxGraphUtils.createValueFromType(compareValue, variantType, compilerConfig);
            builder.setType(variantType).setValue(MaxGraphUtils.createValueFromType(compareValue, variantType, compilerConfig));
        }
        valueBuilder.setBoolFlag(negate);
        builder.setValue(valueBuilder);

        return builder.build();
    }

    public boolean isPrepareKey() {
        return keyIndex > 0;
    }

    public boolean isPrepareValue() {
        return valueIndex > 0;
    }

    public int getCompareKey() {
        return compareKey;
    }

    public void setCompareKey(int compareKey) {
        this.compareKey = compareKey;
    }

    public Object getCompareValue() {
        return compareValue;
    }

    public void setCompareValue(Object compareValue) {
        this.compareValue = compareValue;
    }

    public boolean isNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }
}
