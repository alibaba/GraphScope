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
package com.alibaba.maxgraph.compiler.utils;

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompilerUtilsTest {
    @Test
    public void testParseDataTypeFromPropDataType() {
        com.alibaba.maxgraph.sdkcommon.meta.DataType boolType = CompilerUtils.parseDataTypeFromPropDataType(DataType.BOOL);
        Assert.assertEquals(boolType, com.alibaba.maxgraph.sdkcommon.meta.DataType.BOOL);
        com.alibaba.maxgraph.sdkcommon.meta.DataType shortType = CompilerUtils.parseDataTypeFromPropDataType(DataType.SHORT);
        Assert.assertEquals(shortType, com.alibaba.maxgraph.sdkcommon.meta.DataType.SHORT);
        com.alibaba.maxgraph.sdkcommon.meta.DataType intType = CompilerUtils.parseDataTypeFromPropDataType(DataType.INT);
        Assert.assertEquals(intType, com.alibaba.maxgraph.sdkcommon.meta.DataType.INT);
        com.alibaba.maxgraph.sdkcommon.meta.DataType longType = CompilerUtils.parseDataTypeFromPropDataType(DataType.LONG);
        Assert.assertEquals(longType, com.alibaba.maxgraph.sdkcommon.meta.DataType.LONG);
        com.alibaba.maxgraph.sdkcommon.meta.DataType stringType = CompilerUtils.parseDataTypeFromPropDataType(DataType.STRING);
        Assert.assertEquals(stringType, com.alibaba.maxgraph.sdkcommon.meta.DataType.STRING);
        com.alibaba.maxgraph.sdkcommon.meta.DataType floatType = CompilerUtils.parseDataTypeFromPropDataType(DataType.FLOAT);
        Assert.assertEquals(floatType, com.alibaba.maxgraph.sdkcommon.meta.DataType.FLOAT);
        com.alibaba.maxgraph.sdkcommon.meta.DataType doubleType = CompilerUtils.parseDataTypeFromPropDataType(DataType.DOUBLE);
        Assert.assertEquals(doubleType, com.alibaba.maxgraph.sdkcommon.meta.DataType.DOUBLE);
        com.alibaba.maxgraph.sdkcommon.meta.DataType intListType = CompilerUtils.parseDataTypeFromPropDataType(DataType.INT_LIST);
        Assert.assertEquals(intListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(intListType.getExpression()), "int");
        com.alibaba.maxgraph.sdkcommon.meta.DataType longListType = CompilerUtils.parseDataTypeFromPropDataType(DataType.LONG_LIST);
        Assert.assertEquals(longListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(longListType.getExpression()), "long");
        com.alibaba.maxgraph.sdkcommon.meta.DataType stringListType = CompilerUtils.parseDataTypeFromPropDataType(DataType.STRING_LIST);
        Assert.assertEquals(stringListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(stringListType.getExpression()), "string");
    }
}
