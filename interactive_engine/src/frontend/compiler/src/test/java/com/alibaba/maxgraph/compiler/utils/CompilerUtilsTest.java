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

import com.alibaba.maxgraph.compiler.api.schema.PropDataType;
import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompilerUtilsTest {
    @Test
    public void testParseDataTypeFromPropDataType() {
        DataType boolType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.BOOL);
        Assert.assertEquals(boolType, DataType.BOOL);
        DataType shortType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.SHORT);
        Assert.assertEquals(shortType, DataType.SHORT);
        DataType intType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.INTEGER);
        Assert.assertEquals(intType, DataType.INT);
        DataType longType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.LONG);
        Assert.assertEquals(longType, DataType.LONG);
        DataType stringType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.STRING);
        Assert.assertEquals(stringType, DataType.STRING);
        DataType floatType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.FLOAT);
        Assert.assertEquals(floatType, DataType.FLOAT);
        DataType doubleType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.DOUBLE);
        Assert.assertEquals(doubleType, DataType.DOUBLE);
        DataType intListType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.INTEGER_LIST);
        Assert.assertEquals(intListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(intListType.getExpression()), "int");
        DataType longListType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.LONG_LIST);
        Assert.assertEquals(longListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(longListType.getExpression()), "long");
        DataType stringListType = CompilerUtils.parseDataTypeFromPropDataType(PropDataType.STRING_LIST);
        Assert.assertEquals(stringListType.getType(), InternalDataType.LIST);
        Assert.assertEquals(StringUtils.lowerCase(stringListType.getExpression()), "string");
    }
}
