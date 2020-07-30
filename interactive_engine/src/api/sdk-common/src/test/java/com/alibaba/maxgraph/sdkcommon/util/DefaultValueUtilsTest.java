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
package com.alibaba.maxgraph.sdkcommon.util;


import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.beust.jcommander.internal.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class DefaultValueUtilsTest {

    @Test
    public void checkDefaultValue() throws MaxGraphException {

        Object aTrue = DefaultValueUtils.checkDefaultValue(DataType.BOOL, "true");
        Assert.assertTrue((Boolean)aTrue);

        Object aInt = DefaultValueUtils.checkDefaultValue(DataType.INT, "1");
        Assert.assertTrue((Integer)aInt == 1);

        Assert.expectThrows(MaxGraphException.class, () -> DefaultValueUtils.checkDefaultValue(DataType.INT,
         "1.0f"));

        Object aLong = DefaultValueUtils.checkDefaultValue(DataType.LONG, "2");
        Assert.assertEquals(aLong, 2l);

        Object aFloat = DefaultValueUtils.checkDefaultValue(DataType.FLOAT, "1.0f");
        Assert.assertEquals(aFloat, 1.0f);

        Object aDouble = DefaultValueUtils.checkDefaultValue(DataType.DOUBLE, "1.0d");
        Assert.assertEquals(aDouble,1.0d);

        Object aString = DefaultValueUtils.checkDefaultValue(DataType.STRING, "string");
        Assert.assertEquals(aString, "string");

        DataType dataType = new DataType(InternalDataType.LIST);
        dataType.setExpression("INT");

        Object aList = DefaultValueUtils.checkDefaultValue(dataType, "[1, 2]");
        Assert.assertEquals((List<Integer>)aList, Lists.newArrayList(1, 2));

        dataType.setExpression("LONG");
        aList = DefaultValueUtils.checkDefaultValue(dataType, "[1, 2]");
        Assert.assertEquals((List<Long>)aList, Lists.newArrayList(1l, 2l));

        dataType.setExpression("FLOAT");
        aList = DefaultValueUtils.checkDefaultValue(dataType, "[1.0, 2.0]");
        Assert.assertEquals((List<Float>)aList, Lists.newArrayList(1.0f, 2.0f));

        dataType.setExpression("DOUBLE");
        aList = DefaultValueUtils.checkDefaultValue(dataType, "[1.0, 2.0]");
        Assert.assertEquals((List<Double>)aList, Lists.newArrayList(1.0, 2.0));

        dataType.setExpression("STRING");
        aList = DefaultValueUtils.checkDefaultValue(dataType, "[\"1\", \"2\"]");
        Assert.assertEquals((List<String>)aList, Lists.newArrayList("1", "2"));

        dataType.setExpression("BYTES");
        aList = DefaultValueUtils.checkDefaultValue(dataType, "[[1,2]]");
        List<Byte[]> arrayList = new ArrayList<>();
        arrayList.add(new Byte[]{1, 2});

        Assert.assertEquals((List<Byte[]>)aList, arrayList);

        dataType.setExpression("DOUBLE");
        Assert.expectThrows(MaxGraphException.class, () -> DefaultValueUtils.checkDefaultValue(dataType, "[\"t\", \"sdfsdf\"]"));
    }
}
