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
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTypeTest {

    @Test
    public void test() throws MaxGraphException {

        DataType dataType = new DataType(InternalDataType.SET);
        dataType.setExpression("String");
        dataType.setExpression("Double");

        dataType = new DataType(InternalDataType.MAP);
        dataType.setExpression("Bool,String");
        dataType.setExpression("Bool ,String");
        dataType.setExpression("Bool, String");
        dataType.setExpression("Bool , String");
        dataType.setExpression("INt,StrinG");

        final DataType data1 = new DataType(InternalDataType.MAP);
        Assert.expectThrows(MaxGraphException.class, () -> data1.setExpression("INt1,StrinG"));

        final DataType data2 = new DataType(InternalDataType.LIST);
        data2.setExpression("String");
        data2.setExpression("iNT");
        Assert.expectThrows(MaxGraphException.class, () -> data2.setExpression("INt1,StrinG"));

        final DataType data3 = DataType.INT;
        data3.setExpression("String");
    }

    @Test
    public void testSerializer() throws MaxGraphException {
        DataType dataType = new DataType(InternalDataType.LIST);
        dataType.setExpression("LONG");
        Assert.assertEquals(JSON.toJson("LIST<LONG>"), JSON.toJson(dataType));
        dataType = new DataType(InternalDataType.SET);
        dataType.setExpression("DOUBLE");
        Assert.assertEquals(JSON.toJson("S<DOUBLE>"), JSON.toJson(dataType));
    }

    @Test
    public void testDeserializer() throws MaxGraphException {
        DataType dataType = new DataType(InternalDataType.LIST);
        dataType.setExpression("INT");
        DataType dataType1 = JSON.fromJson(JSON.toJson(dataType), DataType.class);
        Assert.assertEquals(dataType1, dataType);
        dataType = new DataType(InternalDataType.SET);
        dataType.setExpression("DOUBLE");
        dataType1 = JSON.fromJson(JSON.toJson(dataType), DataType.class);
        Assert.assertEquals(dataType, dataType1);
        dataType = DataType.STRING;
        dataType1 = JSON.fromJson(JSON.toJson(dataType), DataType.class);
        Assert.assertEquals(dataType, dataType1);
    }

    static class A {
        public DataType dataType;

        @JsonCreator
        public A(DataType dataType) {
            this.dataType = dataType;
        }
    }
    @Test
    public void testDeserializeMember() throws MaxGraphException {
        DataType dataType = new DataType(InternalDataType.LIST);
        dataType.setExpression("INT");
        A a = new A(dataType);
        DataType dataType1 = JSON.fromJson(JSON.toJson(a), A.class).dataType;
        Assert.assertEquals(dataType1, dataType);
    }
}
