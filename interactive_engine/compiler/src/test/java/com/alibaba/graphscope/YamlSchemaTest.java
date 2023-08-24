/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope;

import com.alibaba.graphscope.common.ir.schema.FileFormatType;
import com.alibaba.graphscope.common.ir.schema.IrGraphSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

public class YamlSchemaTest {
    @Test
    public void testYamlSchema() throws Exception {
        InputStream inputStream =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("./schema/modern.yaml");
        IrGraphSchema graphSchema = new IrGraphSchema(inputStream, FileFormatType.YAML);
        Assert.assertEquals(
                "DefaultGraphVertex{labelId=0, label=person,"
                    + " propertyList=[DefaultGraphProperty{id=0, name=id, dataType=LONG},"
                    + " DefaultGraphProperty{id=1, name=name, dataType=STRING},"
                    + " DefaultGraphProperty{id=2, name=age, dataType=INT}], primaryKeyList=[id]}",
                graphSchema.getElement("person").toString());
    }
}
