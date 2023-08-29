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

package com.alibaba.graphscope.common.ir.meta;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class YamlSchemaTest {
    @Test
    public void yaml_schema_test() throws Exception {
        URL schemaResource =
                Thread.currentThread().getContextClassLoader().getResource("schema/modern.yaml");
        Configs configs =
                new Configs(
                        ImmutableMap.of(
                                GraphConfig.GRAPH_SCHEMA.getKey(),
                                schemaResource.toURI().getPath()));
        IrGraphSchema graphSchema = new IrGraphSchema(new LocalMetaDataReader(configs));
        Assert.assertEquals(
                "DefaultGraphVertex{labelId=0, label=person,"
                    + " propertyList=[DefaultGraphProperty{id=0, name=id, dataType=LONG},"
                    + " DefaultGraphProperty{id=1, name=name, dataType=STRING},"
                    + " DefaultGraphProperty{id=2, name=age, dataType=INT}], primaryKeyList=[id]}",
                graphSchema.getElement("person").toString());
    }
}
