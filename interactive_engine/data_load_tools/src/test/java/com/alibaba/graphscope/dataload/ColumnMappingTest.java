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

package com.alibaba.graphscope.dataload;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ColumnMappingTest {
    @Test
    public void mappingJsonTest() throws Exception {
        URL url = Resources.getResource("meta.json");
        String columnMappingJson = Resources.toString(url, StandardCharsets.UTF_8);
        ColumnMappingMeta columnMappingMeta =
                (new ObjectMapper())
                        .readValue(columnMappingJson, new TypeReference<ColumnMappingMeta>() {})
                        .init();
        String tableName = "knows_1";
        Assert.assertEquals("KNOWS", columnMappingMeta.getLabelName(tableName));
        Assert.assertEquals(1, columnMappingMeta.getElementType(tableName));
        Assert.assertEquals(12, columnMappingMeta.getLabelId("KNOWS"));
        Assert.assertEquals("PERSON", columnMappingMeta.getSrcLabel(tableName));
        Assert.assertEquals("PERSON", columnMappingMeta.getDstLabel(tableName));
    }
}
