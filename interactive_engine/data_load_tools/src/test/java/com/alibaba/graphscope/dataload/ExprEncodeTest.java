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

import com.alibaba.graphscope.dataload.jna.ExprGraphStoreLibrary;
import com.alibaba.graphscope.dataload.jna.type.FfiEdgeData;
import com.alibaba.graphscope.dataload.jna.type.FfiEdgeTypeTuple;
import com.alibaba.graphscope.dataload.jna.type.FfiVertexData;
import com.alibaba.graphscope.dataload.jna.type.ResultCode;
import com.google.common.io.Resources;
import com.sun.jna.Pointer;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ExprEncodeTest {
    private static final ExprGraphStoreLibrary LIB = ExprGraphStoreLibrary.INSTANCE;

    @Test
    public void encodeVertexTest() {
        String labelName = "PERSON";
        String content =
                "2010-01-03T15:10:31.499+00:00|14|Hossein|Forouhar|male|1984-03-11|77.245.239.11|Firefox|fa;ku;en|Hossein14@gmail.com";
        FfiVertexData.ByValue data = getTestVertexData(labelName, content);
        Assert.assertEquals(ResultCode.Success, data.code);
        data.propertyBytes.close();
    }

    @Test
    public void encodeEdgeTest() {
        String labelName = "KNOWS";
        String srcLabel = "PERSON";
        String dstLabel = "PERSON";
        String content = "2010-08-12T06:05:58.299+00:00|14|2199023264850";
        FfiEdgeData.ByValue data = getTestEdgeData(labelName, srcLabel, dstLabel, content);
        Assert.assertEquals(ResultCode.Success, data.code);
        data.propertyBytes.close();
    }

    public static String getTestMetaJson() {
        try {
            URL url = Resources.getResource("meta.json");
            return Resources.toString(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static FfiVertexData.ByValue getTestVertexData(String labelName, String content) {
        Pointer parser = LIB.initParserFromJson(getTestMetaJson());
        Pointer vertexParser = LIB.getVertexParser(parser, labelName);
        FfiVertexData.ByValue data = LIB.encodeVertex(vertexParser, content, '|');
        LIB.destroyVertexParser(vertexParser);
        LIB.destroyParser(parser);
        return data;
    }

    public static FfiEdgeData.ByValue getTestEdgeData(
            String labelName, String srcLabel, String dstLabel, String content) {
        Pointer parser = LIB.initParserFromJson(getTestMetaJson());
        Pointer edgeParser =
                LIB.getEdgeParser(
                        parser, new FfiEdgeTypeTuple.ByValue(labelName, srcLabel, dstLabel));
        FfiEdgeData.ByValue data = LIB.encodeEdge(edgeParser, content, '|');
        LIB.destroyEdgeParser(edgeParser);
        LIB.destroyParser(parser);
        return data;
    }
}
