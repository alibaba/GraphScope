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
import com.alibaba.graphscope.dataload.jna.type.FfiVertexData;
import com.sun.jna.Pointer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WriteGraphTest {
    private static final ExprGraphStoreLibrary LIB = ExprGraphStoreLibrary.INSTANCE;
    private Pointer graphLoader = LIB.initGraphLoader("/tmp", 0);

    @Test
    public void writeVertexTest() {
        IrVertexData vertexData = new IrVertexData();
        String label = "PERSON";
        List<String> contents =
                Arrays.asList(
                        "2010-01-03T15:10:31.499+00:00|14|Hossein|Forouhar|male|1984-03-11|77.245.239.11|Firefox|fa;ku;en|Hossein14@gmail.com",
                        "2010-01-31T13:13:03.929+00:00|16|Jan|Zakrzewski|female|1986-07-05|31.41.169.140|Chrome|pl;en|Jan16@gmx.com;Jan16@zoho.com;Jan16@gmail.com;Jan16@yahoo.com;Jan16@gmx.com",
                        "2010-01-19T13:51:10.863+00:00|27|Wojciech|Ciesla|male|1985-12-07|31.182.127.125|Internet"
                            + " Explorer|pl;en|Wojciech27@gmail.com;Wojciech27@yahoo.com;Wojciech27@gmx.com;Wojciech27@gmail.com");
        Pointer buffer = LIB.initWriteVertex(contents.size());
        for (String content : contents) {
            FfiVertexData.ByValue original = ExprEncodeTest.getTestVertexData(label, content);
            FfiVertexData.ByValue data = createNewFromBytes(vertexData, original);
            Assert.assertTrue(LIB.writeVertex(buffer, data));
        }
        Assert.assertEquals(contents.size(), LIB.finalizeWriteVertex(graphLoader, buffer));
    }

    public FfiVertexData.ByValue createNewFromBytes(
            IrVertexData vertexData, FfiVertexData.ByValue data) {
        return vertexData.toIrVertexData(data).toFfiVertexData();
    }

    @Test
    public void writeEdgeTest() {
        IrEdgeData edgeData = new IrEdgeData();
        String labelName = "KNOWS";
        String srcLabel = "PERSON";
        String dstLabel = "PERSON";
        List<String> contents = Arrays.asList("2010-08-12T06:05:58.299+00:00|14|2199023264850");
        Pointer buffer = LIB.initWriteEdge(contents.size());
        for (String content : contents) {
            FfiEdgeData.ByValue original =
                    ExprEncodeTest.getTestEdgeData(labelName, srcLabel, dstLabel, content);
            FfiEdgeData.ByValue data = createNewFromBytes(edgeData, original);
            Assert.assertTrue(LIB.writeEdge(buffer, data));
        }
        Assert.assertEquals(contents.size(), LIB.finalizeWriteEdge(graphLoader, buffer));
    }

    public FfiEdgeData.ByValue createNewFromBytes(IrEdgeData edgeData, FfiEdgeData.ByValue data) {
        return edgeData.toIrEdgeData(data).toFfiEdgeData();
    }

    @After
    public void after() {
        if (graphLoader != null) {
            LIB.finalizeGraphLoading(graphLoader);
        }
    }
}
