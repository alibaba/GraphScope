/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.ds;

import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.junit.Assert;
import org.junit.Test;

public class VertexArrayTest {

    @Test
    public void test1() {
        VertexRange<Long> vertices = FFITypeFactoryhelper.newVertexRangeLong();
        vertices.SetRange(0L, 100L);
        VertexArray<Long, Long> vertexArray = FFITypeFactoryhelper.newVertexArray(Long.class);
        vertexArray.init(vertices);
        for (Vertex<Long> vertex : vertices) {
            vertexArray.setValue(vertex, 1L);
        }
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(1L));
        }
    }

    @Test
    public void test2() {
        VertexRange<Long> vertices = FFITypeFactoryhelper.newVertexRangeLong();
        vertices.SetRange(0L, 100L);
        VertexArray<Long, Long> vertexArray = FFITypeFactoryhelper.newVertexArray(Long.class);
        vertexArray.init(vertices, 1L);
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(1L));
        }
        vertexArray.setValue(vertices, 2L);
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(2L));
        }
    }
}
