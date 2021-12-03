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

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.VertexRange.Factory;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class VertexRangeTest {

    private Factory factory =
            FFITypeFactory.getFactory(VertexRange.class, "grape::VertexRange<uint64_t>");

    @Test
    public void test1() {
        VertexRange<Long> vertices = factory.create();
        vertices.SetRange(0L, 101L);
        int expectedSum = 5050;
        int cnt = 0;
        for (Vertex<Long> vertex : vertices) {
            cnt += vertex.GetValue();
        }
        Assert.assertEquals(expectedSum, cnt);
    }

    @Test
    public void tes2() {
        VertexRange<Long> vertices = factory.create();
        vertices.SetRange(0L, 101L);
        int expectedSum = 5050;
        AtomicInteger cnt = new AtomicInteger(0);
        vertices.forEach(
                (vertex) -> {
                    cnt.getAndAdd(vertex.GetValue().intValue());
                });
        Assert.assertEquals(expectedSum, cnt.get());
    }
}
