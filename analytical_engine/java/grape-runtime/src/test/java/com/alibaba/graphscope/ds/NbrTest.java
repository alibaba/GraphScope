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
import com.alibaba.graphscope.ds.GrapeNbr.Factory;
import org.junit.Assert;
import org.junit.Test;

public class NbrTest {

    private Factory<Long, Long> factory =
            FFITypeFactory.getFactory(GrapeNbr.class, "grape::Nbr<uint64_t,int64_t>");

    @Test
    public void test1() {
        GrapeNbr<Long, Long> nbr = factory.create(1L);
        Assert.assertEquals(1, nbr.neighbor().GetValue().longValue());
        Assert.assertEquals(1, nbr.neighbor().GetValue().longValue());
    }

    @Test
    public void test2() {
        GrapeNbr<Long, Long> nbr = factory.create(1L, 2L);
        Assert.assertEquals(2L, nbr.data().longValue());
    }

    @Test
    public void test3() {
        GrapeNbr<Long, Long> nbr = factory.create(1L, 2L);
        GrapeNbr<Long, Long> nbr2 = nbr.copy();
        Assert.assertFalse(nbr.getAddress() == nbr2.getAddress());
        Assert.assertTrue(
                nbr.neighbor().GetValue().longValue() == nbr.neighbor().GetValue().longValue());
    }
}
