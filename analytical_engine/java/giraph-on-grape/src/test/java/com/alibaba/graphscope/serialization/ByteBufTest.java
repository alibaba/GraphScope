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
package com.alibaba.graphscope.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ByteBufTest {

    private ByteBuf buf;
    private ByteBufOutputStream bufOutputStream;

    @Before
    public void init() {
        buf = ByteBufAllocator.DEFAULT.buffer(8);
        bufOutputStream = new ByteBufOutputStream(buf);
    }

    @Test
    public void test() throws IOException {
        bufOutputStream.writeInt(0);
        bufOutputStream.writeInt(1);
        assert bufOutputStream.writtenBytes() == 8;
        assert buf.readableBytes() == 8;
        buf.clear();
        bufOutputStream.writeInt(2);
        bufOutputStream.writeInt(3);
        assert buf.getInt(0) == 2;
        assert buf.getInt(4) == 3;
    }
}
