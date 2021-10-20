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
package com.alibaba.maxgraph.common.util;

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class PkHasher {
    private ByteBuffer buffer = ByteBuffer.allocate(64 << 10);

    public long hash(int labelId, List<String> pks) {
        buffer.clear();
        buffer.putInt(labelId);
        for (String pk : pks) {
            buffer.putInt(pk.length());
            buffer.put(pk.getBytes());
        }
        buffer.flip();
        return MurmurHash.hash64(buffer.array(), buffer.limit());
    }

    public static void main(String[] args) {
        PkHasher pkHasher = new PkHasher();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int label = random.nextInt();
            if (label < 0) {
                i--;
                continue;
            }
            Long id = random.nextLong();
            long ans = pkHasher.hash(label, Lists.newArrayList(id.toString()));
            System.out.println(String.format("(%d, %d, %d),", label, id, ans));
        }
    }
}
