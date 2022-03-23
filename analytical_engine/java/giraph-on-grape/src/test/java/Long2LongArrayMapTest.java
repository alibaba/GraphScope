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

import it.unimi.dsi.fastutil.longs.Long2LongArrayMap;

import org.junit.Test;

public class Long2LongArrayMapTest {

    @Test
    public void test1() {
        Long2LongArrayMap map = new Long2LongArrayMap();
        map.put(0, 1);
        map.put(0, 2);
        map.get(0);
    }
}
