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
package com.alibaba.graphscope.parallel.mm;

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListMessageIterable<MSG_T extends Writable> implements MessageIterable<MSG_T> {

    public static int DEFAULT_MESSAGE_ITERABLE_SIZE = 4;

    private List<MSG_T> msgs;

    public ListMessageIterable() {
        msgs = new ArrayList<MSG_T>(DEFAULT_MESSAGE_ITERABLE_SIZE);
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<MSG_T> iterator() {
        return msgs.iterator();
    }

    public void append(MSG_T msg) {
        msgs.add(msg);
    }

    public void clear() {
        msgs.clear();
    }

    public int size() {
        return msgs.size();
    }
}
