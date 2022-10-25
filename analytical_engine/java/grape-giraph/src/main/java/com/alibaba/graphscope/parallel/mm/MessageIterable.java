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

import java.util.Collections;
import java.util.Iterator;

/**
 * An iterable instances holding msgs for a vertex.
 *
 * @param <MSG_T> msg type.
 */
public interface MessageIterable<MSG_T extends Writable> extends Iterable<MSG_T> {

    MessageIterable emptyMessageIterable = new EmptyMessageIterable();

    void append(MSG_T msg);

    void clear();

    int size();

    public static class EmptyMessageIterable<MSG_T_ extends Writable>
            implements MessageIterable<MSG_T_> {

        @Override
        public void append(MSG_T_ msg) {
            throw new IllegalStateException("empty message iterable");
        }

        @Override
        public void clear() {
            throw new IllegalStateException("empty message iterable");
        }

        @Override
        public int size() {
            return 0;
        }

        /**
         * Returns an iterator over elements of type {@code T}.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<MSG_T_> iterator() {
            return Collections.emptyIterator();
        }
    }
}
