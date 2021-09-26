package com.alibaba.graphscope.parallel.mm;

import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.io.Writable;

/**
 * An iterable instances holding msgs for a vertex.
 *
 * @param <MSG_T> msg type.
 */
public interface MessageIterable<MSG_T extends Writable> extends Iterable<MSG_T> {

    void append(MSG_T msg);

    void clear();

    int size();

    MessageIterable emptyMessageIterable = new EmptyMessageIterable();

    public static class EmptyMessageIterable<MSG_T_ extends Writable> implements
        MessageIterable<MSG_T_> {

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
