/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot;

import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcObserverIterator<T> implements CloseableIterator<T> {
    private static final Logger logger = LoggerFactory.getLogger(GrpcObserverIterator.class);

    private static final Object PILL = new Object();
    private BlockingQueue<Object> buffer;
    private Object head;
    private volatile boolean closed = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>();

    public GrpcObserverIterator(int queueSize) {
        this.buffer = new ArrayBlockingQueue<>(queueSize);
        this.head = null;
    }

    @Override
    public boolean hasNext() {
        Throwable t = this.exception.get();
        if (t != null) {
            throw new MaxGraphException(t);
        }
        if (head == null) {
            try {
                head = buffer.take();
            } catch (InterruptedException ie) {
                throw new MaxGraphException(ie);
            }
            if (head == PILL) {
                return false;
            }
        }
        return true;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (head instanceof Throwable) {
            throw new MaxGraphException((Throwable) head);
        }
        T res = (T) head;
        head = null;
        return res;
    }

    // From response
    public void putData(T data) throws InterruptedException {
        if (closed) {
            return;
        }
        this.buffer.put(data);
    }

    // From response
    public void fail(Throwable t) {
        if (closed) {
            return;
        }
        boolean suc = this.exception.compareAndSet(null, t);
        if (!suc) {
            return;
        }
        logger.error("rpc failed", t);
        buffer.offer(t);
    }

    // From response
    public void finish() throws InterruptedException {
        if (closed) {
            return;
        }
        buffer.put(PILL);
    }

    // From iterator consumer
    @Override
    public void close() {
        this.closed = true;
        this.buffer.clear();
    }
}
