package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
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
