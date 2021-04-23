package com.alibaba.maxgraph.v2.frontend.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHolder {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionHolder.class);
    private Throwable exception;

    public ExceptionHolder() {
        this.exception = null;
    }

    public Throwable getException() {
        return exception;
    }

    public void hold(Throwable e) {
        synchronized (this) {
            if (null == exception) {
                exception = e;
            } else {
                logger.error("exception", e);
            }
        }
    }
}
