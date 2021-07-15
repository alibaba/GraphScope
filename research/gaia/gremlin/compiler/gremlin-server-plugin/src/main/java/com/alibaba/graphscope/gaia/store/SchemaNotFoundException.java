package com.alibaba.graphscope.gaia.store;

public class SchemaNotFoundException extends RuntimeException {
    public SchemaNotFoundException(String msg) {
        super(msg);
    }

    public SchemaNotFoundException(String msg, Throwable e) {
        super(msg, e);
    }

    public SchemaNotFoundException(Throwable e) {
        super(e);
    }
}
