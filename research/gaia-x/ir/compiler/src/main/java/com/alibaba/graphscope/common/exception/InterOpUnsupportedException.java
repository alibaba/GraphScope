package com.alibaba.graphscope.common.exception;

import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;

public class InterOpUnsupportedException extends UnsupportedOperationException {
    public InterOpUnsupportedException(Class<? extends InterOpBase> op, String cause) {
        super(String.format("op type {} is unsupported, cause is {}", op, cause));
    }
}
