package com.alibaba.graphscope.common.exception;

import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;

public class InterOpIllegalArgException extends IllegalArgumentException {
    public InterOpIllegalArgException(Class<? extends InterOpBase> opType, String opName, String error) {
        super(String.format("op type {} op name {} returns error {}", opType, opName, error));
    }
}
