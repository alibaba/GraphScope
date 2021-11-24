package com.alibaba.graphscope.common.exception;

import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.jna.type.ResultCode;

public class AppendInterOpException extends RuntimeException {
    public AppendInterOpException(Class<? extends InterOpBase> op, ResultCode resultCode) {
        super(String.format("op type {} returns error result code {}", op, resultCode.name()));
    }
}
