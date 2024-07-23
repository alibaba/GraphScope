package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class PortInUseException extends GrootException {
    public PortInUseException(Throwable t) {
        super(Code.PORT_IN_USE, t);
    }

    public PortInUseException(String msg) {
        super(Code.PORT_IN_USE, msg);
    }

    public PortInUseException(String msg, Throwable t) {
        super(Code.PORT_IN_USE, msg, t);
    }

    public PortInUseException() {
        super(Code.PORT_IN_USE);
    }
}
