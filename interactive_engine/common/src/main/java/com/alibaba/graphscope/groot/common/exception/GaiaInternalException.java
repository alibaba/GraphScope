package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class GaiaInternalException extends GrootException {
    public GaiaInternalException(Throwable t) {
        super(Code.GAIA_INTERNAL_ERROR, t);
    }

    public GaiaInternalException(String msg) {
        super(Code.GAIA_INTERNAL_ERROR, msg);
    }

    public GaiaInternalException(String msg, Throwable t) {
        super(Code.GAIA_INTERNAL_ERROR, msg, t);
    }

    public GaiaInternalException() {
        super(Code.GAIA_INTERNAL_ERROR);
    }
}
