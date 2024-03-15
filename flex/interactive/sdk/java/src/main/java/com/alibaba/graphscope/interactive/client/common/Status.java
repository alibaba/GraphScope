package com.alibaba.graphscope.interactive.client.common;

public class Status {
    enum StatusCode {
        kServerInternalError,
        kInvalidRequest,
        kPermissionDenied,
        kUnknownError,
        kOk,
    }

    private final StatusCode code;
    private final String message;

    public Status(){
        this.code = StatusCode.kUnknownError;
        this.message = "";
    }

    public boolean IsOk() {
        return this.code == StatusCode.kOk;
    }

    public Status(StatusCode code, String message){
        this.code = code;
        this.message = message;
    }


    public static Status ServerInternalError(String message) {
        return new Status(StatusCode.kServerInternalError, message);
    }

    public static Status Ok() {
        return new Status(StatusCode.kOk, "");
    }
}
