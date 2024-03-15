package com.alibaba.graphscope.interactive.client.common;

/***
 * A class which wrap the result of the API
 */
public class Result<T> {
    private final Status status;
    private final T value;

    public Result(Status status, T value) {
        this.status = status;
        this.value = value;
    }

    public Status getStatus() {
        return status;
    }

    public T getValue() {
        return value;
    }

    public static <T> Result<T> Ok(T value) {
        return new Result<T>(Status.Ok(), value);
    }

    public boolean Ok() {
        return status.IsOk();
    }
}
