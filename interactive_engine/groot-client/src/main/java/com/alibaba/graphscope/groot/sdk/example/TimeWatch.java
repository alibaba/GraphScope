package com.alibaba.graphscope.groot.sdk.example;

import java.util.concurrent.TimeUnit;

class TimeWatch {
    long starts;

    public static TimeWatch start() {
        return new TimeWatch();
    }

    private TimeWatch() {
        reset();
    }

    public TimeWatch reset() {
        starts = System.currentTimeMillis();
        return this;
    }

    public long time() {
        long ends = System.currentTimeMillis();
        return ends - starts;
    }

    public long time(TimeUnit unit) {
        return unit.convert(time(), TimeUnit.MILLISECONDS);
    }

    public void status(String prefix) {
        System.out.println(prefix + ": " + time() + " ms");
    }

    public void status() {
        status("Duration");
    }
}
