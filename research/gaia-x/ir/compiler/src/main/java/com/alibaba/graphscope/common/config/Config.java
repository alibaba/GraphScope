package com.alibaba.graphscope.common.config;

import java.util.function.Function;

public class Config<T> {
    private String key;
    private String defaultVal;

    private Function<String, T> parseFunc;

    public Config(String key, String defaultVal, Function<String, T> parseFunc) {
        this.key = key;
        this.defaultVal = defaultVal;
        this.parseFunc = parseFunc;
    }

    public T get(Configs configs) {
        String valStr = configs.get(key, defaultVal);
        try {
            T val = parseFunc.apply(valStr);
            return val;
        } catch (Exception e) {
            throw new IllegalArgumentException("key [" + key + "] val [" + valStr + "] parse failed", e);
        }
    }

    public static Config<Short> shortConfig(String key, short defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), (s) -> Short.parseShort(s));
    }

    public static Config<Integer> intConfig(String key, int defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), (s) -> Integer.parseInt(s));
    }

    public static Config<Long> longConfig(String key, long defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), (s) -> Long.parseLong(s));
    }

    public static Config<String> stringConfig(String key, String defaultVal) {
        return new Config<>(key, defaultVal, Function.identity());
    }

    public static Config<Boolean> boolConfig(String key, boolean defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), (s) -> Boolean.parseBoolean(s));
    }

    public String getKey() {
        return this.key;
    }
}
