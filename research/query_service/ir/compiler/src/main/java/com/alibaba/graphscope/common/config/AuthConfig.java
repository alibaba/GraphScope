package com.alibaba.graphscope.common.config;

public class AuthConfig {
    public static final Config<String> AUTH_USERNAME =
            Config.stringConfig("auth.username", "");

    public static final Config<String> AUTH_PASSWORD =
            Config.stringConfig("auth.password", "");
}
