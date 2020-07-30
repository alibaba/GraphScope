package com.alibaba.maxgraph.admin.security;

import org.apache.commons.lang3.StringUtils;

public class SecurityUtil {
    public static String pathFilter(String filePath) {
        String removeTmpStart = StringUtils.removeStart(filePath, "/tmp/");
        if (StringUtils.contains(removeTmpStart, "/") || StringUtils.contains(removeTmpStart, "..")) {
            return null;
        }
        return filePath;
    }
}
