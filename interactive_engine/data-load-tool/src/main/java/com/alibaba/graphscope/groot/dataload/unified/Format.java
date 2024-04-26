package com.alibaba.graphscope.groot.dataload.unified;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Format {
    public String type; // optional, value: csv
    public Map<String, String> metadata;

    public Format() {
        metadata = new HashMap<>();
    }

    public String getProperty(String key) {
        String key2 = key;
        if (Objects.equals(key, "separator")) {
            key2 = "delimiter";
        }
        return metadata.get(key2);
    }
}
