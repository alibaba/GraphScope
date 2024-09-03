package com.alibaba.graphscope.groot.dataload.unified;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class UniConfig {
    public String graph;
    public LoadingConfig loadingConfig;
    public List<VertexMapping> vertexMappings;
    public List<EdgeMapping> edgeMappings;

    public UniConfig() {
        loadingConfig = new LoadingConfig();
        vertexMappings = new ArrayList<>();
        edgeMappings = new ArrayList<>();
    }

    public String getProperty(String key) {
        String value = loadingConfig.format.getProperty(key);
        if (value == null) {
            // Convert all "." to "_", cuz yaml doesn't allow "." in key field
            String key2 = key.replaceAll("\\.", "_");
            value = loadingConfig.format.getProperty(key2);
        }
        return value;
    }

    public String getProperty(String key, String defaultValue) {
        String val = getProperty(key);
        return (val == null) ? defaultValue : val;
    }

    public static UniConfig fromProperties(String file) throws IOException {
        try (InputStream is = new FileInputStream(file)) {
            return fromInputStream(is);
        }
    }

    public static UniConfig fromInputStream(InputStream in) throws IOException {
        Properties properties = new Properties();
        properties.load(in);
        HashMap<String, String> retMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            retMap.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        UniConfig uniConfig = new UniConfig();
        uniConfig.loadingConfig.format.metadata = retMap;
        return uniConfig;
    }

    public static UniConfig fromYaml(String file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        UniConfig uniConfig = mapper.readValue(new File(file), UniConfig.class);
        return uniConfig;
    }

    public static UniConfig fromFile(String file) throws IOException {
        if (file.endsWith(".yaml") || file.endsWith(".yml")) {
            return UniConfig.fromYaml(file);
        } else {
            return UniConfig.fromProperties(file);
        }
    }
}
