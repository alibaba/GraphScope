package com.alibaba.graphscope.groot.dataload.unified;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class ConfigTest {

    @Test
    public void serializeRoundTrip() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        InputStream in =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("unified_dataloading.yaml");
        UniConfig uniConfig = mapper.readValue(in, UniConfig.class);
        System.out.println(uniConfig);
        System.out.println("----------------------------------------------");
        String out = mapper.writeValueAsString(uniConfig);
        System.out.println(out);
        Assert.assertEquals(0, 0);
    }
}
