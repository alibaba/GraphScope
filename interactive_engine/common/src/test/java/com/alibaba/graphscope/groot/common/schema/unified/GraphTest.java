package com.alibaba.graphscope.groot.common.schema.unified;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class GraphTest {

    @Test
    public void serializeRoundTrip() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        InputStream in =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("unified_schema.yaml");
        Graph graph = mapper.readValue(in, Graph.class);
        System.out.println(graph);
        System.out.println("----------------------------------------------");
        String out = mapper.writeValueAsString(graph);
        System.out.println(out);
        Assert.assertEquals(0, 0);
    }
}
