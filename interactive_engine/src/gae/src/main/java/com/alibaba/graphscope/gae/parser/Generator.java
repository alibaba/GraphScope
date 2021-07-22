package com.alibaba.graphscope.gae.parser;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface Generator {
    Map<String, Object> generate(Map<String, Object> args);

    default String getGraphName(Map<String, Object> args) {
        return (String) args.get("graph");
    }

    default Traversal getTraversal(Map<String, Object> args) {
        return (Traversal) args.get("traversal");
    }

    default String readFileFromResource(String fileName) {
        try {
            InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            File file = new File("/tmp/tmp");
            FileUtils.copyInputStreamToFile(stream, file);
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            file.delete();
            return content;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
