package com.alibaba.maxgraph.tests.frontend.utils;

import com.alibaba.maxgraph.v2.frontend.utils.ReflectionUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReflectionUtilTest {
    @Test
    void testReflectionField() {
        ReflectionObject object = new ReflectionObject();
        ReflectionUtil.setFieldValue(ReflectionObject.class, object, "value", "hello world");
        assertEquals("hello world", ReflectionUtil.getFieldValue(ReflectionObject.class, object, "value"));

        assertThrows(RuntimeException.class, () -> ReflectionUtil.setFieldValue(ReflectionObject.class, object, "value", 123));
        assertThrows(RuntimeException.class, () -> ReflectionUtil.getFieldValue(ReflectionObject.class, object, "value0"));
    }
}

class ReflectionObject {
    private String value;
}
