package com.alibaba.maxgraph.v2.frontend.utils;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * KeyValue related util, convert key value array to map
 */
public class KeyValueUtil {
    /**
     * Convert key value list to map
     *
     * @param keyValues The given key value list
     * @return The result value map
     */
    public static Map<Object, Object> convertToMap(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid key values length " + keyValues.length);
        }
        Map<Object, Object> valueMap = Maps.newHashMap();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            Object value = keyValues[i + 1];
            valueMap.put(key, value);
        }

        return valueMap;
    }

    /**
     * Convert key value list to map
     *
     * @param keyValues The given key value list
     * @return The result value map
     */
    public static Map<String, Object> convertToStringKeyMap(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid key values length " + keyValues.length);
        }
        Map<String, Object> valueMap = Maps.newHashMap();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = String.valueOf(keyValues[i]);
            Object value = keyValues[i + 1];
            valueMap.put(key, value);
        }

        return valueMap;
    }

    /**
     * Parse vertex/edge element id from object
     *
     * @param object The given object
     * @return The result element id
     */
    public static ElementId convertToElementId(Object object) {
        checkNotNull(object, "id object cant be null");

        try {
            if (object instanceof ElementId) {
                return (ElementId) object;
            } else {
                String strObj = object.toString();
                if (StringUtils.startsWith(strObj, "e[")) {
                    String removeEStr = StringUtils.removeStart(strObj, "e[");
                    strObj = StringUtils.substring(removeEStr, 0, removeEStr.indexOf("]"));
                }
                if (StringUtils.startsWith(strObj, "v[")) {
                    strObj = StringUtils.removeEnd(StringUtils.removeStart(strObj, "v["), "]");
                }
                if (StringUtils.contains(strObj, ".")) {
                    String[] array = StringUtils.split(strObj, ".");
                    return new CompositeId(Long.parseLong(array[1]), Integer.parseInt(array[0]));
                } else {
                    return new CompositeId(Long.parseLong(strObj), 0);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("parse element id failed for input " + object, e);
        }
    }
}
