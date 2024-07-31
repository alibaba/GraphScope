package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.proto.groot.AttributeValue;
import com.alibaba.graphscope.proto.groot.RequestOptionsPb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Options that can be supplied on a per request basis.
 */
public class RequestOptions {

    public static final RequestOptions EMPTY = RequestOptions.build().create();

    private final Map<String, Object> parameters;
    private final String traceId;

    private RequestOptions(final Builder builder) {
        this.parameters = builder.parameters;
        this.traceId = builder.traceId;
    }

    public Optional<Map<String, Object>> getParameters() {
        return Optional.ofNullable(parameters);
    }

    public Optional<String> getTraceId() {
        return Optional.ofNullable(traceId);
    }

    public RequestOptionsPb toWriteRequest() {
        RequestOptionsPb.Builder builder = RequestOptionsPb.newBuilder();
        if (traceId != null) {
            builder.setTraceId(traceId);
        }
        if (parameters != null) {
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                AttributeValue.Builder valueBuilder = AttributeValue.newBuilder();
                if (value instanceof Long) {
                    valueBuilder.setLongValue((long) value);
                    continue;
                }
                if (value instanceof Integer) {
                    valueBuilder.setIntValue((int) value);
                    continue;
                }
                if (value instanceof Double) {
                    valueBuilder.setDoubleValue((double) value);
                    continue;
                }
                if (value instanceof String) {
                    valueBuilder.setStringValue((String) value);
                }
                builder.putAttributes(key, valueBuilder.build());
            }
        }
        return builder.build();
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String, Object> parameters = null;
        private String traceId = null;

        public Builder addParameter(final String name, final Object value) {
            if (null == parameters) {
                parameters = new HashMap<>();
            }
            parameters.put(name, value);
            return this;
        }

        public Builder traceId(final String traceId) {
            this.traceId = traceId;
            return this;
        }

        public RequestOptions create() {
            return new RequestOptions(this);
        }
    }
}
