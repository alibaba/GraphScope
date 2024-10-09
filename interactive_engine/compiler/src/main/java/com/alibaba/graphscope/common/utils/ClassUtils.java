/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.utils;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.ExecutionException;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.gremlin.plugin.MetricsCollector;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.proto.frontend.Code;

import io.grpc.Status;
import io.grpc.StatusException;
import io.opentelemetry.api.metrics.LongHistogram;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Callable;

public class ClassUtils {
    public static <T> boolean equalClass(T t1, Class<? extends T> target) {
        return t1.getClass().equals(target);
    }

    public static final <T> T callException(Callable<T> callable, Code errorCode) {
        return callExceptionWithDetails(callable, errorCode, Map.of());
    }

    public static final <T> T callExceptionWithDetails(
            Callable<T> callable, Code errorCode, Map<String, Object> details) {
        try {
            return callable.call();
        } catch (FrontendException e1) {
            e1.getDetails().putAll(details);
            throw e1;
        } catch (ExecutionException e2) {
            throw e2;
        } catch (Exception e3) {
            FrontendException e4 = new FrontendException(errorCode, e3.getMessage(), e3);
            e4.getDetails().putAll(details);
            throw e4;
        }
    }

    /**
     *
     * @param query
     * @param queryId
     * @param upTraceId traceId from upstream, for tracing the whole query process
     * @return
     */
    public static final QueryStatusCallback createQueryStatusCallback(
            BigInteger queryId,
            @Nullable String upTraceId,
            String query,
            MetricsCollector collector,
            @Nullable LongHistogram queryHistogram,
            Configs configs) {
        return new QueryStatusCallback(
                collector,
                queryHistogram,
                new QueryLogger(query, queryId, upTraceId),
                FrontendConfig.QUERY_PRINT_THRESHOLD_MS.get(configs));
    }

    public static final Exception handleExecutionException(
            @Nullable Throwable error, QueryTimeoutConfig timeoutConfig) {
        Exception exception = asException(error);
        Throwable rootCause = ExceptionUtils.getRootCause(exception);
        if (!(rootCause instanceof StatusException)) {
            return exception;
        }
        Status status = ((StatusException) rootCause).getStatus();
        switch (status.getCode()) {
            case DEADLINE_EXCEEDED:
                return new FrontendException(
                        Code.TIMEOUT,
                        getTimeoutError(status.getDescription(), timeoutConfig),
                        error);
            case INTERNAL:
                return new ExecutionException(status.getDescription(), error);
            case UNKNOWN:
            default:
                return new FrontendException(
                        Code.ENGINE_UNAVAILABLE, status.getDescription(), error);
        }
    }

    public static final String getTimeoutError(String error, QueryTimeoutConfig timeoutConfig) {
        return String.format(
                "error: [%s], hint: [%s]",
                error,
                "query exceeds the timeout limit "
                        + timeoutConfig.getExecutionTimeoutMS()
                        + " ms, please increase the config by setting"
                        + " 'query.execution.timeout.ms'");
    }

    private static Exception asException(@Nullable Throwable t) {
        if (t instanceof Exception) {
            return (Exception) t;
        } else {
            return new RuntimeException(
                    (t == null) ? "Unknown error in execution" : t.getMessage(), t);
        }
    }
}
