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

import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.ExecutionException;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.proto.Code;

import io.grpc.Status;

import java.util.concurrent.Callable;

public class ClassUtils {
    public static <T> boolean equalClass(T t1, Class<? extends T> target) {
        return t1.getClass().equals(target);
    }

    public static final <T> T callWithException(Callable<T> callable, Code errorCode) {
        try {
            return callable.call();
        } catch (FrontendException e) {
            throw e;
        } catch (Exception e) {
            throw new FrontendException(errorCode, e.getMessage());
        }
    }

    public static final void runWithException(Runnable runnable, Code errorCode) {
        try {
            runnable.run();
        } catch (FrontendException e) {
            throw e;
        } catch (Exception e) {
            throw new FrontendException(errorCode, e.getMessage());
        }
    }

    public static final Exception handleExecutionException(
            Status status, QueryTimeoutConfig timeoutConfig, String defaultMsg) {
        switch (status.getCode()) {
            case DEADLINE_EXCEEDED:
                String msg =
                        String.format(
                                "error from executor: [%s], hint: [%s]",
                                status.getDescription(),
                                "query exceeds the timeout limit "
                                        + timeoutConfig.getExecutionTimeoutMS()
                                        + " ms, please increase the config by setting"
                                        + " 'query.execution.timeout.ms'");
                return new FrontendException(Code.TIMEOUT, msg);
            case INTERNAL:
            default:
                String errorMsg =
                        status.getDescription() == null ? defaultMsg : status.getDescription();
                return new ExecutionException(errorMsg);
        }
    }
}
