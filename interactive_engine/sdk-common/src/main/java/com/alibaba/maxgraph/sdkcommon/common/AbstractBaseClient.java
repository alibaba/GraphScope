/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.sdkcommon.common;

import com.alibaba.maxgraph.sdkcommon.error.FatalException;
import com.alibaba.maxgraph.sdkcommon.error.RetryableException;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

public abstract class AbstractBaseClient implements Refreshable {

    protected final int retryTime;

    public AbstractBaseClient(int retryTime) {
        this.retryTime = retryTime;
    }

    /**
     * Retry callable task only when `RetryableException` is thrown; Exception on the signature of
     * the callable task is a user defined error, and may not be success if retry;
     *
     * @param callable
     * @param <T>
     * @return callable task result or exception;
     * @throws Exception
     */
    public <T> T callWithRetry(@Nonnull final Callable<T> callable) throws Exception {
        int retryLeft = this.retryTime;
        return callWithRetry(callable, retryLeft);
    }

    public <T> T callWithRetry(@Nonnull final Callable<T> callable, int retryLeft)
            throws Exception {
        while (true) {
            try {
                return callable.call();
            } catch (RetryableException e) {
                refresh();
                if (--retryLeft == 0) {
                    throw new FatalException(e.getRawError());
                }
            }
        }
    }

    public <T> T call(@Nonnull final Callable<T> callable) throws Exception {
        return callable.call();
    }
}
