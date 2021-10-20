/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.sdkcommon.MaxGraphFunctional;
import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.proto.Response;
import com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils;
import com.google.common.base.Strings;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-06 上午10:03
 **/

public class RpcUtils {

    /**
     * EXECUTE provided execution function which return some information to client side. and return the
     * successful {@link Response} if no
     * with proper error code and error message.
     *
     * @param log        logger
     * @param resp       response stream observer of gRPC
     * @param respSetter method reference to set @{@link Response} object in type {@code T}'s builder.
     * @param execution  the function to execute, the result will be send to client size via the response stream
     *                   observer.
     * @param <T>        the response proto type.
     */
    public static <T extends GeneratedMessageV3 & MessageOrBuilder> void execute(
            Logger log,
            StreamObserver<T> resp, Function<Response, ? extends GeneratedMessageV3.Builder> respSetter,
            MaxGraphFunctional.CallableForIterator<T> execution) {
        execute(log, resp, respSetter, null, execution);
    }

    /**
     * EXECUTE provided execution function which return some information to client side. and return the
     * successful {@link Response} if no
     * with proper error code and error message.
     *
     * @param log        logger
     * @param resp       response stream observer of gRPC
     * @param respSetter method reference to set @{@link Response} object in type {@code T}'s builder.
     * @param spanId     spanId for tracer. If not null, this execution will be traced.
     * @param callable   the function to execute, the result will be send to client size via the response stream
     *                   observer.
     * @param <T>        the response proto type.
     */
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3 & MessageOrBuilder> void execute(
            Logger log,
            StreamObserver<T> resp,
            Function<Response, ? extends GeneratedMessageV3.Builder> respSetter,
            String spanId,
            MaxGraphFunctional.CallableForIterator<T> callable) {

        try {
            Iterator<T> v = callable.call();
            while (v.hasNext()) {
                resp.onNext(v.next());
            }
            resp.onCompleted();
        } catch (MaxGraphException e) {
            log.error("", e);
            resp.onNext((T) respSetter.apply(errorResponse(e)).build());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("", e);
            resp.onNext((T) respSetter.apply(errorResponse(e)).build());
            resp.onCompleted();
        } finally {

        }
    }

    /**
     * EXECUTE provided execution function which has {@code void} return type, and return the
     * successful {@link Response} if no
     * with proper error code and error message.
     *
     * @param log      logger
     * @param resp     response stream observer of gRPC
     * @param runnable the function to execute
     */
    public static void executeForDefaultResp(Logger log, StreamObserver<Response> resp,
                                             MaxGraphFunctional.Runnable runnable) {
        try {
            runnable.run();
            resp.onNext(Response.getDefaultInstance());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("", e);
            resp.onNext(errorResponse(e));
            resp.onCompleted();
        }
    }

    public static Response errorResponse(Exception e) {
        if (e instanceof MaxGraphException) {
            return Response.newBuilder()
                    .setErrCode(((MaxGraphException)e).getErrorCode())
                    .setErrMsg(Strings.nullToEmpty(e.getMessage())).build();
        } else {
            return Response.newBuilder().setErrCode(ExceptionUtils.ErrorCode.Unknown.ordinal())
                    .setErrMsg(Strings.nullToEmpty(e.getMessage())).build();
        }
    }

    public static ManagedChannel createChannel(String host, int port) {
        // idle after 5 minutes.
        return createChannel(host, port, 60 * 5);
    }

    /**
     * Create {@link ManagedChannel} with some default parameters.
     *
     * @param host    target host
     * @param port    target port
     * @param idleSec idle time in seconds
     * @return gRPC ManagedChannel
     */
    public static ManagedChannel createChannel(String host, int port, int idleSec) {

        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext().maxInboundMessageSize(Integer.MAX_VALUE);
        if (idleSec > 0) {
            builder.idleTimeout(idleSec, TimeUnit.SECONDS);
        }
        return builder.build();
    }
}
