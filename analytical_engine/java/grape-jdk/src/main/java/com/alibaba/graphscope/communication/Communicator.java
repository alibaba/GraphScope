/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.communication;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_COMMUNICATOR;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Communicator providing useful distributed aggregation methods such as min/min/sum. */
public abstract class Communicator {
    private static Logger logger = LoggerFactory.getLogger(Communicator.class.getName());
    private FFICommunicator communicatorImpl;

    /**
     * This function is set private, not intended to be invokede by user. It is meat to only be
     * called by jni, and let the exceptions accepted by cpp, so they can be obviously displayed.
     *
     * @param appAddr the address of the c++ app instance.
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void initCommunicator(long appAddr)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException,
                    IllegalAccessException {
        Class<FFICommunicator> communicatorClass =
                (Class<FFICommunicator>) FFITypeFactory.getType(GRAPE_COMMUNICATOR);
        Constructor[] constructors = communicatorClass.getConstructors();

        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1
                    && constructor.getParameterTypes()[0].getName().equals("long")) {
                communicatorImpl = communicatorClass.cast(constructor.newInstance(appAddr));
                System.out.println(communicatorImpl);
            }
        }
    }

    /**
     * Obtain the sum of msgIn among all distributed app instances, and put the result in msgOut.
     * MSG_T should be a sub class of FFIMirror.
     *
     * @param msgIn data to be aggregated.
     * @param msgOut placeholder to receive the result.
     * @param <MSG_T> msg type, should be a FFIMirror, DoubleMsg or LongMsg
     * @see com.alibaba.fastffi.FFIMirror
     * @see DoubleMsg
     * @see LongMsg
     */
    public <MSG_T> void sum(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.sum(msgIn, msgOut);
    }

    /**
     * Obtain the min of msgIn among all distributed app instances, and put the result in msgOut.
     * MSG_T should be a sub class of FFIMirror.
     *
     * @param msgIn data to be aggregated.
     * @param msgOut placeholder to received the result.
     * @param <MSG_T> msg type, should be a FFIMirror, DoubleMsg or LongMsg.
     * @see com.alibaba.fastffi.FFIMirror
     * @see DoubleMsg
     * @see LongMsg
     */
    public <MSG_T> void min(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.min(msgIn, msgOut);
    }

    /**
     * Obtain the max of msgIn among all distributed app instances, and put the result in msgOut.
     * MSG_T should be a sub class of FFIMirror.
     *
     * @param msgIn data to be aggregated.
     * @param msgOut placeholder to received the result.
     * @param <MSG_T> msg type, should be a FFIMirror, DoubleMsg or LongMsg.
     */
    public <MSG_T> void max(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.max(msgIn, msgOut);
    }
}
