package com.alibaba.graphscope.parallel.message;

import static com.alibaba.graphscope.utils.CppClassName.INT_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIGen
@CXXHead(value = CORE_JAVA_JAVA_MESSAGES_H)
@FFITypeAlias(value = INT_MSG)
public interface IntMsg extends MsgBase {
    Factory factory = FFITypeFactory.getFactory(Factory.class, IntMsg.class);

    int getData();

    void setData(int value);

    @FFIFactory
    interface Factory {

        /**
         * Create an uninitialized IntMsg.
         *
         * @return msg instance.
         */
        IntMsg create();
        /**
         * Create a DoubleMsg with initial value.
         *
         * @param inData input data.
         * @return msg instance.
         */
        IntMsg create(int inData);
    }
}
