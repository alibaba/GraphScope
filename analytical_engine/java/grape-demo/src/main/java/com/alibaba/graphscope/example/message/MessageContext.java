package com.alibaba.graphscope.example.message;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.stdcxx.StdString;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageContext extends
    VertexDataContext<IFragment<Long, Long, Long, Long>, StdString> implements
    ParallelContextBase<Long, Long, Long, Long> {
    private static final Logger logger = LoggerFactory.getLogger(MessageContext.class);


    public int maxSteps = 10;
    public int currentStep = 0;
    public int threadNum = 1;
    public List<FFIByteVectorOutputStream> msgVectorStream;
    public ThreadPoolExecutor executor;

    @Override
    public void Init(IFragment<Long, Long, Long, Long> iFragment,
        ParallelMessageManager parallelMessageManager, JSONObject jsonObject) {
        createFFIContext(iFragment, StdString.class, false);
        parallelMessageManager.initChannels(threadNum);
        currentStep = 0;
        if (!jsonObject.containsKey("maxSteps")) {
            maxSteps = 10;
        }
        else {
            maxSteps = jsonObject.getInteger("maxSteps");
        }
        if (jsonObject.containsKey("threadNum")) {
            threadNum = jsonObject.getInteger("threadNum");
        }
        else {
            threadNum = 8;
        }
        logger.info("Init MessageContext maxSteps: " + maxSteps + " threadNum: " + threadNum);
        msgVectorStream = new java.util.ArrayList<FFIByteVectorOutputStream>();
        for (int i = 0; i < threadNum; ++i) {
            msgVectorStream.add(new FFIByteVectorOutputStream());
        }
        executor = new ThreadPoolExecutor(threadNum, threadNum, 60L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(100000));
    }

    @Override
    public void Output(IFragment<Long, Long, Long, Long> iFragment) {
        for (int i = 0; i < threadNum; ++i) {
            msgVectorStream.get(i).close();
        }
    }

    public FFIByteVectorOutputStream getMsgVectorStream(int threadId) {
        return msgVectorStream.get(threadId);
    }
}
