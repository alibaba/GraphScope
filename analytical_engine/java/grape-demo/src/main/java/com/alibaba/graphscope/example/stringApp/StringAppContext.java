package com.alibaba.graphscope.example.stringApp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.stdcxx.StdString;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringAppContext
        extends VertexDataContext<IFragment<Long, Long, StringView, StringView>, StdString>
        implements ParallelContextBase<Long, Long, StringView, StringView> {

    private static Logger logger = LoggerFactory.getLogger(StringAppContext.class);

    private int maxSteps;

    /**
     * Called by grape framework, before any PEval. You can initiating data structures need during
     * super steps here.
     *
     * @param frag           The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param jsonObject     String args from cmdline.
     * @see IFragment
     * @see ParallelMessageManager
     * @see JSONObject
     */
    @Override
    public void Init(
            IFragment<Long, Long, StringView, StringView> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, StdString.class, false);
    }

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see IFragment
     */
    @Override
    public void Output(IFragment<Long, Long, StringView, StringView> frag) {
        // output to inner vertex data
        GSVertexArray<StdString> vertexData = data();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        logger.info("Begin output");
        for (long i = 0; i < vertexData.size(); ++i) {
            vertex.setValue(i);
            StdString string = vertexData.get(vertex);
            string.fromJavaString("vertex: " + i);
        }
        logger.info("Finish out");
    }
}
