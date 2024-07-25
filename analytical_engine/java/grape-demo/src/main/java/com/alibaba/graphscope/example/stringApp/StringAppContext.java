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
            // string.fromJavaString("{\"attr\":{\"attr\":{\"label\":\"phone_num\"}},\"matches\":[{\"isKeep\":false,\"iteration\":1,\"matched\":[\"109=0=5000\",\"198=0=5008\",\"312=0=100\"],\"nextRound\":[5001],\"startVertex\":\"312\"},{\"isKeep\":false,\"iteration\":1,\"matched\":[\"109=0=5000\",\"233=0=100\"],\"nextRound\":[5001],\"startVertex\":\"233\"}]}");
            string.fromJavaString(
                    "eyJhdHRyIjp7ImF0dHIiOnsibGFiZWwiOiJwaG9uZV9udW0ifX0sIm1hdGNoZXMiOlt7ImlzS2VlcCI6ZmFsc2UsIml0ZXJhdGlvbiI6MSwibWF0Y2hlZCI6WyIxMDk9MD01MDAwIiwiMTk4PTA9NTAwOCIsIjMxMj0wPTEwMCJdLCJuZXh0Um91bmQiOls1MDAxXSwic3RhcnRWZXJ0ZXgiOiIzMTIifSx7ImlzS2VlcCI6ZmFsc2UsIml0ZXJhdGlvbiI6MSwibWF0Y2hlZCI6WyIxMDk9MD01MDAwIiwiMjMzPTA9MTAwIl0sIm5leHRSb3VuZCI6WzUwMDFdLCJzdGFydFZlcnRleCI6IjIzMyJ9XX0=eCI6IjI2OCJ9XX0=ZSwiaXRlcmF0aW9uIjoxLCJtYXRjaGVkIjpbIjE4NT0wPTUwMDgiLCIyOTU9MD0xMDAiXSwibmV4dFJvdW5kIjpbXSwic3RhcnRWZXJ0ZXgiOiIyOTUifV19cnRleCI6IjMzNCJ9XX0=");
        }
        logger.info("Finish out");
    }
}
