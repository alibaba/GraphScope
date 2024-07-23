package com.alibaba.graphscope.example.circle;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.stdcxx.StdString;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.LongIdParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CirclePIEContext extends VertexDataContext<IFragment<Long, Long, Long, Long>, StdString>
        implements DefaultContextBase<Long, Long, Long, Long> {
    private static final Logger logger = LoggerFactory.getLogger(CirclePIEContext.class);

    public int maxStep = 4;
    public int curStep = 0;
    public List<List<Path>> curPaths; // Paths ending at vertex.
    public List<List<Path>> nextPaths; // New generated path end at vertex this round,
    public List<List<Path>> results; // Paths that are in circle.
    public LongIdParser parser;

    /**
     * Called by grape framework, before any PEval. You can initiating data structures need during
     * super steps here.
     *
     * @param frag           The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param jsonObject     String args from cmdline.
     * @see IFragment
     * @see DefaultMessageManager
     * @see JSONObject
     */
    @Override
    public void Init(IFragment<Long, Long, Long, Long> frag, DefaultMessageManager messageManager, JSONObject jsonObject) {
        long innerVertexNum = frag.getInnerVerticesNum();
        createFFIContext(frag, StdString.class, false);
        if (jsonObject.containsKey("maxStep")) {
            maxStep = jsonObject.getInteger("maxStep");
            return;
        }
        curPaths = new ArrayList<List<Path>>((int) innerVertexNum);
        nextPaths = new ArrayList<List<Path>>((int) innerVertexNum);
        results = new ArrayList<List<Path>>((int) innerVertexNum);
        for (int i = 0; i < innerVertexNum; ++i ){
            curPaths.add(new ArrayList<Path>());
            nextPaths.add(new ArrayList<Path>());
            results.add(new ArrayList<Path>());
        }
        parser = new LongIdParser(frag.fnum(), 1);
    }

    public void addToCurrentPath(Vertex<Long> vertex, Path path) {
        curPaths.get(vertex.getValue().intValue()).add(new Path(path));
    }

    public void addToNextPath(Vertex<Long> vertex, Path path) {
        nextPaths.get(vertex.getValue().intValue()).add(new Path(path));
    }

    public void persistCirclePathInCurrent() {
        for (int i = 0; i < nextPaths.size(); ++i) {
            for (int j = 0; j < nextPaths.get(i).size(); ++j) {
                Path path = nextPaths.get(i).get(j);
                tryToFindCircle(path);
                //Do we need to remove the path?
            }
        }
    }

    public void swapPaths() {
        List<List<Path>> tmp = curPaths;
        curPaths = nextPaths;
        nextPaths = tmp;
        int size = nextPaths.size();
        nextPaths.clear();
        for (int i = 0; i < size; ++i) {
            nextPaths.add(new ArrayList<Path>());
        }
    }

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see IFragment
     */
    @Override
    public void Output(IFragment<Long, Long, Long, Long> frag) {
        // logger.info("finally cur path {}", curPaths);
        // logger.info("finally next path {}", nextPaths);

        GSVertexArray<StdString> vertexArray = data();
        Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
        for (long vid = 0; vid < frag.getInnerVerticesNum(); ++vid) {
            cur.setValue(vid);
            StdString value = (StdString) vertexArray.get(cur);
            value.fromJavaString(results.get((int) vid).toString());
        }
    }

    public void tryToFindCircle(Path path) {
        if (path.isCircle()){
            // logger.info("path is circle {}", path);
            long lid = parser.getOffset(path.top());
            this.results.get((int)lid).add(path);
        }
    }
}
