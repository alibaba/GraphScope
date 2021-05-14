package com.alibaba.graphscope.gaia.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.plan.translator.builder.ConfigBuilder;
import com.alibaba.graphscope.gaia.result.object.GaiaEdge;
import com.alibaba.graphscope.gaia.result.object.GaiaVertex;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RemoteTraverserResultParser extends DefaultResultParser {
    private static final Logger logger = LoggerFactory.getLogger(RemoteTraverserResultParser.class);

    public RemoteTraverserResultParser(ConfigBuilder builder) {
        super(builder);
    }

    @Override
    public List<Object> parseFrom(GremlinResult.Result resultData) {
        List<Object> result = new ArrayList<>();
        if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultData.getPaths().getItemList().forEach(p -> {
                result.add(transform(parsePath(p)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultData.getElements().getItemList().forEach(e -> {
                result.add(transform(parseElement(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.TAG_ENTRIES) {
            resultData.getTagEntries().getItemList().forEach(e -> {
                result.add(transform(parseTagPropertyValue(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.MAP_RESULT) {
            resultData.getMapResult().getItemList().forEach(e -> {
                Map entry = Collections.singletonMap(parsePairElement(e.getFirst()), parsePairElement(e.getSecond()));
                result.add(transform(entry.entrySet().iterator().next()));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE) {
            result.add(transform(parseValue(resultData.getValue())));
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE_LIST) {
            resultData.getValueList().getItemList().forEach(k -> {
                result.add(transform(parseValue(k)));
            });
        } else {
            throw new UnsupportedOperationException("");
        }
        return result;
    }

    @Override
    protected Object parseElement(GremlinResult.GraphElement elementPB) {
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.EDGE) {
            return GaiaEdge.createFromProto(elementPB.getEdge());
        }
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            return GaiaVertex.createFromProto(elementPB.getVertex());
        }
        throw new RuntimeException("graph element type not set");
    }

    @Override
    protected Path parsePath(GremlinResult.Path pathPB) {
        Path path = MutablePath.make();
        pathPB.getPathList().forEach(p -> {
            path.extend(parseElement(p), Collections.EMPTY_SET);
        });
        return path;
    }

    private RemoteTraverser transform(Object object) {
        return new DefaultRemoteTraverser(object, 1);
    }
}
