/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.context;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.arrow.array.BaseArrowArrayBuilder;
import com.alibaba.graphscope.arrow.array.PrimitiveArrowArrayBuilder;
import com.alibaba.graphscope.arrow.array.StringArrowArrayBuilder;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.fragment.BaseArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.mapper.ArrowProjectedFragmentMapper;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.GraphXParallelPIE;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.graphx.utils.GrapeUtils;
import com.alibaba.graphscope.graphx.utils.ScalaFFIFactory;
import com.alibaba.graphscope.graphx.utils.SerializationUtils;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.array.PrimitiveArray;

import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;

public class GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG>
        extends VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T>
        implements ParallelContextBase<Long, Long, VDATA_T, EDATA_T> {

    public static final String pathPrefix = "/tmp/gs_graphx_pie_";

    public static <VD, ED, M> GraphXParallelAdaptorContext<VD, ED, M> createImpl(
            Class<? extends VD> vdClass,
            Class<? extends ED> edClass,
            Class<? extends M> msgClass,
            Function3 vprog,
            Function1 sendMsg,
            Function2 mergeMsg,
            Object initMsg,
            String appName,
            String vineyardSocket,
            EdgeDirection direction) {
        return new GraphXParallelAdaptorContext<VD, ED, M>(
                vdClass,
                edClass,
                msgClass,
                (Function3<Long, VD, M, VD>) vprog,
                (Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, M>>>) sendMsg,
                (Function2<M, M, M>) mergeMsg,
                (M) initMsg,
                appName,
                vineyardSocket,
                direction);
    }

    public static <VD, ED, M> GraphXParallelAdaptorContext<VD, ED, M> create(
            URLClassLoader classLoader, String serialPath) throws ClassNotFoundException {
        Object[] objects = SerializationUtils.read(classLoader, serialPath);
        if (objects.length != 10) {
            throw new IllegalStateException(
                    "Expect 10 deserialzed object, but only got " + objects.length);
        }
        Class<?> vdClass = (Class<?>) objects[0];
        Class<?> edClass = (Class<?>) objects[1];
        Class<?> msgClass = (Class<?>) objects[2];
        Function3 vprog = (Function3) objects[3];
        Function1 sendMsg = (Function1) objects[4];
        Function2 mergeMsg = (Function2) objects[5];
        Object initMsg = objects[6];
        String appName = (String) objects[7];
        String socket = (String) objects[8];
        EdgeDirection direction = (EdgeDirection) objects[9];
        return (GraphXParallelAdaptorContext<VD, ED, M>)
                createImpl(
                        vdClass, edClass, msgClass, vprog, sendMsg, mergeMsg, initMsg, appName,
                        socket, direction);
    }

    private static Logger logger =
            LoggerFactory.getLogger(GraphXParallelAdaptorContext.class.getName());
    private GraphXConf<VDATA_T, EDATA_T, MSG> conf;
    private GraphXParallelPIE<VDATA_T, EDATA_T, MSG> graphXProxy;
    private String appName, vineyardSocket;

    public String getAppName() {
        return appName;
    }

    public GraphXConf getConf() {
        return conf;
    }

    public GraphXParallelPIE<VDATA_T, EDATA_T, MSG> getGraphXProxy() {
        return graphXProxy;
    }

    public GraphXParallelAdaptorContext(
            Class<? extends VDATA_T> vdClass,
            Class<? extends EDATA_T> edClass,
            Class<? extends MSG> msgClass,
            Function3<Long, VDATA_T, MSG, VDATA_T> vprog,
            Function1<EdgeTriplet<VDATA_T, EDATA_T>, Iterator<Tuple2<Long, MSG>>> sendMsg,
            Function2<MSG, MSG, MSG> mergeMsg,
            MSG initialMsg,
            String appName,
            String vineyardSocket,
            EdgeDirection edgeDirection) {
        this.conf = new GraphXConf<>(vdClass, edClass, msgClass);
        // parallelGraphXPIE
        this.graphXProxy =
                new GraphXParallelPIE<VDATA_T, EDATA_T, MSG>(
                        conf, vprog, sendMsg, mergeMsg, initialMsg, edgeDirection);
        this.appName = appName;
        this.vineyardSocket = vineyardSocket;
    }

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
            IFragment<Long, Long, VDATA_T, EDATA_T> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {

        int maxIterations = jsonObject.getInteger("max_iterations");
        logger.info("Max iterations: " + maxIterations);
        int numPart = jsonObject.getInteger("num_part");
        int fnum = frag.fnum();
        int splitSize = (numPart + fnum - 1) / fnum;
        int myParallelism = calcMyParallelism(numPart, splitSize, frag.fid());
        logger.info("frag {} parallelism {}", frag.fid(), myParallelism);
        String workerIdToFidStr = jsonObject.getString("worker_id_to_fid");
        if (workerIdToFidStr == null || workerIdToFidStr.isEmpty()) {
            throw new IllegalStateException("expect worker id to fid mapping");
        }

        try {
            graphXProxy.init(frag, messageManager, maxIterations, myParallelism, workerIdToFidStr);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("initialization error");
        }
        logger.info("create graphx proxy: {}", graphXProxy);
        // NOTE: Currently we don't use this context provided vdata array, just use int class as
        // default vdata class, and we don't use it.
        createFFIContext(frag, (Class<? extends VDATA_T>) Integer.class, false);
    }

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see IFragment
     */
    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {
        PrimitiveArray<VDATA_T> vdArray = graphXProxy.getNewVdataArray();
        BaseArrowProjectedFragment<Long, Long, VDATA_T, EDATA_T> projectedFragment =
                graphXProxy.getProjectedFragment();
        long time0 = System.nanoTime();
        VineyardClient client = ScalaFFIFactory.newVineyardClient();
        FFIByteString ffiByteString = FFITypeFactory.newByteString();
        ffiByteString.copyFrom(this.vineyardSocket);
        client.connect(ffiByteString);
        String filePath = pathPrefix + frag.fid();
        long new_id = 0;
        // copy vdata to arrow
        if (GrapeUtils.isPrimitive(conf.getVdClass())) {
            PrimitiveArrowArrayBuilder.Factory<VDATA_T> vertexDataBuilderFactory =
                    FFITypeFactory.getFactory(
                            PrimitiveArrowArrayBuilder.class,
                            "gs::ArrowArrayBuilder<"
                                    + GrapeUtils.classToStr(conf.getVdClass(), true)
                                    + ">");
            PrimitiveArrowArrayBuilder<VDATA_T> vertexDataBuilder =
                    vertexDataBuilderFactory.create();
            vertexDataBuilder.reserve(vdArray.size());
            for (int i = 0; i < projectedFragment.getInnerVerticesNum(); ++i) {
                vertexDataBuilder.unsafeAppend(vdArray.get(i));
            }
            ArrowProjectedFragmentMapper.Factory<Long, Long, VDATA_T, EDATA_T> factory =
                    FFITypeFactory.getFactory(
                            ArrowProjectedFragmentMapper.class,
                            CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_MAPPER
                                    + "<int64_t,uint64_t,"
                                    + GrapeUtils.classToStr(conf.getVdClass(), true)
                                    + ","
                                    + GrapeUtils.classToStr(conf.getEdClass(), true)
                                    + ">");
            ArrowProjectedFragmentMapper<Long, Long, VDATA_T, EDATA_T> mapper = factory.create();
            new_id =
                    mapper.map(
                                    projectedFragment.getArrowFragment(),
                                    projectedFragment.vertexLabel(),
                                    projectedFragment.edgePropId(),
                                    vertexDataBuilder,
                                    client)
                            .get()
                            .id();
        } else {
            StringArrowArrayBuilder vdBuilder =
                    GrapeUtils.fillComplexArrowArrayBuilder(
                            vdArray, ClassTag.apply(conf.getVdClass()));
            BaseArrowArrayBuilder.Factory<StringView> vertexDataBuilderFactory =
                    FFITypeFactory.getFactory(
                            BaseArrowArrayBuilder.class, "gs::ArrowArrayBuilder<std::string>");
            BaseArrowArrayBuilder<StringView> vertexDataBuilder = vertexDataBuilderFactory.create();
            vertexDataBuilder.setAddress(vdBuilder.getAddress());

            if (conf.isEDPrimitive()) {
                ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, EDATA_T> factory =
                        FFITypeFactory.getFactory(
                                ArrowProjectedFragmentMapper.class,
                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_MAPPER
                                        + "<int64_t,uint64_t,std::string,"
                                        + GrapeUtils.classToStr(conf.getEdClass(), true)
                                        + ">");
                ArrowProjectedFragmentMapper<Long, Long, StringView, EDATA_T> mapper =
                        factory.create();
                new_id =
                        mapper.map(
                                        projectedFragment.getArrowFragment(),
                                        projectedFragment.vertexLabel(),
                                        projectedFragment.edgePropId(),
                                        vertexDataBuilder,
                                        client)
                                .get()
                                .id();
            } else {
                ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, StringView> factory =
                        FFITypeFactory.getFactory(
                                ArrowProjectedFragmentMapper.class,
                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_MAPPER
                                        + "<int64_t,uint64_t,std::string,std::string>");
                ArrowProjectedFragmentMapper<Long, Long, StringView, StringView> mapper =
                        factory.create();
                new_id =
                        mapper.map(
                                        projectedFragment.getArrowFragment(),
                                        projectedFragment.vertexLabel(),
                                        projectedFragment.edgePropId(),
                                        vertexDataBuilder,
                                        client)
                                .get()
                                .id();
            }
        }

        logger.info("Finish adding vertex data");

        long time1 = System.nanoTime();
        logger.info(
                "Finish writing back, cost {} ms, got new frag id {}",
                (time1 - time0) / 1000000,
                new_id);
        FileWriter fileWritter = null;
        try {
            fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            bufferedWriter.write("" + new_id);
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    int calcMyParallelism(int limit, int splitSize, int fid) {
        int begin = Math.min(limit, splitSize * fid);
        int end = Math.min(limit, begin + splitSize);
        return end - begin;
    }
}
