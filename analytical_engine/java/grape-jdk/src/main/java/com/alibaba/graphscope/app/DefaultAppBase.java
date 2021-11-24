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

package com.alibaba.graphscope.app;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

/**
 * The base interface for all <em>sequential</em> PIE apps which work along with {@link
 * SimpleFragment} and {@link DefaultMessageManager}
 *
 * <p>To define your sequential graph algorithms, you should implement this interface and provide
 * the corresponding implementation for {@link DefaultAppBase#PEval(SimpleFragment,
 * DefaultContextBase, DefaultMessageManager)} and {@link DefaultAppBase#IncEval(SimpleFragment,
 * DefaultContextBase, DefaultMessageManager)}
 *
 * <p>User-defined app shall work along with user-defined context, which should be an implementation
 * of {@link DefaultContextBase}.
 *
 * <p>For example, you can implement your app like this.
 *
 * <pre>
 * {
 *         class MyContext implements DefaultContextBase&lt;Long, Long, Long, Double&gt; {
 *                 public void Init(SimpleFragment&lt;Long, Long, Long, Double&gt; frag,
 *                                 DefaultMessageManager messageManager, StdVector&lt;FFIByteString&gt; args) {
 *
 *                 }
 *
 *                 public void Output(SimpleFragment&lt;Long, Long, Long, Double&gt; frag) {
 *
 *                 }
 *         }
 *         class MyApp implements DefaultAppBase&lt;Long, Long, Long, Double, MyContext&gt; {
 *                 public void PEval(SimpleFragment&lt;Long, Long, Long, Double&gt; graph,
 *                                 DefaultContextBase context, DefaultMessageManager messageManager) {
 *
 *                 }
 *
 *                 public void IncEval(SimpleFragment&lt;Long, Long, Long, Double&gt; graph,
 *                                 DefaultContextBase context, DefaultMessageManager messageManager) {
 *
 *                 }
 *         }
 * }
 * </pre>
 *
 * <p>For more examples, please refer to module com.alibaba.graphscope.grape-demo
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 * @param <C> context type
 */
public interface DefaultAppBase<
                OID_T,
                VID_T,
                VDATA_T,
                EDATA_T,
                C extends DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T>>
        extends AppBase<OID_T, VID_T, VDATA_T, EDATA_T, C> {

    /**
     * Partial Evaluation to implement.
     *
     * @param graph fragment. The graph fragment providing accesses to graph data.
     * @param context context. User defined context which manages data during the whole
     *     computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see SimpleFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    void PEval(
            SimpleFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph,
            DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> context,
            DefaultMessageManager messageManager);

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph fragment. The graph fragment providing accesses to graph data.
     * @param context context. User defined context which manages data during the whole
     *     computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see SimpleFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    void IncEval(
            SimpleFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph,
            DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> context,
            DefaultMessageManager messageManager);
}
