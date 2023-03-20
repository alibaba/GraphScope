/*
 * Copyright 2020 Alibaba Group Holding Limited.

 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.HostsChannelFetcher;
import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;

public class GraphServiceMain {
    public final static String GRAPH_STORE_KEY = "graph.store";
    public final static String EXPERIMENTAL = "exp";
    public final static String CSR = "csr";

    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);
        IrMetaFetcher irMetaFetcher = new ExperimentalMetaFetcher(configs);
        RpcChannelFetcher fetcher = new HostsChannelFetcher(configs);

        IrGremlinServer server = new IrGremlinServer();
        if (configs.get(GRAPH_STORE_KEY).equals(CSR)) {
            server.start(
                    configs,
                    irMetaFetcher,
                    fetcher,
                    new IrMetaQueryCallback(irMetaFetcher),
                    TestGraphFactory.MCSR);
        } else {
            server.start(
                    configs,
                    irMetaFetcher,
                    fetcher,
                    new IrMetaQueryCallback(irMetaFetcher),
                    TestGraphFactory.EXPERIMENTAL);
       }
    }
}
