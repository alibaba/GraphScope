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
package org.apache.giraph.comm.netty;

import static org.mockito.Mockito.mock;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyWritableMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NettyClientTest {

    private static Logger logger = LoggerFactory.getLogger(NettyClientTest.class);

    private NettyServer server;
    private NettyClientV2 client;
    private ImmutableClassesGiraphConfiguration configuration;
    private WorkerInfo workerInfo;
    private AggregatorManager aggregatorManager;

    private static String getHostIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new IllegalStateException("Failed to get master host address");
        }
    }

    @Before
    public void prepare() throws InstantiationException, IllegalAccessException {
        ImmutableClassesGiraphConfiguration conf = mock(ImmutableClassesGiraphConfiguration.class);
        aggregatorManager = new AggregatorManagerNettyImpl(conf, 0, 2);
        aggregatorManager.registerAggregator("sum", LongSumAggregator.class);
        aggregatorManager.setAggregatedValue("sum", new LongWritable(0));
        //        when(conf.)
        //        String hostName = System.getenv("HOSTNAME");
        String hostName = getHostIp();
        workerInfo = new WorkerInfo(0, 2, hostName, 30000, null);
        server =
                new NettyServer(
                        conf,
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
        client =
                new NettyClientV2(
                        conf,
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 10; ++i) {
            NettyWritableMessage send = new NettyWritableMessage(new LongWritable(i), 100, "sum");
            Future<NettyMessage> msg = client.sendMessage(send);
            // while (!msg.isDone()) {}
            NettyMessage received = client.getResponse();
            logger.info("reponse for round: " + i + ": " + received);
        }
    }

    @After
    public void close() {
        client.close();
        server.close();
    }
}
