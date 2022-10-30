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

package com.alibaba.RDDReaderTransfer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class RDDReadServer {
    public static String getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces();
                    ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                for (Enumeration inetAddrs = iface.getInetAddresses();
                        inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {
                        if (inetAddr.isSiteLocalAddress()) {
                            return inetAddr.toString();
                        } else if (candidateAddress == null) {
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress.toString();
            }
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException(
                        "The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress.toString();
        } catch (Exception e) {
            UnknownHostException unknownHostException =
                    new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    private static final Logger logger = Logger.getLogger(RDDReadServer.class.getName());

    private Server server;

    private int listen_port_base_ = 50000, listenPort_;

    private int partitionId_;

    private int partitionCnt_;

    private Iterator partitionIter_;

    private final int wait_time = 30;

    private String data_type_;
    private ArrayList<String> essential_names_ =
            new ArrayList<>(Arrays.asList("int", "double", "float", "long", "bool", "string"));

    public RDDReadServer(
            int port_shift, int partition_id, Iterator iter, String data_type, int part_cnt) {
        listenPort_ = listen_port_base_ + port_shift;
        partitionId_ = partition_id;
        partitionIter_ = iter;
        data_type_ = data_type;
        partitionCnt_ = part_cnt;
    }

    public void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(listenPort_).addService(new RDDService()).build().start();
        logger.info("Server started, listening on " + listenPort_);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread() {
                            @Override
                            public void run() {
                                // Use stderr here since the logger may have been reset by its JVM
                                // shutdown hook.
                                logger.info(
                                        "*** shutting down gRPC server since JVM is shutting down");
                                try {
                                    RDDReadServer.this.stop();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                logger.info("*** server shut down");
                            }
                        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(wait_time, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    class RDDService extends GetArrayGrpc.GetArrayImplBase {
        RDDService() {}

        private <EssenType> essential_type buildEssen(EssenType data_val, String type) {
            essential_type essen_data;
            if (type.startsWith("int")) {
                Integer int_val = (Integer) data_val;
                essen_data = essential_type.newBuilder().setIntData(int_val).build();
            } else if (type.startsWith("float")) {
                Float float_val = (Float) data_val;
                essen_data = essential_type.newBuilder().setFloatData(float_val).build();
            } else if (type.startsWith("double")) {
                Double double_val = (Double) data_val;
                essen_data = essential_type.newBuilder().setDoubleData(double_val).build();
            } else if (type.startsWith("string")) {
                String str_val = (String) data_val;
                essen_data = essential_type.newBuilder().setStringData(str_val).build();
            } else if (type.startsWith("long")) { // long
                Long long_val = (Long) data_val;
                essen_data = essential_type.newBuilder().setLongData(long_val).build();
            } else { // null
                essen_data = null;
            }
            return essen_data;
        }

        private <ArrayType> array_type buildArray(ArrayType array_data, String array_info) {
            array_type.Builder arr = array_type.newBuilder();
            String[] array_type = array_info.split(",");
            String[] arr_item = (String[]) (array_data);
            for (Integer i = 0; i < arr_item.length; i++) {
                essential_type essen_data = buildEssen(arr_item[i], array_type[1]);
                arr.addItem(essen_data);
            }
            return arr.build();
        }

        private <BasicType> basic_type buildBasic(BasicType data_val, String type) {
            basic_type.Builder basic_data = basic_type.newBuilder();
            if (type.startsWith("Array")) {
                array_type arr_tmp = buildArray(data_val, type);
                basic_data.setArray(arr_tmp);
            } else {
                essential_type ess_tmp = buildEssen(data_val, type);
                basic_data.setEssen(ess_tmp);
            }
            return basic_data.build();
        }

        private PartitionItem buildPartitionItem() {
            PartitionItem.Builder new_item = PartitionItem.newBuilder();
            if (essential_names_.contains(data_type_) || data_type_.startsWith("Array")) {
                basic_type basic_data = buildBasic(partitionIter_.next(), data_type_);
                new_item.addBasicData(basic_data);
            } else { // tuple
                int idx = 1;
                String[] tuple_type = data_type_.split(":");
                if (tuple_type.length == 3) {
                    Tuple2 tup2 = (Tuple2) partitionIter_.next();
                    while (idx < tuple_type.length) {
                        basic_type new_basic =
                                buildBasic(tup2.productElement(idx - 1), tuple_type[idx]);
                        new_item.addBasicData(new_basic);
                        idx++;
                    }
                } else if (tuple_type.length == 4) {
                    Tuple3 tup3 = (Tuple3) partitionIter_.next();
                    while (idx < tuple_type.length) {
                        basic_type new_basic =
                                buildBasic(tup3.productElement(idx - 1), tuple_type[idx]);
                        new_item.addBasicData(new_basic);
                        idx++;
                    }
                } else {
                    logger.info("type error, currently tuple2 and tuple3 only");
                }
            }
            return new_item.build();
        }

        public void getPartitionInfo(
                PartInfoRequest request, StreamObserver<PartitionInfo> responseObserver) {
            responseObserver.onNext(
                    PartitionInfo.newBuilder()
                            .setPartitionId(partitionId_)
                            .setPartitionCnt(partitionCnt_)
                            .setDataType(data_type_)
                            .build());
            responseObserver.onCompleted();
        }

        public void getPartitionItem(
                ItemRequest request, StreamObserver<PartitionItem> responseObserver) {
            while (partitionIter_.hasNext()) {
                PartitionItem rdd_item = buildPartitionItem();
                responseObserver.onNext(rdd_item);
            }
            responseObserver.onCompleted();
        }

        public void rpcClose(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
            responseObserver.onNext(CloseResponse.newBuilder().setClose(true).build());
            responseObserver.onCompleted();
            final int sleep_time = 100;
            try {
                Thread.sleep(sleep_time);
            } catch (InterruptedException e) {
                server.shutdown();
                e.printStackTrace();
            }
            server.shutdown();
        }
    }
}
