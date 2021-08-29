/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.admin.controller;

import com.alibaba.maxgraph.admin.config.InstanceProperties;
import com.alibaba.maxgraph.admin.entity.CloseInstanceEntity;
import com.alibaba.maxgraph.admin.entity.CreateInstanceEntity;
import com.alibaba.maxgraph.admin.memory.FrontendMemoryStorage;
import com.alibaba.maxgraph.admin.memory.InstanceEntity;
import com.alibaba.maxgraph.admin.security.SecurityUtil;
import com.alibaba.maxgraph.admin.zoo.PathStatValue;
import com.alibaba.maxgraph.admin.zoo.ZookeeperClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/instance")
public class InstanceManagerController {
    private static final Logger logger = LoggerFactory.getLogger(InstanceManagerController.class);
    private static final long LAUNCH_MAX_TIME_LILL = 10 * 60 * 1000;
    private static final String NAME_PATTERN = "[a-z0-9]+[a-z0-9\\-.]*[a-z0-9]+";

    @Autowired
    private InstanceProperties instanceProperties;

    @RequestMapping("hello")
    public String hello() {
        return "Hello Instance Manager";
    }

    private void checkPodNameList(String podNameList) {
        if (StringUtils.isEmpty(podNameList)) {
            throw new IllegalArgumentException("Invalid pod name list for " + podNameList);
        }
        String[] podNameArray = StringUtils.splitByWholeSeparator(podNameList, ",");
        for (String podName : podNameArray) {
            if (!podName.matches(NAME_PATTERN)) {
                throw new IllegalArgumentException("Invalid pod name list for " + podNameList);
            }
        }
    }

    private void checkContainerName(String containerName) {
        if (StringUtils.isEmpty(containerName)) {
            throw new IllegalArgumentException("Invalid container name list for " + containerName);
        }
        if (!containerName.matches(NAME_PATTERN)) {
            throw new IllegalArgumentException("Invalid container name for " + containerName);
        }
    }

    @RequestMapping(value = "create_local", method = RequestMethod.POST)
    public CreateInstanceEntity createLocalInstance(@RequestParam("graphName") String graphName,
                                                    @RequestParam("schemaPath") String schemaPath,
                                                    @RequestParam("vineyardIpcSocket") String vineyardIpcSocket,
                                                    @RequestParam("zookeeperPort") String zookeeperPort,
                                                    @RequestParam("enableGaia") String enableGaia) throws Exception{
        CreateInstanceEntity createInstanceEntity = new CreateInstanceEntity();
        int errorCode;
        String errorMessage = "";
        String stdoutMessage = "";

        try {
            if (!isValidGraphName(graphName)) {
                createInstanceEntity.setErrorCode(-1);
                createInstanceEntity.setErrorMessage("Invalid graph name");
                return createInstanceEntity;
            }
            List<String> createCommandList = new ArrayList<>();

            createCommandList.add(instanceProperties.getCreateScript());
            createCommandList.add("create_gremlin_instance_on_local");
            createCommandList.add(graphName);
            createCommandList.add(schemaPath);
            createCommandList.add("1"); // server id
            createCommandList.add(vineyardIpcSocket);
            createCommandList.add(zookeeperPort);
            createCommandList.add(enableGaia);
            String command = StringUtils.join(createCommandList, " ");
            logger.info("start to create instance with command " + command);
            Process process = Runtime.getRuntime().exec(command);

            List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            infoValueList.addAll(errorValueList);
            stdoutMessage = StringUtils.join(infoValueList, "\n");
            logger.info("Create instance command output: " + stdoutMessage);
            errorCode = process.waitFor();
            if (errorCode == 0) {
              logger.info("Trying to find MAXGRAPH_FRONTEND_PORT");
              Pattern endpointPattern = Pattern.compile("MAXGRAPH_FRONTEND_PORT:\\S+");
              Matcher matcher = endpointPattern.matcher(stdoutMessage);
              if (matcher.find()) {
                String frontendEndpoint = StringUtils.splitByWholeSeparator(StringUtils.removeStart(matcher.group(), "MAXGRAPH_FRONTEND_PORT:"), " ")[0];
                InstanceEntity instanceEntity = new InstanceEntity(frontendEndpoint, "", "", this.instanceProperties.getCloseScript());
                FrontendMemoryStorage.getFrontendStorage().addFrontendEndpoint(graphName, instanceEntity);
                String[] endpointArray = StringUtils.split(frontendEndpoint, ":");
                String ip = endpointArray[0];
                int frontendPort = Integer.parseInt(endpointArray[1]);
                createInstanceEntity.setFrontHost(ip);
                createInstanceEntity.setFrontPort(frontendPort);
                logger.info("Found Maxgraph Frontend with ip: "+ ip + " and port:" + frontendEndpoint);
                if (!this.checkInstanceReady(ip, frontendPort)) {
                  errorCode = -1;
                  errorMessage = "Check instance ready timeout";
                }
              } else {
                errorCode = -1;
                errorMessage = "MAXGRAPH_FRONTEND_PORT match failed.";
              }
              if (errorCode != -1 && enableGaia.equals("True")) {
                logger.info("Trying to find GAIA_FRONTEND_PORT");
                endpointPattern = Pattern.compile("GAIA_FRONTEND_PORT:\\S+");
                matcher = endpointPattern.matcher(stdoutMessage);
                if (matcher.find()) {
                    String frontendEndpoint = StringUtils.splitByWholeSeparator(StringUtils.removeStart(matcher.group(), "GAIA_FRONTEND_PORT:"), " ")[0];
                    // InstanceEntity instanceEntity = new InstanceEntity(frontendEndpoint, "", "", this.instanceProperties.getCloseScript());
                    // FrontendMemoryStorage.getFrontendStorage().addFrontendEndpoint(graphName, instanceEntity);
                    String[] endpointArray = StringUtils.split(frontendEndpoint, ":");
                    String ip = endpointArray[0];
                    int frontendPort = Integer.parseInt(endpointArray[1]);
                    createInstanceEntity.setGaiaFrontHost(ip);
                    createInstanceEntity.setGaiaFrontPort(frontendPort);
                    logger.info("Found Gaia Frontend with ip: "+ ip + " and port:" + frontendEndpoint);
                    if (!this.checkInstanceReady(ip, frontendPort)) {
                    errorCode = -1;
                    errorMessage = "Check instance ready timeout";
                    }
                } else {
                    errorCode = -1;
                    errorMessage = "GAIA_FRONTEND_PORT match failed.";
                }
              }
            }
        } catch (Exception e) {
            errorCode = -1;
            errorMessage = ExceptionUtils.getMessage(e);
        }
        if (errorCode == -1) {
            this.closeInstance(graphName);
        }

        createInstanceEntity.setErrorCode(errorCode);
        createInstanceEntity.setErrorMessage(errorMessage);

        return createInstanceEntity;
    }

    @RequestMapping(value = "create", method = RequestMethod.POST)
    public CreateInstanceEntity createInstance(@RequestParam("graphName") String graphName,
                                               @RequestParam("schemaJson") String schemaJson,
                                               @RequestParam("podNameList") String podNameList,
                                               @RequestParam("containerName") String containerName,
                                               @RequestParam("preemptive") String preemptive,
                                               @RequestParam("gremlinServerCpu") String gremlinServerCpu,
                                               @RequestParam("gremlinServerMem") String gremlinServerMem,
                                               @RequestParam("engineParams") String engineParams,
                                               @RequestParam("zookeeperIp") String zookeeperIp) throws Exception {
        CreateInstanceEntity createInstanceEntity = new CreateInstanceEntity();
        int errorCode;
        String errorMessage;
        int frontendPort = 0;

        String schemaPath = "/tmp/" + graphName + ".json";
        if (!isValidGraphName(graphName) || StringUtils.isEmpty(SecurityUtil.pathFilter(schemaPath))) {
            createInstanceEntity.setErrorCode(-1);
            createInstanceEntity.setErrorMessage("Invalid graph name");
            return createInstanceEntity;
        }
        try {
            checkPodNameList(podNameList);
            checkContainerName(containerName);
        } catch (Exception e) {
            createInstanceEntity.setErrorCode(-1);
            createInstanceEntity.setErrorMessage(e.getMessage());
            return createInstanceEntity;
        }
        OutputStream outputStream = new FileOutputStream(new File(schemaPath));
        IOUtils.write(schemaJson, outputStream, "utf-8");
        outputStream.flush();
        outputStream.close();
        try {
            List<String> createCommandList = new ArrayList<>();
            createCommandList.add(instanceProperties.getCreateScript());
            createCommandList.add("create_gremlin_instance_on_k8s");
            createCommandList.add(graphName);
            createCommandList.add(schemaPath);
            createCommandList.add(podNameList);
            createCommandList.add(containerName);
            createCommandList.add(preemptive);
            createCommandList.add(gremlinServerCpu);
            createCommandList.add(gremlinServerMem);
            createCommandList.add(engineParams);
            createCommandList.add(zookeeperIp);
            String command = StringUtils.join(createCommandList, " ");
            logger.info("start to create instance with command " + command);
            Process process = Runtime.getRuntime().exec(command);
            List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            infoValueList.addAll(errorValueList);
            errorMessage = StringUtils.join(infoValueList, "\n");
            errorCode = process.waitFor();
            if (errorCode == 0) {
                Pattern endpointPattern = Pattern.compile("FRONTEND_PORT:\\S+");
                Matcher matcher = endpointPattern.matcher(errorMessage);
                if (matcher.find()) {
                    String frontendEndpoint = StringUtils.splitByWholeSeparator(StringUtils.removeStart(matcher.group(), "FRONTEND_PORT:"), " ")[0];
                    InstanceEntity instanceEntity = new InstanceEntity(frontendEndpoint, podNameList, containerName, this.instanceProperties.getCloseScript());
                    FrontendMemoryStorage.getFrontendStorage().addFrontendEndpoint(graphName, instanceEntity);
                    String[] endpointArray = StringUtils.split(frontendEndpoint, ":");
                    String ip = endpointArray[0];
                    frontendPort = Integer.parseInt(endpointArray[1]);
                    createInstanceEntity.setFrontHost(ip);
                    createInstanceEntity.setFrontPort(frontendPort);
                    if (!this.checkInstanceReady(ip, frontendPort)) {
                        errorCode = -1;
                        errorMessage = "Check instance ready timeout";
                    }
                } else {
                    errorCode = -1;
                }
            }
        } catch (Exception e) {
            errorCode = -1;
            errorMessage = ExceptionUtils.getMessage(e);
        }
        createInstanceEntity.setErrorCode(errorCode);
        createInstanceEntity.setErrorMessage(errorMessage);
        return createInstanceEntity;
    }

    private boolean checkInstanceReady(String ip, int port) {
        if (System.getenv("MY_POD_NAME") != null) {
          // in kubernetes env
          if (ip.equals("localhost") || ip.equals("127.0.0.1")) {
              // now, used in mac os with docker-desktop kubernetes cluster,
              // which external ip is 'localhost' when service type is 'LoadBalancer'.
              return true;
          }
        }
        MessageSerializer serializer = Serializers.GRYO_V1D0.simpleInstance();
        Map<String, Object> config = new HashMap<String, Object>() {
            {
                this.put("serializeResultToString", true);
            }
        };
        serializer.configure(config, null);
        Cluster cluster = Cluster.build().addContactPoint(ip)
                .port(port)
                .serializer(serializer)
                .create();
	Client client = null;
        long start = System.currentTimeMillis();
        long end = start + LAUNCH_MAX_TIME_LILL;
        while (start < end) {
            try {
                Thread.sleep(10000);
                client = cluster.connect();
                client.submit("g.V().limit(1)").all().get();
                client.close();
                cluster.close();
                return true;
            } catch (Exception e) {
                logger.warn("Execute check query failed", e);
            }
            start = System.currentTimeMillis();
        }
        if (client != null) {
            client.close();
        }
        cluster.close();
        return false;
    }

    @RequestMapping("createByPath")
    public CreateInstanceEntity createInstanceByPath(@RequestParam("graphName") String graphName,
                                                     @RequestParam("schemaPath") String schemaPath,
                                                     @RequestParam("podNameList") String podNameList,
                                                     @RequestParam("containerName") String containerName,
                                                     @RequestParam("externalParams") String externalParams) throws Exception {
        CreateInstanceEntity createInstanceEntity = new CreateInstanceEntity();
        int errorCode;
        String errorMessage;
        int frontendPort = 0;

        if (!isValidGraphName(graphName) || StringUtils.isEmpty(SecurityUtil.pathFilter(schemaPath))) {
            createInstanceEntity.setErrorCode(-1);
            createInstanceEntity.setErrorMessage("Invalid graph name");
            return createInstanceEntity;
        }
        try {
            checkPodNameList(podNameList);
            checkContainerName(containerName);
        } catch (Exception e) {
            createInstanceEntity.setErrorCode(-1);
            createInstanceEntity.setErrorMessage(e.getMessage());
            return createInstanceEntity;
        }
        try {
            List<String> createCommandList = new ArrayList<>();
            createCommandList.add(instanceProperties.getCreateScript());
            createCommandList.add(graphName);
            createCommandList.add(schemaPath);
            createCommandList.add(podNameList);
            createCommandList.add(containerName);
            createCommandList.add(externalParams);
            String command = StringUtils.join(createCommandList, " ");
            logger.info("start to create instance with command " + command);
            Process process = Runtime.getRuntime().exec(command);
            List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            infoValueList.addAll(errorValueList);
            errorMessage = StringUtils.join(infoValueList, "\n");
            errorCode = process.waitFor();
            if (errorCode == 0) {
                Pattern endpointPattern = Pattern.compile("FRONTEND_PORT:\\S+");
                Matcher matcher = endpointPattern.matcher(errorMessage);
                if (matcher.find()) {
                    String frontendEndpoint = StringUtils.splitByWholeSeparator(StringUtils.removeStart(matcher.group(), "FRONTEND_PORT:"), " ")[0];
                    InstanceEntity instanceEntity = new InstanceEntity(frontendEndpoint, podNameList, containerName, this.instanceProperties.getCloseScript());
                    FrontendMemoryStorage.getFrontendStorage().addFrontendEndpoint(graphName, instanceEntity);
                    String[] endpointArray = StringUtils.split(frontendEndpoint, ":");
                    createInstanceEntity.setFrontHost(endpointArray[0]);
                    frontendPort = Integer.parseInt(endpointArray[1]);
                    createInstanceEntity.setFrontPort(frontendPort);
                    if (!this.checkInstanceReady(endpointArray[0], frontendPort)) {
                        errorCode = -1;
                        errorMessage = "Check instance ready timeout";
                    }
                } else {
                    errorCode = -1;
                }
            }
        } catch (Exception e) {
            errorCode = -1;
            errorMessage = ExceptionUtils.getMessage(e);
        }
        createInstanceEntity.setErrorCode(errorCode);
        createInstanceEntity.setErrorMessage(errorMessage);
        return createInstanceEntity;
    }

    @RequestMapping("close")
    public CloseInstanceEntity closeInstance(@RequestParam("graphName") String graphName,
                                             @RequestParam("podNameList") String podNameList,
                                             @RequestParam("containerName") String containerName,
                                             @RequestParam("waitingForDelete") String waitingForDelete) {
        int errorCode;
        String errorMessage;
        try {
            if (!isValidGraphName(graphName)) {
                return new CloseInstanceEntity(-1, "Invalid graph name");
            }
            List<String> closeCommandList = new ArrayList<>();
            closeCommandList.add(instanceProperties.getCloseScript());
            closeCommandList.add("close_gremlin_instance_on_k8s");
            closeCommandList.add(graphName);
            closeCommandList.add(podNameList);
            closeCommandList.add(containerName);
            closeCommandList.add(waitingForDelete);
            String command = StringUtils.join(closeCommandList, " ");
            logger.info("start to close instance with command " + command);
            Process process = Runtime.getRuntime().exec(command);
            List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            infoValueList.addAll(errorValueList);
            errorMessage = StringUtils.join(infoValueList, "\n");
            errorCode = process.waitFor();
            FrontendMemoryStorage.getFrontendStorage().removeFrontendEndpoint(graphName);
        } catch (Exception e) {
            errorCode = -1;
            errorMessage = ExceptionUtils.getMessage(e);
        }
        return new CloseInstanceEntity(errorCode, errorMessage);
    }

    @RequestMapping("close_local")
    public CloseInstanceEntity closeInstance(@RequestParam("graphName") String graphName) {
        int errorCode;
        String errorMessage;

        try{
            if (!isValidGraphName(graphName)) {
                return new CloseInstanceEntity(-1, "Invalid graph name");
            }
            List<String> closeCommandList = new ArrayList<>();
            closeCommandList.add(instanceProperties.getCloseScript());
            closeCommandList.add("close_gremlin_instance_on_local");
            closeCommandList.add(graphName);
            String command = StringUtils.join(closeCommandList, " ");
            logger.info("start to close instance with command " + command);
            Process process = Runtime.getRuntime().exec(command);
            List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            infoValueList.addAll(errorValueList);
            errorMessage = StringUtils.join(infoValueList, "\n");
            errorCode = process.waitFor();
            FrontendMemoryStorage.getFrontendStorage().removeFrontendEndpoint(graphName);
        } catch (Exception e) {
            errorCode = -1;
            errorMessage = ExceptionUtils.getMessage(e);
        }

        return new CloseInstanceEntity(errorCode, errorMessage);
    }

    @RequestMapping("frontend")
    public String queryFrontendEndpoint(@RequestParam("graphName") String graphName) {
        return FrontendMemoryStorage.getFrontendStorage().getFrontendEndpoint(graphName).getFrontEndpoint();
    }

    public boolean isValidGraphName(String graphName) {
        String validPattern = "^\\w{1,128}$";
        return graphName.matches(validPattern);
    }
}
