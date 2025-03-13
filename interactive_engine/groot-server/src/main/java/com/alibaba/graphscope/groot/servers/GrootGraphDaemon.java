package com.alibaba.graphscope.groot.servers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GrootGraphDaemon {
    private static final Logger logger = LoggerFactory.getLogger(GrootGraphDaemon.class);
    private static final String GROOT_STORE_POD_ADMIN_PORT = "GROOT_STORE_POD_ADMIN_PORT";
    private static final int DEFAULT_PORT = 10001;
    private static final String GROOTGRAPH_NAME = "com.alibaba.graphscope.groot.servers.GrootGraph";

    private int healthCheckPort;
    private volatile boolean isShuttingDown = false;

    public GrootGraphDaemon() {
        this.healthCheckPort = getHealthCheckPort();
    }

    private int getHealthCheckPort() {
        String port = System.getenv(GROOT_STORE_POD_ADMIN_PORT);
        if (port != null) {
            try {
                return Integer.parseInt(port);
            } catch (NumberFormatException e) {
                logger.error(
                        "Failed to parse "
                                + GROOT_STORE_POD_ADMIN_PORT
                                + " from environment variable "
                                + port
                                + ", using default port "
                                + DEFAULT_PORT);
                return DEFAULT_PORT;
            }
        }
        return DEFAULT_PORT;
    }

    public static void main(String[] args) {
        GrootGraphDaemon daemon = new GrootGraphDaemon();
        daemon.start();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(healthCheckPort)) {
            logger.info("GrootGraph daemon started on port " + healthCheckPort);
            while (!isShuttingDown) {
                try (Socket clientSocket = serverSocket.accept()) {
                    handleClientRequest(clientSocket);
                } catch (IOException e) {
                    if (!isShuttingDown) {
                        logger.error("Failed to accept client connection", e);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to start daemon", e);
        }
    }

    private void handleClientRequest(Socket clientSocket) {
        try {
            // Read the request from client, only simple shutdown command expected
            byte[] buffer = new byte[1024];
            int readBytes = clientSocket.getInputStream().read(buffer);
            String request = new String(buffer, 0, readBytes, StandardCharsets.UTF_8).trim();

            // Respond to client
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            if (request.toLowerCase().contains("shutdown")) {
                isShuttingDown = true;
                // Shutdown GrootGraph
                shutdownGrootGraph();
                // Respond to client
                out.println("HTTP/1.0 200 OK");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("GrootGraph has been shutdown successfully.");
                out.flush();
                isShuttingDown = false;
            } else {
                out.println("HTTP/1.0 400 Bad Request");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("unknown command, only 'shutdown' is supported");
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void shutdownGrootGraph() {
        try {
            // Execute the command to get the PID of the GrootGraph process
            String[] cmd = {
                "/bin/sh", "-c", "ps -ef | grep -w " + GROOTGRAPH_NAME + " | grep -v grep"
            };
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
            List<Long> pids = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length > 1) {
                    try {
                        long pid = Long.parseLong(parts[1]);
                        pids.add(pid);
                    } catch (NumberFormatException e) {
                        logger.error("Failed to parse PID from: " + line);
                    }
                }
            }
            reader.close();

            for (Long pid : pids) {
                Optional<ProcessHandle> grootGraphProcess = ProcessHandle.of(pid);
                if (grootGraphProcess.isPresent()) {
                    ProcessHandle processHandle = grootGraphProcess.get();
                    processHandle.destroy();
                    if (processHandle.isAlive()) {
                        processHandle.onExit().join();
                    }
                    logger.debug(
                            "GrootGraph process with PID "
                                    + pid
                                    + " has been shut down successfully.");
                }
            }

        } catch (Exception e) {
            logger.error("Failed to shutdown GrootGraph", e);
        }
    }
}
