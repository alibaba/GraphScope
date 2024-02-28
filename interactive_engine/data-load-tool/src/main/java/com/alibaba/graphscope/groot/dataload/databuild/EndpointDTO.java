package com.alibaba.graphscope.groot.dataload.databuild;

public class EndpointDTO {

    private String ip;

    private int port;

    public EndpointDTO(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String toAddress() {
        return ip + ":" + port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
