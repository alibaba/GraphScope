package com.alibaba.graphscope.parallel.utils;

/**
 * Contains global worker's host and port info.
 */
public class NetworkMap {
    private int workerNum;
    private int workerId;

    /** length should be workerNum - 1 */
    private String [] allIpOrHostNames;
    private int[] allPorts;

    public int getWorkerNum(){
        return workerNum;
    }

    public int getSelfWorkerId(){
        return workerId;
    }

    public NetworkMap(int workerId, int workerNum, int initPort, String [] ipOrHostNames){
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.allIpOrHostNames = ipOrHostNames;
        this.allPorts = new int[workerNum];
        for (int i = 0; i < workerNum; ++i){
            allPorts[i] = initPort + i;
        }
    }

    public int getSelfPort(){
        return allPorts[workerId];
    }

    public String getSelfHostNameOrIp(){
        return allIpOrHostNames[workerId];
    }

    public String getAddress(){
        return getSelfHostNameOrIp() + ":" + getSelfPort();
    }

    public String getHostNameForWorker(int dstWorkerId){
        if (dstWorkerId >= workerNum){
            throw new IllegalArgumentException("Expected worker id less than: " + workerNum + " but received: " + dstWorkerId);
        }
        return allIpOrHostNames[dstWorkerId];
    }

    public int getPortForWorker(int dstWorkerId){
        if (dstWorkerId >= workerNum){
            throw new IllegalArgumentException("Expected worker id less than: " + workerNum + " but received: " + dstWorkerId);
        }
        return allPorts[dstWorkerId];
    }

}
