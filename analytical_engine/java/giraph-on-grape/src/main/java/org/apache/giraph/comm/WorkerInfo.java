package org.apache.giraph.comm;

/**
 * Contains information uniquely identify one worker(process) from others.
 */
public class WorkerInfo {
    private int workerId;
    private int workerNum;
    private String dstHostNameOrIp;
    private int initPort;
    private String[] dstHostNameOrIps;
    public WorkerInfo(int workerId, int workerNum, String hostNameOrIp, int port, String [] dstHostNameOrIps){
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.dstHostNameOrIp = hostNameOrIp;
        this.initPort = port;
        this.dstHostNameOrIps =dstHostNameOrIps;
    }

    public void setWorkerId(int workerId){
        this.workerId = workerId;
    }

    public void setWorkerNum(int workerNum){
        this.workerNum = workerNum;
    }

    public int getWorkerId(){
        return workerId;
    }

    public int getWorkerNum(){
        return workerNum;
    }

    /**
     * Return the ip/hostname to which the client should connect.
     * @return hostname or ip
     */
    public String getHost(){
        return dstHostNameOrIp;
    }

    /**
     * Return the port client should connect to.
     * @return the port
     */
    public int getInitPort(){
        return initPort;
    }

    public String [] getDstHostNameOrIps(){
        return dstHostNameOrIps;
    }
}
