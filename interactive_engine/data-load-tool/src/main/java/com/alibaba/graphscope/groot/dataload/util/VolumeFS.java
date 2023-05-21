package com.alibaba.graphscope.groot.dataload.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Volume;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.tunnel.VolumeTunnel;
import com.aliyun.odps.volume.FileSystem;
import com.aliyun.odps.volume.Path;

import java.io.*;
import java.util.*;

public class VolumeFS extends AbstractFileSystem {
    private String projectName;
    private String volumeName;
    private String partSpec;
    FileSystem fs;

    public VolumeFS(Properties properties) {
        projectName = properties.getProperty(Constants.ODPS_VOLUME_PROJECT); // Could be null
        volumeName = properties.getProperty(Constants.ODPS_VOLUME_NAME);
        partSpec = properties.getProperty(Constants.ODPS_VOLUME_PARTSPEC);
    }

    public VolumeFS(JobConf jobConf) throws IOException {
        projectName = jobConf.get(Constants.ODPS_VOLUME_PROJECT);
        volumeName = jobConf.get(Constants.ODPS_VOLUME_NAME);
        partSpec = jobConf.get(Constants.ODPS_VOLUME_PARTSPEC);
    }

    public void setJobConf(JobConf jobConf) {
        jobConf.set(Constants.ODPS_VOLUME_PROJECT, projectName);
        jobConf.set(Constants.ODPS_VOLUME_NAME, volumeName);
        jobConf.set(Constants.ODPS_VOLUME_PARTSPEC, partSpec);
    }

    public void open(TaskContext context, String mode) throws IOException {
        if (mode.equalsIgnoreCase("w")) {
            fs = context.getOutputVolumeFileSystem();
        } else {
            fs = context.getInputVolumeFileSystem();
        }
    }

    public VolumeInfo getVolumeInfo() {
        return new VolumeInfo(projectName, volumeName, partSpec, "__default__");
    }

    public void uploadLocalDirToVolumeCompressed(String path, OutputStream stream)
            throws IOException {
        ZipUtil.compress(path, stream);
    }

    public void copy(String srcFile, String dstFile) throws IOException {
        DataOutputStream outputStream = fs.create(new Path(dstFile));
        FileInputStream fileInputStream = new FileInputStream(srcFile);
        byte[] buffer = new byte[1024];
        while (true) {
            int len = fileInputStream.read(buffer);
            if (len == -1) {
                break;
            }
            outputStream.write(buffer, 0, len);
        }
        outputStream.close();
    }

    public void createDirectory(String dirName) throws IOException {}

    public Map<String, String> setConfig(Odps odps) throws IOException {
        Account account = odps.getAccount();
        AliyunAccount aliyunAccount =
                account instanceof AliyunAccount ? ((AliyunAccount) account) : null;
        if (aliyunAccount == null) {
            throw new IOException("Not an AliyunAccount");
        }
        HashMap<String, String> config = new HashMap<>();
        config.put(Constants.ODPS_ACCESS_ID, aliyunAccount.getAccessId());
        config.put(Constants.ODPS_ACCESS_KEY, aliyunAccount.getAccessKey());
        config.put(Constants.ODPS_ENDPOINT, odps.getEndpoint());
        config.put(Constants.ODPS_VOLUME_PROJECT, projectName);
        config.put(Constants.ODPS_VOLUME_NAME, volumeName);
        config.put(Constants.ODPS_VOLUME_PARTSPEC, partSpec);
        return config;
    }

    public String getQualifiedPath() {
        return "volume://";
    }

    public String readToString(String fileName) throws IOException {
        DataInputStream dis = fs.open(new Path(fileName));
        String str = dis.readUTF();
        dis.close();
        return str;
    }

    public void copyDirectoryRecursively(String path) throws IOException {
        List<String> fileSets = globFiles(path);
        for (int i = 0; i < fileSets.size(); i++) {
            Path dstPath = new Path(fileSets.get(i));
            DataOutputStream outputStream = fs.create(dstPath);
            FileInputStream fileInputStream = new FileInputStream(fileSets.get(i));
            byte[] buffer = new byte[1024];
            while (fileInputStream.read(buffer) != -1) {
                outputStream.write(buffer);
            }
            outputStream.close();
        }
    }

    private void uploadFileTunnel(
            Odps odps,
            String projectName,
            String volumeName,
            String partitionName,
            String fileName,
            String fileContent)
            throws Exception {
        VolumeTunnel volumeTunnel = new VolumeTunnel(odps);

        String[] files = new String[] {fileName};
        VolumeTunnel.UploadSession uploadSession =
                volumeTunnel.createUploadSession(projectName, volumeName, partitionName);
        OutputStream outputStream = uploadSession.openOutputStream(fileName);
        outputStream.write(fileContent.getBytes());
        outputStream.close();
        uploadSession.commit(files);
    }

    public static List<String> globFiles(String path) {
        List<String> fileNameSet = new ArrayList<>();
        File file = new File(path);
        if (file.isFile()) {
            fileNameSet.add(path);
        } else {
            for (File value : file.listFiles()) {
                if (value.isFile()) {
                    fileNameSet.add(value.getPath());
                } else if (value.isDirectory()) {
                    fileNameSet.addAll(globFiles(value.getPath()));
                }
            }
        }

        return fileNameSet;
    }

    public void createVolumeIfNotExists(Odps odps) throws IOException {
        try {
            if (!odps.volumes().exists(projectName, volumeName)) {
                odps.volumes()
                        .create(
                                projectName,
                                volumeName,
                                "created by groot data-load-tools",
                                Volume.Type.OLD,
                                7L);
            }
        } catch (OdpsException e) {
            System.out.println(
                    "Exception during creating volume ["
                            + getVolumeInfo()
                            + "] ["
                            + e.getRequestId()
                            + "] ["
                            + e.getErrorCode()
                            + "]: "
                            + e.getMessage());
            throw new IOException(e.getMessage());
        }
    }

    public void close() {}
}
