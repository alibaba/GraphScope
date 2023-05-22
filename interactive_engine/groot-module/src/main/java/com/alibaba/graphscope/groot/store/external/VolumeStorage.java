package com.alibaba.graphscope.groot.store.external;

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.VolumeTunnel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class VolumeStorage extends ExternalStorage {
    private static final Logger logger = LoggerFactory.getLogger(VolumeStorage.class);
    VolumeTunnel tunnel;
    String projectName;
    String volumeName;
    String partSpec;

    public VolumeStorage(String path, Map<String, String> config) {
        String endpoint = config.get(DataLoadConfig.ODPS_ENDPOINT);
        String accessID = config.get(DataLoadConfig.ODPS_ACCESS_ID);
        String accessKey = config.get(DataLoadConfig.ODPS_ACCESS_KEY);
        AliyunAccount account = new AliyunAccount(accessID, accessKey);

        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);
        tunnel = new VolumeTunnel(odps);

        projectName = config.get(DataLoadConfig.ODPS_VOLUME_PROJECT);
        volumeName = config.get(DataLoadConfig.ODPS_VOLUME_NAME);
        partSpec = config.get(DataLoadConfig.ODPS_VOLUME_PARTSPEC);
    }

    @Override
    public void downloadDataSimple(String srcPath, String dstPath) throws IOException {
        logger.info("Downloading " + srcPath + " to " + dstPath);
        String[] pathItems = srcPath.split("://");
        String fileName = pathItems[1];
        // Read data from the input stream and write it to the output stream.
        byte[] buffer = new byte[1024];
        int bytesRead;
        VolumeTunnel.DownloadSession session;
        try {
            session = tunnel.createDownloadSession(projectName, volumeName, partSpec, fileName);
            try (InputStream inputStream = session.openInputStream()) {
                try (OutputStream outputStream = Files.newOutputStream(Paths.get(dstPath))) {
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
            }
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }
}
