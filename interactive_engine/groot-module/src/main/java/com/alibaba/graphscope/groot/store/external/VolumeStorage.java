package com.alibaba.graphscope.groot.store.external;

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

    public VolumeStorage(String path, Map<String, String> config) throws IOException {
        String endpoint = config.get("odps.endpoint");
        String accessID = config.get("odps.access.id");
        String accessKey = config.get("odps.access.key");
        AliyunAccount account = new AliyunAccount(accessID, accessKey);

        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);
        tunnel = new VolumeTunnel(odps);

        projectName = config.get("odps.volume.project");
        volumeName = config.get("odps.volume.name");
        partSpec = config.get("odps.volume.partspec");
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
