package com.alibaba.graphscope.groot.store.external;

import org.apache.commons.codec.binary.Hex;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public abstract class ExternalStorage {
    public static ExternalStorage getStorage(String path, Map<String, String> config)
            throws IOException {
        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        switch (scheme) {
            case "hdfs":
                return new HdfsStorage(path);
            case "oss":
                return new OssStorage(path, config);
            case "volume":
                return new VolumeStorage(path, config);
            default:
                throw new IllegalArgumentException(
                        "external storage scheme [" + scheme + "] not supported");
        }
    }

    public abstract void downloadData(String srcPath, String dstPath) throws IOException;

    public String getFileMD5(String fileName) throws IOException {
        FileInputStream fis = null;
        try {
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            fis = new FileInputStream(fileName);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fis.read(buffer)) != -1) {
                MD5.update(buffer, 0, length);
            }
            return new String(Hex.encodeHex(MD5.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }
}
