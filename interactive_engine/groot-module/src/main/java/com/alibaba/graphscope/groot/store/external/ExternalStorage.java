package com.alibaba.graphscope.groot.store.external;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public abstract class ExternalStorage {
    private static final Logger logger = LoggerFactory.getLogger(ExternalStorage.class);

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

    public abstract void downloadDataSimple(String srcPath, String dstPath) throws IOException;

    public void downloadData(String srcPath, String dstPath) throws IOException {
        // Check chk
        String chkPath = srcPath.substring(0, srcPath.length() - ".sst".length()) + ".chk";
        String chkLocalPath = dstPath.substring(0, dstPath.length() - ".sst".length()) + ".chk";

        downloadDataSimple(chkPath, chkLocalPath);
        File chkFile = new File(chkLocalPath);
        byte[] chkData = new byte[(int) chkFile.length()];
        try {
            FileInputStream fis = new FileInputStream(chkFile);
            fis.read(chkData);
            fis.close();
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
        String[] chkArray = new String(chkData).split(",");
        if ("0".equals(chkArray[0])) {
            return;
        }
        String chkMD5Value = chkArray[1];
        downloadDataSimple(srcPath, dstPath);
        String sstMD5Value = getFileMD5(dstPath);
        if (!chkMD5Value.equals(sstMD5Value)) {
            logger.error("Checksum failed for " + chkLocalPath + " versus " + dstPath);
            logger.error("Expect [" + chkMD5Value + "], got [" + sstMD5Value + "]");
            throw new IOException("CheckSum failed for " + srcPath);
        } else {
            // The .chk file are now useless
            chkFile.delete();
        }
    }

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
