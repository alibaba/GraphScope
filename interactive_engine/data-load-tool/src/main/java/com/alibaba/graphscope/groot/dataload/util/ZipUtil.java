package com.alibaba.graphscope.groot.dataload.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZipUtil.class);
    private static final int COMPRESS_LEVEL = 1;

    public static void compress(String directory, OutputStream outputStream) throws IOException {
        logger.info("create zip output stream for {}", directory);
        ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(outputStream));
        zipOut.setLevel(COMPRESS_LEVEL);
        try {
            compressDirectory(directory, zipOut);
            zipOut.finish();
        } finally {
            zipOut.close();
        }
    }

    private static void compressDirectory(String path, final ZipOutputStream zipOutputStream)
            throws IOException {
        File file = new File(path);
        if (file.isFile()) {
            compressFile(path, zipOutputStream);
        } else if (file.isDirectory()) {
            File[] array = file.listFiles();
            // process empty directory
            assert array != null;
            if (array.length < 1) {
                logger.warn("zipping empty directory for {}!!!", path);
                ZipEntry ze = new ZipEntry(path + File.separator);
                zipOutputStream.putNextEntry(ze);
                zipOutputStream.closeEntry();
            }
            for (File value : array) {
                if (value.isFile()) {
                    compressFile(path + File.separator + value.getName(), zipOutputStream);
                } else if (value.isDirectory()) {
                    compressDirectory(path + File.separator + value.getName(), zipOutputStream);
                }
            }
        }
    }

    private static void compressFile(String path, final ZipOutputStream zipOutputStream)
            throws IOException {
        ZipEntry ze = new ZipEntry(path);
        zipOutputStream.putNextEntry(ze);
        File file = new File(path);
        Files.copy(file.toPath(), zipOutputStream);
        zipOutputStream.closeEntry();
    }
}
