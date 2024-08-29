package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import com.alibaba.graphscope.example.circle.parallel.formal.PathSerAndDeser;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 磁盘模式读写文件抽象类
 *
 * @author liumin
 * @date 2024/8/19
 */
public abstract class FileObjectStorage implements IObjectStorage {
    private static final Logger logger = LoggerFactory.getLogger(FileObjectStorage.class);
    /**
     * 文件路径
     */
    private String path;
    /**
     * 文件是否追加写入
     */
    private boolean append;
    private File file;
    /**
     * 批量读写文件消息阈值
     */
    private final int msgThreshold;


    public boolean isAppend() {
        return append;
    }

    public String getPath() {
        return path;
    }

    public int getMsgThreshold() {
        return msgThreshold;
    }

    public File getFile() {
        return file;
    }

    public FileObjectStorage(String path, boolean append, int msgThreshold) {
        this.path = path;
        this.append = append;
        this.file = new File(path);
        this.msgThreshold = msgThreshold;
    }

    /**
     * 加载文件
     *
     * @param in 输入流
     * @throws IOException 抛出异常类型
     */
    public abstract void loadObjects(ObjectInputStream in) throws IOException;

    /**
     * 写入文件
     *
     * @param out 输出流
     */
    public abstract void dumpObjects(ObjectOutputStream out);

    @Override
    public void load() {
        long start = System.currentTimeMillis();
        File f = new File(path);
        if (!f.exists()) {
            return;
        }

        try (FileInputStream fileIn = new FileInputStream(f);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(fileIn, 1024);
             ObjectInputStream in = new ObjectInputStream(bufferedInputStream);) {

            clearInMemory();
            while (true) {
                try {
                    loadObjects(in);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("load error", e);
        }
    }

    @Override
    public void dump() {
        try (FileOutputStream fileOutputStream = new FileOutputStream
                (path, append);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream, 1024);
             ObjectOutputStream out = file.length() == 0 ? new ObjectOutputStream(bufferedOutputStream) : new AppendObjectOutputStream(bufferedOutputStream)) {
            dumpObjects(out);
            bufferedOutputStream.flush();
        } catch (Exception e) {
            logger.error("dump error", e);
        }
    }

    protected void clearInDisk(String filePrefix) {
        try (Stream<Path> listedFiles = Files.list(Paths.get("."))) {
            // 列出目录中的所有文件
            List<Path> files = listedFiles.filter(p -> p.getFileName().toString().startsWith(filePrefix)).collect(Collectors.toList());

            // 删除具有指定前缀的文件
            for (Path file : files) {
                Files.deleteIfExists(file);
                logger.info("Deleted: " + file);
            }
        } catch (IOException e) {
            logger.error("clearInDisk error.", e);
        }
    }

    public static void dumpVertexObjects(ObjectOutputStream out, Map<Integer, Set<LongArrayList>> dumpData) {
        try {
            if (dumpData.isEmpty()) {
                return;
            }

            out.writeInt(dumpData.size());
            // from begin to end vertex
            for (Map.Entry<Integer, Set<LongArrayList>> integerListEntry : dumpData.entrySet()) {
                out.writeInt(integerListEntry.getKey());
                Set<LongArrayList> paths = integerListEntry.getValue();
                out.writeInt(paths.size());
                for (LongArrayList path : paths) {
                    PathSerAndDeser.serialize(out, path);
                }
            }

            out.flush();
        } catch (IOException e) {
            logger.error("dumpObjects error", e);
        }
    }
}
