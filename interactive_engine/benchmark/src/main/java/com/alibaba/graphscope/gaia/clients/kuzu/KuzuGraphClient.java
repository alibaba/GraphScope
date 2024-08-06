package com.alibaba.graphscope.gaia.clients.kuzu;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuConnection;
import com.kuzudb.KuzuDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuzuGraphClient implements GraphClient {
    KuzuConnection conn;
    private static Logger logger = LoggerFactory.getLogger(KuzuGraphClient.class);

    public KuzuGraphClient(String dbPath) {
        KuzuDatabase db = new KuzuDatabase(dbPath);
        try {
            this.conn = new KuzuConnection(db);
            logger.info("Connected to kuzu server at " + dbPath);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create connection with kuzu server");
        }
    }

    @Override
    public GraphResultSet submit(String query) {
        try {
            return new KuzuGraphResult(conn.query(query));
        } catch (Exception e) {
            e.printStackTrace();
            return new KuzuGraphResult();
        }
    }

    @Override
    public void close() {
        try {
            conn.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
