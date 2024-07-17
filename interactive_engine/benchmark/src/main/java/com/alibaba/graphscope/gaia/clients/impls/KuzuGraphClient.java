package com.alibaba.graphscope.gaia.clients.impls;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuConnection;
import com.kuzudb.KuzuDatabase;

public class KuzuGraphClient implements GraphClient {
    KuzuConnection conn;

    public KuzuGraphClient(String dbPath) {
        KuzuDatabase db = new KuzuDatabase(dbPath);
        this.conn = new KuzuConnection(db);
    }

    @Override
    public GraphResultSet submit(String query) {
        try {
            return new KuzuGraphResult(conn.query(query));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
