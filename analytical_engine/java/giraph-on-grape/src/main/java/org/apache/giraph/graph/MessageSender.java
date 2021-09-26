package org.apache.giraph.graph;

import java.util.Iterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Providing message related interfaces.
 * @param <OID_T> vertex original id
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 * @param <OUT_MSG_T> out msg type
 */
public interface MessageSender<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, OUT_MSG_T extends Writable> {
    void sendMessage(OID_T id, OUT_MSG_T message);

    void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message);

    void sendMessageToMultipleEdges(Iterator<OID_T> vertexIdIterator, OUT_MSG_T message);

}
