package org.apache.giraph.comm.requests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message payload representing aggregating value to master and to worker. The concrete class object
 * shall be find by aggregator Map.
 */
public class AggregatorMessage extends ByteArrayMessage {

    /**
     * The position where this aggregator resides.
     */
    private String aggregatorId;
    private String value;

    public AggregatorMessage() {

    }

    public String getValue() {
        return value;
    }

    public AggregatorMessage(String aggregatorId, String value,byte[] valueBytes) {
        this.aggregatorId = aggregatorId;
        this.value = value;
        this.setData(valueBytes);
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + aggregatorId.getBytes(StandardCharsets.UTF_8).length + value.getBytes(
            StandardCharsets.UTF_8).length;
    }

    public String getAggregatorId(){
        return aggregatorId;
    }

    @Override
    public NettyMessageType getMessageType() {
        return NettyMessageType.AGGREGATOR_MESSAGE;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        aggregatorId = input.readUTF();
        value = input.readUTF();
        super.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(aggregatorId);
        output.writeUTF(value);
        super.write(output);
    }

    public String toString(){
        return "aggregatorMessage:id[" + aggregatorId + "], value" + value;
    }
}
