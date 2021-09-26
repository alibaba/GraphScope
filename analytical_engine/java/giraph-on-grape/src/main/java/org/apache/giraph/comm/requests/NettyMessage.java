package org.apache.giraph.comm.requests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Base interface for all payload passed through netty communicator.
 */
public interface NettyMessage extends Writable {

    public abstract  int getSerializedSize();

    public abstract NettyMessageType getMessageType();
}
