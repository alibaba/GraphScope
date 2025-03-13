package ldbc.snb.datagen.hadoop.key.updatekey;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateEventKey implements WritableComparable<UpdateEventKey> {

    public long date;
    public int reducerId;
    public int partition;

    public UpdateEventKey() {
    }

    public UpdateEventKey(long date, int reducerId, int partition) {

        this.date = date;
        this.reducerId = reducerId;
        this.partition = partition;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(date);
        out.writeInt(reducerId);
        out.writeInt(partition);
    }

    public void readFields(DataInput in) throws IOException {
        date = in.readLong();
        reducerId = in.readInt();
        partition = in.readInt();
    }

    public int compareTo(UpdateEventKey key) {
        if (reducerId != key.reducerId) return reducerId - key.reducerId;
        if (partition != key.partition) return partition - key.partition;
        if (date < key.date) return -1;
        if (date > key.date) return 1;
        return 0;
    }
}
