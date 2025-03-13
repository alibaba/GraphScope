package ldbc.snb.datagen.hadoop.key;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleKey implements WritableComparable<TupleKey> {
    public long key;
    public long id;

    public TupleKey() {
    }

    public TupleKey(TupleKey tK) {
        this.key = tK.key;
        this.id = tK.id;
    }

    public TupleKey(long key, long id) {
        this.key = key;
        this.id = id;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(key);
        out.writeLong(id);
    }

    public void readFields(DataInput in) throws IOException {
        key = in.readLong();
        id = in.readLong();
    }

    public int compareTo(TupleKey tk) {
        if (key < tk.key) return -1;
        if (key > tk.key) return 1;
        if (id < tk.id) return -1;
        if (id > tk.id) return 1;
        return 0;
    }
}
