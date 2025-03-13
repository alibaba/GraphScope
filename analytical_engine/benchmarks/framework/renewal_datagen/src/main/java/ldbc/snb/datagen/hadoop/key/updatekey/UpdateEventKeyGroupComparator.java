package ldbc.snb.datagen.hadoop.key.updatekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UpdateEventKeyGroupComparator extends WritableComparator {

    protected UpdateEventKeyGroupComparator() {
        super(UpdateEventKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UpdateEventKey keyA = (UpdateEventKey) a;
        UpdateEventKey keyB = (UpdateEventKey) b;
        if (keyA.reducerId != keyB.reducerId) return keyA.reducerId - keyB.reducerId;
        if (keyA.partition != keyB.partition) return keyA.partition - keyB.partition;
        return 0;
    }
}
