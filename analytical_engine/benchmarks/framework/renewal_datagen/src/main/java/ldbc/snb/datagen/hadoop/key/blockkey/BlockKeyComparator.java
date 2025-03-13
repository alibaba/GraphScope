package ldbc.snb.datagen.hadoop.key.blockkey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BlockKeyComparator extends WritableComparator {

    protected BlockKeyComparator() {
        super(BlockKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        BlockKey keyA = (BlockKey) a;
        BlockKey keyB = (BlockKey) b;
        if (keyA.block < keyB.block) return -1;
        if (keyA.block > keyB.block) return 1;
        return keyA.tk.compareTo(keyB.tk);
    }
}
