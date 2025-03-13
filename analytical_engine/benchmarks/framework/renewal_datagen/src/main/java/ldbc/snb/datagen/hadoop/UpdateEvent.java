
package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateEvent implements Writable {

    public enum UpdateEventType {
        ADD_PERSON,
        ADD_LIKE_POST,
        ADD_LIKE_COMMENT,
        ADD_FORUM,
        ADD_FORUM_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_FRIENDSHIP,
        NO_EVENT
    }

    public long date;
    public long dependantDate;
    public String eventData;
    public UpdateEventType type;

    public UpdateEvent(long date, long dependantDate, UpdateEventType type, String eventData) {
        this.date = date;
        this.type = type;
        this.eventData = eventData;
        this.dependantDate = dependantDate;
    }

    public void readFields(DataInput arg0) throws IOException {
        this.date = arg0.readLong();
        this.dependantDate = arg0.readLong();
        this.type = UpdateEventType.values()[arg0.readInt()];
        this.eventData = arg0.readUTF();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(date);
        arg0.writeLong(dependantDate);
        arg0.writeInt(type.ordinal());
        arg0.writeUTF(eventData);
    }
}
