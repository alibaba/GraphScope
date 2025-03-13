
package ldbc.snb.datagen.entities.dynamic.relations;

public class Like {
    public enum LikeType {
        POST,
        COMMENT,
        PHOTO
    }

    public long user;
    public long userCreationDate;
    public long messageId;
    public long date;
    public LikeType type;
}
