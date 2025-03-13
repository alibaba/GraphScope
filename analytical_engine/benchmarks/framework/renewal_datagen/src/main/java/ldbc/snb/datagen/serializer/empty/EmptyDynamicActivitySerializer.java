package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyDynamicActivitySerializer extends DynamicActivitySerializer {

    @Override
    public List<FileName> getFileNames() {
        return Collections.emptyList();
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        //This is left intentionally blank
    }

    @Override
    public void close() {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Forum forum) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Post post) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Comment comment) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Photo photo) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final ForumMembership membership) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Like like) {
        //This is left intentionally blank
    }

    @Override
    public Map initialize(Configuration conf, int reducerId, boolean dynamic, List list) throws IOException {
        return Collections.emptyMap();
    }

}
