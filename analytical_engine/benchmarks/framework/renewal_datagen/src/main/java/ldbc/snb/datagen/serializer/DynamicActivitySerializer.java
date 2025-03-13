package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;

abstract public class DynamicActivitySerializer<TWriter extends HdfsWriter> extends LdbcSerializer<TWriter> {

    abstract protected void serialize(final Forum forum);

    abstract protected void serialize(final Post post);

    abstract protected void serialize(final Comment comment);

    abstract protected void serialize(final Photo photo);

    abstract protected void serialize(final ForumMembership membership);

    abstract protected void serialize(final Like like);

    public void export(final Forum forum) {
        serialize(forum);
    }

    public void export(final ForumMembership forumMembership) {
        serialize(forumMembership);
    }

    public void export(final Post post) {
        serialize(post);
    }

    public void export(Comment comment) { serialize(comment); }

    public void export(Photo photo) { serialize(photo); }

    public void export(Like like) { serialize(like); }

    @Override
    protected boolean isDynamic() {
        return true;
    }

}
