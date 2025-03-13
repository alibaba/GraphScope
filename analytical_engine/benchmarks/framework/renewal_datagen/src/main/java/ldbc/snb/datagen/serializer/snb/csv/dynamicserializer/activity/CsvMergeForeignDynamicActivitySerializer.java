package ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvMergeForeignDynamicActivitySerializer extends DynamicActivitySerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_HASMEMBER_PERSON, FORUM_HASTAG_TAG, PERSON_LIKES_POST,
                PERSON_LIKES_COMMENT, POST, POST_HASTAG_TAG, COMMENT, COMMENT_HASTAG_TAG);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(FORUM).writeHeader(ImmutableList.of("id", "title", "creationDate", "moderator"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("Forum.id", "Person.id", "joinDate"));
        writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("Forum.id", "Tag.id"));
        writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("Person.id", "Post.id", "creationDate"));
        writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("Person.id", "Comment.id", "creationDate"));
        writers.get(POST).writeHeader(ImmutableList.of("id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"));
        writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("Post.id", "Tag.id"));
        writers.get(COMMENT).writeHeader(ImmutableList.of("id", "creationDate", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"));
        writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("Comment.id", "Tag.id"));
    }

    protected void serialize(final Forum forum) {
        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());

        writers.get(FORUM).writeEntry(ImmutableList.of(Long.toString(forum.id()), forum.title(), dateString, Long.toString(forum.moderator().accountId())));

        for (Integer i : forum.tags()) {
            writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(Long.toString(forum.id()), Integer.toString(i)));
        }

    }

    protected void serialize(final Post post) {
        writers.get(POST).writeEntry(ImmutableList.of(
            Long.toString(post.messageId()),
            "",
            Dictionaries.dates.formatDateTime(post.creationDate()),
            post.ipAddress().toString(),
            Dictionaries.browsers.getName(post.browserId()),
            Dictionaries.languages.getLanguageName(post.language()),
            post.content(),
            Integer.toString(post.content().length()),
            Long.toString(post.author().accountId()),
            Long.toString(post.forumId()),
            Integer.toString(post.countryId())
        ));

        for (Integer t : post.tags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                Long.toString(post.messageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Comment comment) {
        writers.get(COMMENT).writeEntry(ImmutableList.of(
            Long.toString(comment.messageId()),
            Dictionaries.dates.formatDateTime(comment.creationDate()),
            comment.ipAddress().toString(),
            Dictionaries.browsers.getName(comment.browserId()),
            comment.content(),
            Integer.toString(comment.content().length()),
            Long.toString(comment.author().accountId()),
            Integer.toString(comment.countryId()),
            comment.replyOf() == comment.postId() ? Long.toString(comment.postId()) : "",
            comment.replyOf() == comment.postId() ? "" : Long.toString(comment.replyOf())
        ));

        for (Integer t : comment.tags()) {
            writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(
                Long.toString(comment.messageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final Photo photo) {
        writers.get(POST).writeEntry(ImmutableList.of(
            Long.toString(photo.messageId()),
            photo.content(),
            Dictionaries.dates.formatDateTime(photo.creationDate()),
            photo.ipAddress().toString(),
            Dictionaries.browsers.getName(photo.browserId()),
            "",
            "",
            Integer.toString(0),
            Long.toString(photo.author().accountId()),
            Long.toString(photo.forumId()),
            Integer.toString(photo.countryId())
        ));

        for (Integer t : photo.tags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(
                Long.toString(photo.messageId()),
                Integer.toString(t)
            ));
        }
    }

    protected void serialize(final ForumMembership membership) {
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(
            Long.toString(membership.forumId()),
            Long.toString(membership.person().accountId()),
            Dictionaries.dates.formatDateTime(membership.creationDate())
        ));
    }

    protected void serialize(final Like like) {
        List<String> arguments = ImmutableList.of(
            Long.toString(like.user),
            Long.toString(like.messageId),
            Dictionaries.dates.formatDateTime(like.date)
        );
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(arguments);
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(arguments);
        }
    }

}
