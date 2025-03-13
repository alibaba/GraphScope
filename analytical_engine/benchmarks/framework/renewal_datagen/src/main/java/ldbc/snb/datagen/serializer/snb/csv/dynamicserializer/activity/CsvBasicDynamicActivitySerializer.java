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

public class CsvBasicDynamicActivitySerializer extends DynamicActivitySerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_CONTAINEROF_POST, FORUM_HASMEMBER_PERSON, FORUM_HASMODERATOR_PERSON, FORUM_HASTAG_TAG,
                PERSON_LIKES_POST, PERSON_LIKES_COMMENT, POST, POST_HASCREATOR_PERSON, POST_HASTAG_TAG, POST_ISLOCATEDIN_PLACE,
                COMMENT, COMMENT_HASCREATOR_PERSON, COMMENT_HASTAG_TAG, COMMENT_ISLOCATEDIN_PLACE, COMMENT_REPLYOF_POST,
                COMMENT_REPLYOF_COMMENT);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(FORUM).writeHeader(ImmutableList.of("id", "title", "creationDate"));
        writers.get(FORUM_CONTAINEROF_POST).writeHeader(ImmutableList.of("Forum.id","Post.id"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(ImmutableList.of("Forum.id","Person.id","joinDate"));
        writers.get(FORUM_HASMODERATOR_PERSON).writeHeader(ImmutableList.of("Forum.id","Person.id"));
        writers.get(FORUM_HASTAG_TAG).writeHeader(ImmutableList.of("Forum.id","Tag.id"));
        writers.get(PERSON_LIKES_POST).writeHeader(ImmutableList.of("Person.id","Post.id","creationDate"));
        writers.get(PERSON_LIKES_COMMENT).writeHeader(ImmutableList.of("Person.id","Comment.id","creationDate"));
        writers.get(POST).writeHeader(ImmutableList.of("id","imageFile","creationDate","locationIP","browserUsed","language","content","length"));
        writers.get(POST_HASCREATOR_PERSON).writeHeader(ImmutableList.of("Post.id","Person.id"));
        writers.get(POST_HASTAG_TAG).writeHeader(ImmutableList.of("Post.id","Tag.id"));
        writers.get(POST_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("Post.id","Place.id"));
        writers.get(COMMENT).writeHeader(ImmutableList.of("id","creationDate","locationIP","browserUsed","content","length"));
        writers.get(COMMENT_HASCREATOR_PERSON).writeHeader(ImmutableList.of("Comment.id","Person.id"));
        writers.get(COMMENT_HASTAG_TAG).writeHeader(ImmutableList.of("Comment.id","Tag.id"));
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeHeader(ImmutableList.of("Comment.id","Place.id"));
        writers.get(COMMENT_REPLYOF_POST).writeHeader(ImmutableList.of("Comment.id","Post.id"));
        writers.get(COMMENT_REPLYOF_COMMENT).writeHeader(ImmutableList.of("Comment.id","Comment.id"));
    }

    protected void serialize(final Forum forum) {
        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());
        writers.get(FORUM).writeEntry(ImmutableList.of(Long.toString(forum.id()), forum.title(),dateString));
        writers.get(FORUM_HASMODERATOR_PERSON).writeEntry(ImmutableList.of(Long.toString(forum.id()),
                Long.toString(forum.moderator().accountId())));
        for (Integer i : forum.tags()) {
            writers.get(FORUM_HASTAG_TAG).writeEntry(ImmutableList.of(Long.toString(forum.id()),Integer.toString(i)));
        }
    }

    protected void serialize(final Post post) {
        writers.get(POST).writeEntry(ImmutableList.of(Long.toString(post.messageId()),"",
                Dictionaries.dates.formatDateTime(post.creationDate()),post.ipAddress().toString(),
                Dictionaries.browsers.getName(post.browserId()),Dictionaries.languages.getLanguageName(post.language()),
                post.content(),Integer.toString(post.content().length())));

        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(Long.toString(post.messageId()),
                Integer.toString(post.countryId())));
        writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(Long.toString(post.messageId()),
                Long.toString(post.author().accountId())));
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(Long.toString(post.forumId()),Long.toString(post.messageId())));
        for (Integer t : post.tags())
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(Long.toString(post.messageId()),Integer.toString(t)));

    }

    protected void serialize(final Comment comment) {
        writers.get(COMMENT).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Dictionaries.dates.formatDateTime(comment.creationDate()),
                comment.ipAddress().toString(),Dictionaries.browsers.getName(comment.browserId()),comment.content(),
                Integer.toString(comment.content().length())));

        if (comment.replyOf() == comment.postId()) {
            writers.get(COMMENT_REPLYOF_POST).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Long.toString(comment.postId())));
        } else {
            writers.get(COMMENT_REPLYOF_COMMENT).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Long.toString(comment.replyOf())));
        }
        writers.get(COMMENT_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Integer.toString(comment.countryId())));
        writers.get(COMMENT_HASCREATOR_PERSON).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Long.toString(comment.author().accountId())));
        for (Integer t : comment.tags())
            writers.get(COMMENT_HASTAG_TAG).writeEntry(ImmutableList.of(Long.toString(comment.messageId()),Integer.toString(t)));

    }

    protected void serialize(final Photo photo) {
        writers.get(POST).writeEntry(ImmutableList.of(Long.toString(photo.messageId()),photo.content(),Dictionaries.dates.formatDateTime(photo.creationDate()),
                photo.ipAddress().toString(),Dictionaries.browsers.getName(photo.browserId()),"","",Integer.toString(0)));

        writers.get(POST_ISLOCATEDIN_PLACE).writeEntry(ImmutableList.of(Long.toString(photo.messageId()),Integer.toString(photo.countryId())));
        writers.get(POST_HASCREATOR_PERSON).writeEntry(ImmutableList.of(Long.toString(photo.messageId()),Long.toString(photo.author().accountId())));
        writers.get(FORUM_CONTAINEROF_POST).writeEntry(ImmutableList.of(Long.toString(photo.forumId()),Long.toString(photo.messageId())));

        for (Integer t : photo.tags()) {
            writers.get(POST_HASTAG_TAG).writeEntry(ImmutableList.of(Long.toString(photo.messageId()),Integer.toString(t)));
        }
    }

    protected void serialize(final ForumMembership membership) {
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(ImmutableList.of(Long.toString(membership.forumId()),Long.toString(membership.person().accountId()),
                Dictionaries.dates.formatDateTime(membership.creationDate())));
    }

    protected void serialize(final Like like) {
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(ImmutableList.of(Long.toString(like.user),Long.toString(like.messageId),Dictionaries.dates.formatDateTime(like.date)));
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(ImmutableList.of(Long.toString(like.user),Long.toString(like.messageId),Dictionaries.dates.formatDateTime(like.date)));
        }
    }

}
