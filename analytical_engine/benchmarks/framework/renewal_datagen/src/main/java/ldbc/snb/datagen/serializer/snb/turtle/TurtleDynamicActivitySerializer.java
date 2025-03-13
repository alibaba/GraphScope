package ldbc.snb.datagen.serializer.snb.turtle;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.RDF;
import ldbc.snb.datagen.vocabulary.SN;
import ldbc.snb.datagen.vocabulary.SNTAG;
import ldbc.snb.datagen.vocabulary.SNVOC;
import ldbc.snb.datagen.vocabulary.XSD;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class TurtleDynamicActivitySerializer extends DynamicActivitySerializer<HdfsWriter> implements TurtleSerializer {

    private long membershipId = 0;
    private long likeId = 0;

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_ACTIVITY);
    }

    @Override
    public void writeFileHeaders() { }

    protected void serialize(final Forum forum) {

        StringBuffer result = new StringBuffer(12000);

        String forumPrefix = SN.getForumURI(forum.id());
        Turtle.addTriple(result, true, false, forumPrefix, RDF.type, SNVOC.Forum);

        Turtle.addTriple(result, false, false, forumPrefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(forum.id()), XSD.Long));

        Turtle.addTriple(result, false, false, forumPrefix, SNVOC.title,
                         Turtle.createLiteral(forum.title()));
        Turtle.addTriple(result, false, true, forumPrefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(forum.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, forumPrefix,
                               SNVOC.hasModerator, SN.getPersonURI(forum.moderator().accountId()));

        for (Integer tag : forum.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

    protected void serialize(final Post post) {

        StringBuffer result = new StringBuffer(2500);

        String prefix = SN.getPostURI(post.messageId());

        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(post.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(post.creationDate()), XSD.DateTime));

        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(post.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(post.browserId())));

        Turtle.addTriple(result, false, false, prefix, SNVOC.content,
                         Turtle.createLiteral(post.content()));
        Turtle.addTriple(result, false, true, prefix, SNVOC.length,
                         Turtle.createDataTypeLiteral(Integer.toString(post.content().length()), XSD.Int));

        Turtle.createTripleSPO(result, prefix, SNVOC.language,
                               Turtle.createLiteral(Dictionaries.languages.getLanguageName(post.language())));

        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(post.countryId())));

        Turtle.createTripleSPO(result, SN.getForumURI(post.forumId()), SNVOC.containerOf, prefix);
        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(post.author().accountId()));

        for (Integer tag : post.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

    protected void serialize(final Comment comment) {
        StringBuffer result = new StringBuffer(2000);

        String prefix = SN.getCommentURI(comment.messageId());

        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Comment);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(comment.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(comment.creationDate()), XSD.DateTime));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(comment.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(comment.browserId())));
        Turtle.addTriple(result, false, false, prefix, SNVOC.content,
                         Turtle.createLiteral(comment.content()));
        Turtle.addTriple(result, false, true, prefix, SNVOC.length,
                         Turtle.createDataTypeLiteral(Integer.toString(comment.content().length()), XSD.Int));

        String replied = (comment.replyOf() == comment.postId()) ? SN.getPostURI(comment.postId()) :
                SN.getCommentURI(comment.replyOf());
        Turtle.createTripleSPO(result, prefix, SNVOC.replyOf, replied);
        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(comment.countryId())));

        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator,
                               SN.getPersonURI(comment.author().accountId()));

        for (Integer tag : comment.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

    protected void serialize(final Photo photo) {
        StringBuffer result = new StringBuffer(2500);

        String prefix = SN.getPostURI(photo.messageId());
        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Post);

        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(photo.messageId()), XSD.Long));

        Turtle.addTriple(result, false, false, prefix, SNVOC.hasImage, Turtle.createLiteral(photo.content()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(photo.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(photo.browserId())));
        Turtle.addTriple(result, false, true, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(photo.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, prefix, SNVOC.hasCreator, SN.getPersonURI(photo.author().accountId()));
        Turtle.createTripleSPO(result, SN.getForumURI(photo.forumId()), SNVOC.containerOf, prefix);
        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn,
                               DBP.fullPrefixed(Dictionaries.places.getPlaceName(photo.countryId())));

        for (Integer tag : photo.tags()) {
            String topic = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasTag, SNTAG.fullPrefixed(topic));
        }
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

    protected void serialize(final ForumMembership membership) {
        String memberhipPrefix = SN.getMembershipURI(SN.formId(membershipId));
        String forumPrefix = SN.getForumURI(membership.forumId());
        StringBuffer result = new StringBuffer(19000);
        Turtle.createTripleSPO(result, forumPrefix, SNVOC.hasMember, memberhipPrefix);

        Turtle.addTriple(result, true, false, memberhipPrefix, SNVOC.hasPerson, SN
                .getPersonURI(membership.person().accountId()));
        Turtle.addTriple(result, false, true, memberhipPrefix, SNVOC.joinDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(membership.creationDate()), XSD.DateTime));
        membershipId++;
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

    protected void serialize(final Like like) {
        StringBuffer result = new StringBuffer(2500);
        long id = SN.formId(likeId);
        String likePrefix = SN.getLikeURI(id);
        Turtle.createTripleSPO(result, SN.getPersonURI(like.user),
                               SNVOC.like, likePrefix);

        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.PHOTO) {
            String prefix = SN.getPostURI(like.messageId);
            Turtle.addTriple(result, true, false, likePrefix, SNVOC.hasPost, prefix);
        } else {
            String prefix = SN.getCommentURI(like.messageId);
            Turtle.addTriple(result, true, false, likePrefix, SNVOC.hasComment, prefix);
        }
        Turtle.addTriple(result, false, true, likePrefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(like.date), XSD.DateTime));
        likeId++;
        writers.get(SOCIAL_NETWORK_ACTIVITY).write(result.toString());
    }

}
