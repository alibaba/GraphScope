package ldbc.snb.datagen.entities.dynamic.messages;


import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.Person.PersonSummary;

import java.util.TreeSet;

public class Comment extends Message {

    private long postId_;
    private long replyOf_;

    public Comment() {
        super();
    }

    public Comment(Comment comment) {
        super(comment.messageId(), comment.creationDate(), comment.author(), comment.forumId(), comment.content(),
              comment.tags(), comment.countryId(), comment.ipAddress(), comment.browserId());
        postId_ = comment.postId();
        replyOf_ = comment.replyOf();
    }

    public Comment(long commentId,
                   long creationDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   int countryId,
                   IP ipAddress,
                   int browserId,
                   long postId,
                   long replyOf
    ) {

        super(commentId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        postId_ = postId;
        replyOf_ = replyOf;
    }

    public void initialize(long commentId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId,
                           long postId,
                           long replyOf) {
        super.initialize(commentId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        postId_ = postId;
        replyOf_ = replyOf;
    }

    public long postId() {
        return postId_;
    }

    public void postId(long id) {
        postId_ = id;
    }

    public long replyOf() {
        return replyOf_;
    }

    public void replyOf(long id) {
        replyOf_ = id;
    }

}
