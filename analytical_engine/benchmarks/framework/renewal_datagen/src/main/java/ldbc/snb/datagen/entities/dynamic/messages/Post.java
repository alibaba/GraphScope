package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.Person.PersonSummary;

import java.util.TreeSet;

public class Post extends Message {

    private int language_;

    /**
     * < @brief The language used in the post.
     */

    public Post() {
        super();
    }

    public Post(long postId,
                long creationDate,
                PersonSummary author,
                long forumId,
                String content,
                TreeSet<Integer> tags,
                int countryId,
                IP ipAddress,
                int browserId,
                int language
    ) {
        super(postId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        language_ = language;
    }

    public void initialize(long postId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId,
                           int language
    ) {
        super.initialize(postId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        language_ = language;
    }

    public int language() {
        return language_;
    }

    public void language(int l) {
        language_ = l;
    }

}
