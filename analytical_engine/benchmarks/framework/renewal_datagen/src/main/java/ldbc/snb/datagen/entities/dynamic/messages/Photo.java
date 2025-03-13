package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.Person.PersonSummary;

import java.util.TreeSet;

public class Photo extends Message {

    private double latt_;
    private double longt_;

    public Photo() {
        super();
    }

    public Photo(long messageId,
                 long creationDate,
                 PersonSummary author,
                 long forumId,
                 String content,
                 TreeSet<Integer> tags,
                 int countryId,
                 IP ipAddress,
                 int browserId,
                 double latt,
                 double longt
    ) {
        super(messageId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        latt_ = latt;
        longt_ = longt;
    }

    public void initialize(long messageId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId,
                           double latt,
                           double longt
    ) {
        super.initialize(messageId, creationDate, author, forumId, content, tags, countryId, ipAddress, browserId);
        latt_ = latt;
        longt_ = longt;
    }

}
