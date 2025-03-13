package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;

public class ForumMembership {
    private long forumId_;
    private long creationDate_;
    private Person.PersonSummary person_;

    public ForumMembership(long forumId, long creationDate, Person.PersonSummary p) {
        assert (p
                .creationDate() + DatagenParams.deltaTime) <= creationDate : "Person creation date is larger than membership";
        forumId_ = forumId;
        creationDate_ = creationDate;
        person_ = new Person.PersonSummary(p);
    }

    public long forumId() {
        return forumId_;
    }

    public void forumId(long forumId) {
        this.forumId_ = forumId;
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long creationDate) {
        creationDate_ = creationDate;
    }

    public Person.PersonSummary person() {
        return person_;
    }

    public void person(Person.PersonSummary p) {
        person_ = p;
    }

}
