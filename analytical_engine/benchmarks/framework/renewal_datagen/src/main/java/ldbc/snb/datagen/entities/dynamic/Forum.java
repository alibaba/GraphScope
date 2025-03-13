package ldbc.snb.datagen.entities.dynamic;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;

import java.util.ArrayList;
import java.util.List;

public class Forum {

    private long id_;
    private Person.PersonSummary moderator_;
    private long creationDate_;
    private String title_;
    private List<Integer> tags_;
    private int placeId_;
    private int language_;
    private List<ForumMembership> memberships_;


    public Forum(long id, long creationDate, Person.PersonSummary moderator, String title, int placeId, int language) {
        assert (moderator
                .creationDate() + DatagenParams.deltaTime) <= creationDate : "Moderator creation date is larger than message creation date";
        memberships_ = new ArrayList<>();
        tags_ = new ArrayList<>();
        id_ = id;
        creationDate_ = creationDate;
        title_ = title;
        placeId_ = placeId;
        moderator_ = new Person.PersonSummary(moderator);
        language_ = language;
    }

    public void addMember(ForumMembership member) {
        memberships_.add(member);
    }

    public long id() {
        return id_;
    }

    public void id(long id) {
        id_ = id;
    }

    public Person.PersonSummary moderator() {
        return moderator_;
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long creationDate) {
        creationDate_ = creationDate;
    }

    public List<Integer> tags() {
        return tags_;
    }

    public void tags(List<Integer> tags) {
        tags_.clear();
        tags_.addAll(tags);
    }

    public String title() {
        return title_;
    }

    public void title(String title) {
        this.title_ = title;
    }

    public List<ForumMembership> memberships() {
        return memberships_;
    }

    public int place() {
        return placeId_;
    }

    public void place(int placeId) {
        placeId_ = placeId;
    }

    public int language() {
        return language_;
    }

    public void language(int l) {
        language_ = l;
    }
}
