package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.Person.PersonSummary;

import java.util.TreeSet;

abstract public class Message {

    private long messageId_;
    private long creationDate_;
    private PersonSummary author_;
    private long forumId_;
    private String content_;
    private TreeSet<Integer> tags_;
    private IP ipAddress_;
    private int browserId_;
    private int countryId_;

    public Message() {
        tags_ = new TreeSet<>();
        ipAddress_ = new IP();
    }

    public Message(long messageId,
                   long creationDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   int countryId,
                   IP ipAddress,
                   int browserId
    ) {

        assert ((author.creationDate() + DatagenParams.deltaTime) <= creationDate);
        messageId_ = messageId;
        creationDate_ = creationDate;
        author_ = new PersonSummary(author);
        forumId_ = forumId;
        content_ = content;
        tags_ = new TreeSet<>(tags);
        countryId_ = countryId;
        ipAddress_ = new IP(ipAddress);
        browserId_ = browserId;
    }

    public void initialize(long messageId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId
    ) {
        messageId_ = messageId;
        creationDate_ = creationDate;
        author_ = new PersonSummary(author);
        forumId_ = forumId;
        content_ = content;
        tags_.clear();
        tags_.addAll(tags);
        countryId_ = countryId;
        ipAddress_.copy(ipAddress);
        browserId_ = browserId;
    }

    public long messageId() {
        return messageId_;
    }

    public void messageId(long id) {
        messageId_ = id;
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long date) {
        creationDate_ = date;
    }

    public PersonSummary author() {
        return author_;
    }

    public long forumId() {
        return forumId_;
    }

    public void forumId(long id) {
        forumId_ = id;
    }

    public String content() {
        return content_;
    }

    public void content(String s) {
        content_ = s;
    }

    public TreeSet<Integer> tags() {
        return tags_;
    }

    public void tags(TreeSet<Integer> tags) {
        tags_.clear();
        tags_.addAll(tags);
    }

    public IP ipAddress() {
        return ipAddress_;
    }

    public void ipAddress(IP ip) {
        ipAddress_.copy(ip);
    }

    public int browserId() {
        return browserId_;
    }

    public void browserId(int browser) {
        browserId_ = browser;
    }

    public int countryId() {
        return countryId_;
    }

    public void countryId(int l) {
        countryId_ = l;
    }
}
