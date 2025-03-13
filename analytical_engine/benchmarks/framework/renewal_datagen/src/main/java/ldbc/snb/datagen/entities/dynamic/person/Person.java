package ldbc.snb.datagen.entities.dynamic.person;

import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class Person implements Writable {


    private long accountId_;
    private long creationDate_;
    private long maxNumKnows_;
    private TreeSet<Knows> knows_;
    private int browserId_;
    private IP ipAddress_;
    private int countryId_;
    private int cityId_;
    private long wallId_;
    private TreeSet<Integer> interests_;
    private int mainInterest_;
    private int universityLocationId_;
    private byte gender_;
    private long birthday_;
    private boolean isLargePoster_;
    private long randomId_;

    private TreeSet<String> emails_;
    private List<Integer> languages_;
    private String firstName_;
    private String lastName_;
    private Map<Long, Long> companies_;
    private long classYear_;

    public static interface PersonSimilarity {
        public float similarity(Person personA, Person personB);
    }

    public static class PersonSummary implements Writable {
        private long accountId_;
        private long creationDate_;
        private int browserId_;
        private int country_;
        private IP ipAddress_;
        private boolean isLargePoster_;

        public PersonSummary() {
            ipAddress_ = new IP();
        }

        public PersonSummary(Person p) {
            accountId_ = p.accountId();
            creationDate_ = p.creationDate();
            browserId_ = p.browserId();
            country_ = p.countryId();
            ipAddress_ = new IP(p.ipAddress());
            isLargePoster_ = p.isLargePoster();
        }

        public PersonSummary(PersonSummary p) {
            accountId_ = p.accountId();
            creationDate_ = p.creationDate();
            browserId_ = p.browserId();
            country_ = p.countryId();
            ipAddress_ = new IP(p.ipAddress());
            isLargePoster_ = p.isLargePoster();
        }

        public void copy(PersonSummary p) {
            accountId_ = p.accountId();
            creationDate_ = p.creationDate();
            browserId_ = p.browserId();
            country_ = p.countryId();
            ipAddress_ = new IP(p.ipAddress());
            isLargePoster_ = p.isLargePoster();
        }

        public long accountId() {
            return accountId_;
        }

        public void accountId(long id) {
            accountId_ = id;
        }

        public long creationDate() {
            return creationDate_;
        }

        public void creationDate(long creationDate) {
            creationDate_ = creationDate;
        }

        public int browserId() {
            return browserId_;
        }

        public void browserId(int browserId) {
            browserId_ = browserId;
        }

        public int countryId() {
            return country_;
        }

        public void countryId(int countryId) {
            country_ = countryId;
        }

        public IP ipAddress() {
            return ipAddress_;
        }

        public void ipAddress(IP ipAddress) {
            ipAddress_ = ipAddress;
        }

        public boolean isLargePoster() {
            return isLargePoster_;
        }

        public void readFields(DataInput arg0) throws IOException {
            accountId_ = arg0.readLong();
            creationDate_ = arg0.readLong();
            browserId_ = arg0.readInt();
            country_ = arg0.readInt();
            ipAddress_.readFields(arg0);
            isLargePoster_ = arg0.readBoolean();
        }

        public void write(DataOutput arg0) throws IOException {
            arg0.writeLong(accountId_);
            arg0.writeLong(creationDate_);
            arg0.writeInt(browserId_);
            arg0.writeInt(country_);
            ipAddress_.write(arg0);
            arg0.writeBoolean(isLargePoster_);
        }
    }

    public Person() {
        knows_ = new TreeSet<>();
        emails_ = new TreeSet<>();
        interests_ = new TreeSet<>();
        languages_ = new ArrayList<>();
        companies_ = new HashMap<>();
        ipAddress_ = new IP();
    }

    public Person(Person p) {
        knows_ = new TreeSet<>();
        emails_ = new TreeSet<>();
        interests_ = new TreeSet<>();
        languages_ = new ArrayList<>();
        companies_ = new HashMap<>();

        accountId_ = p.accountId();
        creationDate_ = p.creationDate();
        maxNumKnows_ = p.maxNumKnows();
        for (Knows k : p.knows()) {
            knows_.add(new Knows(k));
        }

        browserId_ = p.browserId();
        ipAddress_ = new IP(p.ipAddress());

        countryId_ = p.countryId();
        cityId_ = p.cityId();
        wallId_ = p.wallId();
        for (Integer t : p.interests().descendingSet()) {
            interests_.add(t);
        }
        mainInterest_ = p.mainInterest();

        universityLocationId_ = p.universityLocationId();
        gender_ = p.gender();
        birthday_ = p.birthday();
        isLargePoster_ = p.isLargePoster();
        randomId_ = p.randomId();

        for (String s : p.emails().descendingSet()) {
            emails_.add(s);
        }

        for (Integer i : p.languages()) {
            languages_.add(i);
        }

        firstName_ = new String(p.firstName());
        lastName_ = new String(p.lastName());
        for (Map.Entry<Long, Long> c : p.companies().entrySet()) {
            companies_.put(c.getKey(), c.getValue());
        }
        classYear_ = p.classYear();

    }

    public long accountId() {
        return accountId_;
    }

    public void accountId(long id) {
        accountId_ = id;
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long creationDate) {
        creationDate_ = creationDate;
    }

    public long maxNumKnows() {
        return maxNumKnows_;
    }

    public void maxNumKnows(long maxKnows) {
        maxNumKnows_ = maxKnows;
    }

    public TreeSet<Knows> knows() {
        return knows_;
    }

    public void knows(TreeSet<Knows> knows) {
        knows_.clear();
        knows_.addAll(knows);
    }

    public int browserId() {
        return browserId_;
    }

    public void browserId(int browserId) {
        browserId_ = browserId;
    }

    public IP ipAddress() {
        return ipAddress_;
    }

    public void ipAddress(IP ipAddress) {
        ipAddress_.copy(ipAddress);
    }

    public int countryId() {
        return countryId_;
    }

    public void countryId(int countryId) {
        countryId_ = countryId;
    }

    public int cityId() {
        return cityId_;
    }

    public void cityId(int cityId) {
        cityId_ = cityId;
    }

    public long wallId() {
        return wallId_;
    }

    public TreeSet<Integer> interests() {
        return interests_;
    }

    public void interests(TreeSet<Integer> interests) {
        interests_.clear();
        interests_.addAll(interests);
    }

    public int mainInterest() {
        return mainInterest_;
    }

    public void mainInterest(int interest) {
        mainInterest_ = interest;
    }

    public int universityLocationId() {
        return universityLocationId_;
    }

    public void universityLocationId(int location) {
        universityLocationId_ = location;
    }

    public byte gender() {
        return gender_;
    }

    public void gender(byte gender) {
        gender_ = gender;
    }

    public long birthday() {
        return birthday_;
    }

    public void birthday(long birthday) {
        birthday_ = birthday;
    }

    public boolean isLargePoster() {
        return isLargePoster_;
    }

    public void isLargePoster(boolean largePoster) {
        isLargePoster_ = largePoster;
    }

    public long randomId() {
        return randomId_;
    }

    public void randomId(long randomId) {
        randomId_ = randomId;
    }

    public TreeSet<String> emails() {
        return emails_;
    }

    public void emails(TreeSet<String> emails) {
        emails.clear();
        emails_.addAll(emails);
    }

    public List<Integer> languages() {
        return languages_;
    }

    public void languages(List<Integer> languages) {
        languages_.clear();
        languages_.addAll(languages);
    }

    public String firstName() {
        return firstName_;
    }

    public void firstName(String firstName) {
        firstName_ = firstName;
    }

    public String lastName() {
        return lastName_;
    }

    public void lastName(String lastName) {
        lastName_ = lastName;
    }

    public Map<Long, Long> companies() {
        return companies_;
    }

    public long classYear() {
        return classYear_;
    }

    public void classYear(long classYear) {
        classYear_ = classYear;
    }

    public void readFields(DataInput arg0) throws IOException {
        accountId_ = arg0.readLong();
        creationDate_ = arg0.readLong();
        maxNumKnows_ = arg0.readLong();
        int numFriends = arg0.readShort();
        knows_ = new TreeSet<>();
        for (int i = 0; i < numFriends; i++) {
            Knows fr = new Knows();
            fr.readFields(arg0);
            knows_.add(fr);
        }

        browserId_ = arg0.readInt();

        ipAddress_.readFields(arg0);

        countryId_ = arg0.readInt();
        cityId_ = arg0.readInt();
        wallId_ = arg0.readLong();

        byte numTags = arg0.readByte();
        interests_ = new TreeSet<>();
        for (byte i = 0; i < numTags; i++) {
            interests_.add(arg0.readInt());
        }
        mainInterest_ = arg0.readInt();

        universityLocationId_ = arg0.readInt();
        gender_ = arg0.readByte();
        birthday_ = arg0.readLong();
        isLargePoster_ = arg0.readBoolean();
        randomId_ = arg0.readLong();

        int numEmails = arg0.readInt();
        emails_ = new TreeSet<>();
        for (int i = 0; i < numEmails; ++i) {
            emails_.add(arg0.readUTF());
        }
        int numLanguages = arg0.readInt();
        languages_ = new ArrayList<>();
        for (int i = 0; i < numLanguages; ++i) {
            languages_.add(arg0.readInt());
        }
        firstName_ = arg0.readUTF();
        lastName_ = arg0.readUTF();
        int numCompanies = arg0.readInt();
        companies_ = new HashMap<>();
        for (int i = 0; i < numCompanies; ++i) {
            companies_.put(arg0.readLong(), arg0.readLong());
        }
        classYear_ = arg0.readLong();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(accountId_);
        arg0.writeLong(creationDate_);
        arg0.writeLong(maxNumKnows_);
        arg0.writeShort(knows_.size());

        for (Knows f : knows_) {
            f.write(arg0);
        }

        arg0.writeInt(browserId_);
        ipAddress_.write(arg0);

        arg0.writeInt(countryId_);
        arg0.writeInt(cityId_);
        arg0.writeLong(wallId_);

        arg0.writeByte((byte) interests_.size());
        Iterator<Integer> iter2 = interests_.iterator();
        while (iter2.hasNext()) {
            arg0.writeInt(iter2.next());
        }
        arg0.writeInt(mainInterest_);
        arg0.writeInt(universityLocationId_);
        arg0.writeByte(gender_);
        arg0.writeLong(birthday_);
        arg0.writeBoolean(isLargePoster_);
        arg0.writeLong(randomId_);

        arg0.writeInt(emails_.size());
        for (String s : emails_) {
            arg0.writeUTF(s);
        }
        arg0.writeInt(languages_.size());
        for (Integer l : languages_) {
            arg0.writeInt(l);
        }
        arg0.writeUTF(firstName_);
        arg0.writeUTF(lastName_);
        arg0.writeInt(companies_.size());
        for (Map.Entry<Long, Long> e : companies_.entrySet()) {
            arg0.writeLong(e.getKey());
            arg0.writeLong(e.getValue());
        }
        arg0.writeLong(classYear_);
    }

    public static PersonSimilarity personSimilarity;
}
