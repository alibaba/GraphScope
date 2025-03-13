package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.UpdateEvent;
import ldbc.snb.datagen.hadoop.key.updatekey.UpdateEventKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

public class UpdateEventSerializer {


    private SequenceFile.Writer streamWriter_[];
    private List<String> data_;
    private List<String> list_;
    private UpdateEvent currentEvent_;
    private int numPartitions_ = 1;
    private int nextPartition_ = 0;
    private StringBuffer stringBuffer_;
    private long currentDependantDate_ = 0;
    private Configuration conf_;
    private UpdateStreamStats stats_;
    private String fileNamePrefix_;
    private int reducerId_;

    private class UpdateStreamStats {
        public long minDate_ = Long.MAX_VALUE;
        public long maxDate_ = Long.MIN_VALUE;
        public long count_ = 0;
    }

    public UpdateEventSerializer(Configuration conf, String fileNamePrefix, int reducerId, int numPartitions) throws IOException {
        conf_ = conf;
        reducerId_ = reducerId;
        stringBuffer_ = new StringBuffer(512);
        data_ = new ArrayList<>();
        list_ = new ArrayList<>();
        currentEvent_ = new UpdateEvent(-1, -1, UpdateEvent.UpdateEventType.NO_EVENT, new String(""));
        numPartitions_ = numPartitions;
        stats_ = new UpdateStreamStats();
        fileNamePrefix_ = fileNamePrefix;
        try {
            streamWriter_ = new SequenceFile.Writer[numPartitions_];
            FileContext fc = FileContext.getFileContext(conf);
            for (int i = 0; i < numPartitions_; ++i) {
                Path outFile = new Path(fileNamePrefix_ + "_" + i);
                streamWriter_[i] = SequenceFile
                        .createWriter(fc, conf, outFile, UpdateEventKey.class, Text.class, CompressionType.NONE, new DefaultCodec(), new SequenceFile.Metadata(), EnumSet
                                .of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts
                                              .checksumParam(Options.ChecksumOpt.createDisabled()));
                FileSystem fs = FileSystem.get(conf);
                Path propertiesFile = new Path(fileNamePrefix_ + ".properties");
                if (fs.exists(propertiesFile)) {
                    FSDataInputStream file = fs.open(propertiesFile);
                    Properties properties = new Properties();
                    properties.load(file);
                    stats_.minDate_ = Long.parseLong(properties
                                                             .getProperty("ldbc.snb.interactive.min_write_event_start_time"));
                    stats_.maxDate_ = Long.parseLong(properties
                                                             .getProperty("ldbc.snb.interactive.max_write_event_start_time"));
                    stats_.count_ = Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events"));
                    file.close();
                    fs.delete(propertiesFile, true);
                }
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void changePartition() {
        nextPartition_ = (++nextPartition_) % numPartitions_;
    }

    public void writeKeyValue(UpdateEvent event) throws IOException {
        try {
            if (event.date <= Dictionaries.dates.getEndDateTime()) {
                StringBuilder string = new StringBuilder();
                string.append(Long.toString(event.date));
                string.append("|");
                string.append(Long.toString(event.dependantDate));
                string.append("|");
                string.append(Integer.toString(event.type.ordinal() + 1));
                string.append("|");
                string.append(event.eventData);
                string.append("\n");
                streamWriter_[nextPartition_]
                        .append(new UpdateEventKey(event.date, reducerId_, nextPartition_), new Text(string.toString()));
            }
        } catch (IOException e) {
            throw e;
        }
    }

    private String formatStringArray(List<String> array, String separator) {
        if (array.size() == 0) return "";
        stringBuffer_.setLength(0);
        for (String s : array) {
            stringBuffer_.append(s);
            stringBuffer_.append(separator);
        }
        return stringBuffer_.substring(0, stringBuffer_.length() - 1);
    }

    private void beginEvent(long date, UpdateEvent.UpdateEventType type) {
        stats_.minDate_ = stats_.minDate_ > date ? date : stats_.minDate_;
        stats_.maxDate_ = stats_.maxDate_ < date ? date : stats_.maxDate_;
        stats_.count_++;
        currentEvent_.date = date;
        currentEvent_.dependantDate = currentDependantDate_;
        currentEvent_.type = type;
        currentEvent_.eventData = null;
        data_.clear();
    }

    private void endEvent() throws IOException {
        currentEvent_.eventData = formatStringArray(data_, "|");
        writeKeyValue(currentEvent_);
    }

    private void beginList() {
        list_.clear();
    }

    private void endList() {
        data_.add(formatStringArray(list_, ";"));
    }


    public void close() throws IOException {
        try {
            FileSystem fs = FileSystem.get(conf_);
            for (int i = 0; i < numPartitions_; ++i) {
                streamWriter_[i].close();
            }

            if (DatagenParams.updateStreams) {
                OutputStream output = fs.create(new Path(fileNamePrefix_ + ".properties"), true);
                output.write(new String("ldbc.snb.interactive.gct_delta_duration:" + DatagenParams.deltaTime + "\n")
                                     .getBytes());
                output.write(new String("ldbc.snb.interactive.min_write_event_start_time:" + stats_.minDate_ + "\n")
                                     .getBytes());
                output.write(new String("ldbc.snb.interactive.max_write_event_start_time:" + stats_.maxDate_ + "\n")
                                     .getBytes());
                if (stats_.count_ != 0) {
                    output.write(new String("ldbc.snb.interactive.update_interleave:" + (stats_.maxDate_ - stats_.minDate_) / stats_.count_ + "\n")
                                         .getBytes());
                } else {
                    output.write(new String("ldbc.snb.interactive.update_interleave:" + "0" + "\n").getBytes());
                }
                output.write(new String("ldbc.snb.interactive.num_events:" + stats_.count_).getBytes());
                output.close();
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    public void export(Person person) throws IOException {

        currentDependantDate_ = 0;
        beginEvent(person.creationDate(), UpdateEvent.UpdateEventType.ADD_PERSON);
        data_.add(Long.toString(person.accountId()));
        data_.add(person.firstName());
        data_.add(person.lastName());

        if (person.gender() == 1) {
            data_.add("male");
        } else {
            data_.add("female");
        }
        data_.add(Long.toString(person.birthday()));
        data_.add(Long.toString(person.creationDate()));
        data_.add(person.ipAddress().toString());
        data_.add(Dictionaries.browsers.getName(person.browserId()));
        data_.add(Integer.toString(person.cityId()));

        beginList();
        for (Integer l : person.languages()) {
            list_.add(Dictionaries.languages.getLanguageName(l));
        }
        endList();

        beginList();
        for (String e : person.emails()) {
            list_.add(e);
        }
        endList();

        beginList();
        for (Integer tag : person.interests()) {
            list_.add(Integer.toString(tag));
        }
        endList();

        beginList();
        int universityId = person.universityLocationId();
        if (universityId != -1 && person.classYear() != -1) {
            List<String> studyAtData = new ArrayList<>();
            studyAtData.add(Long.toString(Dictionaries.universities.getUniversityFromLocation(universityId)));
            studyAtData.add(Dictionaries.dates.formatYear(person.classYear()));
            list_.add(formatStringArray(studyAtData, ","));
        }
        endList();

        beginList();
        for (Long companyId : person.companies().keySet()) {
            List<String> workAtData = new ArrayList<>();
            workAtData.add(Long.toString(companyId));
            workAtData.add(Dictionaries.dates.formatYear(person.companies().get(companyId)));
            list_.add(formatStringArray(workAtData, ","));
        }
        endList();
        endEvent();
    }

    public void export(Person p, Knows k) throws IOException {
        if (p.accountId() < k.to().accountId()) {
            currentDependantDate_ = Math.max(p.creationDate(), k.to().creationDate());
            beginEvent(k.creationDate(), UpdateEvent.UpdateEventType.ADD_FRIENDSHIP);
            data_.add(Long.toString(p.accountId()));
            data_.add(Long.toString(k.to().accountId()));
            data_.add(Long.toString(k.creationDate()));
            endEvent();
        }
    }

    public void export(Post post) throws IOException {
        currentDependantDate_ = post.author().creationDate();
        beginEvent(post.creationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data_.add(Long.toString(post.messageId()));
        data_.add(empty);
        data_.add(Long.toString(post.creationDate()));
        data_.add(post.ipAddress().toString());
        data_.add(Dictionaries.browsers.getName(post.browserId()));
        data_.add(Dictionaries.languages.getLanguageName(post.language()));
        data_.add(post.content());
        data_.add(Long.toString(post.content().length()));
        data_.add(Long.toString(post.author().accountId()));
        data_.add(Long.toString(post.forumId()));
        data_.add(Long.toString(post.countryId()));

        beginList();
        for (int tag : post.tags()) {
            list_.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Like like) throws IOException {
        currentDependantDate_ = like.userCreationDate;
        if (like.type == Like.LikeType.COMMENT) {
            beginEvent(like.date, UpdateEvent.UpdateEventType.ADD_LIKE_COMMENT);
        } else {
            beginEvent(like.date, UpdateEvent.UpdateEventType.ADD_LIKE_POST);
        }
        data_.add(Long.toString(like.user));
        data_.add(Long.toString(like.messageId));
        data_.add(Long.toString(like.date));
        endEvent();
    }

    public void export(Photo photo) throws IOException {

        currentDependantDate_ = photo.author().creationDate();
        beginEvent(photo.creationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data_.add(Long.toString(photo.messageId()));
        data_.add(photo.content());
        data_.add(Long.toString(photo.creationDate()));
        data_.add(photo.ipAddress().toString());
        data_.add(Dictionaries.browsers.getName(photo.browserId()));
        data_.add(empty);
        data_.add(empty);
        data_.add("0");
        data_.add(Long.toString(photo.author().accountId()));
        data_.add(Long.toString(photo.forumId()));
        data_.add(Long.toString(photo.countryId()));

        beginList();
        for (int tag : photo.tags()) {
            list_.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Comment comment) throws IOException {

        currentDependantDate_ = comment.author().creationDate();
        beginEvent(comment.creationDate(), UpdateEvent.UpdateEventType.ADD_COMMENT);
        data_.add(Long.toString(comment.messageId()));
        data_.add(Long.toString(comment.creationDate()));
        data_.add(comment.ipAddress().toString());
        data_.add(Dictionaries.browsers.getName(comment.browserId()));
        data_.add(comment.content());
        data_.add(Integer.toString(comment.content().length()));
        data_.add(Long.toString(comment.author().accountId()));
        data_.add(Long.toString(comment.countryId()));
        if (comment.replyOf() == comment.postId()) {
            data_.add(Long.toString(comment.postId()));
            data_.add("-1");
        } else {
            data_.add("-1");
            data_.add(Long.toString(comment.replyOf()));
        }
        beginList();
        for (int tag : comment.tags()) {
            list_.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Forum forum) throws IOException {
        currentDependantDate_ = forum.moderator().creationDate();
        beginEvent(forum.creationDate(), UpdateEvent.UpdateEventType.ADD_FORUM);
        data_.add(Long.toString(forum.id()));
        data_.add(forum.title());
        data_.add(Long.toString(forum.creationDate()));
        data_.add(Long.toString(forum.moderator().accountId()));

        beginList();
        for (int tag : forum.tags()) {
            list_.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(ForumMembership membership) throws IOException {
        currentDependantDate_ = membership.person().creationDate();
        beginEvent(membership.creationDate(), UpdateEvent.UpdateEventType.ADD_FORUM_MEMBERSHIP);
        data_.add(Long.toString(membership.forumId()));
        data_.add(Long.toString(membership.person().accountId()));
        data_.add(Long.toString(membership.creationDate()));
        endEvent();
    }

}
