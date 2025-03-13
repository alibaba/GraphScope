package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Message;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.entities.dynamic.relations.Like.LikeType;
import ldbc.snb.datagen.generator.tools.PowerDistribution;
import ldbc.snb.datagen.serializer.PersonActivityExporter;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class LikeGenerator {
    private final PowerDistribution likesGenerator_;
    private Like like;


    public LikeGenerator() {
        likesGenerator_ = new PowerDistribution(1, DatagenParams.maxNumLike, 0.07);
        this.like = new Like();
    }

    public void generateLikes(Random random, final Forum forum, final Message message, LikeType type, PersonActivityExporter exporter) throws IOException {
        int numMembers = forum.memberships().size();
        int numLikes = likesGenerator_.getValue(random);
        numLikes = numLikes >= numMembers ? numMembers : numLikes;
        List<ForumMembership> memberships = forum.memberships();
        int startIndex = 0;
        if (numLikes < numMembers) {
            startIndex = random.nextInt(numMembers - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            ForumMembership membership = memberships.get(startIndex + i);
            long minDate = (message.creationDate() > membership.creationDate() ?
                    message.creationDate() : membership.creationDate()) +
                    DatagenParams.deltaTime;
            long date = Dictionaries.dates.randomDate(random, minDate, Dictionaries.dates
                    .randomSevenDays(random) + minDate);
            assert ((membership.person().creationDate() + DatagenParams.deltaTime) <= date &&
                    (message.creationDate() + DatagenParams.deltaTime) <= date);
            like.user = membership.person().accountId();
            like.userCreationDate = membership.person().creationDate();
            like.messageId = message.messageId();
            like.date = date;
            like.type = type;
            exporter.export(like);
        }
    }
}
