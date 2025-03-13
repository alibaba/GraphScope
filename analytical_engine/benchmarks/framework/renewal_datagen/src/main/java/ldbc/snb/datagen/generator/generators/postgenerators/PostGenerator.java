
package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;


abstract public class PostGenerator {

    private TextGenerator generator_;
    private CommentGenerator commentGenerator_;
    private LikeGenerator likeGenerator_;
    private Post post_;

    static protected class PostInfo {
        public TreeSet<Integer> tags;
        public long date;

        public PostInfo() {
            this.tags = new TreeSet<>();
        }
    }

	
	/* A set of random number generator for different purposes.*/

    public PostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        this.generator_ = generator;
        this.commentGenerator_ = commentGenerator;
        this.likeGenerator_ = likeGenerator;
        this.post_ = new Post();
    }

    /**
     * @brief Initializes the post generator.
     */
    public void initialize() {
        // Intentionally left empty
    }


    public long createPosts(RandomGeneratorFarm randomFarm, final Forum forum, final List<ForumMembership> memberships, long numPosts, long startId, PersonActivityExporter exporter) throws IOException {
        long postId = startId;
        Properties prop = new Properties();
        prop.setProperty("type", "post");
        for (ForumMembership member : memberships) {
            double numPostsMember = numPosts / (double) memberships.size();
            if (numPostsMember < 1.0) {
                double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
                if (prob < numPostsMember) numPostsMember = 1.0;
            } else {
                numPostsMember = Math.ceil(numPostsMember);
            }
            for (int i = 0; i < (int) (numPostsMember); ++i) {
                PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm
                        .get(RandomGeneratorFarm.Aspect.DATE), forum, member);
                if (postInfo != null) {

                    String content = "";
                    content = this.generator_.generateText(member.person(), postInfo.tags, prop);

                    int country = member.person().countryId();
                    IP ip = member.person().ipAddress();
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
                    if (PersonBehavior.changeUsualCountry(random, postInfo.date)) {
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                        country = Dictionaries.places.getRandomCountryUniform(random);
                        random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                        ip = Dictionaries.ips.getIP(random, country);
                    }

                    post_.initialize(SN.formId(SN.composeId(postId++, postInfo.date)),
                                     postInfo.date,
                                     member.person(),
                                     forum.id(),
                                     content,
                                     postInfo.tags,
                                     country,
                                     ip,
                                     Dictionaries.browsers.getPostBrowserId(randomFarm
                                                                                    .get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm
                                                                                    .get(RandomGeneratorFarm.Aspect.BROWSER), member
                                                                                    .person().browserId()),
                                     forum.language());
                    exporter.export(post_);

                    if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                        likeGenerator_.generateLikes(randomFarm
                                                             .get(RandomGeneratorFarm.Aspect.NUM_LIKE), forum, post_, Like.LikeType.POST, exporter);
                    }

                    // generate comments
                    int numComments = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT)
                                                .nextInt(DatagenParams.maxNumComments + 1);
                    postId = commentGenerator_.createComments(randomFarm, forum, post_, numComments, postId, exporter);
                }
            }
        }
        return postId;
    }

    protected abstract PostInfo generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership);
}
