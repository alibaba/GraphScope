package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

public class UniformPostGenerator extends PostGenerator {


    public UniformPostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        super(generator, commentGenerator, likeGenerator);
    }

    @Override
    protected PostInfo generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership) {
        PostInfo postInfo = new PostInfo();
        postInfo.tags = new TreeSet<>();
        Iterator<Integer> it = forum.tags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (postInfo.tags.isEmpty()) {
                postInfo.tags.add(value);
            } else {
                if (randomTag.nextDouble() < 0.05) {
                    postInfo.tags.add(value);
                }
            }
        }
        postInfo.date = Dictionaries.dates.randomDate(randomDate, membership.creationDate() + DatagenParams.deltaTime);
        return postInfo;
    }
}
