package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.statictype.tag.FlashmobTag;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.Distribution;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static ldbc.snb.datagen.DatagenParams.*;

public class FlashmobPostGenerator extends PostGenerator {
    private Distribution dateDistribution_;
    private FlashmobTag[] forumFlashmobTags = null;
    private long flashmobSpan_;
    private long currentForum = -1;

    public FlashmobPostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        super(generator, commentGenerator, likeGenerator);
        dateDistribution_ = new Distribution(DatagenParams.flashmobDistFile);
        long hoursToMillis_ = 60 * 60 * 1000;
        flashmobSpan_ = 72 * hoursToMillis_;
        dateDistribution_.initialize();
    }

    /**
     * @return The index of a random tag.
     * @brief Selects a random tag from a given index.
     * @param[in] tags The array of sorted tags to select from.
     * @param[in] index The first tag to consider.
     */
    private int selectRandomTag(Random randomFlashmobTag, FlashmobTag[] tags, int index) {
        int upperBound = tags.length - 1;
        int lowerBound = index;
        double prob = randomFlashmobTag
                .nextDouble() * (tags[upperBound].prob - tags[lowerBound].prob) + tags[lowerBound].prob;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (tags[midPoint].prob > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    /**
     * @return The index to the earliest flashmob tag.
     * @brief Selects the earliest flashmob tag index from a given date.
     */
    private int searchEarliest(FlashmobTag[] tags, ForumMembership membership) {
        long fromDate = membership.creationDate() + flashmobSpan_ / 2 + DatagenParams.deltaTime;
        int lowerBound = 0;
        int upperBound = tags.length - 1;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (tags[midPoint].date > fromDate) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        if (tags[midPoint].date < fromDate) return -1;
        return midPoint;
    }

    private void populateForumFlashmobTags(Random randomNumPost, Forum forum) {

        TreeSet<Integer> tags = new TreeSet<>();
        for (Integer tag : tags) {
            tags.add(tag);
        }
        List<FlashmobTag> temp = Dictionaries.flashmobs.generateFlashmobTags(randomNumPost, tags, forum
                .creationDate());
        forumFlashmobTags = new FlashmobTag[temp.size()];
        Iterator<FlashmobTag> it = temp.iterator();
        int index = 0;
        int sumLevels = 0;
        while (it.hasNext()) {
            FlashmobTag flashmobTag = new FlashmobTag();
            it.next().copyTo(flashmobTag);
            forumFlashmobTags[index] = flashmobTag;
            sumLevels += flashmobTag.level;
            ++index;
        }
        Arrays.sort(forumFlashmobTags);
        int size = forumFlashmobTags.length;
        double currentProb = 0.0;
        for (int i = 0; i < size; ++i) {
            forumFlashmobTags[i].prob = currentProb;
            currentProb += (double) (forumFlashmobTags[i].level) / (double) (sumLevels);
        }
    }

    protected PostGenerator.PostInfo generatePostInfo(Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership) {
        if (currentForum != forum.id()) {
            populateForumFlashmobTags(randomTag, forum);
            currentForum = forum.id();
        }
        if (forumFlashmobTags.length < 1) return null;
        PostInfo postInfo = new PostInfo();
        int index = searchEarliest(forumFlashmobTags, membership);
        if (index < 0) return null;
        index = selectRandomTag(randomTag, forumFlashmobTags, index);
        FlashmobTag flashmobTag = forumFlashmobTags[index];
        postInfo.tags.add(flashmobTag.tag);

        for (int i = 0; i < maxNumTagPerFlashmobPost - 1; ++i) {
            if (randomTag.nextDouble() < 0.05) {
                int tag = Dictionaries.tagMatrix.getRandomRelated(randomTag, flashmobTag.tag);
                postInfo.tags.add(tag);
            }
        }
        double prob = dateDistribution_.nextDouble(randomDate);
        postInfo.date = flashmobTag.date - flashmobSpan_ / 2 + (long) (prob * flashmobSpan_);
        return postInfo;
    }
}
