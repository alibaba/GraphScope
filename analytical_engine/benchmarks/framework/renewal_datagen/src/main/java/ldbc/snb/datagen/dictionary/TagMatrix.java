package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class TagMatrix {

    private static final String SEPARATOR = " ";

    private TreeMap<Integer, List<Integer>> relatedTags;
    /**
     * < @brief An array of related tags per tag.
     */
    private TreeMap<Integer, List<Double>> cumulative;

    private List<Integer> nonZeroTags;

    /**
     * < @brief The list of tags.
     */

    public TagMatrix() {
        cumulative = new TreeMap<>();
        relatedTags = new TreeMap<>();
        nonZeroTags = new ArrayList<>();
        load(DatagenParams.tagMatrixFile);

    }

    /**
     * @param tagMatrixFileName The tag matrix file name.
     * @brief Loads the tag matrix from a file.
     */
    private void load(String tagMatrixFileName) {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(tagMatrixFileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                int celebrityId = Integer.parseInt(data[0]);
                int topicId = Integer.parseInt(data[1]);
                double cumuluative = Double.parseDouble(data[2]);
                List<Double> cum = cumulative.get(celebrityId);
                if (cum == null) cumulative.put(celebrityId, new ArrayList<>());
                cumulative.get(celebrityId).add(cumuluative);
                List<Integer> related = relatedTags.get(celebrityId);
                if (related == null) relatedTags.put(celebrityId, new ArrayList<>());
                relatedTags.get(celebrityId).add(topicId);
            }
            for (Integer tag : relatedTags.keySet()) {
                nonZeroTags.add(tag);
            }
            dictionary.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param randomTag The random tag number generator.
     * @param tag       The tag identifier.
     * @return The related tag identifier.
     * @brief Gets a random related tag.
     */
    public Integer getRandomRelated(Random randomTag, int tag) {
        int tagId = tag;
        if (relatedTags.get(tagId) == null) {
            tagId = nonZeroTags.get(randomTag.nextInt(nonZeroTags.size()));
        }
        return relatedTags.get(tagId).get(randomTag.nextInt(relatedTags.get(tagId).size()));
    }

    /**
     * @param randomTopic  The random number generator used to select aditional popular tags
     * @param randomTag    The random number generator used to select related tags.
     * @param popularTagId The popular tag identifier.
     * @param numTags      The number of related tags to retrieve.
     * @return The set of related tags.
     * @brief Get a set of related tags.
     */
    public TreeSet<Integer> getSetofTags(Random randomTopic, Random randomTag, int popularTagId, int numTags) {
        TreeSet<Integer> resultTags = new TreeSet<>();
        resultTags.add(popularTagId);
        while (resultTags.size() < numTags) {
            int tagId;
            tagId = popularTagId;

            if (relatedTags.get(tagId) == null) {
                tagId = nonZeroTags.get(randomTag.nextInt(nonZeroTags.size()));
            }

            // Doing binary search for finding the tag
            double randomDis = randomTag.nextDouble();
            int lowerBound = 0;
            int upperBound = relatedTags.get(tagId).size();
            int midPoint = (upperBound + lowerBound) / 2;

            while (upperBound > (lowerBound + 1)) {
                if (cumulative.get(tagId).get(midPoint) > randomDis) {
                    upperBound = midPoint;
                } else {
                    lowerBound = midPoint;
                }
                midPoint = (upperBound + lowerBound) / 2;
            }
            resultTags.add(relatedTags.get(tagId).get(midPoint));
        }
        return resultTags;

    }
}
