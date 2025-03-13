package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Set;


public class TagDictionary {

    private static final String SEPARATOR = "\t";
    private int numPopularTags;
    /**
     * < @brief The number of popular tags. *
     */
    private double tagCountryCorrProb;
    /**
     * < @brief The probability to choose another country when asking for a tag.
     */

    private List<List<Integer>> tagsByCountry;
    /**
     * < @brief The tags by country map.
     */
    private List<List<Double>> tagCummulativeDist;
    /**
     * < @brief The tags by country cumulative distribution.
     */
    private Map<Integer, String> tagClassName;
    /**
     * < @brief The tag class names.
     */
    private Map<Integer, String> tagClassLabel;
    /**
     * < @brief The tag class labels.
     */
    private Map<Integer, Integer> tagClassHierarchy;
    /**
     * < @brief The tag class hierarchy.
     */
    private Map<Integer, Integer> tagTagClass;
    /**
     * < @brief The tag tag classes.
     */
    private Map<Integer, String> tagNames;
    /**
     * < @brief the tag names.
     */
    private Map<Integer, String> tagDescription;         /**< @brief the tag descriptions.*/

    /**
     * @param numCountries       The number of countries.
     * @param tagCountryCorrProb The probability to choose a tag from another country.
     * @brief Constructor
     */
    public TagDictionary(int numCountries, double tagCountryCorrProb) {

        this.tagCountryCorrProb = tagCountryCorrProb;
        this.tagCummulativeDist = new ArrayList<>(numCountries);
        this.tagsByCountry = new ArrayList<>(numCountries);
        this.tagNames = new HashMap<>();
        this.tagTagClass = new HashMap<>();
        this.tagDescription = new HashMap<>();
        this.tagClassName = new HashMap<>();
        this.tagClassLabel = new HashMap<>();
        this.tagClassHierarchy = new HashMap<>();
        for (int i = 0; i < numCountries; i++) {
            tagCummulativeDist.add(new ArrayList<>());
            tagsByCountry.add(new ArrayList<>());
        }
        this.numPopularTags = 0;

        load(DatagenParams.tagsFile,
             DatagenParams.popularTagByCountryFile,
             DatagenParams.tagClassFile,
             DatagenParams.tagClassHierarchyFile);
    }

    /**
     * @param id The tag identifier.
     * @return The name of the tag.
     * @brief Gets the name of a tag.
     */
    public String getName(int id) {
        return tagNames.get(id);
    }


    /**
     * @param id The tag identifier.
     * @return The tag's class identifier.
     * @brief Gets the class of a tag.
     */
    public Integer getTagClass(int id) {
        return tagTagClass.get(id);
    }

    /**
     * @param id The tag class identifier.
     * @return The tag class's name.
     * @brief Gets the name of a tag class.
     */
    public String getClassName(int id) {
        return tagClassName.get(id);
    }

    /**
     * @param id The tag class identifier.
     * @return The label of the tag class.
     * @brief Gets the label of a tag class.
     */
    public String getClassLabel(int id) {
        return tagClassLabel.get(id);
    }

    /**
     * @param id The id of the tag class.
     * @return The parent tag class id.
     * @brief Gets the tag class parent.
     */
    public Integer getClassParent(int id) {
        if (!tagClassHierarchy.containsKey(id)) {
            return -1;
        }
        return tagClassHierarchy.get(id);
    }

    /**
     * @param tagsFileName                The tags file name.
     * @param popularTagByCountryFileName The popular tags by country file name.
     * @param tagClassFileName            The tag classes file name.
     * @param tagClassHierarchyFileName   The tag hierarchy file name.
     * @brief Loads the tag dictionary from files.
     */
    private void load(String tagsFileName, String popularTagByCountryFileName, String tagClassFileName, String tagClassHierarchyFileName) {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(tagClassFileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(data[0]);
                tagClassName.put(classId, data[1]);
                tagClassLabel.put(classId, data[2]);
            }

            dictionary.close();
            dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                          .getResourceAsStream(tagClassHierarchyFileName), "UTF-8"));
            while ((line = dictionary.readLine()) != null) {
                String infos[] = line.split(SEPARATOR);
                Integer classId = Integer.valueOf(infos[0]);
                Integer parentId = Integer.valueOf(infos[1]);
                tagClassHierarchy.put(classId, parentId);
            }

            dictionary.close();
            dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                          .getResourceAsStream(tagsFileName), "UTF-8"));
            while ((line = dictionary.readLine()) != null) {
                String infos[] = line.split(SEPARATOR);
                int tagId = Integer.valueOf(infos[0]);
                Integer classId = Integer.valueOf(infos[1]);
                tagTagClass.put(tagId, classId);
                tagNames.put(tagId, infos[2]);
                tagDescription.put(tagId, infos[3]);
            }

            dictionary.close();
            dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                          .getResourceAsStream(popularTagByCountryFileName), "UTF-8"));
            while ((line = dictionary.readLine()) != null) {
                String infos[] = line.split(" ");
                int countryId = Integer.parseInt(infos[0]);
                int tagId = Integer.parseInt(infos[1]);
                double cummulative = Double.parseDouble(infos[2]);

                tagCummulativeDist.get(countryId).add(cummulative);
                tagsByCountry.get(countryId).add(tagId);
                if (tagId + 1 > numPopularTags) {
                    numPopularTags = tagId + 1;
                }
            }

            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param randomTagOtherCountry The random number generator for choosing another country.
     * @param randomTagCountryProb  The random number generator for choosing a country.
     * @param countryId_            The country id.
     * @return The random tag id.
     * @brief Gets a random tag by country.
     */
    public Integer getaTagByCountry(Random randomTagOtherCountry, Random randomTagCountryProb, int countryId_) {
        int countryId = countryId_;
        if (tagsByCountry.get(countryId).size() == 0 || randomTagOtherCountry.nextDouble() > tagCountryCorrProb) {
            do {
                countryId = randomTagOtherCountry.nextInt(tagsByCountry.size());
            } while (tagsByCountry.get(countryId).size() == 0);
        }

        double randomDis = randomTagCountryProb.nextDouble();
        int lowerBound = 0;
        int upperBound = tagsByCountry.get(countryId).size();
        int curIdx = (upperBound + lowerBound) / 2;

        while (upperBound > (lowerBound + 1)) {
            if (tagCummulativeDist.get(countryId).get(curIdx) > randomDis) {
                upperBound = curIdx;
            } else {
                lowerBound = curIdx;
            }
            curIdx = (upperBound + lowerBound) / 2;
        }

        return tagsByCountry.get(countryId).get(curIdx);
    }

    /**
     * @return The number of popular tags.
     * @brief Gets the number of popular tags.
     */
    public int getNumPopularTags() {
        return numPopularTags;
    }

    /**
     * @param random The random number generator.
     * @param num    The number of tags to retrieve.
     * @return The array of random tags.
     * @brief Gets a number of random tags.
     */
    public Integer[] getRandomTags(Random random, int num) {
        Integer[] result = new Integer[num];
        for (int i = 0; i < num; ) {
            int randomCountry = random.nextInt(tagsByCountry.size());
            List<Integer> tags = tagsByCountry.get(randomCountry);
            if (tags.size() > 0) {
                result[i] = tags.get(random.nextInt(tags.size()));
                ++i;
            }
        }
        return result;
    }

    /**
     * @return The set of tag's names.
     * @brief Gets all the tag names.
     */
    public Set<Integer> getTags() {
        return tagNames.keySet();
    }
}
