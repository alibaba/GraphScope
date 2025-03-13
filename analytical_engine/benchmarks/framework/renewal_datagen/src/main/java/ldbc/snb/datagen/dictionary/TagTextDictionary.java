package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

public class TagTextDictionary {
    private static final String SEPARATOR = "  ";
    private TagDictionary tagDic;
    /**
     * < @brief The tag dictionary. *
     */
    private Map<Integer, String> tagText;
    /**
     * < @brief The tag text. *
     */
    private double reducedTextRatio;

    private StringBuilder returnString = null;

    public TagTextDictionary(TagDictionary tagDic, double reducedTextRatio) {
        this.tagText = new HashMap<>();
        this.tagDic = tagDic;
        this.reducedTextRatio = reducedTextRatio;
        this.returnString = new StringBuilder(1000);
        load(DatagenParams.tagTextFile);
    }

    /**
     * @param fileName The tag text dictionary file name.
     * @brief Loads the dictionary.
     */
    private void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                Integer id = Integer.valueOf(data[0]);
                tagText.put(id, data[1]);
            }
            dictionary.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param id The tag identifier.
     * @return The tag's text.
     * @brief Gets the text associated with the tag.
     */
    public String getTagText(int id) {
        return tagText.get(id);
    }

    /**
     * @param randomTextSize    The random number generator to generate the text's size.
     * @param randomReducedText The random number generator to generate a small text size.
     * @param minSize           The minimum size to generate.
     * @param maxSize           The maximum size to generate.
     * @return
     * @brief Gets a random tag text size.
     */
    public int getRandomTextSize(Random randomTextSize, Random randomReducedText, int minSize, int maxSize) {
        if (randomReducedText.nextDouble() > reducedTextRatio) {
            return randomTextSize.nextInt(maxSize - minSize) + minSize;
        }
        return randomTextSize.nextInt((maxSize >> 1) - minSize) + minSize;
    }

    /**
     * @param randomTextSize The random number generator to generate the size.
     * @param minSize        The minimun text size.
     * @param maxSize        The maximum text size.
     * @return
     * @brief Gets a random large text size.
     */
    public int getRandomLargeTextSize(Random randomTextSize, int minSize, int maxSize) {
        return randomTextSize.nextInt(maxSize - minSize) + minSize;
    }

    /**
     * @param randomTextSize The random number generator to generate the amount of text devoted to each tag.
     * @param tags           The set of tags to generate the text from.
     * @param textSize       The final text size.
     * @return The final text.
     * @brief Generates a text given a set of tags.
     */
    public String generateText(Random randomTextSize, TreeSet<Integer> tags, int textSize) {
        returnString.setLength(0);
        int textSizePerTag = (int) Math.ceil(textSize / (double) tags.size());
        while (returnString.length() < textSize) {
            Iterator<Integer> it = tags.iterator();
            while (it.hasNext() && returnString.length() < textSize) {
                Integer tag = it.next();
                String content = getTagText(tag);
                int thisTagTextSize = Math.min(textSizePerTag, textSize - returnString.length());
                String tagName = tagDic.getName(tag).replace("_", " ");
                tagName = tagName.replace("\"", "\\\"");
                String prefix = "About " + tagName + ", ";
                thisTagTextSize += prefix.length();
                if (thisTagTextSize >= content.length()) {
                    returnString.append(content);
                } else {
                    int startingPos = randomTextSize.nextInt(content.length() - thisTagTextSize + prefix.length());
                    String finalString = content.substring(startingPos, startingPos + thisTagTextSize - prefix
                            .length());
                    returnString.append(prefix);
                    returnString.append(finalString);
                }
            }
        }

        if (returnString.charAt(returnString.length() - 1) != '.') {
            returnString.append(".");
        }
        if (returnString.length() < textSize - 1) {
            returnString.append(" ");
        }
        if (returnString.length() > textSize) {
            returnString.delete(textSize - 1, returnString.length());
            returnString.trimToSize();
        }
        return returnString.toString().replace("|", " ");
    }
}
