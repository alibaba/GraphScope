package ldbc.snb.datagen.generator.generators.textgenerators;

import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.entities.dynamic.person.Person;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public abstract class TextGenerator {
    protected TagDictionary tagDic;
    protected Random random;


    /**
     * < @brief The probability to retrieve an small text.
     */

    public TextGenerator(Random random, TagDictionary tagDic) {
        this.tagDic = tagDic;
        this.random = random;
    }

    /**
     * @brief Loads the dictionary.
     */
    protected abstract void load();

    /**
     * @brief Generates a text given a set of tags.
     */
    public abstract String generateText(Person.PersonSummary person, TreeSet<Integer> tags, Properties prop);

}
