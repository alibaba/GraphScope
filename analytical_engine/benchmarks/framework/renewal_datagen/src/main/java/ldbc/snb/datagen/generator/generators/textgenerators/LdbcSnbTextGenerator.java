package ldbc.snb.datagen.generator.generators.textgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.entities.dynamic.person.Person.PersonSummary;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public class LdbcSnbTextGenerator extends TextGenerator {

    public LdbcSnbTextGenerator(Random random, TagDictionary tagDic) {
        super(random, tagDic);
    }

    @Override
    protected void load() {
        // Intentionally left empty
    }

    @Override
    public String generateText(PersonSummary member, TreeSet<Integer> tags, Properties prop) {
        String content = "";
        if (prop.getProperty("type").equals("post")) {

            int textSize;
            if (member.isLargePoster() && this.random.nextDouble() > (1.0f - DatagenParams.ratioLargePost)) {
                textSize = Dictionaries.tagText
                        .getRandomLargeTextSize(this.random, DatagenParams.minLargePostSize, DatagenParams.maxLargePostSize);
                assert textSize <= DatagenParams.maxLargePostSize && textSize >= DatagenParams.minLargePostSize : "Person creation date is larger than membership";
            } else {
                textSize = Dictionaries.tagText
                        .getRandomTextSize(this.random, this.random, DatagenParams.minTextSize, DatagenParams.maxTextSize);
                assert textSize <= DatagenParams.maxTextSize && textSize >= DatagenParams.minTextSize : "Person creation date is larger than membership";
            }
            content = Dictionaries.tagText.generateText(this.random, tags, textSize);
        } else {
            int textSize;
            if (member.isLargePoster() && this.random.nextDouble() > (1.0f - DatagenParams.ratioLargeComment)) {
                textSize = Dictionaries.tagText
                        .getRandomLargeTextSize(this.random, DatagenParams.minLargeCommentSize, DatagenParams.maxLargeCommentSize);
                assert textSize <= DatagenParams.maxLargeCommentSize && textSize >= DatagenParams.minLargeCommentSize : "Person creation date is larger than membership";
            } else {
                textSize = Dictionaries.tagText
                        .getRandomTextSize(this.random, this.random, DatagenParams.minCommentSize, DatagenParams.maxCommentSize);
                assert textSize <= DatagenParams.maxCommentSize && textSize >= DatagenParams.minCommentSize : "Person creation date is larger than membership";
            }
            content = Dictionaries.tagText.generateText(this.random, tags, textSize);
        }
        return content;
    }

}
