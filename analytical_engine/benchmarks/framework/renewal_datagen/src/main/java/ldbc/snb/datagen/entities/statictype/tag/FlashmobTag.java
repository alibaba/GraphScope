package ldbc.snb.datagen.entities.statictype.tag;

import ldbc.snb.datagen.dictionary.Dictionaries;

public class FlashmobTag implements Comparable<FlashmobTag> {
    public int level;
    public long date;
    public double prob;
    public int tag;

    public int compareTo(FlashmobTag t) {
        if (this.date - t.date < 0) return -1;
        if (this.date - t.date > 0) return 1;
        if (this.date - t.date == 0) return 0;
        return 0;
    }

    public void copyTo(FlashmobTag t) {
        t.level = this.level;
        t.date = this.date;
        t.prob = this.prob;
        t.tag = this.tag;
    }

    public String toString() {
        return "Level: " + level + " Date: " + Dictionaries.dates.formatDateTime(date) + " Tag:" + Dictionaries.tags
                .getName(tag);
    }
}
