package ldbc.snb.datagen.util;

public class StringUtils {

    public static String clampString(String str, int length) {
        if (str.length() > length) return str.substring(0, length);
        return str;
    }
}
