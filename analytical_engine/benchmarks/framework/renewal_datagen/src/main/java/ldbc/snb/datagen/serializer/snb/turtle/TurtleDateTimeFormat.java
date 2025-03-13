package ldbc.snb.datagen.serializer.snb.turtle;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class TurtleDateTimeFormat {

    public static SimpleDateFormat get() {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        dateTimeFormat .setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateTimeFormat;
    }

}
