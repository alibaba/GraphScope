package ldbc.snb.datagen.util.formatter;

import org.apache.hadoop.conf.Configuration;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class LongDateFormatter implements DateFormatter {
    private GregorianCalendar calendar_;
    private int minHour;
    private int minMinute;
    private int minSecond;
    private int minMillisecond;

    public void initialize(Configuration config) {
        calendar_ = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        minHour = calendar_.getActualMinimum(Calendar.HOUR);
        minMinute = calendar_.getActualMinimum(Calendar.MINUTE);
        minSecond = calendar_.getActualMinimum(Calendar.SECOND);
        minMillisecond = calendar_.getActualMinimum(Calendar.MILLISECOND);
    }

    public String formatDate(long date) {
        calendar_.setTimeInMillis(date);
        calendar_.set(Calendar.HOUR, minHour);
        calendar_.set(Calendar.MINUTE, minMinute);
        calendar_.set(Calendar.SECOND, minSecond);
        calendar_.set(Calendar.MILLISECOND, minMillisecond);
        return Long.toString(calendar_.getTimeInMillis());
    }

    public String formatDateTime(long date) {
        return Long.toString(date);
    }
}
