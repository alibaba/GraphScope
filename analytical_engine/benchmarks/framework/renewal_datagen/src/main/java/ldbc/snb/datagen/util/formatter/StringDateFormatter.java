package ldbc.snb.datagen.util.formatter;

import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class StringDateFormatter implements DateFormatter {

    private String formatDateTimeString_ = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private String formatDateString_ = "yyyy-MM-dd";

    private SimpleDateFormat gmtDateTimeFormatter_;
    private SimpleDateFormat gmtDateFormatter_;
    private Date date_;

    public void initialize(Configuration conf) {

        formatDateTimeString_ = conf
                .get("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateTimeFormat", formatDateTimeString_);
        gmtDateTimeFormatter_ = new SimpleDateFormat(formatDateTimeString_);
        gmtDateTimeFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        formatDateString_ = conf
                .get("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateFormat", formatDateString_);
        gmtDateFormatter_ = new SimpleDateFormat(formatDateString_);
        gmtDateFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        date_ = new Date();
    }

    public String formatDateTime(long date) {
        date_.setTime(date);
        return gmtDateTimeFormatter_.format(date_);
    }

    public String formatDate(long date) {
        date_.setTime(date);
        return gmtDateFormatter_.format(date_);
    }

}
