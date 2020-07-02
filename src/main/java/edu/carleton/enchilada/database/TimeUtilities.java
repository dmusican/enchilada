package edu.carleton.enchilada.database;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtilities {

    public static final SimpleDateFormat slash24Format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    public static final SimpleDateFormat slash12Format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    public static final SimpleDateFormat iso8601Format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat iso8601FormatCentral = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    {
        iso8601FormatCentral.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
    }

    public static String slash12TimeToIso8601(String time) throws ParseException {
        return iso8601Format.format(slash12Format.parse(time));
    }

    public static String slash24TimeToIso8601(String time) throws ParseException {
        return iso8601Format.format(slash24Format.parse(time));
    }

    public static Date iso8601ToDate(String time) throws ParseException {
        return iso8601Format.parse(time);
    }

    public static Date iso8601ToDateCentral(String time) throws ParseException {
        return iso8601FormatCentral.parse(time);
    }

    public static String dateToIso8601(Date date) {
        return iso8601Format.format(date);
    }

    // Central time, used for unit tests
    public static String dateToIso8601Central(long secs) {
        return iso8601FormatCentral.format(secs);
    }

}
