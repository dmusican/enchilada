package edu.carleton.enchilada.database;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimeUtilities {

    public static final SimpleDateFormat slash24Format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    public static final SimpleDateFormat slash12Format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    public static final SimpleDateFormat iso8601Format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String slash12TimeToISO8601(String atofmsTime) throws ParseException {
        return iso8601Format.format(slash12Format.parse(atofmsTime));
    }

    public static String slash24TimeToISO8601(String atofmsTime) throws ParseException {
        return iso8601Format.format(slash24Format.parse(atofmsTime));
    }

}
