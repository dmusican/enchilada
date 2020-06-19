package edu.carleton.enchilada.database;

import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static junit.framework.TestCase.assertEquals;

public class TimeUtilitiesTest {

    @Test
    public void slash12ToISO() throws Exception {
        assertEquals("2003-09-02 17:30:38", TimeUtilities.slash12TimeToIso8601("9/2/2003 5:30:38 PM"));
    }

    @Test
    public void slash24ToISO() throws Exception {
        assertEquals("2003-09-02 17:30:38", TimeUtilities.slash24TimeToIso8601("9/2/2003 17:30:38"));
    }

    @Test
    public void isoToDate() throws Exception {
        assertEquals((new GregorianCalendar(2003, Calendar.SEPTEMBER, 2, 17, 30, 38)).getTime(),
                TimeUtilities.iso8601ToDate("2003-09-02 17:30:38"));
    }

    @Test
    public void dateToIso8601() throws Exception {
        assertEquals("2003-09-02 17:30:38",
                TimeUtilities.dateToIso8601(new GregorianCalendar(2003, Calendar.SEPTEMBER, 2, 17, 30, 38).getTime()));
    }

}
