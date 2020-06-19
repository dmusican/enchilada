package edu.carleton.enchilada.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TimeUtilitiesTest {

    @Test
    public void slash12ToISO() throws Exception {
        assertEquals("2003-09-02 17:30:38", TimeUtilities.slash12TimeToISO8601("9/2/2003 5:30:38 PM"));
    }

    @Test
    public void slash24ToISO() throws Exception {
        assertEquals("2003-09-02 17:30:38", TimeUtilities.slash24TimeToISO8601("9/2/2003 17:30:38"));
    }

}
