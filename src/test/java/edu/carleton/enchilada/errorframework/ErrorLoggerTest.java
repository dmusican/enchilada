package edu.carleton.enchilada.errorframework;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ErrorLoggerTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void writeExceptionToLogAndPrompt() {
        assertTrue(ErrorLogger.writeExceptionToLogAndPrompt("Test type","Test message"));
    }
}