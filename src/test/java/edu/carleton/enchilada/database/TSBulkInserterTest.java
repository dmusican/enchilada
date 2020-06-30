package edu.carleton.enchilada.database;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Does what it says.
 *  
 * @author smitht
 * @author jtbigwoo
 */

public class TSBulkInserterTest extends TestCase {
	private TSBulkInserter ins;
	
	Database db;
	
	
	protected void setUp() throws Exception {
		new CreateTestDatabase();
		db = Database.getDatabase("TestDB");
		db.openConnection("TestDB");
		ins = new TSBulkInserter(db);
	}

	protected void tearDown() throws Exception {
		db.closeConnection();
	}
	
	/*
	 * Test method for 'database.TSBulkInserter.addPoint(Date, Float)'
	 */
	public void testAddOnePoint() {
		TreeMap<Date, Float> data = new TreeMap<>();
		Date initialDate = new Date();
		data.put(initialDate, 0f);
		insertAndTest(data, initialDate);
	}
	
	public void testAdd500Points() {
		TreeMap<Date, Float> data = new TreeMap<>();
		Date initialDate = new Date();
		Calendar c = new GregorianCalendar();
		c.setTimeInMillis(initialDate.getTime());
		for (int i = 0; i < 500; i++) {
			data.put(c.getTime(), (float)i);
			
			c.add(Calendar.SECOND, 30);
		}
		insertAndTest(data, initialDate);
	}

	// test to see if we still get out of memory exceptions on big collections
	public void testAdd30000Points() {
		TreeMap<Date, Float> data = new TreeMap<>();
		Date initialDate = new Date();
		Calendar c = new GregorianCalendar();
		c.setTimeInMillis(initialDate.getTime());
		for (int i = 0; i < 30000; i++) {
			data.put(c.getTime(), (float)i);
			
			c.add(Calendar.SECOND, 30);
		}
		insertAndTest(data, initialDate);
	}
	
	private void insertAndTest(Map<Date,Float> data, Date initialDate) {
		ins.startDataset("test coll");
		
		Iterator<Entry<Date, Float>> i = data.entrySet().iterator();

		try {
			// run it
			while (i.hasNext()) {
				Entry<Date,Float> e = i.next();
				ins.addPoint(e.getKey(), e.getValue());
			}
			ins.commit();
			// test it
			Connection con = db.getCon();
			PreparedStatement ps = con.prepareStatement(
					"select atomid, time, value from timeseriesatominfodense where atomid = ?");
			ResultSet rs;
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(initialDate);
			cal.set(Calendar.MILLISECOND, 0);
			initialDate.setTime(cal.getTimeInMillis());

			ps.setInt(1, 22);
			rs = ps.executeQuery();
			assertTrue(rs.next());
			assertEquals(db.getDateFormat().format(initialDate), rs.getString("time"));
			assertEquals(0f, rs.getFloat("value"));
			if (data.size() > 1)
			{
				// check the middle one
				int half = data.size() / 2;
				cal.add(Calendar.SECOND, 30 * half);
				initialDate.setTime(cal.getTimeInMillis());

				ps.setInt(1, 22 + half);
				rs = ps.executeQuery();
				assertTrue(rs.next());
				assertEquals(db.getDateFormat().format(initialDate), rs.getString("time"));
				assertEquals(0f + half, rs.getFloat("value"));
				
				// check the last one
				cal.add(Calendar.SECOND, 30 * (data.size() - half - 1));
				initialDate.setTime(cal.getTimeInMillis());

				ps.setInt(1, 22 + data.size() - 1);
				rs = ps.executeQuery();
				assertTrue(rs.next());
				assertEquals(db.getDateFormat().format(initialDate), rs.getString("time"));
				assertEquals(0f + data.size() - 1, rs.getFloat("value"));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		}
	}

}
