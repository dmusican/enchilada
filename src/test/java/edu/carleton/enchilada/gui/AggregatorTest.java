package edu.carleton.enchilada.gui;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.database.CreateTestDatabase2;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.TimeUtilities;
import edu.carleton.enchilada.externalswing.SwingWorker;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import static junit.framework.TestCase.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AggregatorTest extends TestCase {
	private Database db;
	private Aggregator aggregator;
	
	public AggregatorTest(String aString)
	{
		super(aString);
	}
	
	protected void setUp()
	{
		new CreateTestDatabase2(); 		
		db = Database.getDatabase("TestDB2");
	}
	
	protected void tearDown() throws Exception
	{
		super.tearDown();
		db.closeConnection();
		System.runFinalization();
		System.gc();
		Database.dropDatabase("TestDB2");
	}

	class FauxMainFrame extends MainFrame {
		public FauxMainFrame() {
			super();
		}
		
		public void updateSynchronizedTree(int collectionID) {
			//override by doing nothing
			System.out.println("Tree update would happen to collection " + collectionID);
		}
	}
	
	/**
	 * Test an aggregation with the given aggregation basis, collections, and testing code
	 * @param aggregator the Aggregator (with basis or interval specified) to run the test on
	 * @param collections the collections to aggregate
	 * @param test the test to run (in run())
	 * @author shaferia
	 */
	private void testAggregation(final Aggregator aggregator, final Collection[] collections, 
			final Test test) {
		final ProgressBarWrapper progressBar =
			aggregator.createAggregateTimeSeriesPrepare(collections);
		
		//Exceptions thrown from within the SwingWorker won't be
		//	tossed back down to the test runner automatically.
		final ArrayList<AssertionFailedError> errors = 
			new ArrayList<AssertionFailedError>();
		
		SwingWorker sw = new SwingWorker() {
			boolean done = false;
			
			/**
			 * Run the aggregation, sending errors back if they occur
			 */
			public Object construct() {
				MainFrame.db = db;
				MainFrame mf = null;
				int cID = -1;
				try {
					cID = aggregator.createAggregateTimeSeries("aggregated",collections,
							progressBar, mf);
					return cID;
				} catch (InterruptedException e) {
					e.printStackTrace();
					errors.add(new AssertionFailedError("Aggregation interrupted"));
				} catch (AggregationException e) {
					e.printStackTrace();
					errors.add(new AssertionFailedError("Aggregation failed."));
				}
				
				return cID;
			}
			
			/**
			 * Use toString to function as a done flag
			 */
			public String toString() {
				return (done) ? "Done" : "NotDone";
			}
			
			/**
			 * Run the tests when the aggregation is finished
			 */
			public void finished() {
				try {
					System.out.println("Performing aggregation tests...");
					test.run((Integer) getValue());
					System.out.println("... done performing tests.");
				}
				catch (SQLException e) {
					errors.add(new AssertionFailedError(e.getMessage()));
					e.printStackTrace();
				}
				catch (AssertionFailedError e) {
					errors.add(e);
				}
				finally {
					db.closeConnection();
					progressBar.disposeThis();
					done = true;
					
					synchronized (this) {
						notifyAll();
					}
				}
			}
		};
		sw.start();
		
		//wait for the SwingWorker to finish executing
		synchronized (sw) {
			while (!sw.toString().equals("Done")) {
				try {
					sw.wait();
				}
				catch (InterruptedException ex) {
					System.err.println("Aggregation test interrupted.");
					ex.printStackTrace();
				}
			}
		}
		
		//Throw any AssertionFailedErrors that happened in the SwingWorker
		for (AssertionFailedError up : errors) {
			throw up;
		}
	}
	
	/**
	 * Used by testAggregation for test code wrapper
	 * @author shaferia
	 */
	private interface Test {
		public void run(int CollectionID) throws SQLException;
	}
	
	/**
	 * This test aggregates an ATOFMS collection, a TimeSeries collection,
	 * and an AMS collections, all of which overlap in TestDB2.  It chooses
	 * a collection for each aggregated datatype and tests it.  For ATOFMS and
	 * AMS, it chooses an m/z value and tests it; for TimeSeries, it just tests
	 * a time.
	 *
	 */
	public void testCreateAggregateTimeSeries(){
		db.openConnection();

		aggregator = new Aggregator(null, db, db.getCollection(2));
		Collection[] collections = {db.getCollection(2),
				db.getCollection(4),db.getCollection(5)};
		Test test = new Test() {
			public void run(int CollectionID) throws SQLException {
				Statement stmt = db.getCon().createStatement();

				// check number of collections:
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Collections;\n");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),35);

				// check ATOFMS m/z collection:
				rs = stmt.executeQuery("SELECT AtomID FROM AtomMembership" +
						" WHERE CollectionID = 13 ORDER BY AtomID;\n");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),35);
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),36);
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),37);
				assertFalse(rs.next());

				rs = stmt.executeQuery("SELECT Time, Value FROM " +
						"TimeSeriesAtomInfoDense WHERE AtomID = 35;");
				rs.next();
				assertEquals("2003-09-02 17:30:31", rs.getString(1));
				assertEquals(12, rs.getInt(2));
				assertFalse(rs.next());

				// check TimeSeries collection:
				rs = stmt.executeQuery("SELECT AtomID FROM AtomMembership" +
				" WHERE CollectionID = 29 ORDER BY AtomID;\n");
				assertTrue(rs.next());
				assertEquals(67, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(68, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(69, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(70, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(71, rs.getInt(1));
				assertFalse(rs.next());

				rs = stmt.executeQuery("SELECT Time, Value FROM " +
						"TimeSeriesAtomInfoDense WHERE AtomID = 67;");
				assertTrue(rs.next());
				assertEquals("2003-09-02 17:30:30", rs.getString(1));
				assertEquals(rs.getInt(2),0);
				assertFalse(rs.next());

				// check AMS m/z collections:
				rs = stmt.executeQuery("SELECT AtomID FROM AtomMembership" +
				" WHERE CollectionID = 32 ORDER BY AtomID;\n");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),72);
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),73);
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),74);
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),75);
				assertFalse(rs.next());

				rs = stmt.executeQuery("SELECT Time, Value FROM " +
				"TimeSeriesAtomInfoDense WHERE AtomID = 72;");
				assertTrue(rs.next());
				assertEquals("2003-09-02 17:30:30", rs.getString(1));
				assertEquals(rs.getInt(2),1);
			}
		};
		testAggregation(aggregator, collections, test);
	}

	/**
	 * This was written in response to a bug that the parent collection wasn't 
	 * getting aggregated properly.  All it does is count the number of 
	 * collections when aggregating a parent collection and then count the number
	 * of collections when aggregating the parent's child.  This is ok, since the
	 * test above acutally tests the aggregation.
	 *
	 */
	public void testParentAggregation() {
		db.openConnection();		
		
		aggregator = new Aggregator(null, db, db.getCollection(2));
		Collection[] collections1 = {db.getCollection(2)};
		Test test1 = new Test() {
			public void run(int CollectionID) throws SQLException {
				Statement stmt = db.getCon().createStatement();
				
				// check number of collections:
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Collections;\n");
				assertTrue(rs.next());
				System.out.println("Output = " + rs.getInt(1));
				assertEquals(rs.getInt(1),29);
			}
		};
		testAggregation(aggregator, collections1, test1);
		
		db.openConnection();
		aggregator = new Aggregator(null, db, db.getCollection(3));
		Collection[] collections2 = {db.getCollection(3)};
		Test test2 = new Test() {
			public void run(int CollectionID) throws SQLException {
				Statement stmt = db.getCon().createStatement();
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Collections;\n");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),47);
			}
		};
		testAggregation(aggregator, collections2, test2);
	}
	
	/**
	 * Get the specified date in middle-endian as a Calendar object
	 * @return the Calendar for the given date and time
	 * @author shaferia
	 */
	private Calendar getDate(int month, int date, int year, int hour, int minute, int second) {
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(year, month - 1, date, hour, minute, second);
		return c;
	}
	
	/**
	 * Test interval (time-based) aggregation with an interval of 1 second
	 * for ATOFMS, AMS, and TimeSeries data.
	 * @author shaferia
	 */
	public void testIntervalAggregation() throws Exception {
		Calendar start;
		Calendar end;
		Calendar interval;
		Collection[] collections;
		Test test;
		
		db.openConnection();
		
		//for ATOFMS
		start = getDate(9, 2, 2003, 5+12, 30, 32);
		end = getDate(9, 2, 2003, 5+12, 30, 35);
		// jtbigwoo- this looks a bit funny, but the zero hour for dates is actually 1/1/1970 00:00:00
		interval = getDate(1, 1, 1970, 0, 0, 1);
		aggregator = new Aggregator(null, db, start, end, interval);
		collections = new Collection[] {db.getCollection(2), db.getCollection(3)};
		test = new Test() {
			public void run(int CollectionID) throws SQLException {
				ResultSet rs;
				
				rs = db.getCon().createStatement().executeQuery(
					"SELECT COUNT(*) FROM AtomMembership WHERE CollectionID > 5");
				assertTrue(rs.next());
				assertEquals(37, rs.getInt(1));
				
				//check proper collections hierarchy
				rs = db.getCon().createStatement().executeQuery(
					"SELECT ChildID FROM CollectionRelationships WHERE ParentID = 8 ORDER BY ChildID");
				for (int i = 9; i <= 24; ++i) {
					rs.next();
					assertEquals(rs.getInt(1), i);
				}
				assertFalse(rs.next());
				
				rs = db.getCon().createStatement().executeQuery(
					"SELECT ChildID FROM CollectionRelationships WHERE ParentID = 27 ORDER BY ChildID");
				for (int i = 28; i <= 39; ++i) {
					rs.next();
					assertEquals(rs.getInt(1), i);
				}
				assertFalse(rs.next());
				
				//check that new collections were created
				rs = db.getCon().createStatement().executeQuery(
						"SELECT COUNT(*) FROM Collections");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), 41);
				rs = db.getCon().createStatement().executeQuery(
						"SELECT Datatype FROM Collections WHERE Name='M/Z'");
				assertTrue(rs.next());
				assertEquals(rs.getString(1), "TimeSeries");
				
				//make sure there are no times outside of our boundaries
				rs = db.getCon().createStatement().executeQuery(
						"SELECT COUNT(*) FROM TimeSeriesAtomInfoDense WHERE AtomID > 20 AND " +
						"(Time > '2003-09-02 17:30:35.0' OR Time < '2003-09-02 17:30:32.0')");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),0);
			}
		};
		testAggregation(aggregator, collections, test);
		
		tearDown();
		setUp();
		db.openConnection();
		
		//for TimeSeries
		start = getDate(9, 2, 2003, 5+12, 30, 32);
		end = getDate(9, 2, 2003, 5+12, 30, 35);
		// jtbigwoo- this looks a bit funny, but the zero hour for dates is actually 1/1/1970 00:00:00
		interval = getDate(1, 1, 1970, 0, 0, 1);
		aggregator = new Aggregator(null, db, start, end, interval);
		collections = new Collection[] {db.getCollection(4)};
		test = new Test() {
			public void run(int CollectionID) throws SQLException {
				ResultSet rs;
				
				//check that three Atoms were binned, one for each second
				rs = db.getCon().createStatement().executeQuery(
						"SELECT AtomID FROM AtomMembership WHERE CollectionID = 7 ORDER BY AtomID");
				assertTrue(rs.next());
				assertEquals(31, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(32,  rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(33, rs.getInt(1));
				assertFalse(rs.next());
				
				//check for the new collection
				rs = db.getCon().createStatement().executeQuery(
						"SELECT CollectionID, Name, Datatype FROM Collections WHERE CollectionID > 5 ORDER BY CollectionID");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),6);
				assertEquals(rs.getString(2),"aggregated");
				assertEquals(rs.getString(3),"TimeSeries");
				assertTrue(rs.next());
				assertFalse(rs.next());
				
				//check for properly binned Atoms, correct peak heights
				//	at 17:30:33, value should be 0.4+0.5=0.9 (Atoms 14+15)
				//	at 17:30:34, value should be 0.6=0.6 (Atom 16)
				//	at 17:30:35, value should be 0.7+0.8=1.5 (Atoms 17+18)
				rs = db.getCon().createStatement().executeQuery(
						"SELECT Time, Value FROM TimeSeriesAtomInfoDense WHERE AtomID in (31,32,33) ORDER BY Time");
				assertTrue(rs.next());
				assertEquals(rs.getString(1),"2003-09-02 17:30:33");
				assertEquals(rs.getDouble(2),0.9,1e-5);
				assertTrue(rs.next());
				assertEquals(rs.getString(1),"2003-09-02 17:30:34");
				assertEquals(rs.getDouble(2),0.6,1e-5);
				assertTrue(rs.next());
				assertEquals(rs.getString(1),"2003-09-02 17:30:35");
				assertEquals(rs.getDouble(2),1.5,1e-5);
				assertFalse(rs.next());
			}
		};
		testAggregation(aggregator, collections, test);
		
		tearDown();
		setUp();
		db.openConnection();
		
		//for AMS
		start = getDate(9, 2, 2003, 5+12, 30, 32);
		end = getDate(9, 2, 2003, 5+12, 30, 35);
		// jtbigwoo- this looks a bit funny, but the zero hour for dates is actually 1/1/1970 00:00:00
		interval = getDate(1, 1, 1970, 0, 0, 1);
		aggregator = new Aggregator(null, db, start, end, interval);
		collections = new Collection[] {db.getCollection(5)};
		test = new Test() {
			public void run(int CollectionID) throws SQLException {
				ResultSet rs;
				
				//verify that correct atoms were placed in new aggregation collections
				rs = db.getCon().createStatement().executeQuery(
						"SELECT CollectionID, AtomID FROM AtomMembership WHERE CollectionID > 5 ORDER BY AtomID");
				int[] colls = {9,9,9,10,11};
				int[] atoms = {31,32,33,34,35};
				for (int i = 0; i < colls.length; ++i) {
					assertTrue(rs.next());
					assertEquals(rs.getInt(1), colls[i]);
					assertEquals(rs.getInt(2), atoms[i]);
				}
				assertFalse(rs.next());
				
				//m/z values for AMS in the test database: (1,2,3)
				rs = db.getCon().createStatement().executeQuery(
						"SELECT Name FROM Collections WHERE CollectionID > 5 AND Datatype ='TimeSeries' ORDER BY CollectionID");
				String[] expected = {"aggregated", "AMS", "M/Z", "1", "2", "3"};
				for (String s : expected) {
					assertTrue(rs.next());
					assertEquals(s, rs.getString(1));
				}
				assertFalse(rs.next());
				
				//verify the collection hierarchy
				rs = db.getCon().createStatement().executeQuery(
						"SELECT ParentID, ChildID FROM CollectionRelationships WHERE ChildID > 5 ORDER BY ChildID");
				int[] parents = {1,6,7,8,8,8};
				int[] children = {6,7,8,9,10,11};
				for (int i = 0; i < parents.length; ++i) {
					assertTrue(rs.next());
					assertEquals(rs.getInt(1), parents[i]);
					assertEquals(rs.getInt(2), children[i]);
				}
				assertFalse(rs.next());
				
				//verify the inserted TimeSeries data
				//	by comparing with the AMSAtomInfoSparse values for AtomID in (24,25,26,27,28,29)
				//	and the dense information times for the items
				rs = db.getCon().createStatement().executeQuery(
						"SELECT Time, Value FROM TimeSeriesAtomInfoDense WHERE AtomID in (31,32,33,34,35) ORDER BY Time");
				String[] times = {"2003-09-02 17:30:32", "2003-09-02 17:30:32", "2003-09-02 17:30:32",
						"2003-09-02 17:30:34", "2003-09-02 17:30:35"};
				int[] values = {2, 1, 0, 1, 1};
				for (int i = 0; i < 5; ++i) {
					rs.next();
					assertEquals(rs.getString(1), times[i]);
					assertEquals(rs.getInt(2), values[i]);
				}
				assertFalse(rs.next());
			}
		};
		testAggregation(aggregator, collections, test);
		
		db.closeConnection();
	}

	/**
	 * Test interval (time-based) aggregation with an interval of 36 hours
	 * for ATOFMS data.
	 * @author jtbigwoo
	 */
	public void testLargeIntervalAggregation() throws Exception {
		Calendar start;
		Calendar end;
		Calendar interval;
		Collection[] collections;
		Test test;
		
		db.openConnection();

		// changes the time between particles from one second to one day
		spreadOutParticlesInTime();
		
		//for ATOFMS
		start = getDate(9, 3, 2003, 5+12, 30, 32);
		end = getDate(9, 11, 2003, 5+12, 30, 35);
		// jtbigwoo- this looks a bit funny, but the zero hour for dates is actually 1/1/1970 00:00:00
		interval = getDate(1, 2, 1970, 12, 0, 0);
		aggregator = new Aggregator(null, db, start, end, interval);
		collections = new Collection[] {db.getCollection(2), db.getCollection(3)};
		test = new Test() {
			public void run(int CollectionID) throws SQLException {
				ResultSet rs;
				
				rs = db.getCon().createStatement().executeQuery(
					"SELECT COUNT(*) FROM AtomMembership WHERE CollectionID > 5");
				assertTrue(rs.next());
				assertEquals(55, rs.getInt(1));
				
				//check proper collections hierarchy
				rs = db.getCon().createStatement().executeQuery(
					"SELECT ChildID FROM CollectionRelationships WHERE ParentID = 8 ORDER BY ChildID");
				for (int i = 9; i <= 27; ++i) {
					rs.next();
					assertEquals(rs.getInt(1), i);
				}
				assertFalse(rs.next());
				
				rs = db.getCon().createStatement().executeQuery(
					"SELECT ChildID FROM CollectionRelationships WHERE ParentID = 30 ORDER BY ChildID");
				for (int i = 31; i <= 44; ++i) {
					rs.next();
					assertEquals(rs.getInt(1), i);
				}
				assertFalse(rs.next());
				
				//check that new collections were created
				rs = db.getCon().createStatement().executeQuery(
						"SELECT COUNT(*) FROM Collections");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), 46);
				rs = db.getCon().createStatement().executeQuery(
						"SELECT Datatype FROM Collections WHERE Name='M/Z'");
				assertTrue(rs.next());
				assertEquals(rs.getString(1), "TimeSeries");

				//make sure there are no times outside of our boundaries
				rs = db.getCon().createStatement().executeQuery(
						"SELECT COUNT(*) FROM TimeSeriesAtomInfoDense WHERE AtomID > 30 AND " +
						"(Time > '2003-09-11 17:30:35' OR Time < '2003-09-03 17:30:32')");
				assertTrue(rs.next());
				assertEquals(rs.getInt(1),0);
			}
		};
		testAggregation(aggregator, collections, test);
		
		db.closeConnection();
	}

	/**
	 * Sets the time column for the particles to 
	 * 9/2/2003 00:00:00
	 * 9/3/2003 00:00:00
	 * 9/4/2003 00:00:00
	 * etc. for atomid's 1 through 10
	 * @throws Exception
	 */
	private void spreadOutParticlesInTime() throws Exception {
		String atofms = "update ATOFMSAtomInfoDense set Time = ? where AtomId = ?";
		PreparedStatement stmt = db.getCon().prepareStatement(atofms);

		Calendar cal = getDate(9, 2, 2003, 0, 0, 0);

		for (int i = 0; i < 10; i++) {
			stmt.setString(1, TimeUtilities.dateToIso8601(cal.getTime()));
			stmt.setInt(2, i + 1);
			cal.add(Calendar.DATE, 1);
			cal.add(Calendar.SECOND, 2);
			stmt.executeUpdate();
		}
	}
}