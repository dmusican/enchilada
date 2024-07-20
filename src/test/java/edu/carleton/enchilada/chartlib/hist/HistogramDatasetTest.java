package edu.carleton.enchilada.chartlib.hist;

import java.awt.Color;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import javax.swing.JOptionPane;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import junit.framework.TestCase;

import static org.junit.Assert.assertNotEquals;

public class HistogramDatasetTest extends TestCase {

	Random rand = new Random(31337);
	private HistogramDataset[] baseHist, anotherBaseHist, compHist;
	ArrayList<Integer> keep = new ArrayList<>();
	private final int testMZ = 70;
	private Database db;
	private Database db2;

	protected void setUp() throws Exception {
		super.setUp();
		try {
			Database.getDatabase("TestDB").rebuildDatabase("TestDB");
			Database.getDatabase("TestDB2").rebuildDatabase("TestDB2");
		} catch (ExceptionAdapter ea) {
			if (ea.originalException instanceof SQLException) {
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
			} else {
				throw ea;
			}
		}
			
		//Open database connection:
	   db = Database.getDatabase("TestDB");
	   db2 = Database.getDatabase("TestDB2");
	   db.openConnection();
	   db2.openConnection();
	   Connection con = db.getCon();
	   Connection con2 = db2.getCon();
	   con.setAutoCommit(false);
	   con2.setAutoCommit(false);
	   try (Statement stmt = con.createStatement();
			Statement stmt2 = con2.createStatement()) {
		   stmt.executeUpdate("INSERT INTO Collections VALUES (2,'One', 'one', 'onedescrip', 'ATOFMS')\n");
		   stmt2.executeUpdate("INSERT INTO Collections VALUES (2,'One', 'one', 'onedescrip', 'ATOFMS')\n");


		   // i becomes sorta an atomID.
		   int k = 0;
		   for (int i = 1; i <= 100; i++) {
			   // Create a binned peak list.
			   BinnedPeakList bpl = new BinnedPeakList();
			   int location;
			   int size;
			   String q;
			   for (int j = 0; j < rand.nextInt(60); j++) { // num peaks
				   int maxMZ = 30;
				   location = maxMZ - k;
				   size = (int) (300 * Math.random());
				   bpl.add(location, size);
				   q = "INSERT INTO ATOFMSAtomInfoSparse VALUES(" + i + "," + location + "," + size + "," + size + "," + size + ")\n";
				   stmt.executeUpdate(q);
				   q = "INSERT INTO ATOFMSAtomInfoSparse VALUES(" + (i) + "," + location + "," + size + "," + size + "," + size + ")\n";
				   stmt2.executeUpdate(q);
				   k++;
			   }

			   // put every other binned peak list in what will become the validation
			   // histogram.
			   if (i % 2 == 0) {
				   // add a peak that won't ever exist otherwise, for getSelection
				   location = testMZ;
				   size = (int) (300 * Math.random());

				   bpl.add(location, size);
				   q = "INSERT INTO ATOFMSAtomInfoSparse VALUES(" + i + "," + location + "," + size + "," + size + "," + size + ")\n";
				   stmt.executeUpdate(q);

				   q = "INSERT INTO ATOFMSAtomInfoSparse VALUES(" + (i) + "," + location + "," + size + "," + size + "," + size + ")\n";
				   stmt2.executeUpdate(q);

				   // keep is the list for testIntersect()
				   keep.add(i);
			   }
			   q = "INSERT INTO AtomMembership VALUES(2," + i + ")\n";

			   stmt.executeUpdate(q);
			   q = "INSERT INTO AtomMembership VALUES(2," + (i) + ")\n";
			   stmt2.executeUpdate(q);
		   }

		   try (ResultSet rs = stmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
				   " CollectionID = 2")) {
			   while (rs.next())
				   stmt.addBatch("INSERT INTO InternalAtomOrder VALUES (" + rs.getInt(1) + ",2)");
		   }
		   stmt.executeBatch();
		   con.commit();
		   con.setAutoCommit(true);

		   try (ResultSet rs = stmt2.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
				   " CollectionID = 2")) {
			   while (rs.next())
				   stmt2.addBatch("INSERT INTO InternalAtomOrder VALUES (" + rs.getInt(1) + ",2)");
		   }
		   stmt2.executeBatch();
		   con2.commit();
		   con2.setAutoCommit(true);
	   }

		baseHist = HistogramDataset.analyseBPLs(db.getBPLOnlyCursor(db.getCollection(2)), Color.BLACK);
		anotherBaseHist = HistogramDataset.analyseBPLs(db.getBPLOnlyCursor(db.getCollection(2)), Color.BLACK);
		compHist = HistogramDataset.analyseBPLs(db2.getBPLOnlyCursor(db2.getCollection(2)), Color.BLACK);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		db.closeConnection();
		db.dropDatabaseCommands();
		db2.closeConnection();
		db2.dropDatabaseCommands();
	}

	public void testEquals() {
		// these should be identical.
		for (int i = 0; i < baseHist.length; i++) {
			assertEquals(baseHist[i], anotherBaseHist[i]);
		}
		
		baseHist[0].hists[20] = new ChainingHistogram(0.01f);
		anotherBaseHist[0].hists[20] = null;
		// an empty one should be the same as a null one.
		assertEquals(baseHist[0], anotherBaseHist[0]);
		
		// these should not be equal.
		baseHist[0].hists[20].addPeak(0.4f, 12345);
		assertNotEquals(baseHist[0], anotherBaseHist[0]);

	}
	
	public void testGetSelection() {
		HistogramDataset[] destHist, nextHist;
		
		ArrayList<BrushSelection> selection = new ArrayList<BrushSelection>();
		selection.add(new BrushSelection(0, testMZ, 0, 1));
		
		destHist = HistogramDataset.getSelection(baseHist, selection);
		nextHist = HistogramDataset.getSelection(compHist, selection);
		
		for (int i = 0; i < baseHist.length; i++) {
			assertEquals(destHist[i], nextHist[i]);
		}
	}
	
	public void testIntersect() {
		HistogramDataset[] destHist, nextHist;
		
		destHist = HistogramDataset.intersect(baseHist, keep);
		nextHist = HistogramDataset.intersect(compHist, keep);
		for (int i = 0; i < baseHist.length; i++) {
			assertEquals(destHist[i], nextHist[i]);
		}
	}

}
