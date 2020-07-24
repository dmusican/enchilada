/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is EDAM Enchilada's Database class.
 *
 * The Initial Developer of the Original Code is
 * The EDAM Project at Carleton College.
 * Portions created by the Initial Developer are Copyright (C) 2005
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * Ben J Anderson andersbe@gmail.com
 * David R Musicant dmusican@carleton.edu
 * Anna Ritz ritza@carleton.edu
 * Greg Cipriano gregc@cs.wisc.edu
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */
package edu.carleton.enchilada.dataImporters;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.Vector;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.event.TableModelEvent;

import edu.carleton.enchilada.database.TimeUtilities;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import junit.framework.TestCase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.WriteException;
import edu.carleton.enchilada.gui.AMSTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;
import edu.carleton.enchilada.testRow.ams.GenData;

/**
 * Tests AMS import as implemented in AMSDataSetImporter
 * @author shaferia
 */
public class AMSDataSetImporterTest extends TestCase {
	AMSDataSetImporter importer;
	Database db;
	AMSTableModel table;

	JFrame mf;
	ProgressBarWrapper pbar;

	//was data loaded in this test?
	boolean dataLoaded;
	//the current row in the AMSTableModel
	int curRow = 0;
	//test data files to be deleted
	Vector<String> deleteFiles;

	/**
	 * @see TestCase#setUp()
	 */
	public AMSDataSetImporterTest(String s) {
		super(s);
	}

	/**
	 * Note: does not load any data into the database
	 */
	protected void setUp() {
		try {
			Database.getDatabase("TestDB").rebuildDatabase("TestDB");
		} catch (ExceptionAdapter ea) {
			if (ea.originalException instanceof SQLException) {
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
			} else {
				throw ea;
			}
		}
		db = Database.getDatabase("TestDB");
		assertTrue(db.openConnection());

		curRow = 0;
		deleteFiles = new Vector<String>();
		table = new AMSTableModel();
		dataLoaded = false;
	}

	/**
	 * Create a dataset with the given parameters (as defined in testRow/ams/GenData),
	 * 	save it to disk, and add it to the import table
	 * @param items	the number of AMS items to write
	 * @param mzlen	the maximum in the range of m/z values to consider
	 * @param peaks	a list containing the m/z values at which AMS items should have peaks (in range 1..mzlen)
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the fixed timestep for the particle timeseries
	 * @param fnames	names of the files to generate: {datasetname, timeseriesname, mzname}
	 */
	private void addData(int items, int mzlen, int[] peaks, long tstart, long tdelta) {
		String[] names = new String[]{"Test_" + curRow, "TS_" + curRow, "MZ_" + curRow};
		String[] files = GenData.generate(names, items, mzlen, peaks, tstart, tdelta);

		table.setValueAt(files[0], curRow, 1);
		table.setValueAt(files[1], curRow, 2);
		table.setValueAt(files[2], curRow, 3);

		table.tableChanged(new TableModelEvent(table, curRow));

		for (String s : files)
			deleteFiles.add(s);

		++curRow;
	}

	/**
	 * Load data from the tablemodel and import it
	 * preconditions: 
	 * 	- test database exists and connection is open
	 *  - AMSTableModel table has been filled with data
	 */
	private void loadData() {
		dataLoaded = true;
		mf = new JFrame();
		pbar = new ProgressBarWrapper(mf, "Importing Test AMS Data", 100);
		importer = new AMSDataSetImporter(table, mf, db, pbar);

		try {
			importer.errorCheck();
		} catch (DisplayException ex) {
			ex.printStackTrace();
			fail("Error in inserting AMS table data");
		}

		try {
			importer.collectTableInfo();
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("Error in importing test AMS data");
		}
 	}

	protected void tearDown() throws Exception {
		db.closeConnection();
		System.runFinalization();
		System.gc();

		Database tempDB = Database.getDatabase();
		tempDB.openConnection();
		Connection con = tempDB.getCon();
		Database.dropDatabase("TestDB");
		for (String s : deleteFiles)
			(new File(s)).delete();

		if (dataLoaded) {
			mf.dispose();
		}
		table = null;
	}

	public void testGetName() {
		addData(20, 20, new int[]{4, 10, 11, 15}, 3114199800l, 600);
		loadData();

		try {
			assertEquals(importer.getName(), "Test_0");
		} catch (FileNotFoundException ex) {
			fail("Could not find saved datafile");
		}

		//we shouldn't be able to access the datafile if it's deleted
		//	(also be sure that AMSDataSetImporter closed all its read handles)
		for (String s : deleteFiles) {
			(new File(s)).delete();
			if ((new File(s)).exists()) {
				fail("AMSDataSetImporter didn't close file " + s);
			}
		}
		deleteFiles.removeAllElements();

		try {
			importer.getName();
			fail("Should not be able to find a name for a nonexistent AMS file");
		} catch (FileNotFoundException ex) {
			//pass
		}
	}

	/**
	 * Test the processing of a single dataset (without using collectTableInfo)
	 */
	public void testProcessDataSet() throws Exception {
		int items = 30;
		int mzlen = 30;
		int[] peaks = new int[]{4, 10, 11, 15, 24, 27};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mzlen, peaks, tstart, tdelta);

		dataLoaded = true;
		mf = new JFrame();
		pbar = new ProgressBarWrapper(mf, "Importing Test AMS Data", 100);
		importer = new AMSDataSetImporter(table, mf, db, pbar);

		importer.datasetName = (String) table.getValueAt(0, 1);
		importer.timeSeriesFile = (String) table.getValueAt(0, 2);
		importer.massToChargeFile = (String) table.getValueAt(0, 3);

		//perform the import
		try {
			importer.processDataSet(0);
		}
		catch (WriteException e1) {
			e1.printStackTrace();
			fail("Couldn't process AMS dataset");
		}
		catch (DisplayException e2) {
			e2.printStackTrace();
			fail("Couldn't process AMS dataset");
		}

		//make sure the correct number of items were imported, and that sparse data has the correct length
		testDataLength(items, items * peaks.length);

		//perform deeper testing
		testDataset(items, mzlen, peaks, tstart, tdelta, 2, 1, 1);
	}

	/**
	 * separate from testProcessDataSet so that it has a separate setUp and tearDown
	 */
	public void testProcessDataSetErrors() {
		int items = 20;
		int mzlen = 20;
		int[] peaks = new int[]{4, 10, 11, 15};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mzlen, peaks, tstart, tdelta);

		dataLoaded = true;
		mf = new JFrame();
		pbar = new ProgressBarWrapper(mf, "Importing Test AMS Data", 100);
		importer = new AMSDataSetImporter(table, mf, db, pbar);

		importer.datasetName = (String) table.getValueAt(0, 1);
		importer.timeSeriesFile = (String) table.getValueAt(0, 2);
		importer.massToChargeFile = (String) table.getValueAt(0, 3);

		//test for having no time series file
		assertTrue(new File(deleteFiles.get(1)).renameTo(new File(deleteFiles.get(1) + ".old")));

		try {
			importer.processDataSet(0);
			fail("A WriteException should have been thrown if the AMS time series file does not exist");
		} catch (WriteException ex) {
			// expected
		} catch (DisplayException ex) {
			fail();
		}

		assertTrue(new File(deleteFiles.get(1) + ".old").renameTo(new File(deleteFiles.get(1))));

		//test for having no m/z file
		assertTrue(new File(deleteFiles.get(2)).renameTo(new File(deleteFiles.get(2) + ".old")));

		try {
			importer.processDataSet(0);
			fail("A WriteException should have been thrown if the AMS m/z does not exist");
		} catch (WriteException ex) {
			// expected
		} catch (DisplayException ex) {
			fail();
		}

		assertTrue(new File(deleteFiles.get(2) + ".old").renameTo(new File(deleteFiles.get(2))));

		//test for having no data file
		assertTrue(new File(deleteFiles.get(0)).renameTo(new File(deleteFiles.get(0) + ".old")));

		try {
			importer.processDataSet(0);
			fail("A WriteException should have been thrown if the AMS datafile does not exist");
		} catch (WriteException ex) {
			// expected
		} catch (DisplayException ex) {
			fail();
		}

		assertTrue(new File(deleteFiles.get(0) + ".old").renameTo(new File(deleteFiles.get(0))));
	}

	/**
	 * Test import of a AMS datasets imported separately using AMSDataSetImporter.collectTableInfo
	 */
	public void testCollectTableInfo() throws Exception {
		int items = 30;
		int mzlen = 30;
		int[] peaks = new int[]{4, 10, 11, 15, 24, 27};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mzlen, peaks, tstart, tdelta);

		loadData();

		//make sure the correct number of items were imported, and that sparse data has the correct length
		testDataLength(items, items * peaks.length);

		//perform deeper testing
		testDataset(items, mzlen, peaks, tstart, tdelta, 2, 1, 1);

		//create a second AMS dataset with different values
		int items_2 = 20;
		int mzlen_2 = 40;
		int[] peaks_2 = new int[]{4, 10, 11, 12};
		long tstart_2 = 3114199800l;
		long tdelta_2 = 400;

		curRow = 0;

		//add the other dataset, overwriting the first
		String[] names = new String[]{"Test_" + curRow, "TS_" + curRow, "MZ_" + curRow};
		String[] files = GenData.generate(names, items_2, mzlen_2, peaks_2, tstart_2, tdelta_2);

		table.setValueAt(files[0], curRow, 1);
		table.setValueAt(files[1], curRow, 2);
		table.setValueAt(files[2], curRow, 3);

		table.tableChanged(new TableModelEvent(table, curRow));

		loadData();

		//we should now have more data - make sure we do indeed.
		testDataLength(items + items_2, items * peaks.length + items_2 * peaks_2.length);

		//test the first import more thoroughly...
		testDataset(items, mzlen, peaks, tstart, tdelta, 2, 1, 1);

		//... and the second
		testDataset(items_2, mzlen_2, peaks_2, tstart_2, tdelta_2, 3, 2, items + 1);
	}

	/**
	 * Test import of three AMS datasets imported together 
	 * (as though multiple rows of data were input in ImportAMSDataDialog) 
	 */
	public void testCollectTableInfoBatch() throws Exception {
		int items = 30;
		int mzlen = 30;
		int[] peaks = new int[]{4, 10, 11, 15, 24, 27};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mzlen, peaks, tstart, tdelta);

		int items_2 = 20;
		int mzlen_2 = 40;
		int[] peaks_2 = new int[]{4, 10, 11, 12};
		long tstart_2 = 3114199800l;
		long tdelta_2 = 400;
		addData(items_2, mzlen_2, peaks_2, tstart_2, tdelta_2);

		int items_3 = 100;
		int mzlen_3 = 10;
		int[] peaks_3 = new int[]{2, 4, 6};
		long tstart_3 = 2304123400l;
		long tdelta_3 = 800;
		addData(items_3, mzlen_3, peaks_3, tstart_3, tdelta_3);

		loadData();

		//Test the length and content of data, as with sequential imports
		testDataLength(items + items_2 + items_3,
				items * peaks.length + items_2 * peaks_2.length + items_3 * peaks_3.length);
		testDataset(items, mzlen, peaks, tstart, tdelta, 2, 1, 1);
		testDataset(items_2, mzlen_2, peaks_2, tstart_2, tdelta_2, 3, 2, items + 1);
		testDataset(items_3, mzlen_3, peaks_3, tstart_3, tdelta_3, 4, 3, items + items_2 + 1);
	}

	/**
	 * Ensure that AMSAtomInfoDense, AtomMembership, DataSetMembers, and AMSAtomInfoSparse
	 * 	have the correct number of rows
	 * @param items	the number of expected Items
	 * @param peaks	the number of expected total peaks
	 * 	(if all items have the same number of peaks, items * peaksPerItem)
	 */
	private void testDataLength(int items, int numpeaks) {
		Connection con = db.getCon();
		ResultSet rs = null;
		try {
			rs = con.createStatement().executeQuery("SELECT count(*) FROM AMSAtomInfoDense");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);

			rs = con.createStatement().executeQuery("SELECT count(*) FROM AtomMembership");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);

			rs = con.createStatement().executeQuery("SELECT count(*) FROM DataSetMembers");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);

			rs = con.createStatement().executeQuery("SELECT count(*) FROM AMSAtomInfoSparse");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), numpeaks);
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Couldn't execute query to find length of tables");
		}
	}

	/**
	 * Test to see if the database contains information corresponding to a proper AMS import
	 * 	by querying against the four tables particle information is written to.
	 * @param items	the number of AMS items written
	 * @param mzlen	the maximum in the range of m/z values to consider
	 * @param peaks	a list containing the m/z values at which AMS items have peaks (in range 1..mzlen)
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the fixed timestep for the particle timeseries
	 * @param CollectionID the collection the items were imported into
	 * @param OrigDataSetID the original dataset ID of the imported collection
	 * @param AtomIDStart the starting atomID of the imported items
	 */
	private void testDataset(int items, int mzlen, int[] peaks, long tstart, long tdelta,
							 int CollectionID, int OrigDataSetID, int AtomIDStart) throws Exception {
		double[] peakVals = {0.1, 0.2, 0.3, 0.4, 0.5};

		Connection con = db.getCon();
		ResultSet rs = null;
		//Check dense atom info for ordering and timestamps
		rs = con.createStatement().executeQuery(
				"SELECT * FROM AMSAtomInfoDense WHERE AtomID >= " + AtomIDStart +
						" AND AtomID < " + (AtomIDStart + items));

		//check that timestamps are as expected
		//	(subtract a base time from that used by the calendar and that used by the datafiles)
		long curt = tstart - 3114199800l;
		for (int i = 0; i < items; ++i) {
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), i + AtomIDStart);

			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"));
			String dateRead = rs.getString(2);
			c.setTime(TimeUtilities.iso8601ToDate(dateRead));
			assertEquals((c.getTimeInMillis() - 1034055000000l) / 1000, curt);
			curt += tdelta;
		}

		assertFalse(rs.next());

		//check for correct atom membership
		rs = con.createStatement().executeQuery(
				"SELECT * FROM AtomMembership WHERE CollectionID = " + CollectionID);

		for (int i = 0; i < items; ++i) {
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), CollectionID);
			assertEquals(rs.getInt(2), i + AtomIDStart);
		}

		assertFalse(rs.next());

		//check for the correct OrigDataSetID on each Atom
		rs = con.createStatement().executeQuery(
				"SELECT * FROM DataSetMembers WHERE OrigDataSetID = " + OrigDataSetID);

		for (int i = 0; i < items; ++i) {
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), OrigDataSetID);
			assertEquals(rs.getInt(2), i + AtomIDStart);
		}

		//check that correct peaks were imported
		rs = con.createStatement().executeQuery(
				"SELECT * FROM AMSAtomInfoSparse WHERE AtomID >= " + AtomIDStart +
						" AND AtomID < " + (AtomIDStart + items));

		for (int i = 0; i < items * peaks.length; ++i) {
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), i / peaks.length + AtomIDStart);
			assertEquals(rs.getInt(2), peaks[i % peaks.length]);

			//the 1e-7 tolerance for double comparison is arbitrary
			assertEquals(rs.getDouble(3), peakVals[i % peakVals.length], 1e-7);
		}
	}
}
