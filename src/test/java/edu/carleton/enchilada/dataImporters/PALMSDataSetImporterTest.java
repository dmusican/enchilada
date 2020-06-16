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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.event.TableModelEvent;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import junit.framework.TestCase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.WriteException;
import edu.carleton.enchilada.gui.PALMSTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;
import edu.carleton.enchilada.testRow.palms.GenData;

/**
 * Tests PALMS import as implemented in PALMSDataSetImporter
 * @author rzeszotj based on shaferia
 */
public class PALMSDataSetImporterTest extends TestCase {
	PALMSDataSetImporter importer;
	InfoWarehouse db;
	PALMSTableModel table;
	
	JFrame mf;
	ProgressBarWrapper pbar;
	
	//was data loaded in this test?
	boolean dataLoaded;
	//the current row in the PALMSTableModel
	int curRow = 0;
	//test data files to be deleted
	Vector<String> deleteFiles;
	
	/**
	 * @see TestCase#setUp()
	 */
	public PALMSDataSetImporterTest(String s) {
		super(s);
	}
	
	/**
	 * Note: does not load any data into the database
	 */
	protected void setUp() {
		try {
			Database.rebuildDatabase("TestDB");
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
		assertTrue(db.openConnection("TestDB"));
		
		curRow = 0;
		deleteFiles = new Vector<String>();
		table = new PALMSTableModel();
		dataLoaded = false;
	}
	
	/**
	 * Create a dataset with the given parameters (as defined in testRow/PALMS/GenData),
	 * 	save it to disk, and add it to the import table
	 * @param items	the number of PALMS items to write
	 * @param mznum	the number of mz values to write
	 * @param mzscale a 1 or -1 to denote - or + spectrum
	 * @param peaks	a list containing the positive m/z values at which PALMS items should have peaks
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the fixed timestep for the particle timeseries
	 * @param fnames	names of the files to generate: {datasetname, timeseriesname, mzname}
	 */
	private void addData(int items, int mznum, int mzscale, int[] peaks, long tstart, long tdelta) {
		String[] names = new String[]{"Test_" + curRow};
		String[] files = GenData.generate(names, items, mznum, mzscale, peaks, tstart, tdelta);

		
		table.setValueAt(files[0], curRow, 1);	

		table.tableChanged(new TableModelEvent(table, curRow));
		
		for (String s : files)
			deleteFiles.add(s);
		
		++curRow;
	}
	
	/**
	 * Load data from the tablemodel and import it
	 * preconditions: 
	 * 	- test database exists and connection is open
	 *  - PALMSTableModel table has been filled with data
	 */
	private void loadData() {
		dataLoaded = true;
		mf = new JFrame();
		pbar = new ProgressBarWrapper(mf, "Importing Test PALMS Data", 100);
		importer = new PALMSDataSetImporter(table, mf, db, pbar);
		
		try {
			importer.collectTableInfo();
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("Error in importing test PALMS data");
		}
	}
	
	protected void tearDown() {
		db.closeConnection();
		try {
			System.runFinalization();
			System.gc();
			
			InfoWarehouse tempDB = Database.getDatabase();
			tempDB.openConnection();
			Connection con = tempDB.getCon();
			con.createStatement().executeUpdate("DROP DATABASE TestDB");
			tempDB.closeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
				
		for (String s : deleteFiles)
			(new File(s)).delete();
	
		if (dataLoaded) {	
			mf.dispose();
		}
		table = null;
	}
	
	
	/**
	 * Test the processing of a single dataset (without using collectTableInfo)
	 */
	public void testProcessDataSet() {
		int items = 30;
		int mznum = 150;
		int mzscale = 1;
		int[] peaks = new int[]{3,5,22,54,19,78,143};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mznum, mzscale, peaks, tstart, tdelta);
	
		dataLoaded = true;
		mf = new JFrame();
		pbar = new ProgressBarWrapper(mf, "Importing Test PALMS Data", 100);
		importer = new PALMSDataSetImporter(table, mf, db, pbar);
		
		importer.datasetName = (String) table.getValueAt(0, 1);
		
		//perform the import
		try {
			importer.processDataSet(0);
		}
		catch (WriteException e1) {
			e1.printStackTrace();
			fail("Couldn't process PALMS dataset");
		}
		catch (DisplayException e2) {
			e2.printStackTrace();
			fail("Couldn't process PALMS dataset");
		}
		
		//make sure the correct number of items were imported, and that sparse data has the correct length
		testDataLength(items, items * peaks.length);
		
		//perform deeper testing
		testDataset(items, mznum, mzscale, peaks, tstart, tdelta, 2, 1, 1);
	}

	
	/**
	 * Test import of a PALMS datasets imported separately using PALMSDataSetImporter.collectTableInfo
	 */
	public void testCollectTableInfo() {
		int items = 30;
		int mznum = 150;
		int mzscale = -1;
		int[] peaks = new int[]{3,5,22,54,19,78,143};
		long tstart = 3114199800l;
		long tdelta = 600;
		addData(items, mznum, mzscale, peaks, tstart, tdelta);
		
		loadData();
		
		//make sure the correct number of items were imported, and that sparse data has the correct length
		testDataLength(items, items * peaks.length);
		
		//perform deeper testing
		testDataset(items, mznum, mzscale, peaks, tstart, tdelta, 2, 1, 1);
		
		//create a second PALMS dataset with different values
		int items_2 = 20;
		int mznum_2 = 160;
		int mzscale_2 = 1;
		int[] peaks_2 = new int[]{4,10,11,12,66,99,155};
		long tstart_2 = 3114199800l;
		long tdelta_2 = 400;
		
		curRow = 0;
		
		//add the other dataset, overwriting the first
		String[] names = new String[]{"Test_" + curRow};
		String[] files = GenData.generate(names, items_2, mznum_2, mzscale_2, peaks_2, tstart_2, tdelta_2);
		
		table.setValueAt(files[0], curRow, 1);

		table.tableChanged(new TableModelEvent(table, curRow));
		
		loadData();
		
		//we should now have more data - make sure we do indeed.
		testDataLength(items + items_2, items * peaks.length + items_2 * peaks_2.length);
		
		//test the first import more thoroughly...
		testDataset(items, mznum, mzscale, peaks, tstart, tdelta, 2, 1, 1);
		
		//... and the second
		testDataset(items_2, mznum_2, mzscale_2, peaks_2, tstart_2, tdelta_2, 3, 2, items + 1);
	}

	/**
	 * Test import of three PALMS datasets imported together 
	 * (as though multiple rows of data were input in ImportPALMSDataDialog) 
	 */	
	public void testCollectTableInfoBatch() {
		int items = 30;
		int mznum = 30;
		int mzscale = 1;
		int[] peaks = new int[]{2,7,22};
		long tstart = 3114199800l;
		long tdelta = 400;
		addData(items, mznum, mzscale, peaks, tstart, tdelta);

		int items_2 = 20;
		int mznum_2 = 40;
		int mzscale_2 = -1;
		int[] peaks_2 = new int[]{1,3,6,15};
		long tstart_2 = 3114199800l;
		long tdelta_2 = 500;
		addData(items_2, mznum_2, mzscale_2, peaks_2, tstart_2, tdelta_2);

		int items_3 = 50;
		int mznum_3 = 80;
		int mzscale_3 = 1;
		int[] peaks_3 = new int[]{10,43,56,74};
		long tstart_3 = 3114199800l;
		long tdelta_3 = 600;
		addData(items_3, mznum_3, mzscale_3, peaks_3, tstart_3, tdelta_3);
		
		loadData();
		
		//Test the length and content of data, as with sequential imports
		testDataLength(items + items_2 + items_3, 
				items * peaks.length + items_2 * peaks_2.length + items_3 * peaks_3.length);
		testDataset(items, mznum, mzscale, peaks, tstart, tdelta, 2, 1, 1);
		testDataset(items_2, mznum_2, mzscale_2, peaks_2, tstart_2, tdelta_2, 3, 2, items + 1);
		testDataset(items_3, mznum_3, mzscale_3, peaks_3, tstart_3, tdelta_3, 4, 3, items + items_2 + 1);
	}
	
	/**
	 * Ensure that PALMSAtomInfoDense, AtomMembership, DataSetMembers, and PALMSAtomInfoSparse
	 * 	have the correct number of rows
	 * @param items	the number of expected Items
	 * @param peaks	the number of expected total peaks
	 * 	(if all items have the same number of peaks, items * peaksPerItem)
	 */
	private void testDataLength(int items, int numpeaks) {
		Connection con = db.getCon();
		ResultSet rs = null;
		try {
			rs = con.createStatement().executeQuery("SELECT count(*) FROM ATOFMSAtomInfoDense");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);
			
			rs = con.createStatement().executeQuery("SELECT count(*) FROM AtomMembership");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);
			
			rs = con.createStatement().executeQuery("SELECT count(*) FROM DataSetMembers");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), items);
			
			rs = con.createStatement().executeQuery("SELECT count(*) FROM ATOFMSAtomInfoSparse");
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), numpeaks);			
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Couldn't execute query to find length of tables");
		}		
	}
	
	/**
	 * Test to see if the database contains information corresponding to a proper PALMS import
	 * 	by querying against the four tables particle information is written to.
	 * @param items	the number of PALMS items to write
	 * @param mznum	the number of mz values to write
	 * @param mzscale a 1 or -1 to denote - or + spectrum
	 * @param peaks	a list containing the positive m/z values at which PALMS items should have peaks
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the fixed timestep for the particle timeseries
	 * @param CollectionID the collection the items were imported into
	 * @param OrigDataSetID the original dataset ID of the imported collection
	 * @param AtomIDStart the starting atomID of the imported items
	 */
	private void testDataset(int items, int mznum, int mzscale, int[] peaks, long tstart, long tdelta, 
			int CollectionID, int OrigDataSetID, int AtomIDStart) {
		
		Connection con = db.getCon();
		ResultSet rs = null;
		//Check dense atom info for ordering and timestamps
		try {
			rs = con.createStatement().executeQuery(
					"SELECT * FROM ATOFMSAtomInfoDense WHERE AtomID >= " + AtomIDStart + 
					" AND AtomID < " + (AtomIDStart + items));
			for (int i = 0; i < items; i++) {
				rs.next();
				assertEquals(rs.getInt(1), i + AtomIDStart);
				rs.getTimestamp(2);
				assertEquals(rs.getDouble(3),0.0);
				rs.getDouble(4);
				assertEquals(rs.getInt(5),i+1);
				rs.getString(6);
			}
			
			assertFalse(rs.next());
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Error while testing dense PALMS atom table");
		}
		
		//check for correct atom membership
		try {
			rs = con.createStatement().executeQuery(
					"SELECT * FROM AtomMembership WHERE CollectionID = " + CollectionID);
			
			for (int i = 0; i < items; ++i) {
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), CollectionID);
				assertEquals(rs.getInt(2), i + AtomIDStart);
			}
			
			assertFalse(rs.next());
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Error while testing PALMS atom<->collection membership");
		}
		
		//check for the correct OrigDataSetID on each Atom
		try {
			rs = con.createStatement().executeQuery(
					"SELECT * FROM DataSetMembers WHERE OrigDataSetID = " + OrigDataSetID);
			
			for (int i = 0; i < items; ++i) {
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), OrigDataSetID);
				assertEquals(rs.getInt(2), i + AtomIDStart);
			}
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Error while testing original PALMS dataset membership");
		}
		
		//check that sparse is correct
		try {
			rs = con.createStatement().executeQuery(
					"SELECT * FROM ATOFMSAtomInfoSparse WHERE AtomID >= " + AtomIDStart + 
					" AND AtomID < " + (AtomIDStart + items));
			
			for (int i = 0; i < items * peaks.length; ++i) {
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), i / peaks.length + AtomIDStart);
				//Check peak positions and values
				boolean peakExists = false;
				boolean peakValueExists = false;
				for (int j = 0; j < peaks.length; j++){
					if (peaks[j] == mzscale*rs.getInt(2))
						peakExists = true;
					int value = rs.getInt(3);
					if (value > 0)
						peakValueExists = true;
				}
				assertTrue(peakExists);
				assertTrue(peakValueExists);
				peakExists = false;
				peakValueExists = false;
				//Assume Rel. Area and Height are zeroed
				assertEquals(rs.getInt(4),0);
				assertEquals(rs.getInt(5),0);
			}
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Error while testing PALMS sparse data");
		}
	}
}
