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
 * The Original Code is EDAM Enchilada's ATOFMSDataSetImporter unit test class.
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


/*
 * Created on Aug 25, 2004s
 */
package edu.carleton.enchilada.dataImporters;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.event.TableModelEvent;

import edu.carleton.enchilada.ATOFMS.CalInfo;

import edu.carleton.enchilada.ATOFMS.ATOFMSParticle;
import edu.carleton.enchilada.ATOFMS.PeakParams;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import junit.framework.TestCase;

import edu.carleton.enchilada.gui.ParTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**
 * @author ritza
 * 
 * 
 * the problem here is that CreateTestDatabase populates the database, but
 * we really want to be looking at the particles that are *imported*... huh.
 */
public class ATOFMSDataSetImporterTest extends TestCase {
	ATOFMSDataSetImporter importer;
	Database db;
	ParTableModel table;
	/*
	 * @see TestCase#setUp()
	 */
	public ATOFMSDataSetImporterTest(String aString)
	{
		super(aString);
	}
	
	protected void setUp()
	{
		//TODO: commented this out AR
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
		
		// create table with one entry.
		table = new ParTableModel(true);
		// TODO: insert dummy row.
		String parFile = Paths.get("testRow", "b", "b.par").toString();
		String calFile = Paths.get("testRow", "b", "cal.cal").toString();
		table.setValueAt(parFile, 0, 1);   // dataset
		table.setValueAt(calFile, 0, 2); // mass cal file
		table.setValueAt(10, 0, 4);    // Min height
		table.setValueAt(20, 0, 5);	   // Min area
		table.setValueAt((float) 0.1, 0, 6);  // Min relative area
		table.setValueAt((float) 0.5, 0, 7);  // Max peak error
		table.setValueAt(true, 0, 8);  // autocal
		
		table.tableChanged(new TableModelEvent(table, 0));
		
		JFrame mf = new JFrame();
		final ProgressBarWrapper progressBar = 
			new ProgressBarWrapper(mf, "Importing ATOFMS Datasets", 100);
		//progressBar.constructThis();
		//final JFrame frameRef = frame;
		//final ATOFMSBatchTableModel aRef = a;
		importer = new ATOFMSDataSetImporter(table, mf, db, progressBar);
			
	}
	
	protected void tearDown()
	{
		db.closeConnection();
		System.runFinalization();
		System.gc();
		Database.dropDatabase("TestDB");
		table = null;
	}
	
	
	public void testParVersion() {
		try {
			importer.parFile = new File((String)table.getValueAt(0,1));
			String[] info = importer.parVersion();
			assertTrue(info[0].equals("b"));
			assertTrue(info[1].equals("08/04/2004 15:38:47"));
			assertTrue(info[2].equals("ambient"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This method in turn calls processDataSet,
	 * readParFileAndCreateEmptyCollection() and
	 * readSpectraAndCreateParticle(),
	 * so unit tests are not needed for these two.
	 *
	 */
	public void testCollectTableInfo() throws SQLException, DisplayException, InterruptedException {
		importer.setParentID(0);
		importer.checkNullRows();

		importer.collectTableInfo();
		for(int i=0;i<importer.getNumCollections();i++){
			importer.collectRowInfo();
		}

		
		Statement stmt = db.getCon().createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM ATOFMSAtomInfoDense " +
				"ORDER BY AtomID");
		assertEquals(true,rs.next());
		int rowCount = 1;

		// to construct more tests like below, this is useful.
//		ResultSetMetaData rsmd = rs.getMetaData();
//		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
//			System.out.println("assertEquals("+rs.getObject(i).toString()
//					+",rs.get" + rsmd.getColumnClassName(i) +"("+i+"));");
//			
//		}
		
		String dir = System.getProperty("user.dir");

		assertEquals(1,rs.getInt(1));
		assertEquals("2004-08-04 15:39:13",rs.getString(2));
		assertEquals(1.031E-6,rs.getFloat(3), 0.0001);
		assertEquals(0.0,rs.getFloat(4), 0.0001);
		assertEquals(3129,rs.getInt(5));
		String fileLocation = Paths.get(dir,"testRow","b","b-040804153913-00001.amz").toString();
		assertEquals(fileLocation, rs.getString(6));
		
		while (rs.next()) {
			rowCount++;
		}
		assertEquals(10, rowCount);
		
		ArrayList<Integer> atomIDs = db.getAllDescendedAtoms(db.getCollection(0));
		java.util.Collections.sort(atomIDs);
		for(int i = 1; i <= atomIDs.size(); i++)
			assertEquals(i, atomIDs.get(i).intValue());
		
		assertEquals("b", db.getCollectionName(2));
	
	}
	
	/**
	 * Creates some functionality for a file that can be modified and then have its contents reset.
	 * Potentially useful for writing tests that depend upon files to run.
	 * Must invoke close() to write back the original contents.
	 * @author shaferia
	 */
	public static class ResettableFile {
		private File file;
		private String current;
		private String original;
		
		public ResettableFile(File f) {
			this(f.getAbsolutePath());
		}
		
		public ResettableFile(String filename) {
			file = new File(filename);
			
			if (!file.exists())
				throw new IllegalArgumentException("ResettableFile: file doesn't exist: " + filename);
			
			original = readFile(file);
		}
		
		private String readFile(File f) {
			try {
				StringBuffer in = new StringBuffer();
				java.io.FileReader fw = new java.io.FileReader(f);
				while (fw.ready()) {
					in.append((char) fw.read());
				}
				return in.toString();
			}
			catch (java.io.IOException ex) {
				System.err.println("ResettableFile: couldn't read file: " + f.getAbsolutePath());
				return null;
			}
		}
		
		private void writeFile(File f, String contents) {
			try {
				java.io.FileWriter fw = new java.io.FileWriter(f);
				fw.write(contents);
			}
			catch (java.io.IOException ex) {
				System.err.println("ResettableFile: couldn't write file: " + f.getAbsolutePath());
			}			
		}
		
		public String getCurrent() {
			return current;
		}
		
		public void setCurrent(String current) {
			this.current = current;
			writeFile(file, this.current);
		}
		
		public String getOriginal() {
			return original;
		}
		
		public void replaceFirst(String regex, String replacement) {
			current.replaceFirst(regex, replacement);
			writeFile(file, current);
		}
		
		public void replaceAll(String regex, String replacement) {
			current.replaceAll(regex, replacement);
			writeFile(file, current);
		}
		
		public void close() {
			writeFile(file, original);
		}
	}
	
	/**
	 * @author shaferia
	 */
	public void testReadParFileAndCreateEmptyCollection() {
		try {
			importer.parFile = new File((String)table.getValueAt(0,1));
			Path calFilePath = Paths.get("testRow","b","cal.cal");
			ATOFMSParticle.currCalInfo = new CalInfo(calFilePath.toString(), true);
			ATOFMSParticle.currPeakParams = new PeakParams(10, 20, .1f, .5f);
			importer.readParFileAndCreateEmptyCollection();

			Connection con = db.getCon();
			
			ResultSet rs = con.createStatement().executeQuery(
					"SELECT * FROM Collections WHERE CollectionID = 2");
			
			assertTrue(rs.next());
			assertEquals(rs.getString("Comment"), "ambient");
			assertEquals(rs.getString("Description"), "b: ambient");
			assertEquals(rs.getString("Datatype"), "ATOFMS");
			
			assertFalse(rs.next());
			rs.close();
			
			rs = con.createStatement().executeQuery(
					"SELECT * FROM ATOFMSDataSetInfo WHERE DataSetID = 1");
			assertTrue(rs.next());
			assertEquals(rs.getString("DataSet"), "b");
			assertEquals(rs.getInt("MinHeight"), 10);
			assertEquals(rs.getInt("MinArea"), 20);
			assertEquals(rs.getFloat("MinRelArea"), 0.1f);
			assertTrue(rs.getBoolean("Autocal"));
			
			assertFalse(rs.next());
			rs.close();

			ATOFMSParticle.currCalInfo = new CalInfo(calFilePath.toString(), false);
			importer.readParFileAndCreateEmptyCollection();

		}
		catch (Exception ex) {
			throw new ExceptionAdapter(ex);
		}
		
		try {
			Connection con = db.getCon();
			ResultSet rs = con.createStatement().executeQuery(
					"SELECT * FROM ATOFMSDataSetInfo WHERE DataSetID = 2");
			assertTrue(rs.next());
			assertFalse(rs.getBoolean("Autocal"));
			rs.close();
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Couldn't analyze success of readParFileAndCreateEmptyCollection");
		}
	}
	
	/**
	 * A PrintStream that captures written data in a StringBuffer
	 * Useful for redirecting standard output to a string.
	 * @author shaferia
	 */
	private class StringPrintStream {
		private StringBuffer buf = new StringBuffer();
		private OutputStream os = new OutputStream() {
			public void write(int b) throws IOException {
				buf.append((char) b);
			}
		};
		private PrintStream ps = new PrintStream(os);
		
		public PrintStream getPrintStream() {
			return ps;
		}
		
		/**
		 * Returns everything written since last invocation of flush()
		 * @return content written to this StringPrintStream's PrintStream
		 */
		public String flush() {
			ps.flush();
			return buf.toString();
		}
	}
	
	/**
	 * @author shaferia
	 */
	public void testReadSpectraAndCreateParticle() {
		//Since exceptions aren't thrown back down the stack by the database, 
		//	we need to see if any are thrown
		StringPrintStream ps = new StringPrintStream();
		PrintStream oldErr = System.err;
		System.setErr(ps.getPrintStream());
		
		//Make sure standard output gets set back.
		try {
			try {
				importer.parFile = new File((String)table.getValueAt(0,1));
				String massCalFile = Paths.get("testRow", "b", "cal.cal").toString();
				ATOFMSParticle.currCalInfo = new CalInfo(massCalFile, true);
				ATOFMSParticle.currPeakParams = new PeakParams(10, 20, .1f, .5f);
				importer.numParticles = new int[1];
				importer.numParticles[0] = 10;
				importer.collections = new Collection[1];
				importer.id = 
					db.createEmptyCollectionAndDataset("ATOFMS", 0, "b", 
							"comment", "'null', 'null', 10, 10, 0.005, 1");
				importer.progressBar = new ProgressBarWrapper(null, "Progress", 10);
			}
			catch (java.io.IOException ex) {
				fail("Couldn't set necessary files to create empty collection");
			}
			
			try {
				importer.readSpectraAndCreateParticle();
			}
			catch (java.io.IOException ex) {
				ex.printStackTrace();
				fail("Couldn't read spectra");
			}
			catch (InterruptedException ex) {
				ex.printStackTrace();
				fail("Couldn't read spectra; interrupted.");
			}
			catch (java.text.ParseException ex) {
				ex.printStackTrace();
				fail("Couldn't read spectra");
			}

			try {
				
				Connection con = db.getCon();
				ResultSet rs = con.createStatement().executeQuery(
						"SELECT * FROM ATOFMSAtomInfoDense");
				
				//Compare to expected dense info
				String root = System.getProperty("user.dir");
				Path rootPath = Paths.get(root);

				assertTrue(rs.next());
				assertEquals(1,                     rs.getInt(1));
				assertEquals("2004-08-04 15:39:13", rs.getString(2));
				assertEquals(1.031E-6,              rs.getDouble(3), 1e-10);
				assertEquals(0,                     rs.getInt(4));
				assertEquals(3129,                  rs.getInt(5));
				assertEquals(rootPath.resolve("testRow").resolve("b").resolve("b-040804153913-00001.amz").toString(),
							 rs.getString(6));

				assertTrue(rs.next());
				assertEquals(2,                     rs.getInt(1));
				assertEquals("2004-08-04 15:39:17", rs.getString(2));
				assertEquals(9.96E-7,               rs.getDouble(3), 1e-10);
				assertEquals(0,                     rs.getInt(4));
				assertEquals(2763,                  rs.getInt(5));
				assertEquals(rootPath.resolve("testRow").resolve("b").resolve("b-040804153917-00002.amz").toString(),
						rs.getString(6));

				assertTrue(rs.next());
				assertEquals(3,                     rs.getInt(1));
				assertEquals("2004-08-04 15:39:40", rs.getString(2));
				assertEquals(1.002E-6,              rs.getDouble(3), 1e-10);
				assertEquals(0,                     rs.getInt(4));
				assertEquals(2482,                  rs.getInt(5));
				assertEquals(rootPath.resolve("testRow").resolve("b").resolve("b-040804153940-00003.amz").toString(),
						rs.getString(6));

				assertTrue(rs.next());
				assertEquals(4,                     rs.getInt(1));
				assertEquals("2004-08-04 15:40:10", rs.getString(2));
				assertEquals(9.84e-7,               rs.getDouble(3), 1e-10);
				assertEquals(0,                     rs.getInt(4));
				assertEquals(2948,                  rs.getInt(5));
				assertEquals(rootPath.resolve("testRow").resolve("b").resolve("b-040804154010-00004.amz").toString(),
						rs.getString(6));

				int x = 4;
				for (; x < 10; ++x)
					assertTrue(rs.next());
				assertFalse(rs.next());
				
				rs.close();
				
				//Compare to expected sparse info
//				expected = new ArrayList<String[]>();

				rs = con.createStatement().executeQuery(
						"SELECT * FROM ATOFMSAtomInfoSparse ORDER BY AtomID, PeakLocation");

				assertTrue(rs.next());
				assertEquals(1,           rs.getInt(1));
				assertEquals(-98.0,       rs.getDouble(2), 1e-10);
				assertEquals(10842,       rs.getInt(3));
				assertEquals(0.48205948,  rs.getDouble(4), 1e-5);
				assertEquals(875,         rs.getInt(5));

				assertTrue(rs.next());
				assertEquals(1,         rs.getInt(1));
				assertEquals(39.0,      rs.getDouble(2), 1e-10);
				assertEquals(26017,     rs.getInt(3));
				assertEquals(0.607363,  rs.getDouble(4), 1e-5);
				assertEquals(3525,      rs.getInt(5));

				assertTrue(rs.next());
				assertEquals(2,         rs.getInt(1));
				assertEquals(-96.0,      rs.getDouble(2), 1e-10);
				assertEquals(722,     rs.getInt(3));
				assertEquals(0.6283725,  rs.getDouble(4), 1e-5);
				assertEquals(112,      rs.getInt(5));

				assertTrue(rs.next());
				assertEquals(2,         rs.getInt(1));
				assertEquals(12.0,      rs.getDouble(2), 1e-10);
				assertEquals(5673,     rs.getInt(3));

				assertTrue(rs.next());
				assertEquals(2,         rs.getInt(1));

				assertTrue(rs.next());
				assertEquals(2,         rs.getInt(1));

				assertTrue(rs.next());
				assertEquals(3,         rs.getInt(1));
				assertEquals(-80.0,      rs.getDouble(2), 1e-10);
				assertEquals(10074,     rs.getInt(3));

				assertTrue(rs.next());
				assertEquals(3,         rs.getInt(1));

				assertTrue(rs.next());
				assertEquals(3,         rs.getInt(1));

				for (x =0; x < 18; ++x)
					assertTrue(rs.next());
				assertFalse(rs.next());	
				
				rs.close();
				
				//check InternalAtomOrder
				rs = con.createStatement().executeQuery("SELECT * FROM InternalAtomOrder");
				for (x = 0; x < 10; ++x) {
					assertTrue(rs.next());
					assertEquals(rs.getInt("AtomID"), x + 1);
					assertEquals(rs.getInt("CollectionID"), 2);
	
				}
				
				assertFalse(rs.next());
				
				rs.close();
			}
			catch (SQLException ex) {
				ex.printStackTrace();
				fail("Couldn't analyze success of readSpectraAndCreateParticle");
			}
		}
		catch (Exception e) {
			// If any exception is thrown, test should just fail
			e.printStackTrace();
			fail("Exception thrown.");
		}
		finally {
			String errors = ps.flush();
			System.out.println(errors);
			assertEquals(errors.length(), 0);
			System.setErr(oldErr);
		}

	}


	public void testReadSpectraAndCreateParticleWithMissingParticle()
			throws URISyntaxException, InterruptedException, ParseException, IOException {
		URI testDataURI = ATOFMSDataSetImporterTest.class.getResource("/test-row-missing").toURI();
		Path testDataPath = Paths.get(testDataURI);
		String parFile = testDataPath.resolve("b").resolve("b.par").toString();
		String calFile = testDataPath.resolve("b").resolve("cal.cal").toString();
		table.setValueAt(parFile, 0, 1);   // dataset
		table.setValueAt(calFile, 0, 2); // mass cal file
		table.setValueAt(10, 0, 4);    // Min height
		table.setValueAt(20, 0, 5);       // Min area
		table.setValueAt((float) 0.1, 0, 6);  // Min relative area
		table.setValueAt((float) 0.5, 0, 7);  // Max peak error
		table.setValueAt(true, 0, 8);  // autocal

		table.tableChanged(new TableModelEvent(table, 0));
		//Since exceptions aren't thrown back down the stack by the database,
		//	we need to see if any are thrown
		StringPrintStream ps = new StringPrintStream();
		PrintStream oldErr = System.err;
		System.setErr(ps.getPrintStream());

		//Make sure standard output gets set back.
			try {
				importer.parFile = new File((String) table.getValueAt(0, 1));
				String massCalFile = calFile;
				ATOFMSParticle.currCalInfo = new CalInfo(massCalFile, true);
				ATOFMSParticle.currPeakParams = new PeakParams(10, 20, .1f, .5f);
				importer.numParticles = new int[1];
				importer.numParticles[0] = 10;
				importer.collections = new Collection[1];
				importer.id =
						db.createEmptyCollectionAndDataset("ATOFMS", 0, "b",
														   "comment", "'null', 'null', 10, 10, 0.005, 1");
				importer.progressBar = new ProgressBarWrapper(null, "Progress", 10);
			} catch (java.io.IOException ex) {
				System.out.println(ex.getMessage());
				fail("Couldn't set necessary files to create empty collection");
			}

		importer.readSpectraAndCreateParticle();

	}
}