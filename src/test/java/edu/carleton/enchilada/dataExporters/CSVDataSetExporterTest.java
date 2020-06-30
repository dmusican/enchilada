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
 * Tom Bigwood tom.bigwood@nevelex.com
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
package edu.carleton.enchilada.dataExporters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;

import javax.swing.JFrame;

import edu.carleton.enchilada.collection.Collection;

import junit.framework.TestCase;
import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**
 * Tests CSV export as implemented in CSVDataSetExporter
 * We don't really need to test the db parts since that's already tested in
 * DatabaseTest.  We only test the file stuff, really.
 * 2009-03-12
 * @author jtbigwoo
 */
@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
public class CSVDataSetExporterTest extends TestCase {
	CSVDataSetExporter exporter;
	Database db;
	File csvFile, secondCsvFile;
	
	public CSVDataSetExporterTest(String s) {
		super(s);
	}
	
	/**
	 * @see TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		new CreateTestDatabase();
		
		db = (Database) Database.getDatabase("TestDB");
		if (! db.openConnection("TestDB")) {
			throw new Exception("Couldn't open DB con");
		}
		JFrame mf = new JFrame();
		final ProgressBarWrapper progressBar = 
			new ProgressBarWrapper(mf, "Exporting to CSV", 100);
		//progressBar.constructThis();
		//final JFrame frameRef = frame;
		//final ATOFMSBatchTableModel aRef = a;
		exporter = new CSVDataSetExporter(mf, db, progressBar);
	}

	public void testCollectionExport() throws Exception {
		boolean result;
		Collection coll = db.getCollection(2);
		csvFile = File.createTempFile("test", ".csv");
		result = exporter.exportToCSV(coll, csvFile.getPath(), 30);
		assertTrue("Failure during exportToCSV in a normal export", result);
		
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));

		StringBuilder expectedParticleLine = new StringBuilder();
		StringBuilder expectedSpectrumLine = new StringBuilder();
		for (int i=1; i <= 5; i++) {
			expectedParticleLine.append("****** Particle: particle" + i + " ******,,");
			expectedSpectrumLine.append("Negative Spectrum,,");
		}
		assertEquals(expectedParticleLine.toString(), reader.readLine());
		assertEquals(expectedSpectrumLine.toString(), reader.readLine());
		assertEquals("-30,0.00,-30,15.00,-30,15.00,-30,15.00,-30,15.00,", reader.readLine());
		assertEquals("-29,0.00,-29,0.00,-29,0.00,-29,0.00,-29,0.00,", reader.readLine());
		for (int i = 0; i < 28; i++)
			reader.readLine();
		assertEquals("Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,", reader.readLine());
		assertEquals("0,0.00,0,0.00,0,0.00,0,0.00,0,0.00,", reader.readLine());
		for (int i = 0; i < 30; i++)
			reader.readLine();
		assertNull(reader.readLine());
	}
	
	public void testCollectionExportSmallMZ() throws Exception {
		boolean result;
		Collection coll = db.getCollection(2);
		csvFile = File.createTempFile("test", ".csv");
		result = exporter.exportToCSV(coll, csvFile.getPath(), 1);
		assertTrue("Failure during exportToCSV in a normal export", result);
		
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));

		StringBuilder expectedParticleLine = new StringBuilder();
		StringBuilder expectedSpectrumLine = new StringBuilder();
		for (int i=1; i <= 5; i++) {
			expectedParticleLine.append("****** Particle: particle" + i + " ******,,");
			expectedSpectrumLine.append("Negative Spectrum,,");
		}

		assertEquals(expectedParticleLine.toString(), reader.readLine());
		assertEquals(expectedSpectrumLine.toString(), reader.readLine());
		assertEquals("-1,0.00,-1,0.00,-1,0.00,-1,0.00,-1,0.00,", reader.readLine());
		assertEquals("Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,", reader.readLine());
		assertEquals("0,0.00,0,0.00,0,0.00,0,0.00,0,0.00,", reader.readLine());
		assertEquals("1,0.00,1,0.00,1,0.00,1,0.00,1,0.00,", reader.readLine());
		assertNull(reader.readLine());
	}
	
	public void testBigCollectionExport() throws Exception {
		boolean result;
		// let's make a big collection
		Connection con = db.getCon();
		Statement stmt = con.createStatement();
		for (int i = 12; i < 140; i++)
		{
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (" + i + ",'2003-09-02 17:30:38'," + i + ",0." + i + "," + i + ",'Orig file')\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2," + i + ")\n");
			stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES(" + i + ",2)\n");
		}
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(139,-30,15,0.006,12)");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(139,-20,15,0.006,12)");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(139,0,15,0.006,12)");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(139,20,15,0.006,12)");
		Collection coll = db.getCollection(2);
		csvFile = File.createTempFile("test", ".csv");
		result = exporter.exportToCSV(coll, csvFile.getPath(), 30);
		assertTrue("Failure during exportToCSV in a normal export", result);
		
		secondCsvFile = new File(csvFile.getPath().replace(".csv", "_1.csv"));
		BufferedReader reader = new BufferedReader(new FileReader(secondCsvFile));
		
		assertEquals("****** Particle: Orig file ******,,****** Particle: Orig file ******,,****** Particle: Orig file ******,,****** Particle: Orig file ******,,****** Particle: Orig file ******,,****** Particle: Orig file ******,,", reader.readLine());
		assertEquals("Negative Spectrum,,Negative Spectrum,,Negative Spectrum,,Negative Spectrum,,Negative Spectrum,,Negative Spectrum,,", reader.readLine());
		assertEquals("-30,0.00,-30,0.00,-30,0.00,-30,0.00,-30,0.00,-30,15.00,", reader.readLine());
		assertEquals("-29,0.00,-29,0.00,-29,0.00,-29,0.00,-29,0.00,-29,0.00,", reader.readLine());
		for (int i = 0; i < 28; i++)
			reader.readLine();
		assertEquals("Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,Positive Spectrum,,", reader.readLine());
		assertEquals("0,0.00,0,0.00,0,0.00,0,0.00,0,0.00,0,15.00,", reader.readLine());
		for (int i = 0; i < 30; i++)
			reader.readLine();
		assertEquals(null, reader.readLine());
	}

	public void testHierarchyExport() throws Exception {
		boolean result;
		Collection coll = db.getCollection(2);
		db.moveCollection(db.getCollection(3), coll);
		csvFile = File.createTempFile("test", ".csv");
		result = exporter.exportHierarchyToCSV(coll, csvFile.getPath(), 30);
		assertTrue("Failure during exportHierarchyToCSV in a normal export", result);
		
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));
		
		assertEquals("****** Collection: One ******,,****** Collection: Two ******,,", reader.readLine());
		assertEquals("Collection ID: 2,,Collection ID: 3,,", reader.readLine());
		assertEquals("Parent Collection ID: 0,,Parent Collection ID: 2,,", reader.readLine());
		assertEquals("Negative Spectrum,,Negative Spectrum,,", reader.readLine());
		assertEquals("-30,0.60,-30,0.63,", reader.readLine());
		assertEquals("-29,0.00,-29,0.00,", reader.readLine());
		for (int i = 0; i < 28; i++)
			reader.readLine();
		assertEquals("Positive Spectrum,,Positive Spectrum,,", reader.readLine());
		assertEquals("0,0.00,0,0.00,", reader.readLine());
		for (int i = 0; i < 30; i++)
			reader.readLine();
		assertEquals(null, reader.readLine());
		for (int i = 0; i < 30; i++)
			reader.readLine();
		assertEquals(null, reader.readLine());
	}

	public void tearDown()
	{
		if (csvFile != null) csvFile.delete();
		if (secondCsvFile != null) secondCsvFile.delete();
		db.closeConnection();
	}
}
