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
package edu.carleton.enchilada.dataExporters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.Statement;

import javax.swing.JFrame;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.gui.ParticleAnalyzeWindow;
import junit.framework.TestCase;
import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**
 * Tests MSAnalyze export as implemented in MSAnalyzeDataSetExporter
 * We don't really need to test the db parts since that's already tested in
 * DatabaseText.  We only test the file stuff, really.
 * @author jtbigwoo
 */
public class MSAnalyzeDataSetExporterTest extends TestCase {
	MSAnalyzeDataSetExporter exporter;
	Database db;
	File parFile;
	File setFile;
	
	public MSAnalyzeDataSetExporterTest(String s) {
		super(s);
	}
	
	/**
	 * @see TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		new CreateTestDatabase();
		
		db = (Database) Database.getDatabase("TestDB");
		if (! db.openConnection()) {
			throw new Exception("Couldn't open DB con");
		}
		JFrame mf = new JFrame();
		final ProgressBarWrapper progressBar = 
			new ProgressBarWrapper(mf, "Exporting to MS-Analyze", 100);
		exporter = new MSAnalyzeDataSetExporter(mf, db, progressBar);
	}

	public void testNormalExport() {
		try {
			// Get DB file set up
			Path tempDir = Files.createTempDirectory("access-test");
			tempDir.toFile().deleteOnExit();

			Path accessDataPath = tempDir.resolve("sample-ms-analyze.mdb");
			Files.copy(ParticleAnalyzeWindow.class.getResourceAsStream("/sample-ms-analyze.mdb"),
					   accessDataPath, StandardCopyOption.REPLACE_EXISTING);
			accessDataPath.toFile().deleteOnExit();
			System.out.println(accessDataPath);


			boolean result;
			Collection coll = db.getCollection(2);
			parFile = tempDir.resolve("access-test.par").toFile();
			result = exporter.exportToPar(coll, parFile.getPath(), accessDataPath.toString());
			assertTrue("Failure during exportToPar in a normal export", result);
			
			// check the par file
			assertTrue("Export did not write par file", parFile.exists());
			try (var reader = new BufferedReader(new FileReader(parFile))) {
				assertEquals("First line of .par file is wrong", "ATOFMS data set parameters", reader.readLine());
				String fileName = (new File(parFile.getName().replaceAll("\\.par$", ""))).getName();
				assertEquals("Second line of .par file did not match name of par file", fileName, reader.readLine());
				assertEquals("Third line of .par file had the wrong date", "09/02/2003 17:30:38", reader.readLine());
				assertEquals("Fourth line of .par file didn't match collection comment", coll.getComment(), reader.readLine());
			}

			// check the set file.  set file logic is simpler so just check the first line
			setFile = new File(parFile.getPath().replaceAll("\\.par$", ".set"));
			System.out.println(setFile.getAbsolutePath());
			assertTrue("Export did not write set file", setFile.exists());
			try (var reader = new BufferedReader(new FileReader(setFile))) {

				// The actual file name contains the full path of the filename. Current code trims off the first three
				// characters of the filename when exporting because it trims off the C:\, since MS-Analyze wants
				// a relative path. That C:\ isn't present in the test filename, but it is in the real one.
				assertEquals("1,..\\..\\..\\..\\..\\..\\..\\..\\..\\..\\ticle1, " +
						"1, 65535, 1.000000, 09/02/2003 17:30:38",
						reader.readLine());
			}

			// Try a second time to make sure deleting the first one (in db) works
			result = exporter.exportToPar(coll, parFile.getPath(), accessDataPath.toString());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Caught Exception in testNormalExport");
		}
	}
	
	/**
	 * This is a test for bug 1951538.  If the export routine blew up and left
	 * some temp tables, it wouldn't work a subsequent time.
	 */
	public void testFailureBeforeSuccess()
	{
		try {
			// Get DB file set up
			Path tempDir = Files.createTempDirectory("access-test");
			tempDir.toFile().deleteOnExit();

			Path accessDataPath = tempDir.resolve("sample-ms-analyze.mdb");
			Files.copy(ParticleAnalyzeWindow.class.getResourceAsStream("/sample-ms-analyze.mdb"),
					   accessDataPath, StandardCopyOption.REPLACE_EXISTING);
			accessDataPath.toFile().deleteOnExit();
			System.out.println(accessDataPath);

			boolean result;
			
			Connection con = db.getCon();
			try (Statement stmt = con.createStatement()) {

				// leave a temp table lying around
				// this is what happens when the routine blows up trying to write
				// to the access db
				stmt.executeUpdate("CREATE TABLE temp.ParticlesToExport (AtomID INT " +
						"PRIMARY KEY, Filename TEXT, [Time] DATETIME, [Size] FLOAT, " +
						"LaserPower FLOAT, NumPeaks INT, TotalPosIntegral INT, " +
						"TotalNegIntegral INT)");
			}

			Collection coll = db.getCollection(2);
			parFile = File.createTempFile("test", ".par");
			result = exporter.exportToPar(coll, parFile.getPath(), accessDataPath.toString());
			assertTrue("Failure during exportToPar in a normal export", result);

			// make sure it wrote the par file and the set file
			assertTrue("Export did not write par file", parFile.exists());
			setFile = new File(parFile.getPath().replaceAll("\\.par$", ".set"));
			assertTrue("Export did not write set file", setFile.exists());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			fail("Caught Exception in testNormalExport");
		}
	}
	
	public void tearDown()
	{
		db.closeConnection();
		if (parFile != null)
			assertTrue(parFile.delete());
		if (setFile != null) {
			assertTrue(setFile.delete());
		}
	}
}
