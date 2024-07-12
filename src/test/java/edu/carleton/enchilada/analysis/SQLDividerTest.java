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
 * The Original Code is EDAM Enchilada's KMeans unit test.
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


package edu.carleton.enchilada.analysis;

import edu.carleton.enchilada.ATOFMS.Peak;
import edu.carleton.enchilada.analysis.clustering.Cluster;
import edu.carleton.enchilada.analysis.clustering.ClusterQuery;
import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.NoSubCollectionException;
import junit.framework.TestCase;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/*
 * Created August 19, 2008
 * 
 * 
 * This does not test clustering after attempt to cluster around
 * an empty particle. 
 */


public class SQLDividerTest extends TestCase {
	
	private ClusterQuery qc;
	private Database db;
	String dbName = "testDB";
    Float d = 0.5f;
    int cID = 2;
	private Path tempDir;
	private Path par1;
	private Path par2;
	private Path par3;

	
    /*
     * @see TestCase#setUp()
     */
	protected void setUp() throws Exception {
        super.setUp();
        
        
        new CreateTestDatabase();
		db = Database.getDatabase("TestDB");
		db.openConnection();

		tempDir = Files.createTempDirectory("enchilada-testClust-q");
		par1 = tempDir.resolve("par1.txt");
		par2 = tempDir.resolve("par2.txt");
		par3 = tempDir.resolve("par3.txt");

		PrintWriter pw;
		pw = new PrintWriter(par1.toString());
		pw.println(db.getATOFMSFileName(2));
		ArrayList<Peak> peaks = db.getPeaks(db.getAtomDatatype(2), 2);

		for (Peak peak : peaks) {
			pw.println(peak.massToCharge + "," + peak.value);
		}
		pw.close();

		pw = new PrintWriter(par2.toString());
		pw.println(db.getATOFMSFileName(3));

		peaks = db.getPeaks(db.getAtomDatatype(3), 3);

		for (Peak peak : peaks) {
			pw.println(peak.massToCharge + "," + peak.value);
		}
		pw.close();

		pw = new PrintWriter(par3.toString());
		pw.println("particle11111");

		pw.println("-98.0,13.0");
		pw.println("-97.0,20709.0");
		pw.println("-62.0,19187.0");
		pw.println("-46.0,23734.0");

		pw.println("12.0,2852.0");
		pw.println("15.0,777.0");
		pw.println("18.0,84.0");

		pw.close();

	}
	
	protected void tearDown() throws Exception
	{
		super.tearDown();
		db.closeConnection();
		System.runFinalization();
		System.gc();
	    Database.dropDatabase(dbName);
	    assertTrue(par1.toFile().delete());
	    assertTrue(par2.toFile().delete());
		assertTrue(par3.toFile().delete());
	    assertTrue(tempDir.toFile().delete());
	}
		
	public void testKeepAll() throws SQLException {
		final SQLDivider sqld = new SQLDivider(cID, db, "Divider Test", "a test", "1==1");
		sqld.setCursorType(CollectionDivider.DISK_BASED);
		int newHostId = sqld.divide();

		try (Statement stmt = db.getCon().createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT Name FROM Collections WHERE CollectionId = " + newHostId)) {
			if (rs.next()) {
				String collectionName = rs.getString("Name");
				assertEquals("Divider Test", collectionName);
			} else {
				fail("Did not find new collection.");
			}
		}

		// There should be no more collections than newHostId+1, since start at 0 and it was the last one
		try (Statement stmt = db.getCon().createStatement();
			 ResultSet rs = stmt.executeQuery(
					 "SELECT COUNT(*) AS total FROM Collections")) {
			if (rs.next()) {
				int total = rs.getInt("total");
				assertEquals(newHostId+1, total);
			} else {
				fail("Did not get total.");
			}
		}


	}

}
	
	


