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
 * The Original Code is EDAM Enchilada's SQLServerDatabase unit test class.
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
 * Created on Jul 29, 2004
 *
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package edu.carleton.enchilada.database;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.gui.ProgressBarWrapper;
import junit.framework.TestCase;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.MatchResult;

import javax.swing.JDialog;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database.BPLOnlyCursor;
import edu.carleton.enchilada.database.Database.MemoryBinnedCursor;

import edu.carleton.enchilada.ATOFMS.ATOFMSParticle;
import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.ATOFMS.CalInfo;
import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.ATOFMS.Peak;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.SubSampleCursor;
import edu.carleton.enchilada.atom.ATOFMSAtomFromDB;
import junit.runner.Version;
import org.junit.Test;
import org.sqlite.SQLiteException;

/**
 * Tests database class
 *
 */
public class DatabaseTest extends TestCase {
	private Database db;
	private String dbName = "TestDB";
	private DateFormat df = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

	public DatabaseTest(String aString)
	{
		
		super(aString);
	}
	
	protected void setUp()
	{
		new CreateTestDatabase(); 		
		db = (Database) Database.getDatabase(dbName);
	}
	
	protected void tearDown() throws Exception
	{
		db.closeConnection();
		System.runFinalization();
		System.gc();
//		db.dropDatabaseCommands();
	}

	/**
	 * Assert that the Runnable passed throws an exception
	 * @param r the Runnable to invoke run() on. Note: not invoked in a separate thread.
	 * @author shaferia
	 */
	public static void assertException(Runnable r) {
		try {
			r.run();
			fail("Test should have thrown exception.");
		}
		catch (Exception ex) {
			//test passed.
		}
	}
	
	public void testOpenandCloseConnection() {
		assertTrue(db.openConnection());
		assertTrue(db.closeConnection());
	}

	public void testGetImmediateSubcollections() {
	
		
		db.openConnection(dbName);
		
		ArrayList<Integer> test = db.getImmediateSubCollections(db.getCollection(0));

		assertEquals(4, test.size());
		assertEquals(2, test.get(0).intValue());
		assertEquals(3, test.get(1).intValue());
		assertEquals(4, test.get(2).intValue());
		assertEquals(5, test.get(3).intValue());
		
		ArrayList<Integer> collections = new ArrayList<>();
		collections.add(0);
		collections.add(3);
		test = db.getImmediateSubCollections(collections);
		assertEquals(4, test.size());
		assertEquals(2, test.get(0).intValue());
		assertEquals(3, test.get(1).intValue());
		assertEquals(4, test.get(2).intValue());
		assertEquals(5, test.get(3).intValue());
		
		db.closeConnection();
	}
	
	
	public void testCreateEmptyCollectionAndDataset() {
		db.openConnection(dbName);
		
		int[] ids = db.createEmptyCollectionAndDataset("ATOFMS", 0,
				"dataset",  "comment", "'mCalFile', 'sCalFile', 12, 20, 0.005, 0");
		
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = con.createStatement().executeQuery(
					"SELECT *\n" +
					"FROM ATOFMSDataSetInfo\n" +
					"WHERE DataSetID = " + ids[1]);
			assertTrue(rs.next());
			assertEquals("dataset", rs.getString(2));
			assertEquals("mCalFile", rs.getString(3));
			assertEquals("sCalFile", rs.getString(4));
			assertEquals(12, rs.getInt(5));
			assertEquals(20, rs.getInt(6));
			assertTrue(Math.abs(rs.getFloat(7) - (float)0.005) <= 0.00001);
			assertFalse(rs.next());
			rs = stmt.executeQuery(
					"SELECT * FROM Collections\n" +
					"WHERE CollectionID = " + ids[0]);
			rs.next();
			assertEquals("dataset", rs.getString("Name"));
			assertEquals("comment", rs.getString("Comment"));
			assertFalse(rs.next());
			rs = stmt.executeQuery(
					"SELECT ParentID FROM CollectionRelationships\n" +
					"WHERE ChildID = " + ids[0]);
			assertTrue(rs.next());
			assertEquals(0, rs.getInt(1));
			assertFalse(rs.next());
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.closeConnection();
	}

	public void testCreateEmptyCollection() {
		db.openConnection(dbName);
		int collectionID = db.createEmptyCollection("ATOFMS", 0,"Collection",  "collection","");
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT Name, Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + collectionID);
			assertTrue(rs.next());
			assertEquals("Collection", rs.getString(1));
			assertEquals("collection", rs.getString(2));
			assertFalse(rs.next());
			
			rs = stmt.executeQuery(
					"SELECT ParentID\n" +
					"FROM CollectionRelationships\n" +
					"WHERE ChildID = " + collectionID);
			
			assertTrue(rs.next());
			assertEquals(0, rs.getInt(1));
			assertFalse(rs.next());
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.closeConnection();
	}
	
	public void testRenameCollection()
	{
		db.openConnection(dbName);
		int collectionID = db.createEmptyCollection("ATOFMS", 0,"Collection",  "collection","");
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT Name, Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + collectionID);
			assertTrue(rs.next());
			assertEquals("Collection", rs.getString(1));
			//Checks to see if the internal collection object has the right name initially.			
			assertEquals("Collection", db.getCollection(collectionID).getName());
			//Call the change method
			db.renameCollection(db.getCollection(collectionID), "Collection2");
			//Checks to see if the sql database has the right name after renaming.
			rs = stmt.executeQuery(
					"SELECT Name, Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + collectionID);
			assertTrue(rs.next());
			assertEquals("Collection2", rs.getString(1));
			//Checks to see if the internal collection object has the right name after renaming.
			assertEquals("Collection2", db.getCollection(collectionID).getName());
			rs.close();
			stmt.close();
		}
		catch(SQLException e){
		}
		System.out.println(collectionID);
		db.closeConnection();
	}
	
	/**
	 * @author steinbel
	 * Tests updateInteralAtomOrder() by manually creating sub-collections
	 * then calling the update method.
	 * @throws SQLException 
	 */
	public void testUpdateInternalAtomOrder() throws SQLException{
		String manual = "INSERT INTO Collections VALUES "+
			"(7,'Seven', 'seven', 'sevendescrip', 'ATOFMS')";
		db.openConnection();
		Connection con = db.getCon();
		Statement stmt = con.createStatement();
		stmt.addBatch(manual);
		manual = "DELETE FROM CollectionRelationships WHERE "
			+ "ChildID = 2 OR ChildID = 3";
		stmt.addBatch(manual);
		manual = "INSERT INTO CollectionRelationships VALUES(7,2)"
			+ "INSERT INTO CollectionRelationships VALUES(7,3)";
		stmt.addBatch(manual);
		stmt.executeBatch();
		
		//now update IAO
		db.updateInternalAtomOrder(db.getCollection(7));
		
		//and assert correct insertion
		ResultSet rs = stmt.executeQuery("SELECT * FROM InternalAtomOrder "
				+ "WHERE CollectionID = 7");
		for (int i=1; i<11; i++){
			rs.next();
			assertEquals(rs.getInt(1), i);
		}
		rs.close();
		stmt.close();
		db.closeConnection();
				
	}
	
	/**
	 * @author steinbel
	 * Manually give children to an existing collection, then test.
	 * @throws SQLException 
	 */
	public void testUpdateAncestors() throws SQLException{
		/*
		 * setup collection 7 (atoms 22, 23, 24) and 8 (atoms 25, 26, 27)
		 * as children of 2
		 */
		db.openConnection(dbName);
		Connection con = db.getCon();
		Statement stmt = con.createStatement();
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7, 22)");
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7, 23)");
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7, 24)");
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (8, 25)");
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (8, 26)");
		stmt.executeUpdate("INSERT INTO AtomMembership VALUES (8, 27)");

		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (22, 7)");
		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (23, 7)");
		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (24, 7)");
		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (25, 8)");
		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (26, 8)");
		stmt.executeUpdate("INSERT INTO InternalAtomOrder VALUES (27, 8)");


		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (22,'2003-09-02 17:30:38',22,0.22,22,'tweTwo')");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (23,'2003-09-02 17:30:38',23,0.23,23,'tweThree')");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (24,'2003-09-02 17:30:38',24,0.24,24,'tweFour')");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (25,'2003-09-02 17:30:38',25,0.25,25,'tweFive')");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (26,'2003-09-02 17:30:38',26,0.26,26,'tweSix')");
		stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoDense VALUES (27,'2003-09-02 17:30:38',27,0.27,27,'tweSeven')");

		stmt.executeUpdate("INSERT INTO Collections VALUES (7,'Seven', 'seven', 'sevdescrip', 'ATOFMS')");
		stmt.executeUpdate("INSERT INTO Collections VALUES (8,'Eight', 'eight', 'eightdescrip', 'ATOFMS')");

		stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES (2, 7)");
		 stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES (2, 8)");

		/* Call updateAncestors and check results.*/
		db.updateAncestors(db.getCollection(7));
		
		ResultSet rs = stmt.executeQuery("SELECT * FROM InternalAtomOrder "
				+ "WHERE CollectionID = 2 ORDER BY AtomID");
		
		//test for original particles in collection 2
		for (int i=1; i<6; i++){
			assertTrue(rs.next());
			System.out.println(i + " was i and atomid = " + rs.getInt(1));//TESTING
			assertEquals(rs.getInt(1), i);
		}
			
		//test for particles from children collections 7 and 8
		for (int i=22; i<28; i++){
			assertTrue(rs.next());
			System.out.println(i + " was i and atomid = " + rs.getInt(1));//TESTING
			assertEquals(rs.getInt(1), i);
		}
			
		
		db.closeConnection();
	}

	/**
	 * Copies CollectionID = 3 to CollectionID = 2
	 *
	 */
	public void testCopyCollection() {
		db.openConnection(dbName);
		
		int newLocation = db.copyCollection(db.getCollection(3),db.getCollection(2));
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			
			ResultSet rs = stmt.executeQuery(
					"\n" +
					"SELECT Name, Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = 3");
			Statement stmt2 = con.createStatement();
			ResultSet rs2 = stmt2.executeQuery(
					"\n" +
					"SELECT Name, Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + newLocation);
			assertTrue(rs.next());
			assertTrue(rs2.next());
			assertEquals(rs.getString(1), rs2.getString(1));
			assertEquals(rs.getString(2), rs2.getString(2));
			assertFalse(rs.next());
			assertFalse(rs2.next());
			rs.close();
			rs2.close();
			
			rs = stmt.executeQuery(
					"\n" +
					"SELECT ParentID\n" +
					"FROM CollectionRelationships\n" +
					"WHERE ChildID = " + newLocation);
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertFalse(rs.next());
			rs.close();
			rs = stmt.executeQuery(
					"\n" +
					"SELECT AtomID\n" +
					"FROM AtomMembership\n" +
					"WHERE CollectionID = 3\n" +
					"ORDER BY AtomID");
			rs2 = stmt2.executeQuery(
					"\n" +
					"SELECT AtomID\n" +
					"FROM AtomMembership\n" +
					"WHERE CollectionID = " + newLocation +
					"ORDER BY AtomID");
			while (rs.next())
			{
				assertTrue(rs2.next());
				assertEquals(rs.getInt(1), rs2.getInt(1));
			}
			assertFalse(rs2.next());
			rs = stmt.executeQuery(" SELECT DISTINCT AtomID FROM AtomMembership WHERE " +
					"CollectionID = "+newLocation+" OR CollectionID = 2 ORDER BY AtomID");
			rs2 = stmt2.executeQuery(" SELECT AtomID FROM InternalAtomOrder WHERE " +
					"CollectionID = 2 ORDER BY AtomID");
			while (rs.next())
			{
				assertTrue(rs2.next());
				assertEquals(rs.getInt(1), rs2.getInt(1));
			}
			assertFalse(rs2.next());	
			rs.close();
			stmt.close();
			rs2.close();
			stmt2.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//test to make sure that a collection can't be copied to itself
		//this should print an exception message.
		System.out.println("Two exceptions about copying a collection into itself" +
				" should follow.");
		assertEquals(-1, db.copyCollection(db.getCollection(0), db.getCollection(0)));
		assertEquals(-1, db.copyCollection(db.getCollection(2), db.getCollection(2)));
		
		db.closeConnection();
	}

	public void testMoveCollection() {
		db.openConnection(dbName);
		assertTrue(db.moveCollection(db.getCollection(3),db.getCollection(2)));
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			Statement stmt2 = con.createStatement();
			
			ResultSet rs = stmt.executeQuery(
					"\n" +
					"SELECT ParentID\n" +
					"FROM CollectionRelationships\n" +
					"WHERE ChildID = 3");
			
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertFalse(rs.next());

			rs = stmt.executeQuery(" SELECT AtomID FROM AtomMembership " +
					"WHERE CollectionID = 2 OR CollectionID = 3 ORDER BY AtomID");
			ResultSet rs2 = stmt2.executeQuery(" SELECT AtomID FROM InternalAtomOrder" +
					" WHERE CollectionID = 2 ORDER BY AtomID");
			while (rs.next())
			{
				assertTrue(rs2.next());
				assertEquals(rs.getInt(1), rs2.getInt(1));
			}
			assertFalse(rs2.next());
			db.closeConnection();
			rs.close();
			stmt.close();
			rs2.close();
			stmt2.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.closeConnection();
	}

	/**
	 * Tests the fix of bug #####
	 * @author jtbigwoo 
	 */
	public void testMoveCollectionTwice() {
		db.openConnection(dbName);
		assertTrue(db.moveCollection(db.getCollection(4), db.getCollection(3)));
		assertTrue(db.moveCollection(db.getCollection(3),db.getCollection(2)));
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			Statement stmt2 = con.createStatement();
			
			ResultSet rs = stmt.executeQuery(
					"SELECT ParentID\n" +
					"FROM CollectionRelationships\n" +
					"WHERE ChildID in (3,4) " +
					"ORDER BY ChildID");
			
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertTrue(rs.next());
			assertEquals(3, rs.getInt(1));
			assertFalse(rs.next());

			rs = stmt.executeQuery("SELECT AtomID FROM AtomMembership " +
					"WHERE CollectionID in (2, 3, 4) ORDER BY AtomID");
			ResultSet rs2 = stmt2.executeQuery(" SELECT AtomID FROM InternalAtomOrder" +
					" WHERE CollectionID = 2 ORDER BY AtomID");
			while (rs.next())
			{
				assertTrue(rs2.next()); // if this fails, there are more rows in atom membership than InternalAtomOrder
				assertEquals(rs.getInt(1), rs2.getInt(1));
			}
			assertFalse(rs2.next());
			db.closeConnection();
			rs.close();
			stmt.close();
			rs2.close();
			stmt2.close();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		}
		db.closeConnection();
	}

	public void testInsertATOFMSParticle() {
		db.openConnection(dbName);
		final String filename = "'ThisFile'";
		final String dateString = "'1983-01-19 05:05:00.0'";
		final float laserPower = (float)0.01191983;
		final float size = (float)0.5;
		final float digitRate = (float)0.1;
		final int scatterDelay = 10;
		
		ATOFMSParticle.currCalInfo = new CalInfo();
//		ATOFMSParticle.currPeakParams = new PeakParams(12,20,(float)0.005);
		
		
		int posPeakLocation1 = 19;
		int negPeakLocation1 = -20;
		int peak1Height = 80;
		int posPeakLocation2 = 100;
		int negPeakLocation2 = -101;
		int peak2Height = 100;
		
		ArrayList<ATOFMSPeak> sparseData = new ArrayList<>();
		sparseData.add(new ATOFMSPeak(peak1Height,  peak1Height, 0.1f, posPeakLocation1));
		sparseData.add(new ATOFMSPeak(peak1Height,  peak1Height, 0.1f, negPeakLocation1));
		sparseData.add(new ATOFMSPeak(peak2Height,  peak2Height, 0.1f, posPeakLocation2));
		sparseData.add(new ATOFMSPeak(peak2Height,  peak2Height, 0.1f, negPeakLocation2));

		int collectionID, datasetID;
		collectionID = 2;
		datasetID = db.getNextID();
		int particleID = db.insertParticle(dateString + "," + laserPower + "," + digitRate + ","	
				+ scatterDelay + ", " + filename, sparseData, db.getCollection(collectionID),datasetID,db.getNextID());

		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			
			ResultSet rs = stmt.executeQuery(
					"\n" +
					"SELECT PeakLocation, PeakArea, RelPeakArea," +
					" PeakHeight\n" +
					"FROM ATOFMSAtomInfoSparse \n" +
					"WHERE AtomID = " + particleID + "\n" +
					"ORDER BY PeakLocation ASC");
			
			assertTrue(rs.next());

			assertEquals(rs.getFloat(1), (float) negPeakLocation2);
			assertEquals(rs.getInt(2), peak2Height);
			assertEquals(rs.getFloat(3), (float) 0.1);
			assertEquals(rs.getInt(4), peak2Height);
			
			assertTrue(rs.next());

			assertEquals(rs.getFloat(1), (float) negPeakLocation1);
			assertEquals(rs.getInt(2), peak1Height);
			assertEquals(rs.getFloat(3), (float) 0.1);
			assertEquals(rs.getInt(4), peak1Height);
			
			assertTrue(rs.next());

			assertEquals(rs.getFloat(1), (float) posPeakLocation1);
			assertEquals(rs.getInt(2), peak1Height);
			assertEquals(rs.getFloat(3), (float) 0.1);
			assertEquals(rs.getInt(4), peak1Height);
			
			assertTrue(rs.next());

			assertEquals(rs.getFloat(1), (float) posPeakLocation2);
			assertEquals(rs.getInt(2), peak2Height);
			assertEquals(rs.getFloat(3), (float) 0.1);
			assertEquals(rs.getInt(4), peak2Height);
			
			rs = stmt.executeQuery(
					"\n" +
					"SELECT [Time], LaserPower, [Size], ScatDelay, " +
					"OrigFilename\n" +
					"FROM ATOFMSAtomInfoDense \n" +
					"WHERE AtomID = " + particleID);
			rs.next();
			assertEquals(rs.getString(1), dateString.substring(1, dateString.length() - 1));
			assertEquals(rs.getFloat(2), laserPower);
			assertEquals(rs.getFloat(3), digitRate); // size
			assertEquals(rs.getInt(4), scatterDelay);
			assertEquals(rs.getString(5), filename.substring(1, filename.length() - 1));
			
			rs = stmt.executeQuery(
					"\n" +
					"SELECT CollectionID\n" +
					"FROM AtomMembership\n" +
					"WHERE AtomID = " + particleID);
			rs.next();

			assertEquals(rs.getInt(1), collectionID);
			
			rs = stmt.executeQuery(
					"\n" +
					"SELECT OrigDataSetID\n" +
					"FROM DataSetMembers \n" +
					"WHERE AtomID = " + particleID);
			
			rs.next();
			assertEquals(rs.getInt(1), datasetID);
			db.closeConnection();
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void testGetNextId(){
		db.openConnection();
		
		assertTrue(db.getNextID() >= 0);
	
		db.closeConnection();
	}
	
	public void testOrphanAndAdopt(){
		
		db.openConnection(dbName);
		//Insert 5,21 into the database to tell if an error occurs when an item
		//is present in a parent and its child
		try {
			Connection con1 = db.getCon();
			Statement stmt1 = con1.createStatement();
			String query = "\n" +
				"INSERT INTO AtomMembership VALUES(5,21)\n";
			System.out.println(query);
			stmt1.executeUpdate(query);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		assertTrue(db.orphanAndAdopt(db.getCollection(6)));
		//make sure that the atoms collected before are in collection 4
		ArrayList<Integer> collection5Info = new ArrayList<Integer>();
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			Statement stmt2 = con.createStatement();
			
			ResultSet rs = stmt.executeQuery(" SELECT * FROM InternalAtomOrder WHERE" +
			" CollectionID = 6");
			assertFalse(rs.next());
	
			rs = stmt.executeQuery("\n" +
			"SELECT AtomID\n" +
			"FROM AtomMembership\n" +
			"WHERE CollectionID = 5 ORDER BY AtomID");

			ResultSet rs2 = stmt2.executeQuery(" SELECT AtomID" +
					" FROM InternalAtomOrder WHERE CollectionID = 5 ORDER BY AtomID");
			
			int count = 0;
			while (rs.next()) {
				assertTrue(rs2.next());
				assertEquals(rs.getInt(1), rs2.getInt(1));
				count++;
			}
			assertFalse(rs2.next());
			assertEquals(6, count);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// removed an assert false here - changed the code to give an error
		// if a collectionID is passed that isn't really a collection in the db.
		try {
			db.orphanAndAdopt(db.getCollection(2));
			fail("Is not a subcollection");
		} catch (RuntimeException e) {
			// test should throw an exception
		}
		
		//Make sure orphan and adopt can't be performed on either root
		assertFalse(db.orphanAndAdopt(db.getCollection(0)));
		assertFalse(db.orphanAndAdopt(db.getCollection(1)));
		
		db.closeConnection();
	}

	public void testRecursiveDelete() throws SQLException {
		db.openConnection(dbName);


		ArrayList<Integer> atomIDs = new ArrayList<Integer>();
		ResultSet rs;
		Statement stmt;

		Connection con = db.getCon();
		stmt = con.createStatement();


		//Store a copy of all the relevant tables with just the things that should be left after deletion
		// AKA Figure out what the database should look like after deletion

		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp0;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp0 (AtomID INT);\n");

		//collect a list of AtomID that should be deleted
		stmt.executeUpdate("INSERT INTO temp.temp0 (AtomID) \n" +
				"SELECT AtomID\n"
				+" FROM AtomMembership\n"
				+" WHERE CollectionID = 5 OR CollectionID = 6;\n");

		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp1;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp1 (AtomID INT);\n");
		//ATOFMSAtomInfoDense
		stmt.executeUpdate("INSERT INTO temp.temp1 (AtomID) \n" +
				"SELECT AtomID FROM ATOFMSAtomInfoDense\n"
				+ " WHERE NOT AtomID IN\n"
				+ " 	(SELECT *\n"
				+ " 	FROM temp.temp0)\n;\n");


		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp2;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp2 (AtomID INT, CollectionID INT);\n");
		//AtomMembership
		stmt.executeUpdate("INSERT INTO temp.temp2 (AtomID, CollectionID) \n" +
				"SELECT AtomID, CollectionID FROM AtomMembership\n"
				+ " WHERE NOT AtomID IN\n"
				+ " 	(SELECT *\n"
				+ " 	FROM temp.temp0)\n;\n");

		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp3;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp3 (AtomID INT, PeakLocation INT);\n");
		//ATOFMSAtomInfoSparse
		stmt.executeUpdate("INSERT INTO temp.temp3 (AtomID, PeakLocation) \n" +
				"SELECT AtomID, PeakLocation FROM ATOFMSAtomInfoSparse\n"
				+ " WHERE NOT AtomID IN\n"
				+ " 	(SELECT *\n"
				+ " 	FROM temp.temp0)\n;\n");

		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp4;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp4 (AtomID INT, CollectionID INT);\n");
		//InternalAtomOrder
		stmt.executeUpdate("INSERT INTO temp.temp4 (AtomID, CollectionID) \n" +
				"SELECT AtomID, CollectionID FROM InternalAtomOrder\n"
				+ " WHERE NOT AtomID IN\n"
				+ " 	(SELECT *\n"
				+ " 	FROM temp.temp0)\n;\n");


		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp5;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp5 (ParentID INT, ChildID INT);\n");
		//CollectionRelationships
		stmt.executeUpdate("INSERT INTO temp.temp5 (ParentID, ChildID) \n" +
				"SELECT ParentID, ChildID FROM CollectionRelationships\n"
				+ " WHERE NOT (ChildID = 5"
				+ " OR ParentID = 5);\n");

		stmt.executeUpdate("DROP TABLE IF EXISTS temp.temp6;\n");
		stmt.executeUpdate("CREATE TEMPORARY TABLE temp6 (CollectionID INT);\n");
		//Collections
		stmt.executeUpdate("INSERT INTO temp.temp6 (CollectionID) \n" +
				"SELECT CollectionID FROM Collections\n"
				+ " WHERE NOT (CollectionID = 5 OR CollectionID = 6);\n");


		assertTrue(db.recursiveDelete(db.getCollection(5)));


		//Check to make sure that the database is as it should be
		//Both that information that should be gone is gone
		// and that no information was deleted that shouldn't have been

		//InternalAtomOrder
		rs = stmt.executeQuery(
				"SELECT AtomID, CollectionID FROM InternalAtomOrder X\n"
						+ " WHERE NOT EXISTS\n"
						+ " 	(SELECT *\n"
						+ " 	FROM temp.temp4 Y\n"
						+ "		WHERE X.CollectionID = Y.CollectionID AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp4 X\n"
						+ " WHERE NOT EXISTS\n"
						+ "		(SELECT AtomID, CollectionID FROM InternalAtomOrder Y\n"
						+ "		WHERE X.CollectionID = Y.CollectionID AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());

		//CollectionRelationships
		rs = stmt.executeQuery(
				"SELECT ParentID, ChildID FROM CollectionRelationships X\n"
						+ " WHERE NOT EXISTS (SELECT * FROM temp.temp5 Y\n"
						+ "						WHERE X.ParentID = Y.ParentID AND X.ChildID = Y.ChildID);\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp5 X\n"
						+ " WHERE NOT EXISTS\n"
						+ "		(SELECT ParentID, ChildID FROM CollectionRelationships Y\n"
						+ "						WHERE X.ParentID = Y.ParentID AND X.ChildID = Y.ChildID);\n");
		assertFalse(rs.next());

		//Collections
		rs = stmt.executeQuery(
				"SELECT * FROM Collections\n"
						+ " WHERE NOT CollectionID IN\n"
						+ " 	(SELECT CollectionID\n"
						+ " 	FROM temp.temp6)\n;\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp6 X\n"
						+ " WHERE NOT X.CollectionID IN\n"
						+ "		(SELECT CollectionID FROM Collections);\n");
		assertFalse(rs.next());

		final ProgressBarWrapper progressBar =
				new ProgressBarWrapper(null, "Compacting Database",100);
		progressBar.constructThis();
		progressBar.setIndeterminate(true);
		progressBar.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
		progressBar.setVisible(false);

		assertTrue(db.compactDatabase(progressBar));


		//ATOFMSAtomInfoDense
		rs = stmt.executeQuery(
				"SELECT AtomID FROM ATOFMSAtomInfoDense\n"
						+ " WHERE NOT AtomID IN\n"
						+ " 	(SELECT *\n"
						+ " 	FROM temp.temp1)\n;\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp1\n"
						+ " WHERE NOT AtomID IN\n"
						+ "(SELECT AtomID FROM ATOFMSAtomInfoDense);\n");
		assertFalse(rs.next());

		//AtomMembership
		rs = stmt.executeQuery(
				"SELECT AtomID, CollectionID FROM AtomMembership X\n"
						+ " WHERE NOT EXISTS\n"
						+ " 	(SELECT *\n FROM temp.temp2 Y\n"
						+ "		WHERE X.CollectionID = Y.CollectionID AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp2 X\n"
						+ " WHERE NOT EXISTS\n"
						+ "		(SELECT AtomID, CollectionID FROM AtomMembership Y\n"
						+ "		WHERE X.CollectionID = Y.CollectionID AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());

		//ATOFMSAtomInfoSparse
		rs = stmt.executeQuery(
				"SELECT AtomID, PeakLocation FROM ATOFMSAtomInfoSparse X\n"
						+ " WHERE NOT EXISTS\n"
						+ " 	(SELECT *\n"
						+ " 	FROM temp.temp3 Y\n"
						+ "		WHERE X.PeakLocation = Y.PeakLocation AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());
		rs = stmt.executeQuery(
				"SELECT * FROM temp.temp3 X\n"
						+ " WHERE NOT EXISTS\n"
						+ "		(SELECT * FROM ATOFMSAtomInfoSparse Y\n"
						+ "		WHERE X.PeakLocation = Y.PeakLocation AND X.AtomID = Y.AtomID);\n");
		assertFalse(rs.next());


		stmt.execute("DROP TABLE temp.temp0;\n");
		stmt.execute("DROP TABLE temp.temp1;\n");
		stmt.execute("DROP TABLE temp.temp2;\n");
		stmt.execute("DROP TABLE temp.temp3;\n");
		stmt.execute("DROP TABLE temp.temp4;\n");
		stmt.execute("DROP TABLE temp.temp5;\n");
		stmt.execute("DROP TABLE temp.temp6;\n");


		db.closeConnection();

	}
	
	public void testGetCollectionName(){
		db.openConnection(dbName);

		assertEquals("One", db.getCollectionName(2));
		assertEquals("Two", db.getCollectionName(3));
		assertEquals("Three", db.getCollectionName(4));
		assertEquals("Four", db.getCollectionName(5));
		
		db.closeConnection();
	}
	
	public void testGetCollectionComment(){
		db.openConnection(dbName);

		assertEquals("one", db.getCollectionComment(2));
		assertEquals("two", db.getCollectionComment(3));
		assertEquals("three", db.getCollectionComment(4));
		assertEquals("four", db.getCollectionComment(5));
		
		db.closeConnection();
	}
	
	public void testGetCollectionSize(){
		db.openConnection(dbName);
		
		final String filename = "'FirstFile'";
		final String dateString = "'1983-01-19 05:05:00.0'";
		final float laserPower = (float)0.01191983;
		final float size = (float)5;
		final float digitRate = (float)0.1;
		final int scatterDelay = 10;
		ArrayList<ATOFMSPeak> sparseData = new ArrayList<>();
		int collectionID = 2;
		int datasetID = 1;
		System.out.println(db.getCollectionSize(collectionID));
		assertEquals(5, db.getCollectionSize(collectionID));
		
		db.insertParticle(dateString + "," + laserPower + "," + digitRate + ","	
				+ scatterDelay + ", " + filename, sparseData, db.getCollection(collectionID),datasetID,db.getNextID()+1);
		System.out.println(db.getCollectionSize(collectionID));
		assertEquals(6, db.getCollectionSize(collectionID));
			
		db.closeConnection();
	}

	public void testGetAllDescendedAtoms(){
		db.openConnection(dbName);
		
//		case of one child collection
		int[] expected = {16,17,18,19,20,21};
		ArrayList<Integer> actual = db.getAllDescendedAtoms(db.getCollection(5));
		
		for (int i=0; i<actual.size(); i++)
			assertEquals((int) actual.get(i), expected[i]);
		
//		case of no child collections
		int[] expected2 = {1,2,3,4,5};
		actual = db.getAllDescendedAtoms(db.getCollection(2));
		
		for (int i=0; i<actual.size(); i++)
			assertEquals((int) actual.get(i), expected2[i]);
		
		db.closeConnection();
	}

	/** Depreciated 12/05 - AR
	public void testGetCollectionParticles(){
		db.openConnection();
		
		//we know the particle info from inserting it	
		ArrayList<GeneralAtomFromDB> general = db.getCollectionParticles(db.getCollection(2));
		
		assertEquals(general.size(), 5);
		ATOFMSAtomFromDB actual = general.get(0).toATOFMSAtom();
		assertEquals(actual.getAtomID(), 1);
//		assertEquals(actual.getLaserPower(), 1);  not retrieved in method
		assertEquals(actual.getSize(), (float)0.1);
//		assertEquals(actualgetScatDelay(), 1);	not retrieved in method
		assertEquals(actual.getFilename(), "One");
			
		db.closeConnection();
	}
	*/
	
	public void testRebuildDatabase() {
		db.closeConnection();
		assertTrue(Database.rebuildDatabase(dbName));
		db.openConnection();

		InfoWarehouse mainDB = Database.getDatabase();
		mainDB.openConnection();
		assertTrue(mainDB.isPresent());
		mainDB.closeConnection();
	}



	public void testIsPresent() {
		db.openConnection(dbName);
		InfoWarehouse db = Database.getDatabase(dbName);
		assertTrue(db.isPresent());
		db = Database.getDatabase("shouldntexist");
		assertFalse(db.isPresent());
		
		//uses default database
		db = Database.getDatabase();
		assertTrue(db.isPresent());
		db.closeConnection();
	}
	

	public void ignoreTestExportToMSAnalyzeDatabase() {
		db.openConnection(dbName);
		final ProgressBarWrapper progressBar = 
			new ProgressBarWrapper(null, "Exporting to MS-Analyze",100);
		progressBar.constructThis();
		progressBar.setIndeterminate(true);
		progressBar.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
		progressBar.setVisible(false);
		java.util.Date date = db.exportToMSAnalyzeDatabase(db.getCollection(2),"MSAnalyzeDB","MS-Analyze", null, progressBar);
		db.closeConnection();
		assertEquals("Tue Sep 02 17:30:38 CDT 2003", date.toString());
	}
	
	
	public void testMoveAtom() {
		db.openConnection();
		assertTrue(db.moveAtom(1,1,2));
		assertTrue(db.moveAtom(1,2,1));
		db.closeConnection();
	}
	
	/*public void testMoveAtomBatch() {
		db.openConnection();
		db.atomBatchInit();
		assertTrue(db.moveAtomBatch(1,1,2));
		assertTrue(db.moveAtomBatch(1,2,1));
		db.atomBatchExecute();
		db.closeConnection();
	}*/
	
	/**
	 * Tests AddAtom and DeleteAtomBatch
	 *
	 */
	public void testAddAndDeleteAtom() {
		db.openConnection(dbName);
		db.atomBatchInit();
		assertTrue(db.deleteAtomBatch(1,db.getCollection(1)));
		db.atomBatchExecute();
		assertTrue(db.addAtom(1,1));
		db.closeConnection();		
	}
	
	/**
	 * Tests AddAtomBatch and DeleteAtomsBatch
	 *
	 */
	public void testAddAndDeleteAtomBatch() {
		db.openConnection(dbName);
		db.atomBatchInit();
		assertTrue(db.deleteAtomsBatch("1",db.getCollection(1)));
		assertTrue(db.addAtomBatch(1,1));
		db.atomBatchExecute();
		db.closeConnection();
	}
	
	/**
	 * Tests deleteAtomsBatch, specifically the case where it blows 
	 * memory because of an oversized queue.
	 * 
	 * @author benzaids
	 */
//	public void testBatchMemoryProblem() {
//		try {
//			db.openConnection(dbName);
//			db.atomBatchInit();
//			Random rand = new Random();
//			for (int i = 0; i < 100000; i++) {
//				int atom = rand.nextInt(1000000) + 1;
//				assertTrue(db.deleteAtomBatch(atom, db.getCollection(1)));
//			}
//			db.atomBatchExecute();
//			db.closeConnection();
//			
//		}
//		catch (Exception e){
//			System.out.println("Too many queries in a result blew memory, as expected.");
//			//The test passed.
//		}
//	}
	
	public void testCheckAtomParent() {
		db.openConnection(dbName);
		assertTrue(db.checkAtomParent(1,2));
		assertFalse(db.checkAtomParent(1,4));
		db.closeConnection();
	}
	
	public void testGetAndSetCollectionDescription() {
		db.openConnection(dbName);
		String description = db.getCollectionDescription(2);
		assertTrue(db.setCollectionDescription(db.getCollection(2),"new description"));
		assertEquals("new description", db.getCollectionDescription(2));
		db.setCollectionDescription(db.getCollection(2),description);
		db.closeConnection();
	}
	
	/* Can't try dropping db because it's in use.
	public void testDropDatabase() {
		db.openConnection();
		assertTrue(Database.dropDatabase(dbName));
		setUp();
	} */
	
	public void testGetPeaks() {
		db.openConnection(dbName);
		Peak peak = db.getPeaks("ATOFMS",2).get(0);
		assertEquals(15, ((ATOFMSPeak) peak).area);
		assertEquals(0.006f, ((ATOFMSPeak) peak).relArea);
		assertEquals(12, ((ATOFMSPeak) peak).height);
		db.closeConnection();
	}

/*
	public void testInsertGeneralParticles() {
		db.openConnection();
		 int[] pSpect = {1,2,3};
		 int[] nSpect = {1,2,3};
		EnchiladaDataPoint part = new EnchiladaDataPoint("newpart");
		ArrayList<EnchiladaDataPoint> array = new ArrayList<EnchiladaDataPoint>();
		array.add(part);
		int id = db.insertGeneralParticles(array,1);
		assertTrue(db.checkAtomParent(id,1));
		db.atomBatchInit();
		db.deleteAtomBatch(id,1);
		db.executeBatch();
		db.closeConnection();
		}
*/
	public void testGetAtomDatatype() {
		db.openConnection(dbName);
		assertEquals("ATOFMS", db.getAtomDatatype(2));
		assertEquals("Datatype2", db.getAtomDatatype(18));
		db.closeConnection();
	}
	
	// TODO: ERROR WITH SEED RANDOM!! DON'T KNOW WHY - AR
	public void testSeedRandom()
	{
	    db.openConnection(dbName);
	    //db.getNumber();
	    db.seedRandom(12345);
	    double rand1 = db.getNumber();
	    db.getNumber();
	    db.getNumber();
	    
	    System.out.println();
	    db.seedRandom(12345);
	    double rand2 = db.getNumber();
	    db.getNumber();
	    db.getNumber();
	    
	    System.out.println();
	    db.seedRandom(12345);
	    db.getNumber();
	    db.getNumber();
	    db.getNumber();

		assertEquals(rand1, rand2);
	    db.closeConnection();
	    
	}
	public void testGetParticleInfoOnlyCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getAtomInfoOnlyCursor(db.getCollection(2));
		testCursor(curs);
		db.closeConnection();
	}
	
	public void testGetSQLAtomIDCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getSQLCursor(db.getCollection(2), "ATOFMSAtomInfoDense.AtomID != 50");
		testCursor(curs);

		// jtbigwoo - need to test sql atom id cursor to make sure we can get 
		// the atom id's even when the id's are not sequential
		Statement stmt = null;
		Connection con = db.getCon();
		try
		{
			stmt = con.createStatement();
			stmt.executeUpdate("INSERT INTO Collections VALUES (7, 'Seven', 'seven', 'sevendescrip', 'ATOFMS')\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,1)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,3)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,5)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,7)\n");
			stmt.executeUpdate("INSERT INTO InternalAtomOrder (AtomID, CollectionID) SELECT AtomID, CollectionID FROM AtomMembership WHERE CollectionID = 7");
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			fail("Failed to insert new collection to test getSQLAtomIDCursor");
		}
		curs = db.getSQLAtomIDCursor(db.getCollection(7), "rownum >= 1 and rownum <= 3");
		// https://github.com/dmusican/enchilada/issues/1
		// curs = db.getSQLAtomIDCursor(db.getCollection(7), "rownum >= 2 and rownum <= 4");
		for (int i = 3; i < 8; i = i + 2)
		{
			assertTrue(curs.next());
			assertNotNull(curs.getCurrent());
			assertEquals(i, curs.getCurrent().getID());
		}
		assertFalse(curs.next());	
		curs.next();

		db.closeConnection();
	}
	
	public void testGetSQLCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getSQLCursor(db.getCollection(2), "ATOFMSAtomInfoDense.AtomID != 20");
		testCursor(curs);
		db.closeConnection();
	}
	
	public void testGetPeakCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getPeakCursor(db.getCollection(2));
		testCursor(curs);
		db.closeConnection();
	}
	
	public void testGetBinnedCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getBinnedCursor(db.getCollection(2));
		testCursor(curs);
		db.closeConnection();
	}
	public void testBPLOnlyCursor() throws Exception {
		db.openConnection(dbName);
		BPLOnlyCursor curs;
		try {
			curs = db.getBPLOnlyCursor(db.getCollection(2));
			testCursor(curs);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db.closeConnection();
	}
	public void testGetMemoryBinnedCursor() {
		db.openConnection(dbName);
		Collection c = db.getCollection(2);
		MemoryBinnedCursor curs = db.getMemoryBinnedCursor(c);
		testCursor(curs);	
		db.closeConnection();
	}
	
	public void testGetRandomizedCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = db.getRandomizedCursor(db.getCollection(2));	
		testCursor(curs);	
		db.closeConnection();
	}

	// SubSampleCursor is actually located in the analysis package, but
	// it's much more convenient to test it here since this is where the
	// other cursor tests are.
	public void testSubSampleCursor() {
		db.openConnection(dbName);
		CollectionCursor curs = new SubSampleCursor(
				db.getRandomizedCursor(db.getCollection(2)),0,5);	
		testCursor(curs);	
		db.closeConnection();
	}

	
	
	public void testSubSampleCursor2() throws Exception {
		db.openConnection(dbName);
		CollectionCursor curs = new SubSampleCursor(
				db.getRandomizedCursor(db.getCollection(2)),0,10);	

		ArrayList<ParticleInfo> partInfo = new ArrayList<ParticleInfo>();

		ParticleInfo temp = new ParticleInfo();
		ATOFMSAtomFromDB tempPI = 
			new ATOFMSAtomFromDB(
					1,"One",1,0.1f,
					df.parse("9/2/2003 5:30:38 PM"), 0);
		//int aID, String fname, int sDelay, float lPower, Date tStamp, float size
		temp.setParticleInfo(tempPI);
		temp.setID(1);

		partInfo.add(temp);

		for (int i = 0; i < 5; i++)
		{
			assertTrue(curs.next());
			assertNotNull(curs.getCurrent());
		}
		assertFalse(curs.next());	
		curs.reset();
	}	
	
	
	private void testCursor(BPLOnlyCursor curs) throws Exception
	{
		ArrayList<ParticleInfo> partInfo = new ArrayList<ParticleInfo>();
		
		ParticleInfo temp = new ParticleInfo();
		ATOFMSAtomFromDB tempPI = 
			new ATOFMSAtomFromDB(
					1,"One",1,0.1f,
					df.parse("9/2/2003 5:30:38 PM"), 0);
		//int aID, String fname, int sDelay, float lPower, Date tStamp, float size
		temp.setParticleInfo(tempPI);
		temp.setID(1);

		partInfo.add(temp);
		
		int[] ids = new int[4];
		for (int i = 0; i < 4; i++)
		{
			//assertTrue(curs.getCurrent()!= null);
			assertTrue(curs.next());
			ParticleInfo p = curs.getCurrent();
			ids[i] = p.getID();
			BinnedPeakList b = p.getBinnedList();
			System.out.println(ids[i]);
			System.out.println(b.getPeaks());
		}
		assertFalse(curs.next());	
		curs.reset();

		
		for (int i = 0; i < 4; i++)
		{
			assertTrue(curs.next());
			assertEquals(curs.getCurrent().getID(), ids[i]);
		}
		
		assertFalse(curs.next());
			curs.reset();

	}
	
	private void testCursor(CollectionCursor curs)
	{
		ArrayList<ParticleInfo> partInfo = new ArrayList<ParticleInfo>();
		ParticleInfo temp = new ParticleInfo();
		ATOFMSAtomFromDB tempPI = null;
		try {
		tempPI = 
			new ATOFMSAtomFromDB(
					1,"One",1,0.1f,
					df.parse("9/2/2003 5:30:38 PM"), 0);
		} catch (ParseException pe) {fail("Programmer should have put in a better date");}
		//int aID, String fname, int sDelay, float lPower, Date tStamp, float size
		temp.setParticleInfo(tempPI);
		temp.setID(1);

		partInfo.add(temp);

		int[] ids = new int[5];
		for (int i = 0; i < 5; i++)
		{
			assertTrue(curs.next());
			assertNotNull(curs.getCurrent());
			ids[i] = curs.getCurrent().getID();
		}
		assertFalse(curs.next());	
		curs.reset();
		
		for (int i = 0; i < 5; i++)
		{
			assertTrue(curs.next());
			assertNotNull(curs.getCurrent());
			assertEquals(curs.getCurrent().getID(), ids[i]);
		}
		
		assertFalse(curs.next());
		curs.reset();
		
		for (int i = 0; i < 5; i++)
		{
			assertTrue(curs.next());
			assertNotNull(curs.getCurrent());
			assertEquals(curs.getCurrent().getID(), ids[i]);
		}
		
		assertFalse(curs.next());
		curs.reset();
	}

	/**
	 * @author shaferia
	 */
	public void testJoin()
	{
		int[] intsraw = {1, 2, 3, 4, 5};
		ArrayList<Integer> ints = new ArrayList<Integer>();
		for (int i : intsraw)
			ints.add(i);
		ArrayList<String> strings = new ArrayList<String>();
		for (int i : intsraw)
			strings.add(i + "");
		
		ArrayList<Object> mixed = new ArrayList<Object>();
		mixed.add(new Integer(2));
		mixed.add("hey");
		mixed.add(new Float(2.0));
		
		assertEquals(SQLServerDatabase.join(ints, ","), "1,2,3,4,5");
		assertEquals(SQLServerDatabase.join(ints, ""), "12345");
		assertEquals(SQLServerDatabase.join(new ArrayList<String>(), ","), "");
		assertEquals(SQLServerDatabase.join(ints, "-"), SQLServerDatabase.join(strings, "-"));
		assertEquals(SQLServerDatabase.join(strings, ",,"), "1,,2,,3,,4,,5");
		assertEquals(SQLServerDatabase.join(mixed, "."), "2.hey.2.0");
		
		ArrayList<Integer> oneint = new ArrayList<Integer>();
		oneint.add(new Integer(2));
		
		assertEquals(SQLServerDatabase.join(oneint, ","), "2");
	}

	/**
	 * Use java.util.Foramtter to create a formatted string
	 * @param format the format specification
	 * @param args variables to format
	 * @return the formatted string
	 * @author shaferia
	 */
	private String sprintf(String format, Object... args) {
		return (new java.util.Formatter().format(format, args)).toString();
	}
	
	/**
	 * Print a justified text table of a database table
	 * @param name the database table to output
	 * @author shaferia
	 */
	public static void printDBSection(InfoWarehouse database, String name) {
		printDBSection(database, name, Integer.MAX_VALUE);
	}
	
	/**
	 * Print a justified text table of a database table. Requires an open connection to db.
	 * @param name the database table to output
	 * @param rows a single argument giving the maximum number of rows to output
	 * @author shaferia
	 */
	public static void printDBSection(InfoWarehouse database, String name, int rows) {
		Statement stmt = null;
		ResultSet rs = null;
		
		Connection con = database.getCon();
		try	{
			if (con == null) {
				database.openConnection();
				con = database.getCon();
			}
			
			stmt = con.createStatement();
			rs = stmt.executeQuery(
					";\n" +
					"SELECT * FROM " + name);
			java.sql.ResultSetMetaData rsmd = rs.getMetaData();
			
			ArrayList<String[]> data = new ArrayList<String[]>();
			int colcount = rsmd.getColumnCount();
			int[] cwidth = new int[colcount];
			
			data.add(new String[colcount]);
			for (int i = 0; i < colcount; ++i) {
				data.get(0)[i] = rsmd.getColumnName(i + 1);
				cwidth[i] = data.get(0)[i].length();
			}
			
			for (int i = 1; rs.next() && (i < rows); ++i){
				data.add(new String[colcount]);
				for (int j = 0; j < colcount; ++j) {
					data.get(i)[j] = rs.getObject(j + 1).toString();
					cwidth[j] = Math.max(cwidth[j], data.get(i)[j].length());
				}
			}

			for (int i = 0; i < data.size(); ++i) {
				for (int j = 0; j < data.get(i).length; ++j) {
					System.out.printf(
									"%-" + cwidth[j] + "." + cwidth[j] + "s | ", data.get(i)[j]);
				}
				System.out.println();
			}

			stmt.close();
			rs.close();
		}
		catch (SQLException ex) {
			System.err.println("Couldn't print database section.");
			ex.printStackTrace();
		}
		finally {
			database.closeConnection();
		}
	}

	/**
	 * @author shaferia
	 */
	public void testAddCenterAtom() {	
		db.openConnection(dbName);
		
		assertTrue(db.addCenterAtom(2, 3));
		
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM CenterAtoms WHERE AtomID = 2");
			
			assertTrue(rs.next());
			assertEquals(rs.getInt("AtomID"), 2);
			assertEquals(rs.getInt("CollectionID"), 3);
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		assertTrue(db.addCenterAtom(3, 4));
		
		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM CenterAtoms ORDER BY CollectionID");
			
			assertTrue(rs.next());
			assertEquals(rs.getInt("AtomID"), 2);
			assertEquals(rs.getInt("CollectionID"), 3);
			
			assertTrue(rs.next());
			assertEquals(rs.getInt("AtomID"), 3);
			assertEquals(rs.getInt("CollectionID"), 4);
			
			assertFalse(rs.next());
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		System.out.println("Three primary key constraint errors to follow.");
		//this should print an exception message
		assertFalse(db.addCenterAtom(3, 4));;
		
		//this should print an exception message
		assertFalse(db.addCenterAtom(2, 5));
		
		//this should print an exception message
		assertFalse(db.addCenterAtom(5, 4));
		
		db.closeConnection();
	}

	/**
	 * @author steinbel - got rid of IAO.OrderNumber
	 * @author shaferia
	 * @throws SQLException 
	 */
	public void testAddSingleInternalAtomToTable() throws SQLException {
		db.openConnection(dbName);
		
		//test adding to end
		db.addSingleInternalAtomToTable(6, 2);

	
		Connection con = db.getCon();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(
				"SELECT * FROM InternalAtomOrder WHERE AtomID > 4 " +
				"AND AtomID <8 ORDER BY CollectionID, AtomID");
		
		//since ordernumber isn't relevant anymore, instead checking to make
		//sure that each atomID = 6 got into the correct collection
		rs.next();
		assertEquals(rs.getInt(2), 2);
		assertEquals(rs.getInt(1), 5);
		rs.next();
		assertEquals(rs.getInt(2), 2);
		assertEquals(rs.getInt(1), 6);
		rs.next();
		assertEquals(rs.getInt(2), 3);
		assertEquals(rs.getInt(1), 6);
		rs.next();
		assertEquals(rs.getInt(2), 3);
		assertEquals(rs.getInt(1), 7);
	
	
		
		
		db.atomBatchInit();
		db.deleteAtomsBatch("'1','2','3','4','5'", db.getCollection(2));
		db.atomBatchExecute();
		db.updateInternalAtomOrder(db.getCollection(2));

		//addSingleInternalAtomToTable should order things correctly - make sure that happens
		db.addSingleInternalAtomToTable(4, 2);
		db.addSingleInternalAtomToTable(3, 2);
		db.addSingleInternalAtomToTable(1, 2);
		db.addSingleInternalAtomToTable(5, 3);
		db.addSingleInternalAtomToTable(2, 2);

		try {
			con = db.getCon();
			stmt = con.createStatement();
			rs = stmt.executeQuery(
					"SELECT * FROM InternalAtomOrder WHERE CollectionID = 2");
			
			for (int i = 0; rs.next(); ++i) {
				assertEquals(rs.getInt(1), i + 1);
			}
			assertFalse(rs.next());
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		db.closeConnection();	
	}
	
	/**
	 * @author shaferia
	 */
	public void testAggregateColumn() {
		db.openConnection(dbName);
		
		int[] intsraw = {1,2,3,4,5};
		ArrayList<Integer> ints = new ArrayList<Integer>();
		for (int i : intsraw)
			ints.add(i);
		
		assertEquals(db.aggregateColumn(
				DynamicTable.AtomInfoDense, 
				"AtomID", 
				ints, 
				"ATOFMS"), "15.0");
		
		assertEquals(db.aggregateColumn(
				DynamicTable.AtomInfoDense, 
				"Size", 
				ints, 
				"ATOFMS").substring(0, 7), "0.15000");
		
		ints.remove(0);
		assertEquals(db.aggregateColumn(
				DynamicTable.AtomInfoSparse, 
				"PeakHeight", 
				ints, 
				"ATOFMS"), 12*(2+3+4+5) + ".0");	
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testCreateIndex() {
		db.openConnection(dbName);
		
		assertTrue(db.createIndex("ATOFMS", "Size, LaserPower"));
		assertTrue(db.createIndex("ATOFMS", "Size, Time"));

		try {
			db.createIndex("ATOFMS", "Size, LaserPower");
			fail("Should have been an exception, there are already indices.");
		} catch (ExceptionAdapter e) {
			// test succeeds if no
		}

		try {
			db.createIndex("ATOFMS", "size, time");
			fail("Should have been an exception, there are already indices.");
		} catch (ExceptionAdapter e) {
			// test succeeds if no
		}

		try {
			Connection con = db.getCon();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT * FROM ATOFMSAtomInfoDense WHERE Size = 0.02");
			
			assertTrue(rs.next());
			assertTrue(rs.getFloat("LaserPower") < 2.0001 && rs.getFloat("LaserPower") > 1.9999);
			assertFalse(rs.next());
			
			rs.close();
			stmt.close();
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testExportImportDatabase() {
		db.openConnection(dbName);
		
		System.out.printf("Current working directory is %s\n", System.getProperty("user.dir"));
		
		try {
			//db.exportDatabase("test1.out", 1);
			//java.io.File f = new java.io.File("test1.out");
			//assertTrue(f.exists());
			
			//TODO: Determine status of [(export)(import)]Database
			/*
			tearDown();
			db = Database.getDatabase();
	        try {
				Database.rebuildDatabase(dbName);
			} catch (SQLException e2) {
				e2.printStackTrace();
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
						"  Close any other programs that may be accessing the database and try again.");
			}
			db.openConnection();
			db.importDatabase("test1.out", 1);
			db.exportDatabase("test2.out", 1);
			*/
			
			//f.delete();
		}
		catch (Exception ex) {
			System.out.println("Exception handling file.");
			ex.printStackTrace();
			fail();
		}
		
		db.closeConnection();
	}

	/**
	 * @author shaferia
	 */
// COMMENTED OUT BECAUSE WE DON'T USE getAdjacentAtomInCollection ANYWHERE AND IT DOESN'T WORK CORRECTLY ANYWAY
//	public void testGetAdjacentAtomInCollection() {
//		db.openConnection(dbName);
//		
//		int[] adj = db.getAdjacentAtomInCollection(2, 3, 1);
//		assertEquals(adj[0], 4);
//		assertEquals(adj[1], 4);
//		
//		adj = db.getAdjacentAtomInCollection(3, 7, -1);
//		assertEquals(adj[0], 6);
//		assertEquals(adj[1], 1);
//		
//		adj = db.getAdjacentAtomInCollection(4, 12, 2);
//		assertEquals(adj[0], 13);
//		assertEquals(adj[1], 3);
//		
//		//The following two assertions should print SQLExceptions.
//		
//		adj = db.getAdjacentAtomInCollection(2, 1, -1);
//		assertTrue((adj[0] < 0) && (adj[1] < 0));
//		
//		adj = db.getAdjacentAtomInCollection(2, 5, 1);
//		assertTrue((adj[0] < 0) && (adj[1] < 0));
//		
//		db.closeConnection();
//	}
	
	/**
	 * @author shaferia
	 */
	public void testGetATOFMSFileName() {
		db.openConnection(dbName);

		assertEquals(db.getATOFMSFileName(1), "particle1");
		assertEquals(db.getATOFMSFileName(11), "particle11");
		
		//for non-ATOFMS data - these assertions should print SQLExceptions.
		System.out.println("Three exceptions about being unable to find a filename follow.");
		try {
			db.getATOFMSFileName(12);
			fail("Should have gotten exception");
		} catch (ExceptionAdapter e) {
			assertTrue(e.originalException instanceof SQLException);
			// Test is good
		}

		try {
			db.getATOFMSFileName(15);
			fail("Should have gotten exception");
		} catch (ExceptionAdapter e) {
			assertTrue(e.originalException instanceof SQLException);
			// Test is good
		}

		try {
			db.getATOFMSFileName(22);
			fail("Should have gotten exception");
		} catch (ExceptionAdapter e) {
			assertTrue(e.originalException instanceof SQLException);
			// Test is good
		}

		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetCollectionDatatype() {
		db.openConnection(dbName);
		
		String[] expectedDatatypes = {"ATOFMS", "ATOFMS", "Datatype2", "Datatype2", "Datatype2"};
		for (int i = 0; i < expectedDatatypes.length; ++i)
			assertEquals(db.getCollectionDatatype(i + 2), expectedDatatypes[i]);
		
		//Should print an SQLException
		System.out.println("Exception getting datatype follows.");
		assertEquals(db.getCollectionDatatype(8), "");
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetCollectionIDsWithAtoms() {
		db.openConnection(dbName);

		ArrayList<Integer> colls = new ArrayList<Integer>();
		for (int i = 0; i < 20; ++i)
			colls.add(i);
		
		ArrayList<Integer> ids = db.getCollectionIDsWithAtoms(colls);

		assertEquals(5, ids.size());
		assertEquals((int) ids.get(0), 2);
		assertEquals((int) ids.get(1), 3);
		assertEquals((int) ids.get(2), 4);
		assertEquals((int) ids.get(3), 5);
		assertEquals((int) ids.get(4), 6);
		
		colls = new ArrayList<Integer>();
		assertEquals(0, db.getCollectionIDsWithAtoms(colls).size());
		
		colls.add(-1);
		assertEquals(0, db.getCollectionIDsWithAtoms(colls).size());
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetColNames() {
		db.openConnection(dbName);
		
		//Test Metadata given by database (from ResultSetMetaData) 
		//	against hardcoded MetaData in database
		for (String type : db.getKnownDatatypes()) {
			for (DynamicTable dynt : DynamicTable.values()) {
				ArrayList<String> names = db.getColNames(type, dynt);
				
				try	{
					Connection con = db.getCon();
					Statement stmt = con.createStatement();
					ResultSet rs = stmt.executeQuery(
							";\n" +
							"SELECT * FROM " + db.getDynamicTableName(dynt, type));
					java.sql.ResultSetMetaData rsmd = rs.getMetaData();
					
					for (int i = 1; i < rsmd.getColumnCount() + 1; ++i) {
						assertTrue(names.get(i - 1).equalsIgnoreCase(
								rsmd.getColumnName(i)));
					}
				}
				catch (SQLException ex) {
					//this isn't an error.
					System.out.printf("Couldn't test database MetaData for Table %s %s" +
							" - table doesn't exist.\n", type, dynt);
				}
			}
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetColNamesAndTypes() {
		db.openConnection(dbName);

		//ugh... java.sql.Type isn't an enum.
		//Manually map the int types in the fake enum to String type values
		java.util.HashMap<Integer, String> typeConv = new java.util.HashMap<Integer, String>();
		typeConv.put(-7, "BIT");
		typeConv.put(4, "INT");
		typeConv.put(7, "REAL");
		typeConv.put(12, "VARCHAR(8000)");
		typeConv.put(93, "DATETIME");
		
		//Test Metadata given by database (from ResultSetMetaData) 
		//	against hardcoded MetaData in database, as in testGetColNames
		for (String type : db.getKnownDatatypes()) {
			for (DynamicTable dynt : DynamicTable.values()) {
				ArrayList<ArrayList<String>> names = db.getColNamesAndTypes(type, dynt);
				
				//System.out.printf("*** %s%s *** \n", type, dynt);
				
				try	{
					Connection con = db.getCon();
					Statement stmt = con.createStatement();
					ResultSet rs = stmt.executeQuery(
							";\n" +
							"SELECT * FROM " + db.getDynamicTableName(dynt, type));
					java.sql.ResultSetMetaData rsmd = rs.getMetaData();

					for (int i = 1; i < rsmd.getColumnCount() + 1; ++i) {
						//System.out.printf("Name: %-20.20s - %-20.20s Type: %-20.20s - %-20.20s\n",
						//		names.get(i - 1).get(0), rsmd.getColumnName(i),
						//		names.get(i - 1).get(1), rsmd.getColumnTypeName(i));
						assertTrue(names.get(i - 1).get(0).equalsIgnoreCase(
								rsmd.getColumnName(i)));
						assertEquals(names.get(i - 1).get(1), typeConv.get(rsmd.getColumnType(i)));
					}
				}
				catch (SQLException ex) {
					//this isn't an error.
					System.out.printf("Couldn't test database MetaData for Table %s %s" +
							" - table doesn't exist.\n", type, dynt);
				}
			}
		}
		
		db.closeConnection();		
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetDatabaseVersion() {
		db.openConnection(dbName);
		
		try {
			//Read version information from the database rebuild file
			Scanner scan = new Scanner(new File("SQLServerRebuildDatabase.txt"));
			scan.findWithinHorizon("INSERT INTO DBInfo VALUES \\('Version','(.*)'\\)", 0);
			MatchResult res = scan.match();
			if (res.groupCount() != 1)
				fail("There should only be one version string in SQLServerRebuildDatabase.txt");
			else {
				String filev = res.group(1);
				assertEquals(db.getVersion(), filev);
			}
				
			scan.close();
		}
		catch (Exception ex) {
			fail();
			ex.printStackTrace();
		}
		
		try {
			db.getCon().createStatement().executeUpdate(
					"UPDATE DBInfo SET Value = 'New!' WHERE Name = 'Version'");
			
			assertEquals(db.getVersion(), "New!");
		}
		catch (Exception ex) {
			fail();
			ex.printStackTrace();
		}
		
		try {
			db.getCon().createStatement().executeUpdate(
					"DELETE FROM DBInfo WHERE Name = 'Version'");
		}
		catch (SQLException ex) {
			System.err.println("Error deleting version while testing");
			ex.printStackTrace();
		}
		
		try {
			db.getVersion();
			
			//shouldn't get this far.
			fail();
		}
		catch (Exception ex) {
			System.out.println("This should be an error: \"" + ex.getMessage() + "\"");
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
//  COMMENTED OUT BECAUSE WE DON'T USE getFirstAtomInCollection() ANYMORE - jtbigwoo
//	public void testGetFirstAtomInCollection() {
//		db.openConnection(dbName);
//		
//		assertEquals(db.getFirstAtomInCollection(db.getCollection(2)), 1);
//		assertEquals(db.getFirstAtomInCollection(db.getCollection(3)), 6);
//		
//		try {
//			db.getCon().createStatement().executeUpdate(
//					"DELETE FROM InternalAtomOrder WHERE CollectionID = 2");
//		}
//		catch (SQLException ex) {
//			ex.printStackTrace();
//		}	
//		
//		assertEquals(db.getFirstAtomInCollection(db.getCollection(2)), -99);
//		
//		try {
//			//rebuild the bit deleted above
//			Statement stmt = db.getCon().createStatement();
//			for (int i = 1; i < 6; ++i)
//				stmt.addBatch("INSERT INTO AtomMembership VALUES (2, " + i + ")");
//		}
//		catch (SQLException ex) {
//			ex.printStackTrace();
//		}		
//		
//		db.closeConnection();
//	}
	
	/**
	 * @author shaferia
	 */
	public void testGetKnownDatatypes() {
		db.openConnection(dbName);
		
		ArrayList<String> types = db.getKnownDatatypes();
		assertTrue(types.contains("AMS"));
		assertTrue(types.contains("ATOFMS"));
		assertTrue(types.contains("TimeSeries"));
		assertFalse(types.contains("timeseries"));
		assertFalse(types.contains("NO EXISTENTE!"));
	
		//getKnownDatatypes uses Java for creating distinctness of datatypes:
		//	see if that's the same result as using SQL SELECT DISTINCT
		try {
			ResultSet rs = db.getCon().createStatement().executeQuery(
					"SELECT DISTINCT Datatype FROM MetaData");
			
			for (int i = 0; rs.next(); ++i){
				assertEquals(types.get(i), rs.getString(1));
			}
			
			rs.close();
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetMaxMinDateInCollections() {
		db.openConnection(dbName);
		
		//test with default data
		Collection[] colls = new Collection[1];
		colls[0] = db.getCollection(2);
		
		java.util.Calendar min = java.util.Calendar.getInstance();
		java.util.Calendar max = java.util.Calendar.getInstance();		
		
		db.getMaxMinDateInCollections(colls, min, max);
		assertNotNull(min);
		assertNotNull(max);
		assertEquals(min.getTime().toString(), max.getTime().toString());

		//put in some more diverse dates
		try {
			String pre = "UPDATE ATOFMSAtomInfoDense SET TIME = '%s' WHERE AtomID = %s";
			Statement stmt = db.getCon().createStatement();

			//CollectionID = 2
			stmt.addBatch(sprintf(pre, "2003-09-01 16:30:38", 1));
			stmt.addBatch(sprintf(pre, "2003-09-12 15:30:38", 2));
			stmt.addBatch(sprintf(pre, "2003-08-04 14:30:38", 3));
			stmt.addBatch(sprintf(pre, "2003-07-04 20:30:38", 5));

			//CollectionID = 3
			stmt.addBatch(sprintf(pre, "2003-10-01 16:30:38", 6));
			stmt.addBatch(sprintf(pre, "2003-08-01 17:30:38", 7));
			stmt.addBatch(sprintf(pre, "2003-11-06 14:30:38", 10));
			
			stmt.executeBatch();

			stmt.close();
		}
		catch (SQLException ex) {
			System.err.println("Couldn't update time values in database");
			ex.printStackTrace();
		}
		
		db.getMaxMinDateInCollections(colls, min, max);
		assertEquals("2003-07-04 20:30:38", TimeUtilities.dateToIso8601(min.getTime()));
		assertEquals("2003-09-12 15:30:38", TimeUtilities.dateToIso8601(max.getTime()));
		
		//try on multiple collections
		colls = new Collection[2];
		colls[0] = db.getCollection(3);
		colls[1] = db.getCollection(2);
		
		db.getMaxMinDateInCollections(colls, min, max);
		
		assertEquals("2003-07-04 20:30:38", TimeUtilities.dateToIso8601(min.getTime()));
		assertEquals("2003-11-06 14:30:38", TimeUtilities.dateToIso8601(max.getTime()));
		
		//try with nulls
		min.setTime(new Date(0));
		max.setTime(new Date(0));

		try {
			String pre = "UPDATE ATOFMSAtomInfoDense SET TIME = NULL WHERE AtomID = ";
			Statement stmt = db.getCon().createStatement();
			for (int i = 1; i <= 11; ++i) {
				stmt.addBatch(pre + i);
			}
			stmt.executeBatch();
			stmt.close();
		}
		catch (SQLException ex) {
			System.err.println("Couldn't update time values in database");
			ex.printStackTrace();
		}

		db.getMaxMinDateInCollections(colls, min, max);
		
		assertEquals(0, min.getTimeInMillis());
		assertEquals(0, max.getTimeInMillis());
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetPrimaryKey() {
		db.openConnection(dbName);
		
		ArrayList<String> ret = db.getPrimaryKey("ATOFMS", DynamicTable.DataSetInfo);
		assertNotNull(ret);
		assertEquals(ret.size(), 0);
		
		ret = db.getPrimaryKey("ATOFMS", DynamicTable.AtomInfoDense);
		assertNotNull(ret);
		assertEquals(ret.size(), 0);
		
		ret = db.getPrimaryKey("ATOFMS", DynamicTable.AtomInfoSparse);
		assertNotNull(ret);
		assertEquals(ret.size(), 1);
		assertTrue(ret.get(0).equalsIgnoreCase("PeakLocation"));
		
		ret = db.getPrimaryKey("AMS", DynamicTable.DataSetInfo);
		assertNotNull(ret);
		assertEquals(ret.size(), 0);
		
		ret = db.getPrimaryKey("AMS", DynamicTable.AtomInfoSparse);
		assertNotNull(ret);
		assertEquals(ret.size(), 1);
		assertTrue(ret.get(0).equalsIgnoreCase("PeakLocation"));
		
		ret = db.getPrimaryKey("Datatype2", DynamicTable.AtomInfoSparse);
		assertNotNull(ret);
		assertEquals(ret.size(), 1);
		assertTrue(ret.get(0).equalsIgnoreCase("Delay"));
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetRepresentedCluster() {
		db.openConnection(dbName);
		db.addCenterAtom(2, 2);
		db.addCenterAtom(6, 3);
	
		assertEquals(db.getRepresentedCluster(2), 2);
		assertEquals(db.getRepresentedCluster(6), 3);
		assertEquals(db.getRepresentedCluster(4), -1);
		assertEquals(db.getRepresentedCluster(3), -1);
		
		db.closeConnection();
	}
	
	/**
	 * Casts an array of objects to an array of uncasted Integers
	 * @param o the array to cast
	 * @return an equally-sized array of ints
	 * @author shaferia
	 */
	private int[] castint(Object[] objs) {
		int[] ret = new int[objs.length];
		
		for (int i = 0; i < ret.length; ++i)
			ret[i] = ((Integer)objs[i]).intValue();
		
		return ret;
	}
	
	/**
	 * @author shaferia
	 */
	public void testGetAllDescendantCollections() {
		db.openConnection(dbName);
		
		java.util.Set<Integer> subcolls = db.getAllDescendantCollections(2, true);		
		assertEquals(subcolls.size(), 1);
		assertEquals(((Integer) (subcolls.toArray()[0])), new Integer(2));
		
		subcolls = db.getAllDescendantCollections(2, false);		
		assertEquals(subcolls.size(), 0);	
		
		db.createEmptyCollection("ATOFMS", 2, "Emptycoll", "Hi!", "");
		
		int[] subids = castint(db.getAllDescendantCollections(2, true).toArray(new Integer[0]));
		assertEquals(subids.length, 2);
		assertEquals(subids[0], 2);
		assertEquals(subids[1], 7);
		
		db.createEmptyCollection("ATOFMS", 7, "Emptycoll2", "", "");
		subcolls = db.getAllDescendantCollections(2, true);
		java.util.Set<Integer> comparer = new java.util.TreeSet<Integer>();
		comparer.add(2);
		comparer.add(7);
		comparer.add(8);
		assertEquals(subcolls, comparer);
		
		db.createEmptyCollection("AMS", 7, "stuff", "", "");
		db.createEmptyCollection("AMS", 8, "two", "", "");

		subcolls = db.getAllDescendantCollections(2, true);
		comparer = new java.util.TreeSet<Integer>();
		comparer.add(2);
		comparer.add(7);
		comparer.add(8);
		comparer.add(9);
		comparer.add(10);
		assertEquals(subcolls, comparer);
		
		db.closeConnection();
	}

	/**
	 * @author shaferia
	 */
	public void testContainsDatatype() {
		db.openConnection(dbName);

		assertTrue(db.containsDatatype("AMS"));
		assertTrue(db.containsDatatype("ATOFMS"));
		assertTrue(db.containsDatatype("Datatype2"));
		assertTrue(db.containsDatatype("SimpleParticle"));
		assertTrue(db.containsDatatype("TimeSeries"));
		
		try {
			//won't happen in reality, but quotes ought to be stripped in general
			db.containsDatatype("'ATOFMS'");
			fail();
		}
		catch (RuntimeException ex) {
			assertEquals(
					"Invalid database reserved characters in querying name while checking for existence of datatype 'ATOFMS'",
					ex.getMessage()
			);
		}
		
		try {
			db.containsDatatype("' OR Datatype = 'AMS");
			fail();
		}
		catch (RuntimeException ex) {
			assertEquals(
					"Invalid database reserved characters in querying name while checking for existence of datatype ' OR Datatype = 'AMS",
					ex.getMessage()
			);
		}

		try {
			db.containsDatatype("''AMS''");
			fail();
		}
		catch (RuntimeException ex) {
			assertEquals(
					"Invalid database reserved characters in querying name while checking for existence of datatype ''AMS''",
					ex.getMessage()
			);
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testBatchExecuter() {
		db.openConnection(dbName);
		
		try {
			Statement stmt = db.getCon().createStatement();
			Database.BatchExecuter ex = db.getBatchExecuter(stmt);
			ex.append("UPDATE DBInfo SET Value = 'fooo!' WHERE Name = 'Version'");
			ex.execute();
			stmt.close();
			
			stmt = db.getCon().createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Value FROM DBInfo WHERE Name = 'Version'");
			assertTrue(rs.next());
			assertEquals(rs.getString(1), "fooo!");
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Batch executer did not complete properly");
		}
		
		try {
			Statement stmt = db.getCon().createStatement();
			Database.BatchExecuter ex = db.getBatchExecuter(stmt);
			ex.execute();
			
			fail("Batch executer should not execute empty statements.");
		}
		catch (SQLException ex) {
			//success
		}
		
		try {
			int[][] values = 
			{
					{1,2},	
					{2,5},
					{3,3},
					{4,4},
					{5,1},
					{6,6}
			};
			Statement stmt = db.getCon().createStatement();
			Database.BatchExecuter ex = db.getBatchExecuter(stmt);
			ex.append("DELETE FROM AtomMembership");
			for (int i = 0; i < values.length; ++i)
				//make sure it works for newlines and no newlines on the end.
				ex.append("INSERT INTO AtomMembership VALUES(" + 
						values[i][0] + "," + values[i][1] + ")" + ((i % 2 == 0) ? "" : "\n"));
			ex.execute();
			stmt.close();
			
			stmt = db.getCon().createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM AtomMembership ORDER BY CollectionID");
			for (int i = 0; i < values.length; ++i) {
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), values[i][0]);
				assertEquals(rs.getInt(2), values[i][1]);
			}
			assertFalse(rs.next());
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Problem with multiple BatchExecuter comparison");
		}
		
		db.closeConnection();
	}
	
	/**
	 * @author shaferia
	 */
	public void testInserter() {
		db.openConnection(dbName);		
		try {
			Statement stmt = db.getCon().createStatement();
			Database.BatchExecuter ex = db.getBatchExecuter(stmt);
			Database.Inserter bi = db.getInserter(ex, "Collections");
			bi.close();
			ex.execute();
			bi.cleanUp();
		}
		catch (SQLException ex) {
			fail("Inserting nothing into from bulk file should be okay.");
		}
		
		String iname = "[Couldn't create inserter]";
		try {
			Object[][] values = 
			{
					{2, "Hi!", "Foo!", "Desck!", "ATOFMS"},
					{3, "Baz", "Gir", "Zim", "AMS"},
					{4, "-", "-", "-", "-"}
			};
			
			Statement stmt = db.getCon().createStatement();
			Database.BatchExecuter ex = db.getBatchExecuter(stmt);
			Database.Inserter bi = db.getInserter(ex, "Collections");
			iname = bi.getClass().getName();
			
			db.getCon().createStatement().executeUpdate("DELETE FROM Collections");
			
			for (int i = 0; i < values.length; ++i)
				bi.append(values[i][0] + "," + values[i][1] + "," + 
						values[i][2] + "," + values[i][3] + "," + values[i][4]);
			bi.close();
			ex.execute();
			bi.cleanUp();
			
			
			ResultSet rs = (stmt = db.getCon().createStatement()).executeQuery(
					"SELECT * FROM Collections ORDER BY CollectionID");
			
			for (int i = 0; i < values.length; ++i) {
				assertTrue(rs.next());
				assertEquals(rs.getInt(1), values[i][0]);
				assertEquals(rs.getString(2), values[i][1]);
				assertEquals(rs.getString(3), values[i][2]);
				assertEquals(rs.getString(4), values[i][3]);
				assertEquals(rs.getString(5), values[i][4]);
			}
		}
		catch (SQLException ex) {
			ex.printStackTrace();
			fail("Problem with Inserter " + iname + " execution");
		}
		
		db.closeConnection();
	}
	
	// ***SLH  BulkInsertDataParticles saveDataParticle
	public void testsaveAtofmsParticle_BulkInsertAtofmsParticles() throws SQLException {
		String[] tables= {"ATOFMSAtomInfoDense", "ATOFMSAtomInfoSparse", "AtomMembership", "DataSetMembers",
						  "InternalAtomOrder"};
		String dense_str = "12-30-06 10:59:49, 1.89E-4, 2.4032946, 4286,E:\\Data\\12-29-2003\\h\\h-031230105949-00001.amz";
		String[] sparse_str = {"23.0, 56673, 0.60352063, 2625", "40.0, 5289, 0.05632348, 450", "-16.0, 17893, 0.06354161, 2607"};

		int datasetid = 100;
		int nextAtomID = 100;
		db.openConnection(dbName);
		Database.Data_bulkBucket bkts = db.getDatabulkBucket(tables);

		ArrayList<String> sparse_arraylist = new ArrayList<>();

		Collections.addAll(sparse_arraylist, sparse_str);

		// ATOFMS table insertion
		Collection c = db.getCollection(0);
		assertEquals(db.saveDataParticle(dense_str, sparse_arraylist, c, datasetid, nextAtomID, bkts), nextAtomID);
		// atofms_bkt.close();
		db.BulkInsertDataParticles(bkts);
		db.closeConnection();
		
		//check ATOFMS dense data
		db.openConnection(dbName);
		Statement stmt = db.getCon().createStatement();
		ResultSet rs = stmt.executeQuery(";\n" + "SELECT * FROM ATOFMSAtomInfoDense where AtomID = 100" );
		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 100);
		// Date are the same but different format.
		//assertEquals(rs.getDate(2), "12-30-06");
		assertEquals(rs.getString(2), "12-30-06 10:59:49");
		assertEquals(rs.getFloat(3), (float)1.89E-4 );
		assertEquals(rs.getFloat(4), (float)2.4032946);
		assertEquals(rs.getFloat(5), (float)4286.0);
		assertEquals(rs.getString(6), "E:\\Data\\12-29-2003\\h\\h-031230105949-00001.amz");
		rs.close();
		stmt.close();
		db.closeConnection();

		//check ATOFMS sparse data
		db.openConnection(dbName);
		// First one
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM ATOFMSAtomInfoSparse where PeakLocation = 23.0" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));                          // atomid
		assertEquals(23.0, rs.getDouble(2), 1e-5);          // peakLocation
		assertEquals(56673, rs.getInt(3));                        // peakarea
		assertEquals(0.60352063, rs.getDouble(4), 1e-8);    // relpeakarea
		assertEquals(2625, rs.getInt(5));                         // peakheight
		rs.close();
		// Third one
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM ATOFMSAtomInfoSparse where PeakLocation = -16.0" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));                          // atomid
		assertEquals(-16.0, rs.getDouble(2), 1e-5);          // peakLocation
		assertEquals(17893, rs.getInt(3));                        // peakarea
		assertEquals(0.06354161, rs.getDouble(4), 1e-8);    // relpeakarea
		assertEquals(2607, rs.getInt(5));                         // peakheight

		// check AtomMembership
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM AtomMembership where CollectionID = 0" );
		assertTrue(rs.next());
		assertEquals(0, rs.getInt(1));                          // collectionID
		assertEquals(100, rs.getInt(2));                        // atomID

		// check DataSetMembers
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM DataSetMembers where OrigDataSetID = " + datasetid );
		assertTrue(rs.next());
		assertEquals(datasetid, rs.getInt(1));                          // datasetid
		assertEquals(100, rs.getInt(2));                        // atomID

		// check InternalAtomOrder
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM InternalAtomOrder where CollectionID = 0" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));                        // atomID
		assertEquals(0, rs.getInt(2));                          // collectionID


	}
	
	public void testsaveAmsParticle_BulkInsertAmsParticles() throws SQLException {
		
		String[] tables= {"AMSAtomInfoDense", "AMSAtomInfoSparse"};
		String ams_dense_str = "12-30-06 10:59:50";
		String[] ams_sparse_str = {"14.0,0.0140019", "30.0,0.2438613", "31.0,9.876385E-4", "32.0,4.877227E-4"};
		
		int datasetid = 100;
		int nextAtomID = 100;
		db.openConnection(dbName);
		Database.Data_bulkBucket bkts = db.getDatabulkBucket(tables);

		ArrayList<String> ams_sparse_arraylist = new ArrayList<>();

		Collections.addAll(ams_sparse_arraylist, ams_sparse_str);
		
		// AMS table insertion
		Collection c = db.getCollection(0);
		assertEquals(db.saveDataParticle(ams_dense_str, ams_sparse_arraylist, c, datasetid, nextAtomID, bkts), nextAtomID);
		// atofms_bkt.close();
		db.BulkInsertDataParticles(bkts);
		db.closeConnection();
		
		//check AMS dense data
		db.openConnection(dbName);
		Statement stmt = db.getCon().createStatement();
		ResultSet rs = stmt.executeQuery(";\n" + "SELECT * FROM AMSAtomInfoDense where AtomID = 100" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));
		// Date are the same but different format.
		//assertEquals(rs.getDate(2), "12-30-06");
		assertEquals(rs.getString(2), "12-30-06 10:59:50");
		db.closeConnection();

		//check AMS sparse data
		db.openConnection(dbName);
		// First one
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM AMSAtomInfoSparse where PeakLocation = 14.0" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));                          // atomid
		assertEquals(14.0, rs.getDouble(2), 1e-5);          // peakLocation
		assertEquals(0.0140019, rs.getDouble(3), 1e-8);     // peakheight
		rs.close();
		// Fourth one
		stmt = db.getCon().createStatement();
		rs = stmt.executeQuery(";\n" + "SELECT * FROM AMSAtomInfoSparse where PeakLocation = 32.0" );
		assertTrue(rs.next());
		assertEquals(100, rs.getInt(1));                          // atomid
		assertEquals(32.0, rs.getDouble(2), 1e-5);          // peakLocation
		assertEquals(4.877227E-4, rs.getDouble(3), 1e-8);   // peakheight
		db.closeConnection();

	}



	/**
	 * @author shaferia
	 */
	public void testGetCollection() {
		db.openConnection(dbName);
		
		Collection c = db.getCollection(0);
		assertEquals(c.getDatatype(), "root");
		assertEquals(c.getDescription(), "root");
		assertEquals(c.getComment(), "root for unsynchronized data");
		assertEquals(c.getName(), "ROOT");
		assertEquals(c.getCollectionID(), 0);
		
		c = db.getCollection(1);
		assertEquals(c.getDatatype(), "root");
		assertEquals(c.getDescription(), "root");
		assertEquals(c.getComment(), "root for synchronized data");
		assertEquals(c.getName(), "ROOT-SYNCHRONIZED");
		assertEquals(c.getCollectionID(), 1);
		
		c = db.getCollection(2);
		assertEquals(c.getDatatype(), "ATOFMS");
		assertEquals(c.getDescription(), "onedescrip");
		assertEquals(c.getComment(), "one");
		assertEquals(c.getName(), "One");
		assertEquals(c.getCollectionID(), 2);
		
		db.closeConnection();
	}

	/**
	 * author jtbigwoo
	 */
	public void testUpdateParticleTable()
	{
		Collection c;
		Vector<Vector<Object>> particleInfo = new Vector<Vector<Object>>();
		Connection con;
		Statement stmt;

		String manual = " INSERT INTO Collections VALUES "+
		"(7,'Seven', 'seven', 'sevendescrip', 'ATOFMS')";

		db.openConnection(dbName);
		con = db.getCon();
		try
		{
			stmt = con.createStatement();
			stmt.executeUpdate("INSERT INTO Collections VALUES (7, 'Seven', 'seven', 'sevendescrip', 'ATOFMS')\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,1)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,3)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,5)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES (7,7)\n");
			stmt.executeUpdate("INSERT INTO InternalAtomOrder (AtomID, CollectionID) SELECT AtomID, CollectionID FROM AtomMembership WHERE CollectionID = 7");
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			fail("Failed to insert new collection to test updateParticleTable");
		}
		c = db.getCollection(7);
		db.updateParticleTable(c, particleInfo, 1, 2);
		assertEquals(2, particleInfo.size());
		assertEquals(1, particleInfo.get(0).get(0));
		assertEquals("2003-09-02 17:30:38", particleInfo.get(0).get(1));
		assertEquals("1.0", particleInfo.get(0).get(2));
		assertEquals("0.01", particleInfo.get(0).get(3));
		assertEquals("1", particleInfo.get(0).get(4));
		assertEquals("particle1", particleInfo.get(0).get(5));
		assertEquals(3, particleInfo.get(1).get(0));
		// test defect 1964860 - doesn't get the right stuff when atom id's are not consecutive
		db.updateParticleTable(c, particleInfo, 3, 4);
		assertEquals(2, particleInfo.size());
		assertEquals(5, particleInfo.get(0).get(0));
		assertEquals(7, particleInfo.get(1).get(0));
		
		c = db.getCollection(5);
		db.updateParticleTable(c, particleInfo, 1, 5);
		assertEquals(5, particleInfo.size(), 5);
		assertEquals(16, particleInfo.get(0).get(0));
		assertEquals("0.5", particleInfo.get(0).get(1));
		assertEquals("1.0", particleInfo.get(0).get(2));
		assertEquals(20, particleInfo.get(4).get(0));
	}
	
	/**
	 * author jtbigwoo
	 */
	public void testGetAveragePeakListForCollection() {
		Connection con;
		Collection c;
		BinnedPeakList bpl;

		db.openConnection(dbName);
		con = db.getCon();

		bpl = db.getAveragePeakListForCollection(db.getCollection(2));
		assertEquals(15f, bpl.getAreaAt(-30));
		assertEquals(11.25f, bpl.getAreaAt(30));
		assertEquals(8, bpl.getPeaks().size());
		
		bpl = db.getAveragePeakListForCollection(db.getCollection(3));
		assertEquals(15f, bpl.getAreaAt(-30));
		assertEquals(3f, bpl.getAreaAt(306));

	}
}