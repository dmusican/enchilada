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
 * The Original Code is EDAM Enchilada's SQLException class.
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
 * Created on Aug 25, 2004
 */
package edu.carleton.enchilada.database;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;

import java.sql.*;

/**
 * @author ritza
 */
public class CreateTestDatabase {
	private Connection con;
	private static final char quote = '"';
	private static final String dbname = "TestDB";
	
	public CreateTestDatabase() {
		Database.getDatabase(dbname).rebuildDatabase(dbname);
		Database db = Database.getDatabase(dbname);
		db.openConnection();
		con = db.getCon();

		generateDynamicTables();
    		
	    try {

			Statement stmt = con.createStatement();
			// Create a database with tables mirroring those in the 
			// real one so we can test on that one and make sure we
			// know what the results should be.
			//stmt.executeUpdate("CREATE DATABASE TestDB");
			String queryTemplate = "INSERT INTO ATOFMSAtomInfoDense VALUES (?, '2003-09-02 17:30:38', ?, ?, ?, ?)";
			PreparedStatement pstmt = con.prepareStatement(queryTemplate);
			for (int i=1; i < 12; i++) {
				pstmt.setInt(1, i);
				pstmt.setInt(2, i);
				pstmt.setDouble(3, i/100.);
				pstmt.setInt(4, i);
				pstmt.setString(5, "particle"+i);
				pstmt.addBatch();
			}
			pstmt.executeBatch();

			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (12, 1, 2)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (13, 1, 2)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (14, 1, 1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (15, 3, 21)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (16, .5, 1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (17, .4, 5)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (18, 10, 1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (19, .75, 50)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (20, 1, 2)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoDense VALUES (21, 5, 2)\n");
		
			stmt.executeUpdate("INSERT INTO Collections VALUES (2,'One', 'one', 'onedescrip', 'ATOFMS')\n");
			stmt.executeUpdate("INSERT INTO Collections VALUES (3,'Two', 'two', 'twodescrip', 'ATOFMS')\n");
			stmt.executeUpdate("INSERT INTO Collections VALUES (4,'Three', 'three', 'threedescrip', 'Datatype2')\n");
			stmt.executeUpdate("INSERT INTO Collections VALUES (5,'Four', 'four', 'fourdescrip', 'Datatype2')\n");
			stmt.executeUpdate("INSERT INTO Collections VALUES (6, 'Five', 'five', 'fivedescrip', 'Datatype2')\n");
					
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2,1)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2,2)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2,3)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2,4)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(2,5)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(3,6)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(3,7)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(3,8)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(3,9)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(3,10)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(4,11)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(4,12)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(4,13)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(4,14)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(4,15)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(5,16)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(5,17)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(5,18)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(5,19)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(5,20)\n");
			stmt.executeUpdate("INSERT INTO AtomMembership VALUES(6,21)\n" );

			stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES(0,2)\n");
			stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES(0,3)\n");
			stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES(0,4)\n");
			stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES(0,5)\n");
			stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES(5,6)");

			stmt.executeUpdate("INSERT INTO ATOFMSDataSetInfo VALUES(1,'One','aFile','anotherFile',12,20,0.005,1)");
			stmt.executeUpdate("INSERT INTO Datatype2DataSetInfo VALUES(1,'2003-09-02 17:30:38',100)");

			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,1)\n");
			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,2)\n");
			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,3)\n");
			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,4)\n");
			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,5)\n");
			stmt.executeUpdate("INSERT INTO DataSetMembers VALUES(1,6)\n");

			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(2,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(2,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(3,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(3,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(3,45,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-20,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-10,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(4,20,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-20,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(5,6,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(5,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-306,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(6,306,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-307,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-15,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,230,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(7,300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-430,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-308,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-300,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,70,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,80,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(8,800,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(9,-30,15,0.006,12)\n");
			stmt.executeUpdate("INSERT INTO ATOFMSAtomInfoSparse VALUES(10,-30,15,0.006,12)\n");

			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(11,1,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(11,2,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(12,1,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(12,2,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(12,3,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(13,1,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(13,2,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(13,3,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(14,1,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(15,2,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(15,3,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(16,1,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(16,2,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(17,1,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(18,1,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(19,1,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(20,2,1)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(21,1,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(21,2,0)\n");
			stmt.executeUpdate("INSERT INTO Datatype2AtomInfoSparse VALUES(21,3,0)\n");

			updateInternalAtomOrderTestTable();
	    } catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
		db.closeConnection();
	}
	
	/**
	 * Directly inserts values for Datatype2 and SimpleParticle into MetaData
	 * table and creates appropriate dynamic tables for both datatypes.
	 *
	 */
	public void generateDynamicTables() {
		try {
			Statement stmt = con.createStatement();

			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','DataSetID','INT',1,0,1)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Time','DATETIME',0,0,2)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Number','INT',0,0,3)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','AtomID','INT',1,1,1)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Size','REAL',0,1,2)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Magnitude','REAL',0,1,3)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','AtomID','INT',1,2,1)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Delay','INT',1,2,2)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('Datatype2','Valid','BIT',0,2,3)\n");
			stmt.executeUpdate("CREATE TABLE Datatype2DataSetInfo (DataSetID INT, Time DATETIME, Number INT,  PRIMARY KEY (DataSetID))\n" );
			stmt.executeUpdate("CREATE TABLE Datatype2AtomInfoDense (AtomID INT, Size REAL, Magnitude REAL,  PRIMARY KEY (AtomID))\n" );
			stmt.executeUpdate("CREATE TABLE Datatype2AtomInfoSparse (AtomID INT, Delay INT, Valid BIT, PRIMARY KEY (AtomID, Delay))");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','DataSetID','INT',1,0,0)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','DataSet','VARCHAR(8000)',0,0,1)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','Number','INT',0,0,2)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','AtomID','INT',1,1,0)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','Size','REAL',0,1,1)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','Magnitude','REAL',0,1,2)\n" );
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','AtomID','INT',1,2,0)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','Delay','INT',1,2,1)\n");
			stmt.executeUpdate("INSERT INTO MetaData VALUES ('SimpleParticle','Valid','BIT',0,2,2)\n");
			stmt.executeUpdate("CREATE TABLE SimpleParticleDataSetInfo (DataSetID INT, DataSet VARCHAR(8000), Number INT, PRIMARY KEY(DataSetID))\n" );
			stmt.executeUpdate("CREATE TABLE SimpleParticleAtomInfoDense (AtomID INT, Size REAL, Magnitude REAL, PRIMARY KEY(AtomID))\n" );
			stmt.executeUpdate("CREATE TABLE SimpleParticleAtomInfoSparse (AtomID INT, Delay INT, Valid BIT, PRIMARY KEY (AtomID, Delay))");

		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}

	}
	

	/**
	 * @author steinbel
	 * Could still use some optimization - all I did was take out all references
	 * to IAO.OrderNumber.
	 *
	 */
	private void updateInternalAtomOrderTestTable() {
		try {
			con.setAutoCommit(false);
			Statement queryStmt = con.createStatement();
			Statement insertStmt = con.createStatement();
			// updateInternalAtomOrderTable for CID=2
			ResultSet rs = queryStmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
					" CollectionID = 2");
			while(rs.next())
				insertStmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",2)");
			insertStmt.executeBatch();
			// updateInternalAtomOrderTable for CID=3
			rs = queryStmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
					" CollectionID = 3");
			while(rs.next())
				insertStmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",3)");
			insertStmt.executeBatch();
			// updateInternalAtomOrderTable for CID=4
			rs = queryStmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
					" CollectionID = 4");
			while(rs.next())
				insertStmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",4)");
			insertStmt.executeBatch();
			// updateInternalAtomOrderTable for CID=5
			rs = queryStmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
					" CollectionID = 5 OR CollectionID = 6");
			while(rs.next())
				insertStmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",5)");
			insertStmt.executeBatch();
			// updateInternalAtomOrderTable for CID=6
			rs = queryStmt.executeQuery("SELECT AtomID FROM AtomMembership WHERE" +
					" CollectionID = 6");
			while(rs.next())
				insertStmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",6)");
			insertStmt.executeBatch();
			rs.close();
			queryStmt.close();
			insertStmt.close();
			con.commit();
			con.setAutoCommit(true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new CreateTestDatabase();
	}
}
