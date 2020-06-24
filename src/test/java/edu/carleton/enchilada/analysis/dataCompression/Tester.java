package edu.carleton.enchilada.analysis.dataCompression;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.swing.JOptionPane;

import edu.carleton.enchilada.analysis.DistanceMetric;

import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;

/**
 * Tester class for BIRCH.  Creates and manipulates BIRCHdb.
 * @author ritza
 *
 */
public class Tester {
	static InfoWarehouse db;
	InfoWarehouse tempDB;
	Connection con;
	
	public Tester()
	{
        tempDB = Database.getDatabase();
        tempDB.openConnection();
        con = tempDB.getCon();
        
        try {
			Database.getDatabase("BIRCHdb").rebuildDatabase("BIRCHdb");
		} catch (ExceptionAdapter ea) {
        	if (ea.originalException instanceof SQLException) {
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
			} else {
        		throw ea;
			}
		}
				
		try {
			Statement stmt = con.createStatement();
			// Create a database with tables mirroring those in the 
			// real one so we can test on that one and make sure we
			// know what the results should be.
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (1,'9/2/2003 5:30:38 PM',1,0.1,1,'One') " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (2,'9/2/2003 5:30:38 PM',2,0.2,2,'Two') " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (3,'9/2/2003 5:30:38 PM',3,0.3,3,'Three') " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (4,'9/2/2003 5:30:38 PM',4,0.4,4,'Four')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (5,'9/2/2003 5:30:38 PM',5,0.5,5,'Five')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (6,'9/2/2003 5:30:38 PM',6,0.6,6,'Six')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (7,'9/2/2003 5:30:38 PM',7,0.7,7,'Seven')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (8,'9/2/2003 5:30:38 PM',8,0.8,8,'Eight')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (9,'9/2/2003 5:30:38 PM',9,0.9,9,'Nine')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (10,'9/2/2003 5:30:38 PM',10,0.01,10,'Ten')\n");
		
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO Collections VALUES (2,'One', 'one', 'onedescrip', 'ATOFMS')\n");
					
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO AtomMembership VALUES(2,1)\n" +
					"INSERT INTO AtomMembership VALUES(2,2)\n" +
					"INSERT INTO AtomMembership VALUES(2,3)\n" +
					"INSERT INTO AtomMembership VALUES(2,4)\n" +
					"INSERT INTO AtomMembership VALUES(2,5)\n" +
					"INSERT INTO AtomMembership VALUES(2,6)\n" +
					"INSERT INTO AtomMembership VALUES(2,7)\n" +
					"INSERT INTO AtomMembership VALUES(2,8)\n" +
					"INSERT INTO AtomMembership VALUES(2,9)\n" +
					"INSERT INTO AtomMembership VALUES(2,10)\n");
			
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO InternalAtomOrder VALUES(1,2,1)\n" +
					"INSERT INTO InternalAtomOrder VALUES(2,2,2)\n" +
					"INSERT INTO InternalAtomOrder VALUES(3,2,3)\n" +
					"INSERT INTO InternalAtomOrder VALUES(4,2,4)\n" +
					"INSERT INTO InternalAtomOrder VALUES(5,2,5)\n" +
					"INSERT INTO InternalAtomOrder VALUES(6,2,6)\n" +
					"INSERT INTO InternalAtomOrder VALUES(7,2,7)\n" +
					"INSERT INTO InternalAtomOrder VALUES(8,2,8)\n" +
					"INSERT INTO InternalAtomOrder VALUES(9,2,9)\n" +
					"INSERT INTO InternalAtomOrder VALUES(10,2,10)\n");

			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO CollectionRelationships VALUES(0,2)\n");
			
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO ATOFMSDataSetInfo VALUES(1,'One','aFile','anotherFile',12,20,0.005,1)");
			
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					"INSERT INTO DataSetMembers VALUES(1,1)\n" +
					"INSERT INTO DataSetMembers VALUES(1,2)\n" +
					"INSERT INTO DataSetMembers VALUES(1,3)\n" +
					"INSERT INTO DataSetMembers VALUES(1,4)\n" +
					"INSERT INTO DataSetMembers VALUES(1,5)\n" +
					"INSERT INTO DataSetMembers VALUES(1,6)\n" +
					"INSERT INTO DataSetMembers VALUES(1,7)\n" +
					"INSERT INTO DataSetMembers VALUES(1,8)\n" +
					"INSERT INTO DataSetMembers VALUES(1,9)\n" + 
					"INSERT INTO DataSetMembers VALUES(1,10)\n");
		 
			stmt.executeUpdate(
					"USE BIRCHdb\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(1,-10,1,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(1,15,1,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(1,10,3,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,-20,20,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,-10,10,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,15,11,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,10,13,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,20,20,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(3,50,50,0.006,12)\n" + 
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-20,20,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,0,0,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,1,1,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,15,12,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,10,30,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-10,12,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,15,1,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,10,11,0.006,12)\n" + 
									
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-30,30,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,50,5,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,5,5,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,10,10,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,15,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,20,20,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,30,30,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-10,13,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,15,5,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,10,5,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,20,16,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,18,18,0.006,12)\n" +
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(9,-10,10,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(9,15,0,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(9,10,8,0.006,12)\n" + 
					
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(10,50,48,0.006,12)\n");
					
		  } catch (SQLException e) {
				e.printStackTrace();
			}
		  	tempDB.closeConnection();
	}
	
	public static void main(String[] args) {
		new Tester();
		db = Database.getDatabase("BIRCHdb");
		db.openConnection();
		BIRCH birch = new BIRCH(db.getCollection(2),db,"BIRCH","comment",DistanceMetric.EUCLIDEAN_SQUARED);
		birch.compress();
		db.closeConnection();
	}
	
}
