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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.ArrayList;

import javax.swing.JOptionPane;

/**
 * @author ritza
 */
public class CreateTestDatabase2 {
	public InfoWarehouse tempDB;
	Connection con;
	private static final char quote = '"';
	
	public CreateTestDatabase2() {
        tempDB = Database.getDatabase();
        tempDB.openConnection();
        con = tempDB.getCon();
        try {
			Database.rebuildDatabase("TestDB2");
		} catch (ExceptionAdapter ea) {
        	if (ea.originalException instanceof  SQLException) {
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
			} else {
        		throw ea;
			}
		}
    		
		try {
			if (con.createStatement() == null)
				System.err.println("con should not be null");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	    try {
			Statement stmt = con.createStatement();
			// Create a database with tables mirroring those in the 
			// real one so we can test on that one and make sure we
			// know what the results should be.
			//stmt.executeUpdate("CREATE DATABASE TestDB2");
			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (1,'9/2/2003 5:30:30 PM',1,0.1,1,'One')\n " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (2,'9/2/2003 5:30:31 PM',2,0.2,2,'Two')\n " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (3,'9/2/2003 5:30:34 PM',3,0.3,3,'Three')\n " +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (4,'9/2/2003 5:30:35 PM',4,0.4,4,'Four')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (5,'9/2/2003 5:30:36 PM',5,0.5,5,'Five')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (6,'9/2/2003 5:30:31 PM',6,0.6,6,'Six')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (7,'9/2/2003 5:30:33 PM',7,0.7,7,'Seven')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (8,'9/2/2003 5:30:33 PM',8,0.8,8,'Eight')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (9,'9/2/2003 5:30:35 PM',9,0.9,9,'Nine')\n" +
					"INSERT INTO ATOFMSAtomInfoDense VALUES (10,'9/2/2003 5:30:36 PM',10,0.01,10,'Ten')\n");
					
			stmt.executeUpdate(
					"USE TestDB2\n"+
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (11,'9/2/2003 5:30:30 PM',0.1)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (12,'9/2/2003 5:30:30 PM',0.2)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (13,'9/2/2003 5:30:31 PM',0.3)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (14,'9/2/2003 5:30:33 PM',0.4)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (15,'9/2/2003 5:30:33 PM',0.5)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (16,'9/2/2003 5:30:34 PM',0.6)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (17,'9/2/2003 5:30:35 PM',0.7)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (18,'9/2/2003 5:30:35 PM',0.8)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (19,'9/2/2003 5:30:36 PM',0.9)\n" +
					"INSERT INTO TimeSeriesAtomInfoDense VALUES (20,'9/2/2003 5:30:36 PM',1.0)\n");
			
			stmt.executeUpdate(
					"USE TestDB2\n"+
					"INSERT INTO AMSAtomInfoDense VALUES (21,'9/2/2003 5:30:30 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (22,'9/2/2003 5:30:30 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (23,'9/2/2003 5:30:31 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (24,'9/2/2003 5:30:32 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (25,'9/2/2003 5:30:32 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (26,'9/2/2003 5:30:32 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (27,'9/2/2003 5:30:34 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (28,'9/2/2003 5:30:34 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (29,'9/2/2003 5:30:35 PM')\n" +
					"INSERT INTO AMSAtomInfoDense VALUES (30,'9/2/2003 5:30:36 PM')\n");
				
			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO Collections VALUES (2,'ATOFMS1', 'one', 'onedescrip', 'ATOFMS')\n" +
					"INSERT INTO Collections VALUES (3,'ATOFMS2','two','twodescrip','ATOFMS')\n"+
					"INSERT INTO Collections VALUES (4,'TimeSeries', 'three', 'threedescrip', 'TimeSeries')\n" +
					"INSERT INTO Collections VALUES (5,'AMS', 'four', 'fourdescrip', 'AMS')\n");
			
			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO AtomMembership VALUES(2,1)\n" +
					"INSERT INTO AtomMembership VALUES(2,2)\n" +
					"INSERT INTO AtomMembership VALUES(2,3)\n" +
					"INSERT INTO AtomMembership VALUES(2,4)\n" +
					"INSERT INTO AtomMembership VALUES(2,5)\n" +
					"INSERT INTO AtomMembership VALUES(3,6)\n" +
					"INSERT INTO AtomMembership VALUES(3,7)\n" +
					"INSERT INTO AtomMembership VALUES(3,8)\n" +
					"INSERT INTO AtomMembership VALUES(3,9)\n" +
					"INSERT INTO AtomMembership VALUES(3,10)\n" +
					"INSERT INTO AtomMembership VALUES(4,11)\n" +
					"INSERT INTO AtomMembership VALUES(4,12)\n" +
					"INSERT INTO AtomMembership VALUES(4,13)\n" +
					"INSERT INTO AtomMembership VALUES(4,14)\n" +
					"INSERT INTO AtomMembership VALUES(4,15)\n" +
					"INSERT INTO AtomMembership VALUES(4,16)\n" +
					"INSERT INTO AtomMembership VALUES(4,17)\n" +
					"INSERT INTO AtomMembership VALUES(4,18)\n" +
					"INSERT INTO AtomMembership VALUES(4,19)\n" +
					"INSERT INTO AtomMembership VALUES(4,20)\n" +
					"INSERT INTO AtomMembership VALUES(5,21)\n" +
					"INSERT INTO AtomMembership VALUES(5,22)\n" +
					"INSERT INTO AtomMembership VALUES(5,23)\n" +
					"INSERT INTO AtomMembership VALUES(5,24)\n" +
					"INSERT INTO AtomMembership VALUES(5,25)\n" +
					"INSERT INTO AtomMembership VALUES(5,26)\n" +
					"INSERT INTO AtomMembership VALUES(5,27)\n" +
					"INSERT INTO AtomMembership VALUES(5,28)\n" +
					"INSERT INTO AtomMembership VALUES(5,29)\n" +
					"INSERT INTO AtomMembership VALUES(5,30)\n");

			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO CollectionRelationships VALUES(0,2)\n" +
					"INSERT INTO CollectionRelationships VALUES(2,3)\n" +
					"INSERT INTO CollectionRelationships VALUES(0,4)\n" +
					"INSERT INTO CollectionRelationships VALUES(0,5)\n");

			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO ATOFMSDataSetInfo VALUES(2,'One','aFile','anotherFile',12,20,0.005,1)\n" +
					"INSERT INTO ATOFMSDataSetInfo VALUES(3,'Two','aFile','anotherFile',12,20,0.005,1)\n" +
					"INSERT INTO TimeSeriesDataSetInfo VALUES(4,'Three',4,0)\n" +
					"INSERT INTO AMSDataSetInfo VALUES(5,'Four','aFile','anotherFile','lastFile')\n");

			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO DataSetMembers VALUES(2,1)\n" +
					"INSERT INTO DataSetMembers VALUES(2,2)\n" +
					"INSERT INTO DataSetMembers VALUES(2,3)\n" +
					"INSERT INTO DataSetMembers VALUES(2,4)\n" +
					"INSERT INTO DataSetMembers VALUES(2,5)\n" +
					"INSERT INTO DataSetMembers VALUES(3,6)\n" +
					"INSERT INTO DataSetMembers VALUES(3,7)\n" +
					"INSERT INTO DataSetMembers VALUES(3,8)\n" +
					"INSERT INTO DataSetMembers VALUES(3,9)\n" +
					"INSERT INTO DataSetMembers VALUES(3,10)\n" +
					"INSERT INTO DataSetMembers VALUES(4,11)\n" +
					"INSERT INTO DataSetMembers VALUES(4,12)\n" +
					"INSERT INTO DataSetMembers VALUES(4,13)\n" +
					"INSERT INTO DataSetMembers VALUES(4,14)\n" +
					"INSERT INTO DataSetMembers VALUES(4,15)\n" +
					"INSERT INTO DataSetMembers VALUES(4,16)\n" +
					"INSERT INTO DataSetMembers VALUES(4,17)\n" +
					"INSERT INTO DataSetMembers VALUES(4,18)\n" +
					"INSERT INTO DataSetMembers VALUES(4,19)\n" +
					"INSERT INTO DataSetMembers VALUES(4,20)\n" +
					"INSERT INTO DataSetMembers VALUES(5,21)\n" +
					"INSERT INTO DataSetMembers VALUES(5,22)\n" +
					"INSERT INTO DataSetMembers VALUES(5,23)\n" +
					"INSERT INTO DataSetMembers VALUES(5,24)\n" +
					"INSERT INTO DataSetMembers VALUES(5,25)\n" +
					"INSERT INTO DataSetMembers VALUES(5,26)\n" +
					"INSERT INTO DataSetMembers VALUES(5,27)\n" +
					"INSERT INTO DataSetMembers VALUES(5,28)\n" +
					"INSERT INTO DataSetMembers VALUES(5,29)\n" +
					"INSERT INTO DataSetMembers VALUES(5,30)\n");	
			
			stmt.executeUpdate(
					"USE TestDB2\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(2,30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(3,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(3,30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(3,45,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-20,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,-10,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(4,20,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-300,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,-20,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,6,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(5,30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-306,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-300,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,300,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(6,306,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-307,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-300,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,-15,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,30,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,230,15,0.006,12)\n" +
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(7,300,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-430,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-308,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-300,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,70,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,80,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(8,800,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(9,-30,15,0.006,12)\n" + 
					"INSERT INTO ATOFMSAtomInfoSparse VALUES(10,-30,15,0.006,12)\n");
					
			stmt.executeUpdate(
					"USE TestDB2\n" +
			"INSERT INTO AMSAtomInfoSparse VALUES(21,1,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(21,2,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(22,1,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(22,2,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(22,3,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(23,1,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(23,2,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(23,3,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(24,1,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(25,2,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(25,3,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(26,1,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(26,2,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(27,1,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(28,1,0)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(29,1,1)\n" + 
					"INSERT INTO AMSAtomInfoSparse VALUES(30,2,1)\n" ); 
		
			updateInternalAtomOrderTestTable();
	    } catch (SQLException e) {
			e.printStackTrace();
		}
		tempDB.closeConnection();
	}
	
	/**
	 * Creates temporary test files to be used when testing the 
	 * EnchiladaDataSetImporter.
	 *
	 * @return ArrayList<File> - the files created
	 */
	public ArrayList<File> createEnchFiles(){
		ArrayList<File> files = new ArrayList<File>();
		FileWriter writer;
		
		File enchiladaD = new File("enchilada.dtd");
		try {
			enchiladaD.createNewFile();
			writer = new FileWriter(enchiladaD, true);
			String string = new String("<?xml version=" + quote + "1.0" + quote
				+ " encoding=" + quote + "utf-8" + quote + "?>\n"
				+ "<!ELEMENT enchiladadata (datasetinfo+)>\n"
				+ "<!-- The enchiladadata element MUST have a datatype associated with it. -->\n"
				+ "<!ATTLIST enchiladadata datatype CDATA #REQUIRED>\n"
				+ "<!ELEMENT datasetinfo (field*, atominfodense*)>\n"
				+ "<!-- Each dataset MUST have a name (this is used as a primary key). -->\n"
				+ "<!ATTLIST datasetinfo dataSetName CDATA #REQUIRED>\n"
				+ "<!ELEMENT atominfodense (field*, atominfosparse*)>\n"
				+ "<!ELEMENT atominfosparse (field*)>\n"
				+ "<!ELEMENT field (#PCDATA)>");
			//System.out.println(string);//debugging
			writer.write(string);
			writer.close();
		} catch (IOException e) {
			System.err.println("Problem creating dtd.");
			e.printStackTrace();
		}
		files.add(enchiladaD);
		
		/*
		 * This is what the following file should look like, but I've left out
		 * line breaks in the temporary file.
		 * 
		 *  <?xml version="1.0" encoding="utf-8"?>
		 *	<!DOCTYPE enchiladadata SYSTEM "enchilada.dtd">
		 *
		 *	<enchiladadata datatype="SimpleParticle">
		 *		<datasetinfo dataSetName="Simple Dataset">
		 *			<field>22</field>
		 *			<atominfodense>
		 *	 			<field>33.00</field>
		 *				<field>42.00</field>
		 *			</atominfodense>
		 *			<atominfodense>
		 *				<field>1.00</field>
		 *				<field>2.00</field>
		 *				<atominfosparse>
		 *					<field>4</field>
		 *					<field>1</field>
		 *				</atominfosparse>
		 *			</atominfodense>
		 *	
		 *			<atominfodense>
		 *				<field>21.0</field>
		 *				<field>12.2</field>
		 *				<atominfosparse>
		 *					<field>9</field>
		 *					<field>0</field>
		 *				</atominfosparse>
		 *				<atominfosparse>
		 *					<field>8</field>
		 *					<field>0</field>
		 *				</atominfosparse>
		 *			</atominfodense>
		 *			<atominfodense>
		 *				<field>56.6</field>
		 *				<field>76.5</field>
		 *				<atominfosparse>
		 *					<field>10</field>
		 *					<field>1</field>
		 *				</atominfosparse>
		 *				<atominfosparse>
		 *					<field>1</field>
		 *					<field>0</field>
		 *				</atominfosparse>
		 *				<atominfosparse>
		 *					<field>2</field>
		 *					<field>1</field>
		 *				</atominfosparse>
		 *			</atominfodense>
		 *		</datasetinfo>
		 *	</enchiladadata>
		 */
		
		File testData = new File("test.ed");
		try {
			testData.createNewFile();
			writer = new FileWriter(testData, true);
			String string = new String("<?xml version=" + quote + "1.0"  + quote + 
					" encoding=" + quote + "utf-8" + quote + "?>" +
					"<!DOCTYPE enchiladadata SYSTEM " + quote + "enchilada.dtd" +
					quote + "> <enchiladadata datatype=" + quote + "SimpleParticle"
					+ quote + "> <datasetinfo dataSetName=" + quote + "Simple Dataset"
					+ quote + "> <field>22</field> <atominfodense> <field>33.00</field>"
					+ "<field>42.00</field> </atominfodense> <atominfodense>"
					+ "<field>1.00</field> <field>2.00</field>"
					+ "<atominfosparse>	<field>4</field>"
					+ "<field>1</field> </atominfosparse> </atominfodense>"
					+ "<atominfodense> <field>21.0</field> <field>12.2</field>"
					+ "<atominfosparse>"
					+ "<field>9</field> <field>0</field> </atominfosparse>"
					+ "<atominfosparse>"
					+ "<field>8</field> <field>0</field> </atominfosparse>"
					+ "</atominfodense> <atominfodense> <field>56.6</field>"
					+ "<field>76.5</field> <atominfosparse table=" + quote +
					quote + "> <field>10</field> <field>1</field>"
					+ "</atominfosparse> <atominfosparse> " +
							"<field>1</field> <field>0</field>"
					+ "</atominfosparse> <atominfosparse>" +
							" <field>2</field> <field>1</field>"
					+ "</atominfosparse> </atominfodense> </datasetinfo>"
					+ "</enchiladadata>");
			//System.out.println(string);//debugging
			writer.write(string);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		files.add(testData);
		
		return files;
	}
	
	/**
	 * Creates temporary test files to be used when testing the 
	 * DynamicTableGenerator.
	 *
	 * @return ArrayList<File> - the files created
	 */
	public ArrayList<File> createMetaFiles(){
		ArrayList<File> files = new ArrayList<File>();
		FileWriter writer;
		
		File metaD = new File("meta.dtd");
		try {
			metaD.createNewFile();
			writer = new FileWriter(metaD, true);
			writer.write("<?xml version=" + quote + "1.0" + quote+ " " +
				"encoding=" + quote + "utf-8" + quote + "?>\n"
				+ "<!ELEMENT metadata (datasetinfo, atominfodense, atominfosparse)>\n"
				+ "<!-- The metadata element MUST have a datatype associated with it -->\n"
				+ "<!ATTLIST metadata datatype CDATA #REQUIRED>\n"
				+ "<!ELEMENT datasetinfo (field*)>\n"
				+ "<!ELEMENT atominfodense (field*)>\n"
				+ "<!ELEMENT atominfosparse (field*)>\n"
				+ "<!ELEMENT atominfosparse (field*)>\n"
				+ "<!ELEMENT field (#PCDATA)>\n"
				+ "<!-- Attributes are the type of data in that field, and whether"
				+ " or not it is a primary key - used for the AIS table.  -->\n"
				+ "<!ATTLIST field \n"
				+ "type CDATA #REQUIRED\n"
				+ "primaryKey (true | false) " + quote + "false" + quote + "\n"
				+ ">");
			writer.close();
			
		} catch (IOException e) {
			System.err.println("Problem creating dtd.");
			e.printStackTrace();
		}
		files.add(metaD);
		
		File testMeta = new File("test.md");
		try {
			testMeta.createNewFile();
			writer = new FileWriter(testMeta, true);
			writer.write("<?xml version=" + quote + "1.0" + quote + " encoding="
					+ quote + "utf-8" + quote + "?>\n"
					+ "<!DOCTYPE metadata SYSTEM " + quote + "meta.dtd" + quote + ">\n"
					+ "<metadata datatype=" + quote + "SimpleParticle" + quote + ">\n"
					+ "<datasetinfo> \n"
					+ "<field type=" + quote + "int" + quote + ">Number</field>\n"
					+ "</datasetinfo> \n"
					+ "<atominfodense> \n"
					+ "<field type=" + quote + "real" + quote + ">Size</field>\n"
					+ "<field type=" + quote + "real" + quote + ">Magnitude</field>\n"
					+ "</atominfodense>\n"
					+ "<atominfosparse table=" + quote + quote + ">\n"
					+ "<field type=" + quote + "int" + quote + " primaryKey="
					+ quote + "true" + quote + ">Delay</field>\n"
					+ "<field type=" + quote + "bit" + quote + ">Valid</field>\n"
					+ "</atominfosparse>\n"
					+ "</metadata>");
			writer.close();
		} catch (IOException e) {
			System.err.println("Error creating temporary simpleparticle .md file");
			e.printStackTrace();
		}
		files.add(testMeta);
		
		File testMetaBad = new File("testBad.md");
		try {
			testMetaBad.createNewFile();
			writer = new FileWriter(testMetaBad, true);
			writer.write("<?xml version=" + quote + "1.0" + quote + " encoding="
					+ quote + "utf-8" + quote + "?>\n"
					+ "<!DOCTYPE metadata SYSTEM " + quote + "meta.dtd" + quote + ">\n"
					+ "<metadata datatype=" + quote + "BadExample" + quote + ">\n"
					+ "<datasetinfo> \n"
					+ "<field type=" + quote + "int" + quote + ">atomid</field>\n"
					+ "</datasetinfo>\n"
					+ "<atominfodense>\n"
					+ "<field type=" + quote + "real" + quote + ">Size</field>\n"
					+ "<field type=" + quote + "real" + quote + ">Magnitude</field>\n"
					+ "</atominfodense>\n"
					+ "<atominfosparse table=" + quote + quote + ">\n"
					+ "<field type=" + quote + "int" + quote + " primaryKey="
					+ quote + "true" + quote + ">Delay</field>\n"
					+ "<field type=" + quote + "bit" + quote + ">Valid</field>\n"
					+ "</atominfosparse>\n"
					+ "</metadata>");
			writer.close();
		} catch (IOException e) {
			System.err.println("Error creating temporary badexample .md file");
			e.printStackTrace();
		}
		files.add(testMetaBad);
		return files;
	}
	
	private void updateInternalAtomOrderTestTable() {
		try {
			Statement stmt = con.createStatement();
			// updateInternalAtomOrderTable for CID=2
			ResultSet rs = stmt.executeQuery("USE TestDB2 SELECT AtomID FROM AtomMembership WHERE CollectionID = 2 OR CollectionID = 3");
			while(rs.next())
				stmt.addBatch("USE TestDB2 INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",2)");
			stmt.executeBatch();
			
			// updateInternalAtomOrderTable for CID=3
			rs = stmt.executeQuery("USE TestDB2 SELECT AtomID FROM AtomMembership WHERE CollectionID = 3");
			while(rs.next())
				stmt.addBatch("USE TestDB2 INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",3)");
			stmt.executeBatch();
			
			// updateInternalAtomOrderTable for CID=4
			rs = stmt.executeQuery("USE TestDB2 SELECT AtomID FROM AtomMembership WHERE CollectionID = 4");
			while(rs.next())
				stmt.addBatch("USE TestDB2 INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",4)");
			stmt.executeBatch();
			
			// updateInternalAtomOrderTable for CID=5
			rs = stmt.executeQuery("USE TestDB2 SELECT AtomID FROM AtomMembership WHERE CollectionID = 5");
			while(rs.next())
				stmt.addBatch("USE TestDB2 INSERT INTO InternalAtomOrder VALUES ("+rs.getInt(1)+",5)");
			stmt.executeBatch();
			
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	public static void main(String[] args) {
		new CreateTestDatabase2();
	}
}
