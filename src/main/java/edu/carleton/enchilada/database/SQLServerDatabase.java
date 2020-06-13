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
 * The Original Code is EDAM Enchilada's SQLServerDatabase class.
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


/*
 * Created on Jul 20, 2004
 *
 * 
 */
package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.MainFrame;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Makes database work with SQL Server
 * @author andersbe, shaferia
 */
public class SQLServerDatabase extends Database
{	
	//This isn't used anymore. Safe for deletion.
	private static int instance = 0;
	
	/**
	 * Connect to the database using default settings, or overriding them with
	 * the SQLServer section from config.ini if applicable.
	 */
	public SQLServerDatabase()
	{
		url = "localhost";
		port = "1433";
		database = MainFrame.dbname;
		loadConfiguration("SQLServer");
	}
	
	public SQLServerDatabase(String dbName) {
		this();
		database = dbName;
	}

	/**
	 * @see InfoWarehouse.java#isPresent()
	 */
	public boolean isPresent() {
		return isPresentImpl("EXEC sp_helpdb");
	}

	public String getRebuildScriptFilename() {
		return "SQLServerRebuildDatabase.txt";
	}

	/**
	 * Open a connection to a MySQL database:
	 * uses the jtds driver from jtds-*.jar
	 * TODO: change security model
	 */
	public boolean openConnection() {
		return openConnectionImpl(
				//Use this string to connect to the default SQL Server 2005 instance
				"jdbc:jtds:sqlserver://localhost;databaseName=SpASMSdb;SelectMethod=cursor;",
				//Use this string to connect to a SQL Server Express instance
				//"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;databaseName=SpASMSdb;SelectMethod=cursor;",
				"SpASMS",
				"finally");

	}
	public boolean openConnection(String s) {
		return openConnectionImpl(
				//Use this string to connect to the default SQL Server 2005 instance
				"jdbc:jtds:sqlserver://localhost;databaseName="+s+";SelectMethod=cursor;",
				//Use this string to connect to a SQL Server Express instance
				//"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;databaseName="+s+";SelectMethod=cursor;",
				"SpASMS",
				"finally");

	}
	/**
	 * Open a connection to a MySQL database:
	 * uses the jtds driver from jtds-*.jar
	 * TODO: change security model
	 */
	public boolean openConnectionNoDB() {
		return openConnectionImpl(
				//Use this string to connect to the default SQL Server 2005 instance
				"jdbc:jtds:sqlserver://localhost;SelectMethod=cursor;",
				//Use this string to connect to a SQL Server Express instance
				//"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;SelectMethod=cursor;",
				"SpASMS",
				"finally");

	}
	
	/**
	 * @return the SQL Server native DATETIME format
	 */
	public DateFormat getDateFormat() {
		return new SimpleDateFormat("MM-dd-yy HH:mm:ss");
	}
	
	/**
	 * @return a BatchExecuter that uses StringBuilder concatenation
	 * to build queries. According to earlier documentation,  this is 
	 * faster than equivalent addBatch() and executeBatch()
	 */
	protected BatchExecuter getBatchExecuter(Statement stmt) {
		return new StringBatchExecuter(stmt);
	}
	
	protected class StringBatchExecuter extends BatchExecuter {
		private StringBuilder sb;
		public StringBatchExecuter(Statement stmt) {
			super(stmt);
			sb = new StringBuilder();
		}
		
		public void append(String sql) throws SQLException  {
			if (sql.endsWith("\n"))
				sb.append(sql);
			else {
				sb.append(sql);
				sb.append("\n");
			}
		}

		public void execute() throws SQLException {
			stmt.execute(sb.toString());
		}
	}

	/**
	 * @return a SQL Server BulkInserter that reads from comma-delimited bulk files
	 */
	protected BulkInserter getBulkInserter(BatchExecuter stmt, String table) {
		return new BulkInserter(stmt, table) {
			protected String getBatchSQL() {
				return "BULK INSERT " + table + " FROM '" + tempFile.getAbsolutePath() + "' WITH (FIELDTERMINATOR=',')";
			}
		};
	}

	/**
	 * insertParticle takes a string of dense info, a string of sparse info,
	 * the collection, the datasetID and the nextID and inserts the info
	 * into the dynamic tables based on the collection's datatype.
	 * @param dense - string of dense info
	 * @param sparse - string of sparse info
	 * @param collection - current collection
	 * @param datasetID - current datasetID
	 * @param nextID - next ID
	 * @param importing - true if importing, false if inserting for other reason
	 * @return nextID if successful
	 */
	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
							  Collection collection,
							  int datasetID, int nextID, boolean importing)
	{
		//System.out.println("next AtomID: "+nextID);
		try {
			Statement stmt = con.createStatement();
			//System.out.println("Adding batches");
			BatchExecuter sql = getBatchExecuter(stmt);
			sql.append("INSERT INTO " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + " VALUES (" +
					nextID + ", " + dense + ")");
			sql.append("INSERT INTO AtomMembership " +
					"(CollectionID, AtomID) " +
					"VALUES (" +
					collection.getCollectionID() + ", " +
					nextID + ")\n");
			sql.append("INSERT INTO DataSetMembers " +
					"(OrigDataSetID, AtomID) " +
					"VALUES (" +
					datasetID + ", " +
					nextID + ")\n");

			String tableName = getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype());

			Inserter bi = getBulkInserter(sql, tableName);
			for (ATOFMSPeak peak : sparse) {
				bi.append(nextID + "," + peak.toCommaDelimitedString());

			}
			bi.close();

			sql.execute();

			stmt.close();
			bi.cleanUp();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception inserting atom.  Please check incoming data for correct format.");
			System.err.println("Exception inserting particle.");
			e.printStackTrace();

			return -1;
		}
		if (!importing)
			updateInternalAtomOrder(collection);
		else
			addInternalAtom(nextID, collection.getCollectionID());
		return nextID;
	}


}