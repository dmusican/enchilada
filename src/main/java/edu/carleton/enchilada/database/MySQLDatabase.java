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
 * The Original Code is EDAM Enchilada's MySQLDatabase class.
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

package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.gui.MainFrame;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Makes database work with MySQL
 * @author shaferia
 */
public class MySQLDatabase extends Database {
	
	/**
	 * Connect to the database using default settings, or overriding them with
	 * the MySQL section from config.ini if applicable.
	 */
	public MySQLDatabase() {
		url = "localhost";
		port = "3306";
		database = MainFrame.dbname;
		loadConfiguration("MySQL");
	}
	
	public MySQLDatabase(String dbName) {
		this();
		database = dbName;
	}
	
	/**
	 * @see InfoWarehouse.java#isPresent()
	 */
	public boolean isPresent() {
		return isPresentImpl("SHOW DATABASES");
	}

	public String getRebuildScriptFilename() {
		return "MySQLRebuildDatabase.txt";
	}

	/**
	 * Open a connection to a MySQL database:
	 * uses the jdbc driver from mysql-connector-java-*-bin.jar
	 * TODO: change security model
	 */
	public boolean openConnection() {
		return openConnectionImpl(
				"jdbc:mysql://" + url + ":" + port + "/" + database,
				"root",
				"sa-account-password");
	}
	
	/**
	 * Open a connection to a MySQL database:
	 * uses the jdbc driver from mysql-connector-java-*-bin.jar
	 * TODO: change security model
	 */
	public boolean openConnectionNoDB() {
		return openConnectionImpl(
				"jdbc:mysql://" + url + ":" + port,
				"root",
				"sa-account-password");
	}
	
	/**
	 * @return the MySQL native DATETIME format
	 */
	public DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}
	
	/**
	 * @return a BatchExecuter that completes batches of commands with
	 * addBatch() and executeBatch(), as MySQL does not support multi-command strings
	 */
	protected BatchExecuter getBatchExecuter(Statement stmt) {
		return new BatchBatchExecuter(stmt);
	}
	
	protected class BatchBatchExecuter extends BatchExecuter {
		public BatchBatchExecuter(Statement stmt) {
			super(stmt);
		}
		
		public void append(String sql) throws SQLException {
			stmt.addBatch(sql);
		}

		public void execute() throws SQLException {
			stmt.executeBatch();
		}
	}
	
	/**
	 * @return a BulkInserter that provides MySQL with a way to read SQL Server - formatted bulk files.
	 */
	protected Inserter getBulkInserter(BatchExecuter stmt, String table) {
		return new BulkInserter(stmt, table) {
			protected String getBatchSQL() {
				return "LOAD DATA INFILE '" + tempFile.getAbsolutePath().replaceAll("\\\\", "\\\\\\\\") 
					+ "' INTO TABLE " + table + 
					" FIELDS TERMINATED BY ','" +
					" ENCLOSED BY '\\''" + 
					" ESCAPED BY '\\\\'";
			}
		};
	}
	
	/**
	 * Use to indicate that a method isn't complete - 
	 * goes one frame up on the stack trace for information.
	 */
	public void notdone() {
		StackTraceElement sti = null;
		try {
			throw new Exception();
		}
		catch (Exception ex) {
			sti = ex.getStackTrace()[1];
		}
		
		String message = "Not done: ";
		message += sti.getClassName() + "." + sti.getMethodName();
		message += "(" + sti.getFileName() + ":" + sti.getLineNumber() + ")";
		System.err.println(message);
	}

	public boolean openConnection(String dbName) {
		// TODO Auto-generated method stub
		return false;
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
							  int datasetID, int nextID, boolean importing) {
		throw new UnsupportedOperationException();
	}


}
