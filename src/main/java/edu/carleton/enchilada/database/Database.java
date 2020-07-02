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

package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.AMSPeak;
import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.ATOFMS.Peak;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.DummyNormalizer;
import edu.carleton.enchilada.analysis.Normalizer;
import edu.carleton.enchilada.analysis.clustering.ClusterInformation;
import edu.carleton.enchilada.analysis.clustering.PeakList;
import edu.carleton.enchilada.atom.ATOFMSAtomFromDB;
import edu.carleton.enchilada.collection.AggregationOptions;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.gui.LabelingIon;
import edu.carleton.enchilada.gui.ProgressBarWrapper;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSetWriter;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.mssql.InsertIdentityOperation;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/* 
 * Maybe a good way to re-factor this file is to separate out methods that
 * are used by importers from those used by clustering code, and so on.
 * It might work well, or it might not...
 */

/**
 * Encapsulates InfoWarehouse functionality for a relational database
 * @author andersbe, shaferia
 * 
 * Mass spectrum querying added by Michael Murphy, University of Toronto, 2013.
 * 
 */
public abstract class Database {
    private static final String accessDBURLPrefix = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb)};DBQ=";
    private static final String accessDBURLSuffix = ";READONLY=false}";

	protected Connection con;
	protected String url;
	protected String port;
	protected String database;
	
	//the name of this database, for debugging and error reporting purposes
	private String dbType;
	
	// for batch stuff
	private Statement batchStatement;
	private ArrayList<Integer> alteredCollections;
	private PreparedStatement bulkInsertStatementAtomMembership;
	private PreparedStatement bulkInsertStatementInternalAtomOrder;

	protected boolean isDirty = false;
	public boolean isDirty(){
		return isDirty;
	}

	private int randomSeed = 0;

	/**
	 * Construct an instance of either SQLServerDatabase or MySQLDatabase
	 * @param dbname the name of the database to use (SpASMSdb, TestDB, etc)
	 * @return an InfoWarehouse backed by a relational database
	 */
	public static Database getDatabase(String dbName) {
		return new SQLiteDatabase(dbName);
	}
	public String getDatabaseName(){
		return database;
	}
	/**
	 * Construct an instance of either SQLServerDatabase or MySQLDatabase
	 * @return an InfoWarehouse backed by a relational database
	 */
	public static Database getDatabase() {
		return new SQLiteDatabase();
	}
	
	/**
	 * Load information from config.ini for the database with the given name
	 * @param dbname the name of the database (MSDE, MySQL) to use
	 */
	protected void loadConfiguration(String dbname) {
		dbType = dbname;
		File f = new File("config.ini");
		try {
			Scanner scan = new Scanner(f);
			while (scan.hasNext()) {
				String tag = scan.next();
				String val = scan.next();
				if (scan.hasNext())
					scan.nextLine();
				
				if (tag.equalsIgnoreCase("db_url:")) { url = val; }
				else if (tag.equalsIgnoreCase(dbname + "_db_port:")) { port = val; }
			}
			scan.close();
		} catch (FileNotFoundException e) { 
			// Don't worry if the file doesn't exist... 
			// just go on with the default values 
		}
	}
	
	public String getName() {
		return dbType;
	}
	
	public void clearCache(){
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("dbcc freeproccache");
			System.out.println("db cache cleared");
		}
		catch (Exception e1) {
			System.out.println("Error clearing the database cache.");
		}
	}
	
	/**
	 * Find if the database is present
	 * @param command the SQL to get a list of databases
	 * @return true if present
	 */
	protected boolean isPresentImpl(String command) {
		boolean foundDatabase = false;
		String testdb = database;
		try {
			database = "";
			openConnectionNoDB();
			Connection con = getCon();
			Statement stmt = con.createStatement();

			// See if database exists.
			ResultSet rs = stmt.executeQuery(command);
			while (!foundDatabase && rs.next()) {
				if (rs.getString(1).equalsIgnoreCase(testdb))
					foundDatabase = true;
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.displayException(null,"Error in testing if "+testdb+" is present.");
		}
		database = testdb;
		return foundDatabase;
	}
	
	/**
	 * Retrieve the {@link java.sql.Connection Connection} for this database
	 */
	public Connection getCon() {
		return con;
	}
	
	/**
	 * Open a connection to the database
	 * @param connectionstr the connection string to be used with DriverManager.getConnection
	 * @param user username
	 * @param pass password
	 * @return true on success
	 */
	protected boolean openConnectionImpl(String connectionstr, String user, String pass) {
		con = null;
		try {
			con = DriverManager.getConnection(connectionstr, user, pass);
			con.setAutoCommit(true);
			//ResultSet rs = con.createStatement().executeQuery("SELECT db_name()");
			//rs.next();
			//System.out.println("use database: "+rs.getString(1));
			
		} catch (Exception e) {
			ErrorLogger.writeExceptionToLogAndPrompt("Database","Failed to establish a connection to " + database);
			System.err.println("Failed to establish a connection to database");
			System.err.println(e);
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Close the connection to this database
	 * @return true on success
	 */
	public boolean closeConnection()
	{
		if (con != null)
		{
			try {
				con.close();
			} catch (Exception e) {
				ErrorLogger.writeExceptionToLogAndPrompt("Database","Could not close the connection to " + database);
				System.err.println("Could not close the connection: ");
				System.err.println(e);
				return false;
			}
			return true;
		}
		else
			return false;
	}

	/**
	 * Opens a connection to the database, flat file, memory structure,
	 * or whatever you're working with.
	 * @return true on success
	 */
	public abstract boolean openConnection();

	/**
	 * Opens a connection to the database, flat file, memory structure,
	 * or whatever you're working with.
	 * @return true on success
	 */
	public abstract boolean openConnectionNoDB();

	/**
	 * @return true if this resource is available for use
	 */
	public abstract boolean isPresent();

	/**
	 * Get the date format used by this InfoWarehouse
	 * @return a DateFormat object that can format Dates inserted into this InfoWarehouse.
	 */
	public abstract DateFormat getDateFormat();

	public abstract boolean openConnection(String dbName);

	public abstract String getRebuildScriptFilename();

	public abstract int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
									   Collection collection,
									   int datasetID, int nextID, boolean importing);

	/**
	 * Abstract representation of something that adds and executes batches.
	 * Used since SQL Server can execute batches more quickly with StringBuilders -
	 * 	but these are incompatible in MySQL.
	 * @author shaferia
	 */
	protected abstract class BatchExecuter {
		protected Statement stmt;
		
		/**
		 * Create a BatchExecuter that will add to the given statement
		 * @param stmt the statement to add SQL to
		 */
		public BatchExecuter(Statement stmt) {
			this.stmt = stmt;
		}
		
		/**
		 * Add an SQL string to the statement
		 * @param sql the query to add
		 */
		public abstract void append(String sql) throws SQLException;
		
		/**
		 * Execute all SQL held in this BatchExecuter
		 */
		public abstract void execute() throws SQLException;
	}
	
	protected abstract class Inserter {
		protected BatchExecuter stmt;
		protected String table;
		
		public Inserter(BatchExecuter stmt, String table) {
			this.stmt = stmt;
			this.table = table;
		}
		
		/**
		 * Add values to the series of INSERT commands or bulk file that
		 * this Inserter will insert
		 * @param values the delimited set of values to insert
		 * @throws SQLException
		 */
		public abstract void append(String values) throws SQLException;
		
		/**
		 * @return free any resources used by this Inserter
		 */
		public abstract void close();
		
		/**
		 * Get rid of any files or other system stuff we've created.
		 */
		public abstract void cleanUp();
	}
	
	/**
	 * Executes bulk insert statements using bulk files
	 * @author shaferia
	 */
	protected abstract class BulkInserter extends Inserter {
		protected File tempFile;
		protected BufferedWriter file;
		
		public BulkInserter(BatchExecuter stmt, String table) {
			super(stmt, table);
			try {
				//tempFile = File.createTempFile("bulkfile", ".txt");
				tempFile = new File("TEMP"+File.separator+"bulkfile"+".txt");
				tempFile.deleteOnExit();
				file = new BufferedWriter(new FileWriter(tempFile));
			}
			catch (IOException ex) {
				System.err.println("Couldn't create bulk file " + tempFile.getAbsolutePath() + "" + 
						" for table " + table);
				ex.printStackTrace();
			}
			try {
				stmt.append(getBatchSQL());
			}
			catch (SQLException ex) {
				System.err.println("Couldn't attach bulk SQL for table " + table);
				ex.printStackTrace();
			}
		}
		
		/**
		 * @return the SQL string needed to import the batch file
		 */
		protected abstract String getBatchSQL();
		
		public void append(String values) throws SQLException {
			try {
				file.write(values);
				file.newLine();
			}
			catch (IOException ex) {
				throw new SQLException("Couldn't write to file: " + tempFile.getAbsolutePath());
			}
		}
		
		public void close() {
			try {
				file.close();
			}
			catch (IOException ex) {
				System.err.println("Couldn't close bulk file " + tempFile.getAbsolutePath() +
						" for table " + table);
				ex.printStackTrace();
			}
		}
		
		public void cleanUp() {
			tempFile.delete();
		}
	}
	
	/**
	 * Basic BulkBucket class to construct a flat file and to prepare statement for Bulk Inserting.
	 * @author SLH
	 */
	public class BulkBucket
	{
		protected BufferedWriter file;
		protected String table;
		protected File tempFile;
		
	    public BulkBucket(String tableName) throws SQLException {

			table = tableName;

			try {
				tempFile = File.createTempFile(table, ".txt");
//				tempFile = new File("TEMP"+File.separator+"table"+".txt");
				tempFile.deleteOnExit();
				System.out.println(tempFile);
				file = new BufferedWriter(new FileWriter(tempFile));
			}
			catch (IOException ex) {
				System.err.println("Couldn't create bulk file " + table +
						".txt for table " + table);
				ex.printStackTrace();
			}
		}

		public void append(String values) throws SQLException {
			try {
				file.write(values);
				file.newLine();
			}
			catch (IOException ex) {
				throw new SQLException("Couldn't write to file: " + tempFile.getAbsolutePath());
			}
		}

		public void close() {
			try {
				file.close();
			}
			catch (IOException ex) {
				System.err.println("Couldn't close bulk file " + tempFile.getAbsolutePath() +
						" for table " + table);
				ex.printStackTrace();
			}
		}

		public String sqlCmd() {
			return "BULK INSERT " + table + " FROM '" + tempFile.getAbsolutePath() + "' WITH (FIELDTERMINATOR=',')\n";
		}
		
		public void cleanUp() {
			tempFile.delete();
		}
	}
	
	/**
	 * Data_bulkBucket class constructs mutiple flat files for Bulk Inserting AMS/ATOFMS/Time Series particle data.
	 * It extends BulkBucket class for mutiple data source insertion. 
	 * @author SLH
	 */
	public class Data_bulkBucket
	{
		String[] tables;
		PreparedStatement[] buckets;

		public Data_bulkBucket(String[] tables){

			this.tables = tables;
			buckets = new PreparedStatement[tables.length];

			try {
				for(int i = 0; i<tables.length; i++)
					if (tables[i].equals("ATOFMSAtomInfoDense"))
						buckets[i] = con.prepareStatement("INSERT INTO ATOFMSAtomInfoDense VALUES(?,?,?,?,?,?);");
					else if (tables[i].equals("ATOFMSAtomInfoSparse"))
						buckets[i] = con.prepareStatement("INSERT INTO ATOFMSAtomInfoSparse VALUES(?,?,?,?,?);");
					else if (tables[i].equals("AMSAtomInfoDense"))
						buckets[i] = con.prepareStatement("INSERT INTO AMSAtomInfoDense VALUES(?,?);");
					else if (tables[i].equals("AMSAtomInfoSparse"))
						buckets[i] = con.prepareStatement("INSERT INTO AMSAtomInfoSparse VALUES(?,?,?);");
					else if (tables[i].equals("AtomMembership"))
						buckets[i] = con.prepareStatement("INSERT INTO AtomMembership VALUES(?,?);");
					else if (tables[i].equals("DataSetMembers"))
						buckets[i] = con.prepareStatement("INSERT INTO DataSetMembers VALUES(?,?);");
					else if (tables[i].equals("InternalAtomOrder"))
						buckets[i] = con.prepareStatement("INSERT INTO InternalAtomOrder VALUES(?,?);");
					else
						throw new UnsupportedOperationException("Unknown type of data: " + tables[i]);
			}
			catch(SQLException e) {
				throw new ExceptionAdapter(e);
			}
		}

		public void executeBuckets() {
			try {
				con.setAutoCommit(false);
				for (PreparedStatement bucket : buckets)
					bucket.executeBatch();
				con.commit();
				con.setAutoCommit(true);
			} catch (SQLException throwables) {
				throw new ExceptionAdapter(throwables);
			}
		}

		public void close() {
			try {
				for(int i = 0; i<tables.length; i++)
					buckets[i].close();
			} catch (SQLException e) {
				throw new ExceptionAdapter(e);
			}
		}
	}


	/**
	 * @param the names of the database tables for different type of particle data
	 * @return Data_bulkBucket 
	 * @author SLH
	 */
	public Data_bulkBucket getDatabulkBucket(String[] tables) {
		return new Data_bulkBucket(tables);
	}

	/**
	 * BulkInsertDataParticles executes the particle data bulk insertion batch statements
	 * @param bigBucket - Data_bulkBucket object holding different datatype of particle data
	 * @return  
	 * @author SLH
	 */
    public void BulkInsertDataParticles(Data_bulkBucket bigBucket)
	{
		bigBucket.executeBuckets();
	}
    
	/**
	 * saveDataParticle takes a string of dense info, a string of sparse info, 
	 * the collection, the datasetID and the nextID and Prepare temporary files 
	 * for bulk insertion into the dynamic tables based on the collection's datatype. 
	 * @param dense - string of dense info
	 * @param sparse - string of sparse info
	 * @param collection - current collection
	 * @param datasetID - current datasetID
	 * @param nextID - next ID
	 * @return nextID if successful
	 * @author SLH
	 */
	public int saveDataParticle(String dense, ArrayList<String> sparse,
			Collection collection,
			int datasetID, int nextID, Data_bulkBucket bigBucket)
	{
		int num_tables = bigBucket.tables.length;
		String commaDelimitedRegex = "\\s*,\\s*";
		try {
			for(int i =0; i<num_tables; i++) {
				PreparedStatement bucket = bigBucket.buckets[i];
				if(bigBucket.tables[i].equals("ATOFMSAtomInfoDense")) {
					String[] delimitedValues = dense.split(commaDelimitedRegex);
					bucket.setInt(1, nextID);                                    // atomid
					bucket.setString(2, delimitedValues[0]);                         // timestamp
					bucket.setDouble(3, Double.parseDouble(delimitedValues[1]));     // laserpower
					bucket.setDouble(4, Double.parseDouble(delimitedValues[2]));     // size
					bucket.setInt(5, Integer.parseInt(delimitedValues[3]));          // scatdelay
					bucket.setString(6, delimitedValues[4]);                         // origfilename
					bucket.addBatch();
				}

				else if(bigBucket.tables[i].equals("ATOFMSAtomInfoSparse")) {
					for (String row: sparse) {
						String[] delimitedValues = row.split(commaDelimitedRegex);
						bucket.setInt(1, nextID);                                    // atomid
						bucket.setDouble(2, Double.parseDouble(delimitedValues[0]));                         // peaklocation
						bucket.setInt(3, Integer.parseInt(delimitedValues[1]));     // peakarea
						bucket.setDouble(4, Double.parseDouble(delimitedValues[2]));     // relpeakarea
						bucket.setInt(5, Integer.parseInt(delimitedValues[3]));          // peakheight
						bucket.addBatch();
					}
				}

				else if(bigBucket.tables[i].equals("AMSAtomInfoDense")) {
					String[] delimitedValues = dense.split(commaDelimitedRegex);
					bucket.setInt(1, nextID);                // atom id
					bucket.setString(2, delimitedValues[0]);     // timestamp
					bucket.addBatch();
				}

				else if(bigBucket.tables[i].equals("AMSAtomInfoSparse")) {
					for (String row: sparse) {
						String[] delimitedValues = row.split(commaDelimitedRegex);
						bucket.setInt(1, nextID);                                       // atomid
						bucket.setDouble(2, Double.parseDouble(delimitedValues[0]));    // peaklocation
						bucket.setDouble(3, Double.parseDouble(delimitedValues[1]));    // relpeakarea
						bucket.addBatch();
					}
				}

				else if(bigBucket.tables[i].equals("AtomMembership")) {
					bucket.setInt(1, collection.getCollectionID());
					bucket.setInt(2, nextID);
					bucket.addBatch();
				}

				else if(bigBucket.tables[i].equals("DataSetMembers")) {
					bucket.setInt(1, datasetID);
					bucket.setInt(2, nextID);
					bucket.addBatch();
				}

				else if(bigBucket.tables[i].equals("InternalAtomOrder")) {
					bucket.setInt(1, nextID);
					bucket.setInt(2, collection.getCollectionID());
					bucket.addBatch();
				}

				else {
					throw new UnsupportedOperationException();
				}
			}
		
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception inserting atom.  Please check incoming data for correct format.");
			System.err.println("Exception inserting particle.");
			e.printStackTrace();

			return -1;
		}
		return nextID;	// this is ignored.
	}
	
	/**
	 * Executes bulk insert statements with a sequence of INSERTs.
	 * @author shaferia
	 */
	protected class BatchInserter extends Inserter {
		public BatchInserter(BatchExecuter stmt, String table) {
			super(stmt, table);
		}
		
		public void append(String values) throws SQLException {
			stmt.append("INSERT INTO " + table + " VALUES(" + values + ")");
		}
		
		public void close() {}
		
		public void cleanUp() {}
	}
	
	/**
	 * rebuilds the database; sets the static tables.
	 * @param dbName
	 * @return true if successful
	 */
	public boolean rebuildDatabase(String dbName) {
		boolean success = dropDatabase(dbName);
		if (!success) {
			ErrorLogger.writeExceptionToLogAndPrompt(dbName, "Error rebuilding database (dropping, in particular).");
			throw new RuntimeException("Error in rebuilding database.");
		}
		Database blankDb = Database.getDatabase("");
		blankDb.createDatabaseCommands(dbName);

		Scanner in = null;
		Connection con = null;

		// Run all the queries in the SQLServerRebuildDatabase.txt file, which
		// inserts all of the necessary tables.
		Database db = Database.getDatabase(dbName);
		try {
			db.openConnection(dbName);
			con = db.getCon();
			in = new Scanner(new File(db.getRebuildScriptFilename()));
			String query = "";
			StringTokenizer token;
			// loop through license block
			while (in.hasNext()) {
				query = in.nextLine();
				token = new StringTokenizer(query);
				if (token.hasMoreTokens()) {
					String s = token.nextToken();
					if (s.equals("CREATE"))
						break;
				}
			}
			// Update the database according to the stmts.
			con.createStatement().executeUpdate(query);
			
			while (in.hasNext()) {
				query = in.nextLine();
				//System.out.println(query);
				con.createStatement().executeUpdate(query);
			}
			
		} catch (IOException | SQLException e) {
			throw new ExceptionAdapter(e);
		} finally {
			if (db != null)
				db.closeConnection();
			if (in != null)
				in.close();
			
		}
		return true;
	}

	public void createDatabaseCommands(String dbName) {
		// Connect to SQL Server independent of a particular database,
		// and drop and add the database.
		// This code works under the assumption that a user called SpASMS has
		// already been created with a password of 'finally'. This user must have
		// already been granted appropriate privileges for adding and dropping
		// databases and tables.
		Database blankDb = Database.getDatabase("");
		try {
			blankDb.openConnectionNoDB();
			Statement stmt = blankDb.getCon().createStatement();
			//TODO-POSTGRES
			//stmt.executeUpdate("create database " + dbName);
			stmt.executeUpdate("create database \"" + dbName + "\"");
			//String sql = "ALTER DATABASE "+dbName+" SET RECOVERY SIMPLE";
			//stmt.executeUpdate(sql);
			//TODO-POSTGRES
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(blankDb.getName(),"Error rebuilding database.");
			System.err.println("Error in rebuilding database.");
			throw new ExceptionAdapter(e);
		} finally {
			blankDb.closeConnection();
		}
	}

	/**
	 * Drops the given database.
	 * @param dbName the database to drop
	 * @return
	 */
	public static boolean dropDatabase(String dbName) {
		Database db = null;
		Connection con = null;
		
		// Connect to SQL Server independent of a particular database,
		// and drop and add the database.
		// This code works under the assumption that a user called SpASMS has
		// already been created with a password of 'finally'. This user must have
		// already been granted appropriate privileges for adding and dropping
		// databases and tables.
		try {
			db = Database.getDatabase(dbName);
			db.dropDatabaseCommands();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(db.getName(),"Error dropping database.");
			throw new ExceptionAdapter(e);
		} finally {
			if (db != null)
				db.closeConnection();
		}
		return true;
	}

	/**
	 * Specific commands to drop database.
	 * @throws SQLException
	 */
	public void dropDatabaseCommands() throws SQLException {
		Connection con;
		if (isPresent()) {
			con = getCon();
			Statement stmt = con.createStatement();
			//stmt.executeUpdate("drop database " + dbName);
			stmt.executeUpdate("drop database \"" + getDatabaseName() + "\"");
			//TODO-POSTGRES
			stmt.close();
		}
		//TODO-POSTGRES
		con = getCon();
		Statement stmt = con.createStatement();
		stmt.executeUpdate("DROP DATABASE IF EXISTS \"" + getDatabaseName() + "\"");
		stmt.close();
		//TODO-POSTGRES
	}

	/**
	 * Create an index on some part of AtomInfoDense for a datatype.  Possibly
	 * useful if you're going to be doing a *whole lot* of queries on a
	 * particular field or set of fields.  For syntax in the fieldSpec, look
	 * at an SQL reference.  If it's just one field, just put the name of the
	 * column there.
	 * 
	 * @author smitht
	 * @return true if the index was successfully created, false otherwise.
	 */
	public boolean createIndex(String dataType, String fieldSpec) {
		String table = getDynamicTableName(DynamicTable.AtomInfoDense, dataType);
		
		String indexName = "index_" + fieldSpec.replaceAll("[^A-Za-z0-9]","");
		String s = "CREATE INDEX " + indexName + " ON " + table 
			+ " (" + fieldSpec + ")";
		
		try {
			Statement stmt = con.createStatement();
			boolean ret = stmt.execute(s);
			stmt.close();
			return true;
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * Returns a list of indexed columns in an AtomInfoDense table.
	 * 
	 * @author smitht
	 */
	public Set<String> getIndexedColumns(String dataType) throws SQLException {
		Set<String> indexed = new HashSet<String>();
		
		String table = this.getDynamicTableName(DynamicTable.AtomInfoDense, dataType);
		
		Statement stmt = con.createStatement();
		ResultSet r = stmt.executeQuery("EXEC sp_helpindex " + table);
		
		String[] tmp;
		while (r.next()) {
			tmp = r.getString("index_keys").split(", ");
			for (int i = 0; i < tmp.length; i++) {
				indexed.add(tmp[i]);
			}
		}
		
		r.close();
		stmt.close();
		
		return indexed;
	}
	
	/**
	 * Returns the adjacent atom for the collection, according to InternalAtomOrder.
	 * @param collection	The ID of the collection under scrutiny.
	 * @param currentID		The current atom's ID.
	 * @param position		1 for next atomID, -1 for previous atomID.
	 * @return	index[0] The ID of the adjacent atom in the collection.
	 * 			index[1] The position of the adjacent atom in the collection.
	 */
// COMMENTED OUT BECAUSE WE DON'T USE THIS FOR ANYTHING AND IT'S NOT RELIABLE ANYWAY - jtbigwoo
//	public int[] getAdjacentAtomInCollection(int collection, int currentID, int position){
//		int nextID = -99;
//		int pos = -77;
//		String query = "";
//		//have to deal with which window ("page" of collection) we're starting in
//		int startingID = getFirstAtomInCollection(getCollection(collection));
//		
//		//we want the starting id of the window in which this atom lives
//		int i = currentID / 1000;
//		for (int j = 0; j<i; j++)
//			if ( !((startingID + 1000) >= currentID) )
//				startingID +=1000;
//			
//		//if looking for previous atom
//		if (position <0){
//			query = "SELECT MAX(AtomID) FROM InternalAtomOrder " +
//			"WHERE (CollectionID = " + collection + ") AND (AtomID < " + 
//			currentID + ")";
//		} else if (position >0){	//if looking for next atom
//			query = "SELECT MIN(AtomID)FROM InternalAtomOrder " +
//			"WHERE (CollectionID = " + collection + ") AND (AtomID > " +
//			currentID + ")";
//		}
//		
//		Statement stmt;
//		try {
//			stmt = con.createStatement();
//			ResultSet rs = stmt.executeQuery(query);
//			rs.next();
//			if (rs.getInt(1) > 0){
//				nextID = rs.getInt(1);
//				pos = ((nextID - startingID)+1); //calculate the row number 
//			}
//			stmt.close();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		return new int[]{nextID, pos};
//	}
	
	/**
	 * @author steinbel
	 * Updates InternalAtomOrder
	 * 
	 * @param atomID
	 * @param toParentID
	 */	
	public void addSingleInternalAtomToTable(int atomID, int toParentID) {
		//update InternalAtomOrder; have to iterate through all
		// atoms sequentially in order to insert it. 
		Statement stmt;
		boolean exists = false;
		try {
			con.setAutoCommit(false);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT AtomID FROM" +
					" InternalAtomOrder WHERE CollectionID = "+toParentID + " ORDER BY AtomID");

			while (rs.next()){
				if (rs.getInt(1) == atomID)
					exists = true;
			}
				
			if (!exists){
				stmt.addBatch("INSERT INTO InternalAtomOrder VALUES ("
							+atomID+","+toParentID+")");	
				stmt.executeBatch();
			}
			con.commit();
			con.setAutoCommit(true);
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Deletes a collection and unlike orphanAndAdopt() also recursively
	 * deletes all direct descendents.
	 * This method merely selects the collections to be deleted and stores them in #deleteTemp
	 * 
	 * @param progressBar
	 * @return true on success. 
	 */
	public boolean compactDatabase(ProgressBarWrapper progressBar)
	{
		try {
			con.setAutoCommit(false);
			Statement stmt = con.createStatement();
			Statement typesStmt = con.createStatement();
			stmt.execute("DROP TABLE IF EXISTS temp.CollectionsToCompact;\n");
			stmt.execute("CREATE TEMPORARY TABLE CollectionsToCompact (CollectionID int, \n PRIMARY KEY([CollectionID]));\n");
			// Update the InternalAtomOrder table:  Assumes that subcollections
			// are already updated for the parentCollection.
			// clear InternalAtomOrder table of the deleted collection and all subcollections.

			stmt.execute("INSERT INTO temp.CollectionsToCompact (CollectionID) \n" +
					"   SELECT DISTINCT CollectionID\n" +
					"   FROM AtomMembership\n" +
					"   WHERE AtomMembership.CollectionID NOT IN " +
					"      (SELECT CollectionID\n" +
					"      FROM Collections\n" +
					"      );\n");


			stmt.execute("DELETE FROM AtomMembership\n"
					+ "WHERE CollectionID IN (SELECT * FROM temp.CollectionsToCompact);\n");

			stmt.execute("DELETE FROM InternalAtomOrder\n"
					+ "WHERE CollectionID IN (SELECT * FROM temp.CollectionsToCompact);\n");

			stmt.execute("DROP TABLE IF EXISTS temp.AtomsToCompact;\n");
			stmt.execute("CREATE TEMPORARY TABLE temp.AtomsToCompact (AtomID int, \n PRIMARY KEY([AtomID]));\n");

			ResultSet types = typesStmt.executeQuery("SELECT DISTINCT Datatype FROM MetaData");

			while(types.next()){

				String datatype = types.getString(1);
				String sparseTableName = getDynamicTableName(DynamicTable.AtomInfoSparse,datatype);
				String denseTableName = getDynamicTableName(DynamicTable.AtomInfoDense,datatype);

				stmt.addBatch("DELETE FROM temp.AtomsToCompact;\n");
				stmt.addBatch("INSERT INTO temp.AtomsToCompact (AtomID) \n" +
						"	SELECT AtomID\n" +
						"	FROM "+denseTableName+"\n" +
						"	WHERE AtomID NOT IN " +
						"		(SELECT AtomID\n" +
						"		FROM AtomMembership\n" +
						"		);\n");

				// Also: We could delete all the particles from the particles
				// table IF we want to by now going through the particles 
				// table and choosing every one that does not exist in the 
				// Atom membership table and deleting it.  However, this would
				// remove particles that were referenced in the DataSetMembers 
				// table.  If we don't want this to happen, comment out the 
				// following code, which also removes all references in the 
				// DataSetMembers table:
				//System.out.println(1);
				stmt.addBatch("DELETE FROM DataSetMembers\n" +
				"WHERE AtomID IN (SELECT * FROM temp.AtomsToCompact);\n");
				// it is ok to call atominfo tables here because datatype is
				// set from recursiveDelete() above.
				// note: Sparse table may not necessarily exist. So check first.
				Statement existsStmt = con.createStatement();
				ResultSet sparseTablePresent = existsStmt.executeQuery(
						"SELECT * FROM sqlite_master WHERE tbl_name= '" + sparseTableName + "' AND type='table'");
				if (sparseTablePresent.next()) {
					stmt.addBatch("DELETE FROM " + sparseTableName + "\n" +
							"WHERE " + sparseTableName + ".AtomID IN temp.AtomsToCompact");
				}
				sparseTablePresent.close();
				stmt.addBatch("DELETE FROM " + denseTableName + "\n" +
						"WHERE " + denseTableName + ".AtomID IN temp.AtomsToCompact");

				//This separation is necessary!!
				// SQL Server parser is stupid and if you create, delete, and recreate a temporary table
				// the parser thinks you're doing something wrong and will die.
				if(progressBar.wasTerminated()){
					stmt.clearBatch();
					stmt.addBatch("DROP TABLE temp.CollectionsToCompact;\n");
					typesStmt.close();
					stmt.executeBatch();
					con.commit();
					con.setAutoCommit(true);
					stmt.close();
					return false;
				}
				stmt.executeBatch();
				stmt.clearBatch();
			}

			types.close();
			stmt.execute("DROP TABLE temp.CollectionsToCompact;\n");
			stmt.execute("DROP TABLE temp.AtomsToCompact;\n");

			isDirty = false;
			con.commit();
			con.setAutoCommit(true);
			stmt.close();
			//updateAncestors(0);
		} catch (Exception e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Exception deleting collection.");
			System.err.println("Exception deleting collection: ");
			throw new ExceptionAdapter(e);
		}
		return true;
	}
	
	/**
	 * This method, used by VersionChecker, returns the version string that the
	 * database contains, which hopefully corresponds to the structure of the
	 * database.
	 * @return the version string, or "No database version" if the database is from before version strings.
	 * @throws IllegalStateException
	 */
	public String getVersion() {
		String version;
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT Value FROM DBInfo WHERE Name = 'Version'");
			if (! rs.next()) {
				throw new Exception("Inconsistent DB State?");
				// no version in DB (though this shouldn't happen?)
			} else {
				version = rs.getString(1);
			}
		} catch (SQLException e) {
			version = "No database version";
		} catch (Exception e) {
			throw new IllegalStateException("Can't understand what state the DB is in. (has version field but no value)");
		}
		return version;
	}
	
	/**
	 * Returns a hashmap representing the hierarchy of subcollections.
	 * Each key is a parentID which hashes to an ArrayList<Integer> of its ChildIDs ordered by ChildID 
	 * @param collection
	 * @return the subcollection hierarchy represented as a HashMap
	 * @author Jamie Olson
	 */
	public HashMap<Integer,ArrayList<Integer>> getSubCollectionsHierarchy(Collection collection)
	{
		HashMap<Integer,ArrayList<Integer>> subHierarchy = new HashMap<Integer,ArrayList<Integer>>();
		HashMap<Integer,ArrayList<Integer>> completeHierarchy = new HashMap<Integer,ArrayList<Integer>>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT *\n" +
					"FROM CollectionRelationships\n" +
					"ORDER BY ParentID, ChildID");
			int parent = -1;
			ArrayList<Integer> subChildren = new ArrayList<Integer>();
			while(rs.next())
			{
				if(rs.getInt("ParentID")==parent){
					subChildren.add(new Integer(rs.getInt("ChildID")));
				}else{
					completeHierarchy.put(parent, subChildren);
					subChildren = new ArrayList<Integer>();
					parent = rs.getInt("ParentID");
					subChildren.add(new Integer(rs.getInt("ChildID")));
				}
				
			}
			completeHierarchy.put(parent, subChildren);
			
			ArrayList<Integer> allSubChildren = new ArrayList<Integer>();
			allSubChildren.add(collection.getCollectionID());
			while(!allSubChildren.isEmpty()){
				int nextParent = allSubChildren.get(0);
				if(completeHierarchy.containsKey(nextParent)){
				subHierarchy.put(nextParent, completeHierarchy.get(new Integer(nextParent)));
				allSubChildren.addAll(completeHierarchy.get(new Integer(nextParent)));
				}
				allSubChildren.remove(0);
			}
			
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception grabbing subchildren in GetImmediateSubCollections.");
			System.err.println("Exception grabbing subchildren:");
			System.err.println(e);
		}
		return subHierarchy;
	}
	
	/**
	 * gets the collection name.
	 */
	public String getCollectionName(int collectionID) {
		String name = "";
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Name\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " +
					collectionID);
			rs.next();
			name = rs.getString("Name");
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving the collection name for collectionID "+collectionID);
			System.err.println("Exception grabbing the collection name:");
			System.err.println(e);
		}
		return name;
	}
	
	/* Create Empty Collections */
	
	/**
	 * Creates an empty collection with no atomic analysis units in it.
	 * @param parent	The key to add this collection under (0 
	 * 					to add at the root).
	 * @param name		What to call this collection in the interface.
	 * @param datatype collection's datatype
	 * @param comment	A comment for this collection
	 * @return			The collectionID of the resulting collection
	 */
	public int createEmptyCollection( String datatype,
			int parent, 
			String name, 
			String comment,
			String description)
	{
		if (description.length() == 0)
			description = "Name: " + name + " Comment: " + comment;
		
		int nextID = -1;
		try {
			Statement stmt = con.createStatement();
			//Assert datatype is valid.  (Only valid options given in GUI, but
			//still want to double-check.)
			ResultSet rs = stmt.executeQuery("SELECT DISTINCT Datatype \n" +
					"FROM Metadata \n" +
					"WHERE Datatype = '" + datatype + "'");
			assert(rs.next()) : "The datatype of the new collection doesn't exist.";
			
			// Get next CollectionID:
			rs = stmt.executeQuery("SELECT MAX(CollectionID)\n" +
			"FROM Collections\n");
			rs.next();
			nextID = rs.getInt(1) + 1;
			
			stmt.executeUpdate("INSERT INTO Collections\n" +
					"(CollectionID, Name, Comment, Description, Datatype)\n" +
					"VALUES (" +
					Integer.toString(nextID) + 
					", '" + removeReservedCharacters(name) + "', '" 
					+ removeReservedCharacters(comment) + "', '" + 
					removeReservedCharacters(description) + "', '" + datatype + "')");
			stmt.executeUpdate("INSERT INTO CollectionRelationships\n" +
					"(ParentID, ChildID)\n" +
					"VALUES (" + Integer.toString(parent) +
					", " + Integer.toString(nextID) + ")");
			
			
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception creating empty collection.");
			System.err.println("Exception creating empty collection:");
			e.printStackTrace();
			return -1;
		}
		return nextID;
	}	
	
	/**
	 * createEmptyCollectionAndDataset is used for the initial 
	 * importation of data.  It creates an empty collection
	 * which can then be filled using insertATOFMSParticle, using the 
	 * return values as parameters.
	 * 
	 * Don't include the name of the dataset in the list of params -
	 * it will be added by the method.
	 * @param parent The ID of the parent to insert this collection at
	 * (0 for root)
	 * @param datatype
	 * @param datasetName The name of the dataset, 
	 * @param comment The comment from the dataset
	 * @param params - string of parameters for query
	 * @return int[0] = collectionID, int[1] = datasetID
	 */
	public int[] createEmptyCollectionAndDataset(String datatype, int parent,  
			String datasetName, String comment, String params)
	{
		int[] returnVals = new int[2];
		
		datasetName = removeReservedCharacters(datasetName);
		
		// What do we want to put as the description?
		returnVals[0] = createEmptyCollection(datatype, parent, datasetName, comment, datasetName + ": " + comment);
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(DataSetID) " +
					"FROM " + getDynamicTableName(DynamicTable.DataSetInfo,datatype));
			
			if (rs.next())
				returnVals[1] = rs.getInt(1)+1;
			else
				returnVals[1] = 0;
			
			//Changed back to use datasetName separately from params to fix
			//importation from MSAnalyze.  TODO: We should discuss.  ~Leah
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO " + getDynamicTableName(DynamicTable.DataSetInfo,datatype) 
					+ " VALUES(" + returnVals[1] + ",'" + datasetName + "'");
			if(params.length()>0)
				sql.append(","+ params + ")");
			else
				sql.append(")");
			
			//System.out.println(sql.toString); //debugging
			stmt.execute(sql.toString());	
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception creating the new dataset.");
			System.err.println("Exception creating the dataset entries:");
			e.printStackTrace();
		}
		return returnVals;
	}
	
	/**
	 * Create a new collection from an array list of atomIDs which 
	 * have yet to be inserted into the database.  Not used as far as
	 * I can tell.
	 * 
	 * @param parentID	The key of the parent to insert this
	 * 					collection (0 to insert at root level)
	 * @param name		What to call this collection
	 * @param datatype  collection's datatype
	 * @param comment	What to leave as the comment
	 * @param atomType	The type of atoms you are inserting ("ATOFMSParticle" most likely
	 * @param atomList	An array list of atomID's to insert into the 
	 * 					database
	 * @return			The CollectionID of the new collection, -1 for
	 * 					failure.
	 *//*
	 public int createCollectionFromAtoms( String datatype,
	 int parentID,
	 String name,
	 String comment,
	 ArrayList<String> atomList)
	 {
	 int collectionID = createEmptyCollection(datatype,
	 parentID, 
	 name,
	 comment,"");
	 Collection collection = getCollection(collectionID);
	 if (!insertAtomicList(datatype, atomList,collection))
	 return -1;
	 return collectionID;
	 }*/
	
	/* Copy and Move Collections */
	
	/**
	 * @author steinbel - altered method Oct. 2006
	 * Similar to moveCollection, except instead of removing the 
	 * collection and its unique children, the original collection 
	 * remains with original parent and a duplicate with a new id is 
	 * assigned to the new parent.  
	 * @param collectionID The collection id of the collection to move.
	 * @param toParentID The collection id of the new parent.  
	 * @return The collection id of the copy.  
	 */
	public int copyCollection(Collection collection, Collection toCollection)
	{
		int newID = -1;
		try {
			if (collection.getCollectionID() == toCollection.getCollectionID())
				throw new SQLException("Cannot copy a collection into itself");

			con.setAutoCommit(false);
			Statement stmt = con.createStatement();
			
			// Get Collection info:
			ResultSet rs = stmt.executeQuery("SELECT Name, Comment, Description\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " +
					collection.getCollectionID());
			boolean next = rs.next();
			assert (next) : "Error copying collection information";
			newID = createEmptyCollection(collection.getDatatype(),
					toCollection.getCollectionID(), 
					rs.getString(1), rs.getString(2),rs.getString(3));
			Collection newCollection = getCollection(newID);
			String description = getCollectionDescription(collection.getCollectionID());
			if (description  != null)
				setCollectionDescription(newCollection, getCollectionDescription(collection.getCollectionID()));

			rs = stmt.executeQuery("SELECT AtomID\n" +
					"FROM AtomMembership\n" +
					"WHERE CollectionID = " +
					collection.getCollectionID());
			PreparedStatement pstmt = con.prepareStatement(
					"INSERT INTO AtomMembership (CollectionID, AtomID) VALUES (?,?)");
			while (rs.next())
			{
				pstmt.setInt(1, newID);
				pstmt.setInt(2, rs.getInt("AtomID"));
				pstmt.addBatch();
			}

			pstmt.executeBatch();
			con.commit();
			con.setAutoCommit(true);
			rs.close();
			// Get Children
			ArrayList<Integer> children = getImmediateSubCollections(collection);
			for (int i = 0; i < children.size(); i++) {
				copyCollection(getCollection(children.get(i)), newCollection);			
			}
			
			stmt.close();
			
			// update new collection's ancestors.
			updateAncestors(newCollection);
			return newID;
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception copying collection "+collection.getName());
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * @author steinbel - altered Oct. 2006
	 * 
	 * Moves a collection and all its children from one parent to 
	 * another.  If the subcollection was the only child of the parent
	 * containing a particular atom, that atom will be removed from 
	 * the parent, if there are other existing subcollections of the 
	 * parent containing particles also belonging to this collection, 
	 * those particles will then exist both in the current collection and
	 * its parent.
	 * 
	 * To avoid removing particles, use copyCollection instead.
	 * @param collectionID The collection id of the collection to move.
	 * @param toParentID The collection id of the new parent.
	 * @return True on success. 
	 */
	public boolean moveCollection(Collection collection, 
			Collection toCollection)
	{
		try { 
			int col = collection.getCollectionID();
			int toCol = toCollection.getCollectionID();
			
			Statement stmt = con.createStatement();
			stmt.executeUpdate("UPDATE CollectionRelationships\n" +
					"SET ParentID = " + toCol + "\n" +
					"WHERE ChildID = " + col);
			
			// update InternalAtomOrder table.
			updateAncestors(collection);
			stmt.close();
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception moving the collection "+collection.getName());
			System.err.println("Error moving collection: ");
			System.err.println(e);
			return false;
		}
		return true;
	}
	
	/**
	 * Overloaded method for particles with no datasets (e.g., cluster centers).
	 * @param dense
	 * @param sparse
	 * @param collection
	 * @param nextID
	 * @return
	 */
	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
			Collection collection, int nextID){
		return insertParticle(dense, sparse, collection, -1, nextID);
	}
	
	/**
	 * For backward compatibility - assumes NOT an import if not given boolean.
	 */
	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
			Collection collection,
			int datasetID, int nextID){
		return insertParticle(dense, sparse, collection, datasetID, nextID, false);
	}
	


	
	/**
	 * Inserts particles.  Not used yet, but it was here.  
	 * @return the last atomID used.
	 *//*
	 public int insertGeneralParticles(String datatype, ArrayList<String> particles, 
	 Collection collection) {
	 ArrayList<Integer> ids = new ArrayList<Integer>();
	 int atomID = getNextID();
	 for (int i = 0; i < particles.size(); i++) {
	 ids.add(new Integer(atomID));
	 atomID++;
	 }
	 insertAtomicList(datatype, particles, collection);
	 return atomID-1;
	 }*/
	
	/**
	 * adds an atom to a collection.
	 * @return true if successful
	 */
	public boolean addAtom(int atomID, int parentID)
	{
		if (parentID == 0)
		{
			System.err.println("Root cannot own any atoms");
			return false;
		}
		
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO AtomMembership \n" +
					"VALUES(" + parentID + ", " + atomID + ")");
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception adding atom "+atomID+"to AtomMembership.");
			System.err.println("Exception adding atom to " +
			"AtomMembership table");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * adds an atom to the batch statement
	 * @return true if successful.
	 */
	public boolean addAtomBatch(int atomID, int parentID)
	{
		if (parentID == 0)
		{
			System.err.println("Root cannot own any atoms");
			return false;
		}
		
		try {
			batchStatement.addBatch(
					"INSERT INTO AtomMembership \n" +
					"VALUES(" + parentID + ", " + atomID + ")");
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception adding atom "+atomID+" to AtomMembership.");
			System.err.println("Exception adding atom to " +
			"AtomMembership table");
			e.printStackTrace();
			return false;
		}
		if (!alteredCollections.contains(new Integer(parentID)))
			alteredCollections.add(new Integer(parentID));
		return true;
	}
	
	/* Delete Atoms */
	
	/**
	 * Removes an empty collection. Ensure that the collection is empty before deleting it.
	 * @param collection the collection to remove
	 * @return true on success
	 * @author shaferia
	 */
	public boolean removeEmptyCollection(Collection collection) {
		try {
			int id = collection.getCollectionID();
			
			//ensure that this collection does not have subcollections or atoms
			PreparedStatement pstmt = con.prepareStatement(
					"SELECT AtomMembership.AtomID, CenterAtoms.AtomID, CollectionRelationships.ParentID " +
					"FROM AtomMembership, CenterAtoms, CollectionRelationships WHERE " +
					"AtomMembership.CollectionID = ? " +
					"AND CenterAtoms.CollectionID = ? " +
					"AND CollectionRelationships.ParentID = ?");
			pstmt.setInt(1, id);
			pstmt.setInt(2, id);
			pstmt.setInt(3, id);
			ResultSet rs = pstmt.executeQuery();

			if (rs.next()) {
				System.err.println("Collection " + id + 
						" is not empty; cannot remove it with removeEmptyCollection");
				pstmt.close();
				return false;
			}
			else {
				pstmt.close();
				
				//delete the collection
				Statement stmt = con.createStatement();
				stmt.addBatch("DELETE FROM Collections WHERE CollectionID = " + id);
				stmt.addBatch("DELETE FROM CollectionRelationships WHERE ChildID = " + id);
				stmt.executeBatch();
				stmt.close();
				return true;
			}
		}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error removing empty collection.");
			System.err.println("Error removing empty collection");
			ex.printStackTrace();
			return false;			
		}
	}
	
	/**
	 * orphanAndAdopt() essentially deletes a collection and assigns 
	 * the ownership of all its children (collections and atoms) to 
	 * their grandparent collection.  
	 * @param collectionID The ID of the collection to remove. 
	 * @return true on success.
	 */
	public boolean orphanAndAdopt(Collection collection)
	{
		try {
			// parentID is now set to the parent of the current 
			// collection
			int parentID = getParentCollectionID(collection.getCollectionID());
			if (parentID == -1)
				return false;
			else if (parentID < 2)
			{
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Cannot perform the Orphan And Adopt operation on root level collections.");
				throw new RuntimeException("Cannot perform this operation on root level collections.");
			}
			
			Statement stmt = con.createStatement();
			
			// Get rid of the current collection in 
			// CollectionRelationships 
			stmt.execute("DELETE FROM CollectionRelationships\n" + 
					"WHERE ChildID = " + 
					Integer.toString(collection.getCollectionID()));
			
			//This gets all the original atoms that belong to the parentCollection;
			Collection parentCollection = getCollection(parentID);
			//ResultSet rs = getAllAtomsRS(parentCollection);
			
			// Find the child collections of this collection and 
			// move them to the parent.  
			ArrayList<Integer> subChildren = getImmediateSubCollections(collection);
			for (int i = 0; i < subChildren.size(); i++)
			{
				moveCollection(getCollection(subChildren.get(i).intValue()), 
						parentCollection);
			}
			
			//this query updates the AtomMembership database so that all the collectionIDs are set to
			//parentID when the CollectionID is the child's CollectionID and when AtomID has
			//the child's CollectionID but not the parent's CollectionID
			//so if we have
			// {(2,100), (2, 101), (2, 103), (5, 99), (5, 100), (5, 101)}
			//where 2 is the parent and 5 is the child and the first number denotes the CollectionID
			//and the second number denotes the AtomID
			//we want it to change all the 5s to 2s except when the corresponding AtomID is already
			//in the set of 2s.  So we want to change (5, 99) but not (5, 100) and (5, 101).
			
			String query = "UPDATE AtomMembership SET CollectionID = " + 
			parentID + " WHERE CollectionID = " + collection.getCollectionID()+ 
			" and AtomID in (select AtomID from AtomMembership where CollectionID = " + collection.getCollectionID() + 
			" and AtomID not in (select AtomID from AtomMembership where CollectionID = " + parentID + "))";

			stmt.executeUpdate(query);
			
			// Delete the collection now that everything has been 
			// moved.  Updates the InternalAtomOrder table as well.
			recursiveDelete(collection);
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error executing Orphan and Adopt.");
			System.err.println("Error executing orphan and Adopt");
			throw new ExceptionAdapter(e);
		}
		
		return true;
	}
	
	/**
	 * Deletes a collection and unlike orphanAndAdopt() also recursively
	 * deletes all direct descendents.
	 * This method deletes all references to a collection in InternalAtomOrder, CollectionRelationships, CenterAtoms, and Collections.
	 * Atom information is deleted seperately through compactDatabase
	 * 
	 * @param collectionID The id of the collection to delete
	 * @return true on success. 
	 */
	public boolean recursiveDelete(Collection collection)
	{
		String datatype = collection.getDatatype();
		try {
			Statement stmt = con.createStatement();
			con.setAutoCommit(false);
			stmt.executeUpdate("DROP TABLE IF EXISTS temp.CollectionsToDelete;");
			stmt.executeUpdate("CREATE TEMPORARY TABLE CollectionsToDelete " +
					                  "(CollectionID int, PRIMARY KEY([CollectionID]));");

			// Update the InternalAtomOrder table:  Assumes that subcollections
			// are already updated for the parentCollection.
			// clear InternalAtomOrder table of the deleted collection and all subcollections.
			HashMap<Integer,ArrayList<Integer>> hierarchy =  getSubCollectionsHierarchy(collection);
			
			Iterator<Integer> allsubcollections = hierarchy.keySet().iterator();

			String queryTemplate = "INSERT INTO temp.CollectionsToDelete VALUES(?)";
			PreparedStatement pstmt = con.prepareStatement(queryTemplate);

			pstmt.setInt(1, collection.getCollectionID());
			pstmt.addBatch();
			while(allsubcollections.hasNext()){
				Integer nextParent = allsubcollections.next();
				for(Integer childID : hierarchy.get(nextParent)){
					assert(this.getCollection(childID).getDatatype().equals(datatype));
					pstmt.setInt(1, childID);
					pstmt.addBatch();
				}
			}
			pstmt.executeBatch();

			stmt.executeUpdate("DELETE FROM InternalAtomOrder\n"
					+ "WHERE CollectionID IN (SELECT * FROM temp.CollectionsToDelete);\n");
			stmt.executeUpdate("DELETE FROM CollectionRelationships\n"
					+ "WHERE ParentID IN (SELECT * FROM temp.CollectionsToDelete)\n"
					+ "OR ChildID IN (SELECT * FROM temp.CollectionsToDelete);\n");
			stmt.executeUpdate("DELETE FROM CenterAtoms\n"
					+ "WHERE CollectionID IN (SELECT * FROM temp.CollectionsToDelete);\n");
			stmt.executeUpdate("DELETE FROM Collections\n"
					+ "WHERE CollectionID IN (SELECT * FROM temp.CollectionsToDelete);\n");

			stmt.executeUpdate("DROP TABLE temp.CollectionsToDelete;\n");
			isDirty = true;
			con.commit();
			con.setAutoCommit(true);
			stmt.close();

		} catch (Exception e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Exception deleting collection.");
			System.err.println("Exception deleting collection: ");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Renames a collection.
	 * @param collection the collection to rename
	 * @param newName the new name for the collection
	 * @return true on success
	 * @author atlasr
	 */
	public boolean renameCollection(Collection collection, String newName) {
		try {
			String query = "UPDATE Collections SET Name = ? WHERE CollectionID = ?"; 
			PreparedStatement stmt = con.prepareStatement(query);
			stmt.setString(1, newName);
			stmt.setInt(2, collection.getCollectionID());
			stmt.executeUpdate();
			stmt.close();
			return true;
			}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error renaming collection.");
			System.err.println("Error renaming collection");
			ex.printStackTrace();
			return false;			
		}
	}	
	
	/**
	 * Get the immediate subcollections for the given collection.
	 * @param collection
	 * @return arrayList of atomIDs of subchildren.
	 */
	public ArrayList<Integer> getImmediateSubCollections(Collection collection)
	{
		ArrayList<Integer> subChildren = new ArrayList<Integer>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ChildID\n" +
					"FROM CollectionRelationships\n" +
					"WHERE ParentID = " +
					Integer.toString(collection.getCollectionID())+" ORDER BY ChildID");
			while(rs.next())
			{
				subChildren.add(new Integer(rs.getInt("ChildID")));
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception grabbing subchildren in GetImmediateSubCollections.");
			System.err.println("Exception grabbing subchildren:");
			System.err.println(e);
		}
		return subChildren;
	}
	
	/**
	 * puts an atom-delete call in the atom batch for each atomID in string.
	 * @return true if successful. 
	 */
	public boolean deleteAtomsBatch(String atomIDs, Collection collection) {
		try {
			batchStatement.addBatch(
					"DELETE FROM AtomMembership \n" +
					"WHERE CollectionID = " + collection.getCollectionID() + "\n" +
					"AND AtomID IN (" + atomIDs + ")");
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception deleting atoms "+atomIDs);
			System.err.println("Exception parents from " +
			"parent membership table.");
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	/**
	 * puts an atom-delete call in the atom batch
	 * @return true if successful.
	 */	
	public boolean deleteAtomBatch(int atomID, Collection collection) {
		try {
			batchStatement.addBatch(
					"DELETE FROM AtomMembership \n" +
					"WHERE CollectionID = " + collection.getCollectionID() + "\n" +
					"AND AtomID = " + atomID);
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception deleting atom "+atomID);
			System.err.println("Exception adding a batch statement to " +
			"delete atoms from AtomMembership.");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/* Move Atoms */
	
	/**
	 * moves an atom from one collection to another.
	 * @return true if successful
	 */
	public boolean moveAtom(int atomID, int fromParentID, int toParentID)
	{
		if (toParentID == 0)
		{
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Cannot move atoms to the root collection.");
			System.err.println("Cannot move atoms to the root " +
			"collection.");
			return false;
		}
		
		try {
			Statement stmt = con.createStatement();
			//System.out.println("AtomID: " + atomID + " from: " + 
			//		fromParentID + " to: " + toParentID);
			stmt.executeUpdate(
					"UPDATE AtomMembership\n" +
					"SET CollectionID = " + toParentID + "\n" +
					"WHERE AtomID = " + atomID + " AND CollectionID = " +
					fromParentID);
			stmt.execute("DELETE FROM InternalAtomOrder WHERE AtomID = " + 
					atomID + " AND CollectionID = " + fromParentID);
			addSingleInternalAtomToTable(atomID, toParentID);
			updateAncestors(getCollection(fromParentID));
			updateAncestors(getCollection(toParentID));
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception updating AtomMembership table.");
			System.err.println("Exception updating membership table");
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * Associates a cluster center with the collection of atoms it represents.
	 * @param atomID	The atomID of the cluster center.
	 * @param collID	The collectionID for the collection this center represents.
	 * @return
	 */
	public boolean addCenterAtom(int atomID, int collID) {
		boolean success = false;
		try {
			Statement stmt = con.createStatement();
			String query = "INSERT INTO CenterAtoms \n" +
							"VALUES ("+ atomID + ", " + collID + ")";
			stmt.execute(query);
			stmt.close();
			success = true;
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
		return success;
	}
	
	/**
	 * adds a move-atom call to a batch statement.
	 * @return true if successful
	 * 
	 * NOT USED AS OF 12/05
	 
	 public boolean moveAtomBatch(int atomID, int fromParentID, int toParentID)
	 {
	 if (toParentID == 0)
	 {
	 System.err.println("Cannot move atoms to the root " +
	 "collection.");
	 return false;
	 }
	 
	 try {
	 Statement stmt = con.createStatement();
	 //System.out.println("AtomID: " + atomID + " from: " + 
	  //		fromParentID + " to: " + toParentID);
	   stmt.addBatch(
	   "UPDATE AtomMembership\n" +
	   "SET CollectionID = " + toParentID + "\n" +
	   "WHERE AtomID = " + atomID + " AND CollectionID = " +
	   fromParentID);
	   stmt.close();
	   } catch (SQLException e) {
	   new ExceptionDialog("SQL Exception updating AtomMembership table.");
	   System.err.println("Exception updating membership table");
	   e.printStackTrace();
	   }
	   return true;
	   }
	   */
	
	/* Atom Batch Init and Execute */
	
	/**
	 * initializes atom batches for moving atoms and adding atoms.
	 */
	public void atomBatchInit() {
		try {
			batchStatement = con.createStatement();
			alteredCollections = new ArrayList<Integer>();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception occurred initializing AtomBatch functionality.");
			e.printStackTrace();
		}
	}
	
	/**
	 * @author steinbel - altered Oct. 2006
	 * Executes the current batch
	 */
	public void atomBatchExecute() {
		try {
			batchStatement.executeBatch();
			for (int i = 0; i < alteredCollections.size(); i++)
				updateInternalAtomOrder(getCollection(alteredCollections.get(i)));
			
			//when the parents of all the altered collections are the same
			//don't need to update the parent FOR EACH subcollection, just at end
			ArrayList<Collection> parents = new ArrayList<Collection>();
			Collection temp;
			for (int i = 0; i < alteredCollections.size(); i++){ 
				temp = getCollection(alteredCollections.get(i)).getParentCollection();
				if (! parents.contains(temp))
					parents.add(temp);
			}
			//only update each distinct parent once
			for (int i=0; i<parents.size(); i++)
				updateAncestors(parents.get(i));
			batchStatement.close();

		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception executing batch atom adds and inserts.");
			System.out.println("Exception executing batch atom adds " +
			"and inserts");
			e.printStackTrace();
		}
	}

	/**
	 * Initializes atom batches for moving atoms and adding atoms.
	 * @throws Exception
	 */
	public void bulkInsertInit()  {
		try {
			bulkInsertStatementAtomMembership =
					con.prepareStatement("INSERT INTO AtomMembership VALUES (?, ?)");
			bulkInsertStatementInternalAtomOrder =
					con.prepareStatement("INSERT INTO InternalAtomOrder VALUES (?, ?)");
			alteredCollections = new ArrayList<>();
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
	}

	/**
	 * Executes bulk insertion of atoms into database, from the list contained
	 * in the bulkInsertFile.
	 * 
	 * @author olsonja
	 */
	
	public void bulkInsertExecute() {
		try {
			long time = System.currentTimeMillis();
			
			if (bulkInsertStatementAtomMembership == null || bulkInsertStatementInternalAtomOrder == null) {
				throw new RuntimeException("Must initialize bulk insert first!");
			}

			bulkInsertStatementAtomMembership.executeBatch();
			bulkInsertStatementInternalAtomOrder.executeBatch();

			System.out.println("Time: " + (System.currentTimeMillis()-time));
			System.out.println("done inserting now time for altering collections");
			
			time = System.currentTimeMillis();
			System.out.println("alteredCollections.size() " + alteredCollections.size());
			for (int i = 0; i < alteredCollections.size(); i++)
				updateInternalAtomOrder(getCollection(alteredCollections.get(i)));
			
			//when the parents of all the altered collections are the same
			//don't need to update the parent FOR EACH subcollection, just at end
			ArrayList<Collection> parents = new ArrayList<Collection>();
			Collection temp;
			for (int i = 0; i < alteredCollections.size(); i++){ 
				temp = getCollection(alteredCollections.get(i)).getParentCollection();
				if (! parents.contains(temp))
					parents.add(temp);
			}
			//only update each distinct parent once
			for (int i=0; i<parents.size(); i++)
				updateAncestors(parents.get(i));
			if (batchStatement != null) {
				batchStatement.close();
			}
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception executing batch atom adds and inserts.");
			throw new ExceptionAdapter(e);
		}
		
	}
	
	/**
	 * Adds an atom to a list of atoms to be
	 * inserted into the database when the bulkInsertExecute() method is called.
	 * 
	 * @param atomID - integer
	 * @param parentID - integer
	 * 
	 */
	public void bulkInsertAtom(int atomID,int parentID) throws Exception{
		if (bulkInsertStatementAtomMembership == null || bulkInsertStatementInternalAtomOrder == null) {
			throw new RuntimeException("Must initialize bulk insert first!");
		}

		if (!alteredCollections.contains(parentID))
			alteredCollections.add(parentID);
		
		//alteredCollections.add(parentID);
		bulkInsertStatementAtomMembership.setInt(1, parentID);
		bulkInsertStatementAtomMembership.setInt(2, atomID);
		bulkInsertStatementAtomMembership.addBatch();;
		bulkInsertStatementInternalAtomOrder.setInt(1, atomID);
		bulkInsertStatementInternalAtomOrder.setInt(2, parentID);
		bulkInsertStatementInternalAtomOrder.addBatch();
	}
	/* Get functions for collections and table names */
	
	/**
	 * Gets immediate subcollections for a given collection
	 * @param collections
	 * @return arraylist of collectionIDs
	 */
	public ArrayList<Integer> getImmediateSubCollections(
			ArrayList<Integer> collections)
			{
		ArrayList<Integer> subChildren = new ArrayList<Integer>();
		
		String query = 
			"SELECT DISTINCT ChildID\n" +
			"FROM CollectionRelationships\n" +
			"WHERE ParentID IN (" + join(collections, ",") + ")";
		
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next())
			{
				subChildren.add(new Integer(rs.getInt("ChildID")));
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception grabbing subchildren in GetImmediateSubCollections.");
			System.err.println("Exception grabbing subchildren:");
			System.err.println(e);
		}
		return subChildren;
			}
	
	/**
	 * returns a collection given a collectionID.
	 */
	public Collection getCollection(int collectionID) {
		Collection collection;
		boolean isPresent = false;
		String datatype = "";
		Statement stmt;
		try {
			stmt = con.createStatement();
			String query = "SELECT CollectionID FROM Collections WHERE CollectionID = "+collectionID;
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				if (rs.getInt(1) == collectionID) {
					isPresent = true;
					break;
				}
			}
			
			if (isPresent) {
				rs = stmt.executeQuery("SELECT Datatype FROM Collections WHERE CollectionID = " + collectionID);
				rs.next();
				datatype = rs.getString(1);
			}
			else {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving collection for collectionID "+collectionID);
				throw new IllegalArgumentException("collectionID not created yet!! " + collectionID);
			}
			stmt.close();
			
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving collection for collectionID "+collectionID);
			System.err.println("error creating collection");
			e.printStackTrace();
			return null;
		}
		return new Collection(datatype,collectionID,this);
	}
	
	/**
	 * gets the collection comment.
	 */
	public String getCollectionComment(int collectionID) {
		String comment = "";
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Comment\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " +
					collectionID);
			rs.next();
			comment = rs.getString("Comment");
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving the collection comment for collectionID "+collectionID);
			System.err.println("Exception grabbing the collection comment:");
			System.err.println(e);
		}
		return comment;
	}
	
	/**
	 * gets the collection's datatype
	 */
	public String getCollectionDatatype (int collectionID){
		String datatype = "";
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT Datatype \n" +
					"FROM Collections \n" +
					"WHERE CollectionID = " + collectionID);
			rs.next();
			datatype = rs.getString("Datatype");
			stmt.close();
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Error retrieving the collection's datatype for collection " + collectionID);
			throw new ExceptionAdapter(e);
		}
		return datatype;
	}
	/**
	 * gets the collection description for the given collectionID
	 */
	public String getCollectionDescription(int collectionID)
	{
		String descrip = "";
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT Description\n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + collectionID);
			rs.next();
			descrip = rs.getString("Description");
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving the collection description for collectionID "+collectionID);
			System.err.println("Error retrieving Collection " +
			"Description.");
			e.printStackTrace();
		}
		return descrip;
	}
	
	/**
	 * gets the collection size
	 */
	public int getCollectionSize(int collectionID) {
		int returnThis = -1;
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT COUNT(AtomID) FROM InternalAtomOrder WHERE CollectionID = " + collectionID);
			boolean test = rs.next();
			assert (test): "error getting atomID count.";
			returnThis = rs.getInt(1);
			stmt.close();
		} catch (SQLException e1) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving the collection size for collectionID "+collectionID);
			System.err.println("Error selecting the size of " +
			"the table");
			e1.printStackTrace();
		}
		return returnThis;
	}
	
	// returns an arraylist of non-empty collection ids from the original collection of collectionIDs.
	// If you want to include children, then include them in the Set that is passed.
	public ArrayList<Integer> getCollectionIDsWithAtoms(java.util.Collection<Integer> collectionIDs) {
		ArrayList<Integer> ret = new ArrayList<Integer>();
		
		if (collectionIDs.size() > 0) {
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT DISTINCT CollectionID FROM AtomMembership WHERE CollectionID in (" + join(collectionIDs, ",") + ")");
				
				while (rs.next())
					ret.add(rs.getInt("CollectionID"));
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving collections with atoms.");
				System.err.println("Error retrieving collections with atoms.");
				e.printStackTrace();
			}
		}
		
		return ret;
	}
	
	public static String join(java.util.Collection collection, String delimiter) {
		// Blecch... java should be able to do this itself...
		
		StringBuffer sb = new StringBuffer();
		boolean firstElement = true;
		for (Object o : collection) {
			if (!firstElement)
				sb.append(delimiter);
			sb.append(o);
			firstElement = false;
		}
		
		return sb.toString();
	}
	
	/**
	 * gets all the atoms underneath the given collection.
	 */
	public ArrayList<Integer> getAllDescendedAtoms(Collection collection) {
		ArrayList<Integer> results = new ArrayList<Integer>(1000);
		try {
			ResultSet rs = getAllAtomsRS(collection);
			while(rs.next())
				results.add(new Integer(rs.getInt(1)));
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving children of the collection.");
			System.err.println("Error retrieving children.");
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * Gets the parent collection ID using a simple query.
	 */
	public int getParentCollectionID(int collectionID) {
		int parentID = -1;
		try {
			Statement stmt = con.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT ParentID\n" +
					"FROM CollectionRelationships\n" + 
					"WHERE ChildID = " + collectionID);
			
			// If there is no entry in the table for this collectionID,
			// it doesn't exist, so return false
			if(rs.next())
				parentID = rs.getInt("ParentID");
			
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving parentID of the collection.");
			System.err.println("Error retrieving parentID of the collection.");
			e.printStackTrace();
		}
		
		return parentID;
	}
	
	/**
	 * Returns all collectionIDs beneath the given collection, optionally including it.
	 */
	public Set<Integer> getAllDescendantCollections(int collectionID, boolean includeTopLevel) {
		
		// Construct a set of all collections that descend from this one,
		// including this one.
		ArrayList<Integer> lookUpNext = new ArrayList<Integer>();
		boolean status = lookUpNext.add(new Integer(collectionID));
		assert status : "lookUpNext queue full";
		
		Set<Integer> descCollections = new HashSet<Integer>();
		if (includeTopLevel)
			descCollections.add(new Integer(collectionID));
		
		// As long as there is at least one collection to lookup, find
		// all subchildren for all of these collections. Add them to the
		// set of all collections we have visited and plan to visit
		// then next time (if we haven't). (This is essentially a breadth
		// first search on the graph of collection relationships).
		while (!lookUpNext.isEmpty()) {
			ArrayList<Integer> subChildren =
				getImmediateSubCollections(lookUpNext);
			lookUpNext.clear();
			for (Integer col : subChildren)
				if (!descCollections.contains(col)) {
					descCollections.add(col);
					lookUpNext.add(col);
				}
		}
		
		return descCollections;
	}
	
	public Set<Integer> getAllDescendantCollectionsNew(int collectionID, boolean includeTopLevel) {
		
		// Construct a set of all collections that descend from this one,
		// including this one.
		ArrayList<Integer> lookUpNext = new ArrayList<Integer>();
		boolean status = lookUpNext.add(new Integer(collectionID));
		assert status : "lookUpNext queue full";
		
		Set<Integer> descCollections = new HashSet<Integer>();
		if (includeTopLevel)
			descCollections.add(new Integer(collectionID));
		
		// As long as there is at least one collection to lookup, find
		// all subchildren for all of these collections. Add them to the
		// set of all collections we have visited and plan to visit
		// then next time (if we haven't). (This is essentially a breadth
		// first search on the graph of collection relationships).
		while (!lookUpNext.isEmpty()) {
			ArrayList<Integer> subChildren =
				getImmediateSubCollections(lookUpNext);
			lookUpNext.clear();
			for (Integer col : subChildren)
				if (!descCollections.contains(col)) {
					descCollections.add(col);
					lookUpNext.add(col);
				}
		}
		
		return descCollections;
	}
	/**
	 * This method has CHANGED as of 11/15/05.
	 * It used to recurse through all the collection's subcollections
	 * and create a temporary table of all the atoms.  Now, it only needs
	 * a simple query from the InternalAtomOrder table. Note that the  
	 * InternalAtomOrderTable must be completely updated for this to work.
	 * 
	 * It also used to return an InstancedResultSet, which is not just
	 * a plain ResultSet.
	 * @param collection
	 * @return - resultset
	 */
	public ResultSet getAllAtomsRS(Collection collection)
	{
		ResultSet returnThis = null;
		try {
			Statement stmt = con.createStatement();
			returnThis = stmt.executeQuery("SELECT AtomID " +
					"FROM InternalAtomOrder WHERE CollectionID = " + 
					collection.getCollectionID() + " ORDER BY AtomID");
			//NOTE: Atoms are already ordered by AtomID, and this might be
			// redundant.  If needed, you can take this out to optimize and 
			// only order when needed in each method. - AR
			//stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving children of the collection.");
			e.printStackTrace();
		}
		return returnThis;
	}
	
	/**
	 * gets an arraylist of ATOFMS Particles for the given collection.
	 * Unique to ATOFMS data - not used anymore except for unit tests.  
	 *
	 *DEPRECIATED 12/05 - AR
	 *
	 public ArrayList<GeneralAtomFromDB> getCollectionParticles(Collection collection)
	 {
	 ArrayList<GeneralAtomFromDB> particleInfo = 
	 new ArrayList<GeneralAtomFromDB>(1000);
	 try {
	 ResultSet rs = getAllAtomsRS(collection);
	 DateFormat dFormat = 
	 new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
	 while(rs.next()) {
	 System.out.println(rs.getInt(1));
	 particleInfo.add(new GeneralAtomFromDB(rs.getInt(1),this));
	 }
	 } catch (SQLException e) {
	 System.err.println("Error collecting particle " +
	 "information:");
	 e.printStackTrace();
	 }
	 return particleInfo;
	 }
	 */
	
	/**
	 * update particle table returns a vector<vector<Object>> for the gui's 
	 * particles table.  All items are taken from AtomInfoDense, and all 
	 * items are strings except for the atomID, which is used to produce 
	 * graphs.
	 * 
	 * This will only return 1000 particles at a time. - steinbel
	 */
	public Vector<Vector<Object>> updateParticleTable(Collection collection, Vector<Vector<Object>> particleInfo, int lowIndex, int highIndex) {
		assert (highIndex - lowIndex < 1000) : "trying to collect over 1000 particles at a time!";
		particleInfo.clear();
		ArrayList<String> colNames = getColNames(collection.getDatatype(),DynamicTable.AtomInfoDense);
		// This isn't a registered datatype... oops
		if (colNames.size() == 0)
			return null;
		
		try {
			Statement stmt = con.createStatement();
			
			StringBuffer query = new StringBuffer();
			query.append("SELECT ");
			for (int i = 0; i < colNames.size(); i++)
			{
				query.append(colNames.get(i));
				query.append(",");
			}
			query.setLength(query.length() - 1);
			query.append(" FROM \n(\n");
			query.append("SELECT ");
			query.append(getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()));
			query.append(".*, ROW_NUMBER() OVER (ORDER BY InternalAtomOrder.AtomID) as rowNum FROM ");
			query.append(getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()));
			query.append(", InternalAtomOrder\n");
			query.append("WHERE ");
			query.append(getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()));
			query.append(".AtomID = InternalAtomOrder.AtomID\n");
			query.append("AND InternalAtomOrder.CollectionID = ");
			query.append(collection.getCollectionID());
			query.append("\n) temptable \n");
			query.append("WHERE rowNum >= ");
			query.append(lowIndex);
			query.append(" AND rowNum <= ");
			query.append(highIndex);
			query.append(" ORDER BY AtomID\n"); 
			ResultSet rs = stmt.executeQuery(query.toString());//changed with IAO change - steinbel 9.19.06
			
			while(rs.next())
			{
				Vector<Object> vtemp = new Vector<Object>(colNames.size());
				vtemp.add(rs.getInt(1)); // Integer for atomID
				for (int i = 2; i <= colNames.size(); i++) 
					vtemp.add(rs.getString(i));
				particleInfo.add(vtemp);
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception collecting particle information.");
			System.err.println("Error collecting particle " +
			"information:");
			e.printStackTrace();
		}
		return particleInfo;
	}
	
	public void exportDatabase(String filename,int fileType) throws FileNotFoundException {
		DatabaseConnection dbconn = null;
		IDataSet dataSet = null;
		PrintWriter output = new PrintWriter(filename);
		dbconn = new DatabaseConnection(con);
		try {
			ITableFilter filter = new DatabaseSequenceFilter(dbconn);
			dataSet = new FilteredDataSet(filter, dbconn.createDataSet());
			switch(fileType){
			case 1:
				FlatXmlDataSet.write(dataSet, output);
				break;
			case 2:
				XlsDataSet.write(dataSet,new FileOutputStream(filename));
				break;
			case 3:
				FileWriter dummy = new FileWriter(filename);
				dummy.write("#this is a dummy file\n" +
						"#the real data is stored in the directory of the same name");
				String dirName = filename.substring(0,filename.lastIndexOf("."));
				File dir = new File(dirName);
				boolean success = dir.mkdir();
			    if (!success) {
			    	throw new FileNotFoundException();
			    }
			    CsvDataSetWriter.write(dataSet,dir);
			    break;
			default:
				System.err.println("Invalid fileType: "+fileType);
			}
			
			//
			//dbconn.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void importDatabase(String filename, int fileType) throws FileNotFoundException {
		DatabaseConnection dbconn = null;
		//IDataSet dataSet = null;
		FileInputStream input = new FileInputStream(filename);
		try {
			dbconn = new DatabaseConnection(con);
			//dataSet = dbconn.createDataSet();
			FlatXmlDataSet data = new FlatXmlDataSet(input);
			InsertIdentityOperation.CLEAN_INSERT.execute(dbconn, data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*try {
			dbconn.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
	}
	
	/**
	 * Finds a list of data backup locations, those added by sp_addumpdevice
	 * @return A list of maps which define the backup locations: each element of the list has K=>V
	 * "name" => the name of the backup
	 * "type" => the type of backup (Disk, Tape)
	 * "path" => the pathname of the backup
	 * @author shaferia
	 */
	public ArrayList<HashMap<String, String>> getBackupLocations() {
		ArrayList<HashMap<String,String>> locations = new ArrayList<HashMap<String, String>>();
		
		try {
			//SQL Server 2005
			ResultSet rs = con.createStatement().executeQuery("SELECT name,type,physical_name FROM sys.backup_devices");
			
			//SQL Server 2000
			//ResultSet rs = con.createStatement().executeQuery("SELECT name,cntrltype,phyname FROM master..sysdevices");
			while (rs.next()) {
				HashMap<String,String> loc = new HashMap<String,String>();
				loc.put("name", rs.getString(1));
				String type = null;
				switch (rs.getInt(2)) {
					case 2: type = "Disk"; break;
					case 5: type = "Tape"; break;
					default: type = "Other"; break;
				}
				loc.put("type", type);
				loc.put("path", rs.getString(3));
				if (type.equals("Disk")) {
					locations.add(loc);
					File f = new File(rs.getString(3));
					if (f.exists())
						loc.put("size", (f.length()/1024) + " KB");
					else
						loc.put("size", "Empty");
				}
			}
		} catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Could not retrieve backup device list");
			System.err.println("Error getting backup device list");
			ex.printStackTrace();
		}
		
		return locations;
	}
	
	/**
	 * Add a database backup location as a file
	 * @param name the name of the backup file to add (as referenced in the master database)
	 * @param path the path of the backup file to add
	 * @return true on success
	 * @author shaferia
	 */
	public boolean addBackupFile(String name, String path) {
		boolean success = false;
		try {
			name = removeReservedCharacters(name);
			String query = "EXEC sp_addumpdevice \'disk\', \'" + name + "\', \'" + path + "\'";
			con.createStatement().execute(query);
			
			success = true;
		}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Error adding backup file");
			System.err.println("Error adding backup file - executing sp_adddumpdevice");
			ex.printStackTrace();			
		}

		return success;
	}
	
	/**
	 * Remove a database backup location
	 * @param name the name of the backup file to delete (as referenced in the master database)
	 * @param delfile delete the associated file?
	 * @return true on success
	 * @author shaferia
	 */
	public boolean removeBackupFile(String name, boolean delfile) {
		boolean success = false;
		try {
			name = removeReservedCharacters(name);
			String query = "EXEC sp_dropdevice \'" + name + "\'";
			if (delfile)
				query += ", \'DELFILE\'";
			con.createStatement().execute(query);

			success = true;
		}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Error removing backup file");
			System.err.println("Error removing backup file - executing sp_dropdevice");
			ex.printStackTrace();			
		}

		return success;		
	}
	
	/**
	 * Backup the database to the backup location with the specified name
	 * @param name the name of the backup location
	 * @return output returned from the server, or "Failed" if failure
	 */
	public String backupDatabase(String name) {
		String ret = "Failed";
		
		try {
			String query = "BACKUP DATABASE " + database + " TO " + name + " WITH INIT";
			Statement stmt = con.createStatement();
			stmt.execute(query);
			
			SQLWarning output = stmt.getWarnings();
			StringBuffer message = new StringBuffer(output.getMessage());
			while ((output = output.getNextWarning()) != null) {
				message.append("\n" + output.getMessage());
			}
			
			ret = message.toString();
		}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), 
					"Error backing up database to " + name);
			System.err.println("Error backing up database - executing BACKUP DATABASE");
			ex.printStackTrace();
			ret = ex.getMessage();
		}
		
		return ret;
	}

	/**
	 * Restore the database from the backup location with the specified name
	 * @param name the name of the backup location
	 * @return output returned from the server, or "Failed" if failure
	 */
	public String restoreDatabase(String name) {
		String ret = "Failed";
		
		try {
			//close the connection and use a non-database-specific one
			closeConnection();
			Database db = (Database) getDatabase("");
			db.openConnectionNoDB();
			
			String query = "RESTORE DATABASE " + database + " FROM " + name + " WITH REPLACE";
			Statement stmt = db.getCon().createStatement();
			stmt.execute(query);
			
			SQLWarning output = stmt.getWarnings();
			StringBuffer message = new StringBuffer(output.getMessage());
			while ((output = output.getNextWarning()) != null) {
				message.append("\n" + output.getMessage());
			}
			
			ret = message.toString();
			
			//return to using the original Cfonnection
			db.closeConnection();
			openConnection();
		}
		catch (SQLException ex) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), 
					"Error restoring database from " + name);
			System.err.println("Error restoring database - executing RESTORE DATABASE");
			ex.printStackTrace();
			ret = ex.getMessage();
		}
		
		return ret;
	}
	
	/**
	 * gets the dynamic table name according to the datatype and the table
	 * type.
	 * @param table
	 * @param datatype
	 * @return table name.
	 */
	public String getDynamicTableName(DynamicTable table, String datatype) {
		assert (!datatype.equals("root")) : "root isn't a datatype.";
		
		if (table == DynamicTable.DataSetInfo) 
			return datatype + "DataSetInfo";
		if (table == DynamicTable.AtomInfoDense)
			return datatype + "AtomInfoDense";
		if (table == DynamicTable.AtomInfoSparse)
			return datatype + "AtomInfoSparse";
		else return null;
	}
	

	/**
	 * Gets the datatype of a given atom.  
	 * @param atomID
	 * @return
	 */
	public String getAtomDatatype(int atomID) {
		String datatype = "";
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Collections.Datatype " +
					"FROM Collections,AtomMembership WHERE " +
					"AtomMembership.AtomID = " + atomID + " AND " +
					"Collections.CollectionID = " +
			"AtomMembership.CollectionID");	
			rs.next();
			datatype = rs.getString(1);
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception getting the datatype for atom "+atomID);
			System.err.println("error getting atom's datatype");
			e.printStackTrace();
		}
		return datatype;
	}
	
	
	/* Set functions for collections */
	
	/**
	 * Changes the collection description
	 * @return true if successful
	 */
	public boolean setCollectionDescription(Collection collection,
			String description)
	{
		description = removeReservedCharacters(description);
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate(
					"UPDATE Collections\n" +
					"SET Description = '" + description + "'\n" +
					"WHERE CollectionID = " + collection.getCollectionID());
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception updating collection description.");
			System.err.println("Error updating collection " +
			"description:");
			e.printStackTrace();
		}
		return true;
	}
	
	
	/* Misc */
	
	/**
	 * getNextID returns the next possible ID for an atom.
	 * @return ID
	 */
	public int getNextID() {
		try {
			Statement stmt = con.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT MAX(AtomID) FROM AtomMembership");
			
			int nextID;
			if(rs.next())
				nextID = rs.getInt(1) + 1;
			else
				nextID = 0;
			stmt.close();
			return nextID;
			
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception finding the maximum atomID.");
			System.err.println("Exception finding max atom id.");
			e.printStackTrace();
		}
		
		return -1;
	}
	
	/**
	 * exports a collection to the MSAnalyze database by making up 
	 * the necessary data to import (.par file, etc).
	 * @param collection the collection to export
	 * @param newName the name of the par file without the ".par" part.
	 * @param sOdbcConnection the name of the system data source to export to
	 * -- ignored if a fileName is supplied.
	 * @param fileName the path to the access database to export to--
	 * overrides sOdbcConnection
	 * @param progressBar
	 * @return date associated with the mock dataset.
	 */
	public java.util.Date exportToMSAnalyzeDatabase(
			Collection collection, 
			String newName, 
			String sOdbcConnection,
			String fileName,
			ProgressBarWrapper progressBar)// throws InterruptedException
	{
		if (! collection.getDatatype().equals("ATOFMS")) {
			throw new RuntimeException(
					"trying to export the wrong datatype for MSAnalyze: " 
					+ collection.getDatatype());
		}
		DateFormat dFormat = null;
		Date startTime = null;
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		} catch (ClassNotFoundException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error loading ODBC bridge driver.");
			System.err.println("Error loading ODBC bridge driver");
			e.printStackTrace();
			return null;
		}
		try {
			Connection odbcCon;
			
			if (fileName != null) {
				try {
			        fileName = fileName.replace('\\', '/').trim();

					odbcCon = DriverManager.getConnection(
							accessDBURLPrefix + fileName + accessDBURLSuffix);
					System.out.println(accessDBURLPrefix + fileName + accessDBURLSuffix);
				}
				catch (SQLException se) {
					//putting in specific error message here to help the user 
					//if something goes wrong. the generic message at the end
					//of this method is not helpful
					ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error connecting to the file path " + fileName + " please ensure the file exists.");
					System.err.println("Error connecting to Access database");
					se.printStackTrace();
					return null;
				}
			}
			else {
				try {
					odbcCon = DriverManager.getConnection(
							"jdbc:odbc:" + sOdbcConnection);
					System.out.println("jdbc:odbc:" + sOdbcConnection);
				}
				catch (SQLException se) {
					//putting in specific error message here to help the user 
					//if something goes wrong. the generic message at the end
					//of this method is not helpful
					ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error connecting to the system data source name " + sOdbcConnection + " please the name and the file exist.");
					System.err.println("Error connecting to Access database");
					se.printStackTrace();
					return null;
				}
			}
			
			Statement odbcStmt = odbcCon.createStatement();
			Statement stmt = con.createStatement();

			// Create a table containing the values that will be 
			// exported to the particles table in MS-Analyze
			try {
				stmt.execute(
						"IF (OBJECT_ID('tempdb..#ParticlesToExport') " +
						"IS NOT NULL)\n" +
						"	DROP TABLE #ParticlesToExport\n" +
						"CREATE TABLE #ParticlesToExport (AtomID INT " +
						"PRIMARY KEY, Filename TEXT, [Time] DATETIME, [Size] FLOAT, " +
						"LaserPower FLOAT, NumPeaks INT, TotalPosIntegral INT, " +
						"TotalNegIntegral INT)\n");
			}
			catch (SQLException se) {
				//putting in specific error message here to help the user 
				//if something goes wrong. the generic message at the end
				//of this method is not helpful
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error setting up temporary table in SQL server.  Please ensure you have appropriate access to the database.");
				System.err.println("Error connecting to Access database");
				se.printStackTrace();
				return null;
			}
			
			stmt.execute(		
					"INSERT INTO #ParticlesToExport\n" +
					"(AtomID,Filename, [Time], [Size], LaserPower)\n" +
					"(\n" +
					"	SELECT " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + ".AtomID, OrigFilename, [Time], [Size], LaserPower\n" +
					"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + ", InternalAtomOrder" +
					"	WHERE " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + 
					".AtomID = InternalAtomOrder.AtomID AND InternalAtomOrder.CollectionID = " + collection.getCollectionID() + ")\n" +
					
					"UPDATE #ParticlesToExport\n" +
					"SET NumPeaks = \n" +
					"	(SELECT COUNT(AtomID)\n" +
					"		FROM " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + "\n" +
					"			WHERE " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + ".AtomID = #ParticlesToExport.AtomID),\n" +
					"TotalPosIntegral = \n" +
					"	(SELECT SUM (PeakArea)\n" +
					"		FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
					"			WHERE " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".AtomID = #ParticlesToExport.AtomID\n" +
					"			AND " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".PeakLocation >= 0),\n" +
					"TotalNegIntegral =\n" +
					"	(SELECT SUM (PeakArea)\n" +
					"		FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
					"			WHERE " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".AtomID = #ParticlesToExport.AtomID\n" +
					"			AND " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".PeakLocation < 0)\n"
			);
			
			// Find the start time of our mock dataset, use this
			// as the timestamp for the dataset since this might
			// potentially be a collection that wasn't originally
			// an MSA dataset and is rather some strange 
			// amalgam of them, or a selection from one.  This 
			// means that if you simply import a dataset and then
			// export it right away, its timestamp will not 
			// necessarily match the same dataset peaklisted in
			// MSA since the timestamp of an MSA dataset is from 
			// the start time listed in the .par file which often
			// is a little earlier, and which most likely 
			// represents the time they switched on the 
			// ATOFMS machine in MS-Control.
			// TODO: Use rGetAllDescended in order to get the real first
			// atom
			
			ResultSet rs = stmt.executeQuery(
					"SELECT MIN ([Time])\n" +
			"FROM #ParticlesToExport");
			Date endTime;
			startTime = endTime = null;
			long unixTime;
			
			if (rs.next())
			{
				startTime = new Date(rs.getTimestamp(1).getTime());
				//startTime = startTime.substring(0, startTime.length()-2);
				unixTime = startTime.getTime() / 1000;
			}
			else
			{
				unixTime = 0;
				//endTime = "";
			}
			// find the end time in the same manner
			rs = stmt.executeQuery(
					"SELECT MAX([Time])\n" +
			"FROM #ParticlesToExport\n");
			if (rs.next())
			{
				endTime = new Date(rs.getTimestamp(1).getTime());
				//endTime = endTime.substring(0, startTime.length()-2);
			}
			String comment = " ";
			
			// Get the comment for the current collection to use
			// as the comment for the dataset
			rs = stmt.executeQuery(
					"SELECT Comment \n" +
					"FROM Collections\n" +
					"WHERE CollectionID = " + collection.getCollectionID());
			if (rs.next())
				comment = rs.getString(1);
			if (comment.length() == 0)
				comment = "Imported from Edam-Enchilada";
			
			int hitParticles = 0;
			
			// find out how many particles are in the collection
			// and pretend like that is the number of particles
			// hit in a powercycle.  
			rs = stmt.executeQuery("SELECT COUNT(AtomID) from #ParticlesToExport");
			
			if (rs.next())
				hitParticles = rs.getInt(1);
			
			newName = newName.concat(Long.toString(unixTime));
			
			odbcStmt = odbcCon.createStatement();
			
			// Make an entry for this collection in the datasets
			// table.  Since the particles from this dataset
			// might have been peaklisted separately, enter zeroes
			// for those values, and for the missed particles.
			try {
			odbcStmt.executeUpdate(
					"DELETE FROM DataSets\n" +
					"WHERE Name = '" + newName + "'\n");
			}
			catch (SQLException se) {
				//putting in specific error message here to help the user 
				//if something goes wrong. the generic message at the end
				//of this method is not helpful
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error updating the MS-Analyze database.  Please ensure you have update access to the database.");
				System.err.println("Error updating the Access database");
				se.printStackTrace();
				return null;
			}

			System.out.println(
					"INSERT INTO DataSets\n" +
					"VALUES ('" + newName + "', '" + 
					(startTime.getTime() / 1000) + 
					"', '" + (endTime.getTime() / 1000) + 
					"', " + hitParticles + ", 0, 0, " +
					"0, 0, '" + comment + "')");
			dFormat = DateFormat.getDateTimeInstance();
			odbcStmt.executeUpdate(
					"INSERT INTO DataSets\n" +
					"VALUES ('" + newName + "', '" + 
					dFormat.format(startTime) + 
					"', '" + dFormat.format(endTime) + "', " +
					hitParticles + ", 0, 0, " +
					"0, 0, '" + comment + "')");
			
			
			// get the values for the particles table
			//so we can export them to MS Access
			rs = stmt.executeQuery(
					"SELECT * \n" +
			"FROM #ParticlesToExport\n");
			odbcStmt.execute(
					"DELETE FROM Particles\n" +
					"WHERE DataSet = '" + newName + "'\n");

			while(rs.next())
			{
				odbcStmt.addBatch(
						"INSERT INTO Particles\n" +
						"(DataSet, Filename, [Time], Size, " +
						"LaserPower, " +
						"NumPeaks,TotalPosIntegral, " +
						"TotalNegIntegral)\n" +
						"VALUES ('" + newName +"', '" + 
						(new File(rs.getString("Filename"))).getName() +
						"', '" + 
						dFormat.format(new Date(
								rs.getTimestamp("Time").
								getTime())) + 
								"', " + 
								rs.getFloat("Size") + ", " + 
								rs.getFloat("LaserPower") +
								", " + rs.getInt("NumPeaks") + ", " + 
								rs.getInt("TotalPosIntegral") + ", " + 
								rs.getInt("TotalNegIntegral") +
				")");
			}
			odbcStmt.executeBatch();
			stmt.executeUpdate(
			"DROP TABLE #ParticlesToExport");
			stmt.executeUpdate(
					"IF (OBJECT_ID('tempdb..#PeaksToExport') " +
					"IS NOT NULL)\n" +
					"	DROP TABLE #PeaksToExport\n" +
					"CREATE TABLE #PeaksToExport\n" +
					"(OrigFilename TEXT, " +
					"PeakLocation FLOAT, PeakArea INT, " +
					"RelPeakArea " +
					"FLOAT, PeakHeight INT)\n" +
					"\n" +
					"\n" +
					"\n" +
					"INSERT INTO #PeaksToExport\n" +
					"(OrigFilename, PeakLocation, PeakArea, " +
					"RelPeakArea, PeakHeight)\n" +
					"(SELECT OrigFilename, PeakLocation, " +
					"PeakArea, RelPeakArea, PeakHeight\n" +
					"FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ", InternalAtomOrder, " 
					+ getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + "\n" +
					"	WHERE (" + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".AtomID = InternalAtomOrder.AtomID)\n" +
					"   AND (InternalAtomOrder.CollectionID = " + collection.getCollectionID() + ")" +
					"	AND (" + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + ".AtomID = " +
					getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + ".AtomID)" +
			")\n");
			
			rs = stmt.executeQuery(
					"SELECT OrigFilename, PeakLocation, " +
					"PeakArea, " +
					"RelPeakArea, PeakHeight\n" +
			"FROM #PeaksToExport");
			odbcStmt.executeUpdate(
					"DELETE FROM Peaks \n" +
					"WHERE DataSet = '" + newName + "'");
			
			int counter=0; //THIS IS A HACK, IT PREVENTS AN OUT OF MEMORY ERROR BELOW.
			//Despite its hack-like nature, we think this is a reasonable way to do it.  We tried to do it in
			//more elegant ways (such as catching the heap space error, and executing the batch then), but 
			//that caused more problems.
			while (rs.next())
			{
				odbcStmt.addBatch(
						"INSERT INTO Peaks\n" +
						"(DataSet, Filename, MassToCharge, " +
						"PeakArea, " +
						"RelPeakArea, PeakHeight)" +
						"VALUES\n" +
						"(\n" +
						"	'" + newName + "', '" + 
						(new File(rs.getString(1))).getName() + "', " +
						rs.getFloat(2) + ", " + rs.getInt(3) + 
						", " +
						rs.getFloat(4) + ", " + rs.getInt(5) +
				")");
				counter++;
				if(counter>=256) //256 is an arbitrary choice, not too big, but better than executing batch every time.
				{
					odbcStmt.executeBatch();
					counter=0;
				}				
			}
			odbcStmt.executeBatch();
			stmt.execute("DROP TABLE #PeaksToExport");
			odbcCon.close();
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception exporting to MSAccess database.");
			System.err.println("SQL error exporting to " +
			"Access database:");
			e.printStackTrace();
			return null;
		}
		return startTime;
	}
	
	/**
	 * Checks to see if the atom id is a member of the collectionID.
	 * @return true if atom is a member of the collection.
	 */
	public boolean checkAtomParent(int AtomID, int isMemberOf)
	{
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(
					"Select *\n" +
					"FROM AtomMembership\n" +
					"WHERE AtomID = " + AtomID + 
					" AND CollectionID = " + isMemberOf);
			
			if (rs.next())
			{
				rs.close();
				return true;
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception checking atom's parentage.");
			System.err.println("Error checking parentage:");
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Replaces characters which would interrupt SQL Server's 
	 * parsing of a string with their escape equivalents
	 * @param s String to modify
	 * @return The same string except in an acceptable string for
	 * SQL Server
	 */
	private String removeReservedCharacters(String s)
	{
		//Replace additional characters as necessary
		s = s.replace("'","''");
		//s = s.replace('"', ' ');
		return s;
	}
	
	/** (non-Javadoc)
	 * @see database.InfoWarehouse#getPeaks(int)
	 * 
	 * gets an arraylist of peaks given a datatype and atomID.  
	 * ATOFMS-specific.
	 */
	public ArrayList<Peak> getPeaks(String datatype, int atomID) 
	{
		ResultSet rs = null;
		try {
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(
					"SELECT * FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,datatype) + " WHERE AtomID = " +
					atomID + " ORDER BY PeakLocation ");
			//stmt.close();
		} catch (SQLException e) {
			System.err.println("Error selecting peaks");
			e.printStackTrace();
		}
		ArrayList<Peak> returnThis = new ArrayList<Peak>();
		try {
			if(datatype.equals("ATOFMS")){
				float location = 0, relArea = 0;
				int area = 0, height = 0;
				
			while(rs.next())
			{
				location = rs.getFloat("PeakLocation");
				area = rs.getInt("PeakArea");
				relArea = rs.getFloat("RelPeakArea");
				height = rs.getInt("PeakHeight");
				returnThis.add(new ATOFMSPeak(
						height,
						area,
						relArea,
						location));
			} 
			}else if(datatype.equals("AMS")){
				float location = 0, height = 0;
				while(rs.next())
				{
					location = rs.getFloat("PeakLocation");
					height = rs.getFloat("PeakHeight");
					System.out.println("AMS db, height: "+height);
					returnThis.add(new AMSPeak(
							height,
							location));
				} 
			}
			
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving peaks.");
			System.err.println("Error using the result set");
			e.printStackTrace();
		}
		return returnThis;
	}
	
	/* Cursor classes */
	private class ClusteringCursor implements CollectionCursor {
		protected InstancedResultSet irs;
		protected ResultSet rs;
		protected Statement stmt = null;
		private Collection collection;
		private ClusterInformation cInfo;
		private String datatype;
		
		public ClusteringCursor(Collection collection, ClusterInformation cInfo) {
			super();
			this.collection = collection;
			datatype = collection.getDatatype();
			this.cInfo = cInfo;
			rs = getAllAtomsRS(collection);
		}
		
		public boolean next() {
			try {
				return rs.next();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a clustering cursor.");
				System.err.println("Error checking the " +
						"bounds of " +
				"the ResultSet.");
				e.printStackTrace();
				return false;
			}
		}
		
		public ParticleInfo getCurrent() {
			
			ParticleInfo particleInfo = new ParticleInfo();
			try {
				particleInfo.setID(rs.getInt(1));
				particleInfo.setBinnedList(getPeakListfromAtomID(rs.getInt(1)));
			}catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a clustering cursor.");
				System.err.println("Error retrieving the " +
				"next row");
				e.printStackTrace();
				return null;
			}
			return particleInfo; 
		}
		
		public void close() {
			try {
				rs.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a clustering cursor.");
				e.printStackTrace();
			}
		}
		
		public void reset() {
			try {
				rs.close();
				rs = getAllAtomsRS(collection);
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a clustering cursor.");
				System.err.println("Error resetting a " +
						"resultset " +
				"for that collection:");
				e.printStackTrace();
			}	
		}
		
		public ParticleInfo get(int i) throws NoSuchMethodException {
			throw new NoSuchMethodException("Not implemented in disk based cursors.");
		}
		
		public BinnedPeakList getPeakListfromAtomID(int id) {
			BinnedPeakList peakList;
			if (cInfo.normalize)
				peakList = new BinnedPeakList(new Normalizer());
			else
				peakList = new BinnedPeakList(new DummyNormalizer());
			try {
				ResultSet listRS;
				Statement stmt2 = con.createStatement();
				if (cInfo.automatic) {
					listRS = stmt2.executeQuery("SELECT " + join(cInfo.valueColumns, ",") +
							" FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, datatype) +
							" WHERE AtomID = " + id);
					
					listRS.next();
					for (int i = 1; i <= cInfo.valueColumns.size(); i++) {
						//TODO: this is a hack; fix.
						try {
							peakList.addNoChecks(i, listRS.getFloat(i));
						} catch (SQLException e) {
							peakList.addNoChecks(i, listRS.getInt(i));
						}
					}
				}
				else {
					listRS = stmt2.executeQuery("SELECT " + 
							cInfo.keyColumn + ", " + cInfo.valueColumns.iterator().next() +  
							" FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse, datatype) + 
							" WHERE AtomID = " + id);
					while (listRS.next()) 
						peakList.add(listRS.getFloat(1), listRS.getFloat(2));
				} 
				stmt2.close();
				listRS.close();
			}catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a clustering cursor.");
				System.err.println("Error retrieving the " +
				"next row");
				e.printStackTrace();
				return null;
			}
			return peakList;		
		}
		
		public boolean isNormalized() {
			return cInfo.normalize;
		}
	}
	
	/**
	 * Memory Clustering Cursor.  Returns binned peak info for a given atom,
	 * info kept in memory.
	 */
	private class MemoryClusteringCursor extends ClusteringCursor {
		Database db;
		boolean firstPass = true;
		int position = -1;
		
		ArrayList<ParticleInfo> storedInfo = null;
		
		public MemoryClusteringCursor(Collection collection, ClusterInformation cInfo) {
			super (collection, cInfo);
			storedInfo = new ArrayList<ParticleInfo>(100);
		}
		
		public void reset()
		{
			if (firstPass) {
				storedInfo.clear();
				super.reset();
			}
			position = -1;
		}
		
		public boolean next()
		{
			position++;
			if (firstPass)
			{
				boolean superNext = super.next();
				if (superNext)
					storedInfo.add(super.getCurrent());
				else
					firstPass = false;
				return superNext;
			}
			else
				return (position < storedInfo.size());
		}
		
		public ParticleInfo getCurrent()
		{
			return storedInfo.get(position);
		}
		
		public ParticleInfo get(int i)
		{
			if (firstPass)
				if (i < position)
					return storedInfo.get(i);
				else
					return null;
			else
				return storedInfo.get(i);
		}
		
		public BinnedPeakList getPeakListfromAtomID(int atomID) {
			for (ParticleInfo particleInfo : storedInfo) {
				if (particleInfo.getID() == atomID)
					return particleInfo.getBinnedList();
			}
			return super.getPeakListfromAtomID(atomID);
		}
	}
	
	/**
	 * A cursor that returns only AtomID and binnedpeaklist.
	 * About 15 times faster than a BinnedCursor.
	 * 
	 * @author smitht
	 *
	 *
	 * Modified by christej to implement CollectionCursor instead of Iterator
	 */
	public class BPLOnlyCursor implements CollectionCursor{
		private ResultSet rs;
		private Statement stmt;
		public int currID; // the current atomID.
		public int collID;
		private BPLOnlyCursor(Collection coll) throws SQLException {
			collID = coll.getCollectionID();
			stmt = con.createStatement();
			rs = stmt.executeQuery(
				"select AtomID, PeakLocation, PeakArea " +
				"FROM ATOFMSAtomInfoSparse WHERE AtomID in " +
				"(SELECT AtomID FROM InternalAtomOrder " +
				"Where CollectionID = "+coll.getCollectionID()+") " +
				"order by AtomID;");
			if (! rs.next()) {
				throw new SQLException("Empty collection or a problem!");
			}
			currID = rs.getInt(1);
		}
		
		public void close() {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		public ParticleInfo get(int i) throws NoSuchMethodException {
			throw new NoSuchMethodException("Not implemented in disk based cursors.");
		}

		public ParticleInfo getCurrent() {
			ParticleInfo p = new ParticleInfo();
			try {
				int retAtom = currID;
				BinnedPeakList bpl = new BinnedPeakList();
				while (rs.getInt(1) == retAtom) {
					bpl.add(rs.getInt(2), rs.getInt(3));
	
					if (! rs.next()) {
						break;
					}
				}
				if (! rs.isAfterLast())
					currID = rs.getInt(1);
	
				p.setBinnedList(bpl);
				p.setID(retAtom);
				return p;
			} catch (SQLException e) {
				throw new NoSuchElementException(e.toString());
			}
		}

		public BinnedPeakList getPeakListfromAtomID(int id) {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("getPeakListfromAtomID not implemented for BPLOnlyCursor");
		}

		public boolean next() {
			try {
				return ! rs.isAfterLast();
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}

		public void reset() {
			try {
				rs.close();
				String q = "select AtomID, PeakLocation, PeakArea " +
				"FROM ATOFMSAtomInfoSparse WHERE AtomID in " +
				"(SELECT AtomID FROM InternalAtomOrder " +
				"Where CollectionID = "+collID +") " +
				"order by AtomID;";
				rs = stmt.executeQuery(q);
				if (! rs.next()) {
					throw new SQLException("Empty collection or a problem!");
				}
				currID = rs.getInt(1);
				
				//System.out.println(currID);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}

		
	}
	
	// returns the centroid associated with a given cluster collection
	// makes post-processing of clustering solutions easier
	// Michael Murphy 2014
	public class CentroidCursor implements CollectionCursor{
		private ResultSet rs;
		private Statement stmt;
		private boolean isEmpty = false;
		public int currID = -1; // the current atomID.
		public int collID;
		private CentroidCursor(Collection coll) throws SQLException {
			collID = coll.getCollectionID();
			stmt = con.createStatement();
			rs = stmt.executeQuery(
				"select AtomID, PeakLocation, PeakArea " +
				"FROM ATOFMSAtomInfoSparse WHERE AtomID in " +
				"(SELECT AtomID FROM CenterAtoms " +
				"Where CollectionID = "+coll.getCollectionID()+") " +
				"order by AtomID;");
			if (! rs.next()) {
				isEmpty = true;
			} else {
				currID = rs.getInt(1);
			}
		}
		
		public boolean isEmpty() {
			return isEmpty;
		}
		
		public void close() {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		public ParticleInfo get(int i) throws NoSuchMethodException {
			throw new NoSuchMethodException("Not implemented in disk based cursors.");
		}

		public ParticleInfo getCurrent() {
			ParticleInfo p = new ParticleInfo();
			try {
				int retAtom = currID;
				BinnedPeakList bpl = new BinnedPeakList();
				while (rs.getInt(1) == retAtom) {
					bpl.add(rs.getInt(2), rs.getInt(3));
	
					if (! rs.next()) {
						break;
					}
				}
				if (! rs.isAfterLast())
					currID = rs.getInt(1);
	
				p.setBinnedList(bpl);
				p.setID(retAtom);
				return p;
			} catch (SQLException e) {
				throw new NoSuchElementException(e.toString());
			}
		}

		public BinnedPeakList getPeakListfromAtomID(int id) {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("getPeakListfromAtomID not implemented for CentroidCursor");
		}

		public boolean next() {
			try {
				return ! rs.isAfterLast();
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}

		public void reset() {
			try {
				rs.close();
				String q = "select AtomID, PeakLocation, PeakArea " +
				"FROM ATOFMSAtomInfoSparse WHERE AtomID in " +
				"(SELECT AtomID FROM CenterAtoms " +
				"Where CollectionID = "+collID +") " +
				"order by AtomID;";
				//System.out.println(q);
				rs = stmt.executeQuery(q);
				if (! rs.next()) {
					throw new SQLException("Empty collection or a problem!");
				}
				currID = rs.getInt(1);
				
				//System.out.println(currID);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}

		
	}

	/**
	 * AtomInfoOnly cursor.  Returns atom info.
	 */
	private class AtomInfoOnlyCursor 
	implements CollectionCursor {
		protected InstancedResultSet irs;
		protected ResultSet partInfRS = null;
		protected Statement stmt = null;
		Collection collection;
		
		public AtomInfoOnlyCursor(Collection col) {
			super();
			assert(col.getDatatype().equals("ATOFMS")) : "Wrong datatype for cursor.";
			collection = col;
			try {
				stmt = con.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String q = "SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
						" LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
						" InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
						" AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID";
			try {
				partInfRS = stmt.executeQuery(q);
				/*partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
						" LaserPower, [Time] FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
						" InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
						" AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID");*/
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void reset()
		{		
			try {
				partInfRS.close();
				partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
						" LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
						" InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
						" AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID");
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
				System.err.println("SQL Error resetting " +
				"cursor: ");
				e.printStackTrace();
			}
		}
		
		public boolean next() {
			try {
				return partInfRS.next();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
				System.err.println("Error checking the " +
						"bounds of " +
				"the ResultSet.");
				e.printStackTrace();
				return false;
			}
		}
		
		/* (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		public ParticleInfo getCurrent() {
			try {
				ParticleInfo particleInfo = new ParticleInfo();
				particleInfo.setParticleInfo(
						new ATOFMSAtomFromDB(
								partInfRS.getInt(1),
								partInfRS.getString(2),
								partInfRS.getInt(3),
								partInfRS.getFloat(4),
								TimeUtilities.iso8601ToDate(partInfRS.getString(5)),
								partInfRS.getFloat(6)));
				particleInfo.setID(particleInfo.getATOFMSParticleInfo().getAtomID());
				return particleInfo; 
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
				System.err.println("Error retrieving the next row");
				e.printStackTrace();
				return null;
			} catch (Exception e) {
				throw new ExceptionAdapter(e);
			}
		}
		
		public void close() {
			try {
				stmt.close();
				partInfRS.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
				e.printStackTrace();
			}
		}
		
		public ParticleInfo get(int i) 
		throws NoSuchMethodException {
			throw new NoSuchMethodException("Not implemented in disk based cursors.");
		}
		
		public BinnedPeakList getPeakListfromAtomID(int atomID) {
			BinnedPeakList peakList = new BinnedPeakList(new Normalizer());
			try {
				ResultSet rs = 
					con.createStatement().executeQuery(
							"SELECT PeakLocation,PeakArea\n" +
							"FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
							"WHERE AtomID = " + atomID);
				while(rs.next()) {
					peakList.add(
							rs.getFloat(1),
							rs.getInt(2));
				}
				rs.close();
				return peakList;
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
				System.err.println("Error retrieving peak " +
				"list.");
				e.printStackTrace();
				return null;
			}
		}
	}
	
	/**
	 * A cursor that only returns AtomID for a given collection and SQL query string.
	 * Similar to SQLCursor, but retrieves no associated particle information.
	 * @author shaferia
	 * @author jtbigwoo - updated to use rownum in the where clause since 
	 * AtomID is not a reliable column to query on if you want a certain number
	 * of particles (e.g. AtomID >= 1 and AtomID <= 5 does _not_ guarantee 
	 * you'll get five particles--rownum >= 1 and rownum <= 5 does.)
	 */
	private class SQLAtomIDCursor implements CollectionCursor {
		private final String query;
		private Statement stmt;
		private ResultSet rs;
		
		/**
		 * Sets up a cursor that returns only the AtomIDs in the collection 
		 * that match the supplied where clause.  The where clause can include
		 * any AtomInfoDense columns that you want and also rownum which is the
		 * index of the particle in InternalAtomOrder
		 * @param collection The collection to retrieve AtomIDs from
		 * @param where the SQL clause that specifies which AtomIDs to retrieve
		 * 
		 * MM: This was modified considerably to allow querying of mass spectral peak data.
		 * It's probably slower now that it's querying both tables, but this was the
		 * "least invasive" way to do it.
		 * 
		 */

		public SQLAtomIDCursor(Collection collection, String where) {
			int collectionID = collection.getCollectionID();
			String densewhere = "";
			String sparsewhere = "";
			String[] splitwhere = where.split(";", -1);

			if (splitwhere[0].length() > 0)
				densewhere += "WHERE " + splitwhere[0];
			if (splitwhere.length > 1 && splitwhere[1].length() > 0)
				sparsewhere += "AND " + splitwhere[1];

			String densename = getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype());
			String sparsename = getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype());
			// MM: This query allows Enchilada to filter for criteria from both the dense and sparse datatables
			query = "SELECT DISTINCT AtomID FROM (\n" + // DISTINCT should be redundant here...
					"    SELECT DISTINCT d.*, ROW_NUMBER() OVER (ORDER BY InternalAtomOrder.AtomID) AS rownum\n" +
					"        FROM " + densename + " AS d, InternalAtomOrder\n" +
					"        WHERE InternalAtomOrder.CollectionID = " + collectionID + " AND d.AtomID = InternalAtomOrder.AtomID\n" +
					"        AND d.AtomID IN (\n" +
					"            SELECT DISTINCT AtomID FROM (\n" +
					"                SELECT s.*\n" +
					"                FROM " + sparsename + " AS s, InternalAtomOrder\n" +
					"                    WHERE InternalAtomOrder.CollectionID = " + collectionID + " AND s.AtomID = InternalAtomOrder.AtomID " + sparsewhere + "\n" +
					"            ) AS subtable\n" +
					"        )\n" +
					") AS temptable " + densewhere;

			try {
				stmt = getCon().createStatement();
				rs = stmt.executeQuery(query);
			}
			catch (SQLException ex) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception creating SQLAtomIDCursor.");
				ex.printStackTrace();
			}
		}

		public void close() {
			try {
				stmt.close();
				rs.close();
			}
			catch (SQLException ex) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception closing SQLAtomIDCursor.");
				ex.printStackTrace();
			}
		}

		public ParticleInfo get(int i) throws NoSuchMethodException {
			throw new NoSuchMethodException(
					"SQLAtomIDCursor does not implement get(int)");
		}

		public ParticleInfo getCurrent() {
			ParticleInfo p = new ParticleInfo();
			try {
				p.setID(rs.getInt(1));
				return p;
			}
			catch (SQLException ex) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception retrieving AtomID with SQLAtomIDCursor.");
				ex.printStackTrace();
				return null;
			}
		}

		/**
		 * Not implemented for an AtomIDCursor - the datatype could theoretically
		 * not have any spectrum information.
		 */
		public BinnedPeakList getPeakListfromAtomID(int id) {
			return null;
		}

		public boolean next() {
			try {
				return rs.next();
			}
			catch (SQLException ex) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Could not advance to next AtomID with SQLAtomIDCursor");
				return false;
			}
		}

		public void reset() {
			try {
				stmt = getCon().createStatement();
				rs = stmt.executeQuery(query);
			}
			catch (SQLException ex) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception resetting SQLAtomIDCursor.");
				ex.printStackTrace();
			}			
		}
	}
	
	/**
	 * SQL Cursor.  Returns atom info with a given "where" clause.
	 */
	private class SQLCursor extends AtomInfoOnlyCursor
	{
		private Statement stmt;
		private String where;
		public Collection collection;
		Database db;
		/**
		 * @param collectionID
		 */
		public SQLCursor(Collection col, String where, Database db) {
			super(col);
			collection = col;
			this.where = where;
			this.db = db;
			try {
				stmt = con.createStatement();
				partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
						" LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
						" InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
						" AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID" +
						" AND " + where);
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a SQL cursor.");
				e.printStackTrace();
			}
		}
		
		public void close() {
			try {
				stmt.close();
				super.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a SQL cursor.");
				e.printStackTrace();
			}
		}
		public void reset()
		{
			
			try {
				partInfRS.close();
				partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
						" LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
						" InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
						" AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID" +
						" AND " + where);
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a SQL cursor.");
				System.err.println("SQL Error resetting cursor: ");
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Peak Cursor.  Returns peak info for a given atom.
	 */
	private class PeakCursor extends AtomInfoOnlyCursor
	{	
		protected Statement stmt = null;
		protected ResultSet peakRS = null;
		Collection collection;
		
		public PeakCursor(Collection col)
		{
			super (col);
			collection = col;
			try {
				stmt = con.createStatement();
				
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a Peak cursor.");
				e.printStackTrace();
			}
		}
		
		public ParticleInfo getCurrent()
		{
			// This should get overridden in other classes,
			//however, its results from here should be used.
			
			ParticleInfo pInfo = new ParticleInfo();
			PeakList pList = new PeakList();
			ArrayList<Peak> aPeakList = new ArrayList<Peak>();
			try {
				pList.setAtomID(partInfRS.getInt(1));
				peakRS = stmt.executeQuery("SELECT PeakHeight, PeakArea, " +
						"RelPeakArea, PeakLocation\n" +
						"FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
						"WHERE AtomID = " + pList.getAtomID());
				while (peakRS.next())
				{
					aPeakList.add(new ATOFMSPeak(peakRS.getInt(1), peakRS.getInt(2), peakRS.getFloat(3),
							peakRS.getFloat(4)));
				}
				pList.setPeakList(aPeakList);
				pInfo.setPeakList(pList);
				pInfo.setID(pList.getAtomID());
				peakRS.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a Peak cursor.");
				e.printStackTrace();
			}
			
			return pInfo;
		}
		
		public void close(){
			try {
				peakRS.close();
				super.close();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a Peak cursor.");
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Binned Cursor.  Returns binned peak info for a given atom.
	 */
	private class BinnedCursor extends PeakCursor {
		
		/**
		 * @param collectionID
		 */
		public BinnedCursor(Collection collection) {
			super(collection);
		}
		
		public ParticleInfo getCurrent()
		{
			ParticleInfo sPInfo = super.getCurrent();
			sPInfo.setBinnedList(bin(sPInfo.getPeakList().getPeakList()));
			return sPInfo;
		}
		
		private BinnedPeakList bin(ArrayList<Peak> peakList)
		{
			BinnedPeakList bPList = new BinnedPeakList(new Normalizer());
			
			Peak temp;
			
			for(int i = 0; i < peakList.size(); i++)
			{
				temp = peakList.get(i);
				bPList.add((float)temp.massToCharge, (float)temp.value);
			}
			return bPList;
		}
	}
	
	/**
	 * Randomized Cursor.  Returns randomized atom info.
	 *
	 * NOTE:  Randomization cursor info found at 
	 * http://www.sqlteam.com/item.asp?ItemID=217
	 * 
	 * Updated 12/05 at http://www.sqlteam.com/item.asp?ItemID=217
	 * 
	 * Updated again 6/06 at http://www.sqlteam.com/item.asp?ItemID=8747
	 * which has a much faster method for random ordering, but it will
	 * only work on Windows 2000 and above.
	 */
	private class RandomizedCursor extends BinnedCursor {
		protected Statement stmt = null;
		private double randMultiplier;
		private Collection collection;

		public RandomizedCursor(Collection col) {
			super(col);
			collection = col;
			randMultiplier = new Random(randomSeed).nextDouble();

			try {
				stmt = con.createStatement();
				executeRandomizedCursorQuery();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a Randomized cursor.");
				System.err.println("Could not randomize atoms.");
				e.printStackTrace();
			}
		}

		public void close() {
			super.close();
		}
		public void reset()
		{
			try {
				partInfRS.close();
				executeRandomizedCursorQuery();
			} catch (SQLException e) {
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a Randomized cursor.");
				System.err.println("SQL Error resetting cursor: ");
				e.printStackTrace();
			}
		}

		private void executeRandomizedCursorQuery() throws SQLException {
			// Cheap way of getting seedable, repeatable random ordering
			// https://stackoverflow.com/a/24511461/490329
			String q = "SELECT " + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + ".AtomID, OrigFilename, ScatDelay," +
					" LaserPower, [Time], Size FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + ", InternalAtomOrder WHERE" +
					" InternalAtomOrder.CollectionID = " + collection.getCollectionID() +
					" AND " + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + ".AtomID = InternalAtomOrder.AtomID" +
					" ORDER BY substr(ATOFMSAtomInfoDense.AtomId * " + randMultiplier + ", length(ATOFMSAtomInfoDense.AtomId) + 2)";
			partInfRS = stmt.executeQuery(q);
		}
	}
	
	/**
	 * Memory Binned Cursor.  Returns binned peak info for a given atom,
	 * info kept in memory.
	 */
	public class MemoryBinnedCursor extends BinnedCursor {
		Database db;
		boolean firstPass = true;
		int position = -1;
		
		ArrayList<ParticleInfo> storedInfo = null;
		
		public MemoryBinnedCursor(Collection collection) {
			super (collection);
			storedInfo = new ArrayList<ParticleInfo>(100);
		}
		
		public void reset()
		{
			if (firstPass) {
				storedInfo.clear();
				super.reset();
			}
			position = -1;
		}
		
		public boolean next()
		{
			position++;
			if (firstPass)
			{
				boolean superNext = super.next();
				if (superNext)
					storedInfo.add(super.getCurrent());
				else
					firstPass = false;
				return superNext;
			}
			else
				return (position < storedInfo.size());
		}
		
		public ParticleInfo getCurrent()
		{
			return storedInfo.get(position);
		}
		
		public ParticleInfo get(int i)
		{
			if (firstPass)
				if (i < position)
					return storedInfo.get(i);
				else
					return null;
			else
				return storedInfo.get(i);
		}
		
		public BinnedPeakList getPeakListfromAtomID(int atomID) {
			for (ParticleInfo particleInfo : storedInfo) {
				if (particleInfo.getID() == atomID)
					return particleInfo.getBinnedList();
			}
			return new BinnedPeakList(new Normalizer());
		}
	}
	
	/**
	 * get method for atomInfoOnlyCursor.
	 */
	public CollectionCursor getAtomInfoOnlyCursor(Collection collection)
	{
		return new AtomInfoOnlyCursor(collection);
	}

	/**
	 * get method for SQLCursor.
	 */
	public CollectionCursor getSQLAtomIDCursor(Collection collection, 
			String where)
	{
		return new SQLAtomIDCursor(collection, where);
	}
	
	/**
	 * get method for SQLCursor.
	 */
	public CollectionCursor getSQLCursor(Collection collection, 
			String where)
	{
		return new SQLCursor(collection, where, this);
	}
	
	/**
	 * get method for peakCursor.
	 */
	public CollectionCursor getPeakCursor(Collection collection)
	{
		return new PeakCursor(collection);
	}
	
	/**
	 * get method for BPLOnlyCursor.
	 */
	public BPLOnlyCursor getBPLOnlyCursor(Collection collection) throws SQLException {
		return new BPLOnlyCursor(collection);
	}
	
	public CentroidCursor getCentroidCursor(Collection collection) throws SQLException {
		return new CentroidCursor(collection);
	}
	
	/**
	 * get method for BinnedCursor.
	 */
	public CollectionCursor getBinnedCursor(Collection collection)
	{
		return new BinnedCursor(collection);
	}
	
	/**
	 * get method for MemoryBinnedCursor.
	 */
	public MemoryBinnedCursor getMemoryBinnedCursor(Collection collection)
	{
		return new MemoryBinnedCursor(collection);
	}
	
	/**
	 * get method for randomizedCursor.
	 */
	public CollectionCursor getRandomizedCursor(Collection collection)
	{
		return new RandomizedCursor(collection);
	}
	
	/**
	 * get method for ClusteringCursor.
	 */
	public CollectionCursor getClusteringCursor(Collection collection, ClusterInformation cInfo)
	{
		return new ClusteringCursor(collection, cInfo);
	}
	
	/**
	 * Seeds the random number generator.
	 */
	public void seedRandom(int seed) {
		randomSeed = seed;
	}
	
	/**
	 * getColNamesAndTypes returns an arraylist of strings of the column names for the given table
	 * and datatype.  Not used yet, but may be useful in the future.  
	 * @param datatype
	 * @param table - dynamic table you want
	 * @return arraylist of column names and their types.
	 */
	public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table) {
		ArrayList<ArrayList<String>> colNames = new ArrayList<ArrayList<String>>();
		ArrayList<String> temp;	
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ColumnName, ColumnType FROM MetaData " +
					"WHERE Datatype = '" + datatype + "' " +
					"AND TableID = " + table.ordinal() + " ORDER BY ColumnOrder");

			while (rs.next()) {
				temp = new ArrayList<String>();
				temp.add(rs.getString(1));
				temp.add(rs.getString(2));
				colNames.add(temp);
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving column names.");
			System.err.println("Error retrieving column names");
			e.printStackTrace();
		}
		return colNames;
	}
	
	/**
	 * getColNamesAndTypes returns an arraylist of strings of the column names for the given table
	 * and datatype.  Not used yet, but may be useful in the future.  
	 * @param datatype
	 * @param table - dynamic table you want
	 * @return arraylist of column names.
	 */
	public ArrayList<String> getColNames(String datatype, DynamicTable table) {
		ArrayList<String> colNames = new ArrayList<String>();
		
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ColumnName FROM MetaData " +
					"WHERE Datatype = '" + datatype + "' " +
					"AND TableID = " + table.ordinal() + " ORDER BY ColumnOrder");
			
			while (rs.next()) 
				colNames.add(rs.getString(1));
			
			stmt.close();
		} catch (SQLException e) {
			System.err.println("Error retrieving column names");
			e.printStackTrace();
		}
		return colNames;
	}
	
	//TODO: @@identity isn't in MySQL
	public int saveMap(String name, Vector<int[]> mapRanges) {
		int valueMapID = -1;
		
		try{
			Statement stmt = con.createStatement();		
			ResultSet rs 
			= stmt.executeQuery("SET NOCOUNT ON " +
					"INSERT ValueMaps (Name) Values('" + removeReservedCharacters(name) + "') " +
					"SELECT @@identity " +
			"SET NOCOUNT OFF");
			rs.next();
			valueMapID = rs.getInt(1);
			rs.close();
			
			for (int i = 0; i < mapRanges.size(); i++) {
				int[] range = mapRanges.get(i);
				
				stmt.execute(
						"INSERT ValueMapRanges (ValueMapID, Value, Low, High) " +
						"VALUES ("+valueMapID+","+range[0]+","+range[1]+","+range[2]+")");
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception inserting new value map range.");
			System.err.println("Error inserting new value map range");
			e.printStackTrace();
		}
		return valueMapID;
	}
	
	public Hashtable<Integer, String> getValueMaps() {
		Hashtable<Integer, String> valueMaps = new Hashtable<Integer, String>();
		
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ValueMapID, Name from ValueMaps");
			
			while (rs.next())
				valueMaps.put(rs.getInt("ValueMapID"), rs.getString("Name"));
			rs.close();
			stmt.close();
		}
		catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving value maps");
			System.err.println("Error getting value maps from database.");
			e.printStackTrace();
		}
		
		return valueMaps;
	}
	
	public Vector<int[]> getValueMapRanges() {
		Vector<int[]> valueMapRanges = new Vector<int[]>();
		
		try {
			Statement stmt = con.createStatement();
			ResultSet rs 
			= stmt.executeQuery("SELECT ValueMapID, Value, Low, High " +
					"FROM ValueMapRanges " +
			"ORDER BY ValueMapID, Low ");
			
			while (rs.next())
				valueMapRanges.add(new int[] {rs.getInt("ValueMapID"), rs.getInt("Value"), rs.getInt("Low"), rs.getInt("High") });
			rs.close();
			stmt.close();
		}
		catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving value map ranges");
			System.err.println("Error getting value map ranges from database.");
			e.printStackTrace();
		}
		
		return valueMapRanges;
	}
	
	public int applyMap(String mapName, Vector<int[]> map, Collection collection) {
		int oldCollectionID = collection.getCollectionID();
		String dataType = collection.getDatatype();
		
		int newCollectionID = createEmptyCollection(dataType, oldCollectionID, mapName, "", "");
		String tableName = getDynamicTableName(DynamicTable.AtomInfoDense, dataType);
		
		int nextAtomID = getNextID();
		String mapStatement = "CASE";
		for (int i = 0; i < map.size(); i++) {
			int[] curMap = map.get(i);
			mapStatement += " WHEN T.Value >= " + curMap[1] + " AND T.Value < " + curMap[2] + " THEN " + curMap[0];
		}
		mapStatement += " ELSE NULL END";
		
		String query = 
			"DECLARE @atoms TABLE ( " +
			"   NewAtomID int IDENTITY(" + nextAtomID + ",1), " +
			"   OldAtomID int " +
			") " +
			
			" insert @atoms (OldAtomID) " +
			" select AtomID from AtomMembership where CollectionID = " + oldCollectionID +
			
			" insert AtomMembership (CollectionID, AtomID) " +
			" select " + newCollectionID + ", NewAtomID" +
			" from @atoms A" +
			
			" insert " + tableName + " (AtomID, Time, Value) " +
			" select A.NewAtomID, T.Time, " + mapStatement + " as Value" +
			" from " + tableName + " T " +
			" join @atoms A on (A.OldAtomID = T.AtomID)";
		
		System.out.println(query);
		
		try {
			Statement stmt = con.createStatement();
			stmt.execute(query);
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception creating new mapped collection");
			System.err.println("Error creating new mapped collection.");
			e.printStackTrace();
		}
		
		return newCollectionID;
	}
	
	/** 
	 * Gets the maximum and minimum dates of the atoms in all collections 
	 * about to be aggregated.
	 */
	public void getMaxMinDateInCollections(Collection[] collections, Calendar minDate, Calendar maxDate) {
		String cIDs = "";
		ArrayList<String> infoDenseNames = new ArrayList<String>();
		for (int i = 0; i < collections.length;i++) {
			cIDs += collections[i].getCollectionID();
			if (i != collections.length-1)
				cIDs += ",";
			String infoDenseStr = collections[i].getDatatype()+"AtomInfoDense";
			if (!infoDenseNames.contains(infoDenseStr)) {
				//System.out.println(infoDenseStr);
				infoDenseNames.add(infoDenseStr);
			}
		}
		assert (infoDenseNames.size() > 0):"no datatypes defined.";
		
		try{
			Statement stmt = con.createStatement();
			stmt.executeUpdate("CREATE INDEX iao_index ON InternalAtomOrder (CollectionID);\n");

			StringBuilder sqlStr = new StringBuilder();
			sqlStr.append("SELECT MAX(Time) as MaxTime, MIN(Time) as MinTime\nFROM(\n");
			sqlStr.append("SELECT AtomID, Time FROM "+ infoDenseNames.get(0)+"\n");
			for (int i = 1; i < infoDenseNames.size(); i++)
				sqlStr.append("UNION SELECT AtomID, Time FROM " + infoDenseNames.get(i)+"\n");
			sqlStr.append(") AID, InternalAtomOrder IAO\n"+
					"WHERE IAO.CollectionID in ("+cIDs+")\n" +
							"AND AID.AtomID = IAO.AtomID;\n");

			ResultSet rs = stmt.executeQuery(sqlStr.toString());
			if (rs.next()) {
				String minTime = rs.getString("MinTime");
				if (!rs.wasNull()) {
					Date minT = TimeUtilities.iso8601ToDate(minTime);
					minDate.setTime(minT);
				}

				String maxTime = rs.getString("MaxTime");
				if (!rs.wasNull()) {
					Date maxT = TimeUtilities.iso8601ToDate(maxTime);
					maxDate.setTime(maxT);
				}
			}

			stmt.executeUpdate("DROP INDEX iao_index;\n");

			rs.close();
			stmt.close();
		} catch (SQLException | ParseException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "Exception retrieving max time for collections.");
			System.err.println("Exception retrieving max time for collections");
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * Creates a table of all the appropriate atomIDs and the binned
	 * times.  If the collection is the one that the aggregation is based on,
	 * the table is just a copy of the original one.
	 * 
	 * NOTE:  ALSO HAS A LIST OF ALL ATOM IDS IN COLLECTION!
	 */
	public void createTempAggregateBasis(Collection c, Collection basis) {
		System.out.println("collection 1: " + c.getCollectionID() + "\ncollection 2: "+basis.getCollectionID());
		File tempFile = null;
		// grabbing the times for all subcollectionIDs
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS temp.TimeBins;\n");
			stmt.executeUpdate("CREATE TEMPORARY TABLE TimeBins (AtomID INT, BinnedTime datetime, PRIMARY KEY (AtomID));\n");
			// if aggregation is based on this collection, copy table
			if (c.getCollectionID() == basis.getCollectionID()) {	
				System.out.println("copying table...");
				stmt.executeUpdate("INSERT INTO temp.TimeBins (AtomID, BinnedTime)\n"+
						"SELECT AID.AtomID, Time FROM "+getDynamicTableName(DynamicTable.AtomInfoDense, c.getDatatype())+" AID,\n"+
						"InternalAtomOrder IAO \n"+
						"WHERE IAO.AtomID = AID.AtomID\n" +
						"AND CollectionID = "+c.getCollectionID()+"\n"+
				"ORDER BY Time;\n");
			}
			// else, perform a join merge on the two collections.
			else {
				Statement stmt1 = con.createStatement();
				Statement stmt2 = con.createStatement();
				// get distinct times from basis collection
				ResultSet basisRS = stmt1.executeQuery("SELECT DISTINCT Time \n" +
						"FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, basis.getDatatype()) + " AID,\n" +
						"InternalAtomOrder IAO \n"+
						"WHERE IAO.AtomID = AID.AtomID\n" +
						"AND CollectionID = "+basis.getCollectionID()+"\n"+
				"ORDER BY Time;\n");
				// get all times from collection to bin.
				ResultSet collectionRS = stmt2.executeQuery("SELECT AID.AtomID, Time \n" +
						"FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, c.getDatatype()) + " AID,\n" +
						"InternalAtomOrder IAO \n"+
						"WHERE IAO.AtomID = AID.AtomID\n" +
						"AND CollectionID = "+c.getCollectionID()+"\n"+
				"ORDER BY Time;\n");
				
				// initialize first values:
				String nextBin = null;
				boolean test = basisRS.next();
				assert (test) : "no basis times for collection!";
				String currentBin = basisRS.getString(1);
				collectionRS.next();
				int atomID = collectionRS.getInt(1);
				String collectionTime = collectionRS.getString(2);
				boolean next = true;

				// We skip the times before the first bin.
				while (collectionTime.compareTo(currentBin) < 0) {
					next = collectionRS.next();
					if (!next)
						break;
					else {
						atomID = collectionRS.getInt(1);
						collectionTime = collectionRS.getString(2);
					}
				}
				//	while the next time bin is legal...

				PreparedStatement pstmt = con.prepareStatement("INSERT INTO temp.TimeBins VALUES (?,?)");
				int counter = 0;
				while (next && basisRS.next()) {
					nextBin = basisRS.getString(1);
					// while collectionTime is within bin, insert it in table.
					while (collectionTime.compareTo(nextBin) < 0) {
						pstmt.setInt(1, atomID);
						pstmt.setString(2, currentBin);
//						stmt.executeUpdate("INSERT INTO temp.TimeBins VALUES ("+atomID+",'"+currentBin+"');\n");
						pstmt.addBatch();
						counter++;
						next = collectionRS.next();
						if (!next)
							break;
						else {
							atomID = collectionRS.getInt(1);
							collectionTime = collectionRS.getString(2);
						}
						if (counter > 500) {
							pstmt.executeBatch();
							counter = 0;
							pstmt.clearBatch();
						}
					}
					currentBin = nextBin;
				}
				pstmt.executeBatch();

				//	We skip the times after the last bin.
				stmt1.close();
				stmt2.close();
			}
			stmt.close();
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
			}
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
	}

	/**
	 * Assign each Atom in c to a 'bin' and  store this information into the #TimeBins table
	 * This method uses the start, end and interval parameters and 
	 * builds the SQL statement and then creates the #TimeBins Table;
	 * 
	 * NOTE:  ALSO HAS A LIST OF ALL ATOM IDS IN COLLECTION!
	 */
	public void createTempAggregateBasis(Collection c, Calendar start, Calendar end, Calendar interval) {
		System.out.println("collection: "+c.getCollectionID()+"\nstart: "+start.getTimeInMillis()+"\nend: "+end.getTimeInMillis()+"\ninterval: "+interval.getTimeInMillis());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar increment = (Calendar) start.clone();
		int counter = 0;
		try (
			Statement stmt = con.createStatement();
			Statement stmt1 = con.createStatement();
		) {
			stmt.executeUpdate("DROP TABLE IF EXISTS temp.TimeBins;");
			stmt.executeUpdate("CREATE TEMPORARY TABLE TimeBins (AtomID INT, BinnedTime datetime, PRIMARY KEY (AtomID));");
			PreparedStatement timeBinsInsertStmt = con.prepareStatement("INSERT INTO temp.TimeBins VALUES (?, ?);");
			counter++;
			// get all times from collection to bin.
			ResultSet collectionRS = stmt1.executeQuery("SELECT AID.AtomID, Time \n" +
					"FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, c.getDatatype()) + " AID,\n" +
					"InternalAtomOrder IAO \n" +
					"WHERE IAO.AtomID = AID.AtomID\n" +
					"AND CollectionID = " + c.getCollectionID() + "\n" +
					"ORDER BY Time, AID.AtomID;\n");

			// initialize first values:
			collectionRS.next();
			int atomID = collectionRS.getInt(1);
			Date collectionTime = TimeUtilities.iso8601ToDate(collectionRS.getString(2));
			Date basisTime = increment.getTime();
			Date nextTime = null;
			boolean next = true;

			// if there are times before the first bin, skip them.
			while (basisTime.compareTo(collectionTime) > 0) {
				next = collectionRS.next();
				if (!next)
					break;
				else {
					atomID = collectionRS.getInt(1);
					collectionTime = TimeUtilities.iso8601ToDate(collectionRS.getString(2));
				}
			}
			// while the next time bin is legal...
			while (next) {
				increment.add(Calendar.DATE, interval.get(Calendar.DATE) - 1);
				increment.add(Calendar.HOUR, interval.get(Calendar.HOUR_OF_DAY));
				increment.add(Calendar.MINUTE, interval.get(Calendar.MINUTE));
				increment.add(Calendar.SECOND, interval.get(Calendar.SECOND));
				nextTime = increment.getTime();
				while (next && nextTime.compareTo((Date) collectionTime) > 0) {
					timeBinsInsertStmt.setInt(1, atomID);
					timeBinsInsertStmt.setString(2, dateFormat.format(basisTime));
					timeBinsInsertStmt.addBatch();
					//stmt.executeUpdate("INSERT INTO temp.TimeBins VALUES ("+atomID+",'"+dateFormat.format(basisTime)+"');");
					counter++;
					next = collectionRS.next();
					if (!next)
						break;
					atomID = collectionRS.getInt(1);
					collectionTime = TimeUtilities.iso8601ToDate(collectionRS.getString(2));
					if (counter > 1000) {
						timeBinsInsertStmt.executeBatch();
						counter = 0;
						timeBinsInsertStmt.clearBatch();
					}
				}
				if (nextTime.compareTo(end.getTime()) > 0)
					next = false;
				else
					basisTime = nextTime;


			}

			// if there are still more times, skip them.
			timeBinsInsertStmt.executeBatch();
		} catch (SQLException | ParseException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception creating aggregate basis temp table");
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * Deletes the most recent temp aggregate basis table.
	 */
	public void deleteTempAggregateBasis() {
		try {
			Statement stmt = con.createStatement();
			stmt.execute("DROP TABLE IF EXISTS temp.TimeBins;\n");
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception deleting aggregate basis temp table");
			throw new ExceptionAdapter(e);
		}
	}
	
	public boolean beginTransaction(){
		try {
			con.setAutoCommit(false);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean rollbackTransaction(){
		try {
			System.out.println("rolling back to savepoint");
			long start = System.currentTimeMillis();
			con.rollback();
			long stop = System.currentTimeMillis();
			con.setAutoCommit(true);
			System.out.println("rolled back to savepoint in "+(stop-start)+" milliseconds.");
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean commitTransaction(){
		try{
			con.commit();
			con.setAutoCommit(true);//stop doing transactions
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	/**
	 * This creates the populates a time-series collection and returns the CollectionID of the new Collection.  
	 * This only handles ATOFMS and TIME SERIES data, which will need to
	 * be changed.
	 * 
	 * @return CollectionID of the new collection or -1 if aggregation failed or was cancelled
	 */
	public boolean createAggregateTimeSeries(ProgressBarWrapper progressBar, int rootCollectionID, Collection curColl, int[] mzValues)
			throws InterruptedException {
		int collectionID = curColl.getCollectionID();
		String collectionName = curColl.getName();
		AggregationOptions options = curColl.getAggregationOptions();
		try {

			Statement stmt = con.createStatement();

			//	create #atoms table
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS tmpatoms " + "" +
					"(NewAtomID INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
					" Time DateTime, \n MZLocation int, \n Value real, \n UNIQUE (MZLocation,Time));\n");
			stmt.executeUpdate("DELETE FROM tmpatoms");
			setNextAtomID(getNextID());

			// create the root collection

			// Create and Populate #atoms table with appropriate information.
			/* IF DATATYPE IS ATOFMS */
			if (curColl.getDatatype().equals("ATOFMS")) {
				if (mzValues == null) {
					ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error! Collection: " + collectionName + " doesn't have any peak data to aggregate!");
					System.err.println("Collection: " + collectionID + "  doesn't have any peak data to aggregate!");
					System.err.println("Collections need to overlap times in order to be aggregated.");
					return false;
				}

				aggreateATOFMSUpdate(progressBar, rootCollectionID, mzValues, collectionName, options);

				/* IF DATATYPE IS TIME SERIES */
			} else if (curColl.getDatatype().equals("TimeSeries")) {
				aggregateTimeSeriesUpdate(progressBar, rootCollectionID, collectionName, options);
			}
			/* IF DATATYPE IS AMS */
			else if (curColl.getDatatype().equals("AMS")) {
				if (mzValues == null || mzValues.length == 0) {
					ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Collection: " + collectionName + " doesn't have any peak data to aggregate");
					System.err.println("Collection: " + collectionID + "  doesn't have any peak data to aggregate");
					System.err.println("Collections need to overlap times in order to be aggregated.");
					return false;
				}

				aggregateAMSUpdate(progressBar, rootCollectionID, mzValues, collectionName, options);
			}

			stmt.close();

			return true;
		} catch (SQLException e) {
			try{
				con.setAutoCommit(true);
			} catch (SQLException ex) {
				System.err.println("problem setting auto-commit");
				ex.printStackTrace();
				System.exit(1);
			}
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception aggregating collection: " + collectionName);
			throw new ExceptionAdapter(e);
		}
	}

	private void setNextAtomID(int nextID) throws SQLException {
		Statement stmt = con.createStatement();
		// SQLite stores the last sequence number added, so need to subtract one
		// https://stackoverflow.com/questions/692856/set-start-value-for-autoincrement-in-sqlite/692871#692871
		int sqlLiteSequenceNumber = nextID - 1;
		stmt.executeUpdate("UPDATE SQLITE_SEQUENCE SET seq = " + sqlLiteSequenceNumber + " WHERE name='tmpatoms'");
		stmt.executeUpdate("\n" +
				"INSERT INTO sqlite_sequence (name,seq) SELECT 'tmpatoms', " + sqlLiteSequenceNumber + " WHERE NOT EXISTS \n" +
				"           (SELECT changes() AS change FROM sqlite_sequence WHERE change <> 0);");
	}

	private void aggregateAMSUpdate(ProgressBarWrapper progressBar, int rootCollectionID, int[] mzValues,
									String collectionName, AggregationOptions options) throws SQLException {
		//create and insert MZ Values into temporary #mz table.
		Statement stmt = con.createStatement();

		stmt.addBatch("DROP TABLE IF EXISTS temp.mz;\n");
		stmt.addBatch("CREATE TEMPORARY TABLE mz (Value INT);\n");
		for (int mzValue : mzValues) {
			stmt.addBatch("INSERT INTO temp.mz VALUES(" + mzValue + ");\n");
		}
		// went back to Greg's JOIN methodology, but retained #mz table, which speeds it up.
		stmt.addBatch("INSERT INTO tmpatoms (Time, MZLocation, Value) \n" +
				"SELECT BinnedTime, MZ.Value AS Location,"+options.getGroupMethodStr()+"(PeakHeight) AS PeakHeight \n"+
				"FROM temp.TimeBins TB\n" +
				"JOIN AMSAtomInfoSparse AIS on (TB.AtomID = AIS.AtomID)\n"+
				"JOIN temp.mz MZ on (abs(AIS.PeakLocation - MZ.Value) < "+options.peakTolerance+")\n"+
				"GROUP BY BinnedTime,MZ.Value\n"+
				"ORDER BY Location, BinnedTime;\n");

		// build 2 child collections - one for time series, one for M/Z values.
		int newCollectionID = createEmptyCollection("TimeSeries", rootCollectionID, collectionName, "", "");
		int mzRootCollectionID = createEmptyCollection("TimeSeries", newCollectionID, "M/Z", "", "");
		int mzPeakLoc, mzCollectionID;
		// for each mz value specified, make a new child collection and populate it.
		for (int j = 0; j < mzValues.length; j++) {
			mzPeakLoc = mzValues[j];
			mzCollectionID = createEmptyCollection("TimeSeries", mzRootCollectionID, mzPeakLoc + "", "", "");
			progressBar.increment("  " + collectionName + ", M/Z: " + mzPeakLoc);
			stmt.addBatch("INSERT INTO AtomMembership (CollectionID, AtomID) \n" +
					"select " + mzCollectionID + ", NewAtomID from tmpatoms WHERE MZLocation = "+mzPeakLoc+"\n" +
					"ORDER BY NewAtomID;\n");
			stmt.addBatch("INSERT INTO TimeSeriesAtomInfoDense (AtomID, Time, Value) \n" +
					"select NewAtomID, Time, Value from tmpatoms WHERE MZLocation = "+mzPeakLoc+
					" ORDER BY NewAtomID;\n");
		}
		stmt.addBatch("DROP TABLE temp.mz;\n");
		progressBar.increment("  Executing M/Z Queries...");
		stmt.executeBatch();
	}

	private void aggregateTimeSeriesUpdate(ProgressBarWrapper progressBar, int rootCollectionID, String collectionName,
										   AggregationOptions options) throws SQLException {
		Statement stmt = con.createStatement();

		stmt.addBatch("INSERT INTO tmpatoms (Time, Value) \n" +
				"select BinnedTime, " + options.getGroupMethodStr() + "(AID.Value) AS Value \n" +
				"from temp.TimeBins TB \n" +
				"join TimeSeriesAtomInfoDense AID on (TB.AtomID = AID.AtomID) \n"+
				"group by BinnedTime \n" +
				"order by BinnedTime;\n");

		int newCollectionID = createEmptyCollection("TimeSeries", rootCollectionID, collectionName, "", "");
		stmt.addBatch("INSERT INTO AtomMembership (CollectionID, AtomID) \n" +
				"select " + newCollectionID + ", NewAtomID from tmpatoms;\n");

		stmt.addBatch("INSERT INTO TimeSeriesAtomInfoDense (AtomID, Time, Value) \n" +
				"select NewAtomID, Time, Value from tmpatoms;\n");
		progressBar.increment("  " + collectionName);
		stmt.executeBatch();
	}

	private void aggreateATOFMSUpdate(ProgressBarWrapper progressBar, int rootCollectionID, int[] mzValues,
									  String collectionName, AggregationOptions options)
			throws SQLException, InterruptedException {
		int newCollectionID = createEmptyCollection("TimeSeries", rootCollectionID, collectionName, "", "");
		Statement stmt = con.createStatement();

		// if length actually is 0, do nothing
		if (mzValues.length != 0) {


			//This code does all of the joins in SQL.

			// went back to Greg's JOIN methodology, but retained #mz table, which speeds it up.
			// collects the sum of the Height/Area over all atoms at a given Time and for a specific m/z
			stmt.addBatch("INSERT INTO tmpatoms (Time, MZLocation, Value) \n" +
					"SELECT BinnedTime, AIS.PeakLocation AS Location,"+options.getGroupMethodStr()+"(PeakHeight) AS PeakHeight \n"+
					"FROM temp.TimeBins TB\n" +
					"JOIN ATOFMSAtomInfoSparse AIS on (TB.AtomID = AIS.AtomID)\n"+
					"GROUP BY BinnedTime,AIS.PeakLocation\n"+
					"ORDER BY Location, BinnedTime;\n");


			// build 2 child collections - one for particle counts time-series,
			// one for M/Z values time-series.
			int mzRootCollectionID = createEmptyCollection("TimeSeries", newCollectionID, "M/Z", "", "");
			int mzPeakLoc, mzCollectionID;
			// for each mz value specified, make a new child collection and populate it.
			for (int j = 0; j < mzValues.length; j++) {
				if(progressBar.wasTerminated()){
					throw new InterruptedException();
				}
				mzPeakLoc = mzValues[j];
				mzCollectionID = createEmptyCollection("TimeSeries", mzRootCollectionID, mzPeakLoc + "", "", "");
				progressBar.increment("  " + collectionName + ", M/Z: " + mzPeakLoc);
				stmt.addBatch("INSERT INTO AtomMembership (CollectionID, AtomID) \n" +
						"select " + mzCollectionID + ", NewAtomID from tmpatoms WHERE MZLocation = "+mzPeakLoc+"\n" +
						"ORDER BY NewAtomID;\n");
				stmt.addBatch("INSERT INTO TimeSeriesAtomInfoDense (AtomID, Time, Value) \n" +
						"select NewAtomID, Time, Value from tmpatoms WHERE MZLocation = "+mzPeakLoc+
						" ORDER BY NewAtomID;\n");
			}

			// if the user tried to cancel, STOP
			if(progressBar.wasTerminated()){
				throw new InterruptedException();
			}
			progressBar.increment("  Executing Queries...");
			// if the particle count is selected, produce that time series as well.
			// NOTE:  QUERY HAS CHANGED DRASTICALLY SINCE GREG'S IMPLEMENTATION!!!
			// it now tracks number of particles instead of sum of m/z particles.
			//System.out.println("Statement: "+sql.toString());
			progressBar.setIndeterminate(true);
			long start = System.currentTimeMillis();
			stmt.executeBatch();
			long stop = System.currentTimeMillis();
			System.out.println("executed in "+(stop-start)+" milliseconds.");

		}

		stmt.clearBatch();
		setNextAtomID(getNextID());
		stmt.executeUpdate("DELETE FROM tmpatoms");
		if (options.produceParticleCountTS) {
			int combinedCollectionID = createEmptyCollection("TimeSeries", newCollectionID, "Particle Counts", "", "");
			stmt.addBatch("INSERT INTO tmpatoms (Time, Value) \n" +
					"SELECT BinnedTime, COUNT(AtomID) AS IDCount FROM temp.TimeBins TB\n"+
					"GROUP BY BinnedTime\n"+
					"ORDER BY BinnedTime;\n");
			stmt.addBatch("INSERT INTO AtomMembership (CollectionID, AtomID) \n" +
					"select " + combinedCollectionID + ", NewAtomID from tmpatoms;\n");
			stmt.addBatch("INSERT INTO " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " (AtomID, Time, Value) \n" +
					"select NewAtomID, Time, Value from tmpatoms;\n");

			progressBar.increment("  " + collectionName + ", Particle Counts");
			long start = System.currentTimeMillis();
			stmt.executeBatch();
			long stop = System.currentTimeMillis();
			System.out.println("executed in "+(stop-start)+" milliseconds.");
		}
		// if the user tried to cancel, STOP
		if(progressBar.wasTerminated()){
			throw new InterruptedException();
		}
	}


	public int[] getValidSelectedMZValuesForCollection(Collection collection, Date startDate, Date endDate) {
		Set<Integer> collectionIDs = collection.getCollectionIDSubTree();
		AggregationOptions options = collection.getAggregationOptions();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		File tempFile = null;
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = null;
			ArrayList<Integer> peakLocs = new ArrayList<Integer>();
			StringBuilder sql = new StringBuilder();
			
//			if we want to get all mz values:
			if (options.allMZValues) {
				rs = stmt.executeQuery("select distinct PeakLocation as RoundedPeakLocation " +
						"from "+
						getDynamicTableName(DynamicTable.AtomInfoSparse, collection.getDatatype())+
						" AIS, InternalAtomOrder IAO, "+
						getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype())+
						" AID \n"+
						"WHERE IAO.CollectionID = "+collection.getCollectionID()+"\n"+
						"AND IAO.AtomID = AIS.AtomID \n" +
						"AND IAO.AtomID = AID.AtomID \n" +
						"AND AID.Time >= '"+dateFormat.format(startDate)+"'\n"+
						"AND AID.Time <= '"+dateFormat.format(endDate)+"'\n"+
				"ORDER BY RoundedPeakLocation;\n");
				while (rs.next()){
					peakLocs.add(rs.getInt("RoundedPeakLocation"));
				}
				rs.close();

			// if there's a list of mz values:
			} else if (options.mzValues != null && options.mzValues.size() > 0)
			{
				sql.append("IF object_id('tempdb..#mz') IS NOT NULL\n"+
						"DROP TABLE #mz;\n");
				sql.append("CREATE TABLE #mz (Value INT);\n");
				// Only bulk insert if client and server are on the same machine...
				if (url.equals("localhost")) {
					PrintWriter bulkFile = null;
					try {
						//tempFile = File.createTempFile("bulkfile", ".txt");
						tempFile = new File("TEMP"+File.separator+"bulkfile"+".txt");
						tempFile.deleteOnExit();
						bulkFile = new PrintWriter(new FileWriter(tempFile));
					} catch (IOException e) {
						System.err.println("Trouble creating " + tempFile.getAbsolutePath() + "");
						e.printStackTrace();
					}
					for (int i = 0; i < options.mzValues.size(); i++){
						bulkFile.println(options.mzValues.get(i));
					}
					bulkFile.close();
					sql.append("BULK INSERT #mz\n" +
							"FROM '" + tempFile.getAbsolutePath() + "'\n" +
							"WITH (FIELDTERMINATOR=',');\n");
				} else {
					for (int i = 0; i < options.mzValues.size(); i++) {
						sql.append("INSERT INTO #mz VALUES ("+options.mzValues.get(i)+");\n");
					}
				}	
				stmt.execute(sql.toString());
				/* If Datatype is ATOFMS */
				rs = stmt.executeQuery("select distinct MZ.Value as RoundedPeakLocation " +
						"from "+
						getDynamicTableName(DynamicTable.AtomInfoSparse, collection.getDatatype())+
						" AIS, InternalAtomOrder IAO, #mz MZ, "+
						getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype())+
						" AID \n"+
						"WHERE IAO.CollectionID = "+collection.getCollectionID()+"\n"+
						"AND IAO.AtomID = AIS.AtomID \n" +
						"AND IAO.AtomID = AID.AtomID \n"+
						"AND PeakLocation = MZ.Value \n"+
						"AND AID.Time >= '"+dateFormat.format(startDate)+"'\n"+
						"AND AID.Time <= '"+dateFormat.format(endDate)+"'\n"+
				"ORDER BY MZ.Value;\n");
				while (rs.next()){
					peakLocs.add(rs.getInt("RoundedPeakLocation"));
				}
				stmt.execute("DROP TABLE #mz;\n");
				rs.close();
			} else {
				System.err.println("BAD AGGREGATION OPTIONS");
				
			}

			stmt.close();
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
			}
			
			int[] ret = new int[peakLocs.size()];
			int i = 0;
			for (int peakLoc : peakLocs)
				ret[i++] = peakLoc;
			
			return ret;
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception creating finding M/Z values within collection");
			System.err.println("Error creating finding M/Z values within collection.");
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ArrayList<Date> getCollectionDates(Collection seq1, Collection seq2){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		int parentSeq1 = this.getParentCollectionID(seq1.getCollectionID());
		int parentSeq2 = -1;
		if(seq2 != null) parentSeq2 = this.getParentCollectionID(seq2.getCollectionID());
		String selectAllTimesStr = 
			"SELECT DISTINCT T.Time\n" +
			"FROM (SELECT CollectionID, Time, Value\n" +
			"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D\n" +
			" 	JOIN AtomMembership M on (D.AtomID = M.AtomID)) T\n" +
			"	JOIN CollectionRelationships CR on (CollectionID = CR.ChildID)" +
			"WHERE ParentID = " + parentSeq1+"";
		if (seq2 != null) {
			selectAllTimesStr += "\nOR ParentID = " + parentSeq2 + "";
		}
		selectAllTimesStr += "\n     Order BY Time";
		selectAllTimesStr += ";";
		
		ArrayList<Date> retData = new ArrayList<Date>();
		
		try{
			Statement stmt = con.createStatement();
			System.out.println("DATES:\n"+selectAllTimesStr);
			ResultSet rs;
			rs = stmt.executeQuery(selectAllTimesStr);
			while (rs.next()) {
				String dateTime = rs.getString("Time");
				if (dateTime != null)
					retData.add(parser.parse(dateTime));	
			}
			
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retData;
	}
	
	//Overload so that exporting of >2 collections to CVS is possible
	public ArrayList<Date> getCollectionDates(Collection[] collections){
		//This should NEVER happen!
		if (collections.length < 1)
			return null;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		int parentSeq1 = this.getParentCollectionID(collections[0].getCollectionID());

		String selectAllTimesStr = 
			"SELECT DISTINCT T.Time\n" +
			"FROM (SELECT CollectionID, Time, Value\n" +
			"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D\n" +
			" 	JOIN AtomMembership M on (D.AtomID = M.AtomID)) T\n" +
			"	JOIN CollectionRelationships CR on (CollectionID = CR.ChildID)" +
			"WHERE ParentID = " + parentSeq1+"";
		
		//Tack on additional groups
		for (int i = 1; i < collections.length; i++) {
			int parentSeq2 = this.getParentCollectionID(collections[i].getCollectionID());
			selectAllTimesStr += "\nOR ParentID = " + parentSeq2 + "";
		}

		selectAllTimesStr += "\n     Order BY Time";
		selectAllTimesStr += ";";
		
		ArrayList<Date> retData = new ArrayList<Date>();
		
		try{
			Statement stmt = con.createStatement();
			System.out.println("DATES:\n"+selectAllTimesStr);
			ResultSet rs;
			rs = stmt.executeQuery(selectAllTimesStr);
			while (rs.next()) {
				String dateTime = rs.getString("Time");
				if (dateTime != null)
					retData.add(parser.parse(dateTime));	
			}
			
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retData;
	}
	
	/* @guru Jamie Olson
	 * @see database.InfoWarehouse#getConditionalTSCollectionData(collection.Collection, java.util.ArrayList, java.util.ArrayList)
	 */
	public Hashtable<java.util.Date, Double> getConditionalTSCollectionData(Collection seq,
			ArrayList<Collection> conditionalSeqs, ArrayList<String> conditionStrs) {
		/*String s = null;
		s.charAt(5);
		*/
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		ArrayList<String> columnsToReturn = new ArrayList<String>();
		columnsToReturn.add("Ts1Value");
		int parentSeq1 = this.getParentCollectionID(seq.getCollectionID());
		
		
		String atomSelStr = "SELECT CollectionID, Time, Value\n FROM " +
		getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + 
		" D \nJOIN AtomMembership M ON (D.AtomID = M.AtomID)";
		
		
		
		String selectStr = "SELECT T.Time as Time";
		String tableJoinStr = "FROM (" + atomSelStr + ") T \n";
		String collCondStr = "WHERE T.CollectionID = " + seq.getCollectionID() + " \n";
		
		if (conditionStrs.size() > 0) {
			for (int i = 0; i < conditionalSeqs.size(); i++) {
				tableJoinStr += "JOIN (" + atomSelStr + ") C" + i + " on (C" + i + ".Time = T.Time) \n";
				//selectStr += ", C" + i + ".Value as C" + i + "Value";
				if (i == 0)
				{
					collCondStr += "AND ((C" + i + ".CollectionID = " + conditionalSeqs.get(i).getCollectionID();
					collCondStr += " AND "+conditionStrs.get(i)+")\n";
				}
				else
				{
					if (conditionStrs.get(i).contains("AND"))
					{
						collCondStr += "AND (C" + i + ".CollectionID = " + conditionalSeqs.get(i).getCollectionID();	
						collCondStr += " AND "+conditionStrs.get(i).substring(5, conditionStrs.get(i).length())+")\n";
					}
					else //OR
					{
						collCondStr += "OR (C" + i + ".CollectionID = " + conditionalSeqs.get(i).getCollectionID();
						collCondStr += " AND "+conditionStrs.get(i).substring(4, conditionStrs.get(i).length())+")\n";
					}
				}
				
				/*
				collCondStr += "AND C" + i + ".CollectionID = " + conditionalSeqs.get(i).getCollectionID() + "\n";
				if (i == 0)
					collCondStr += "AND "+conditionStrs.get(i)+"\n";
				else
					collCondStr += conditionStrs.get(i)+"\n";*/
			}
		}
		collCondStr += ")";
		
		String timesStr = selectStr + " \n" + tableJoinStr + collCondStr;
		String sqlStr;
		sqlStr = "SELECT S.Time as Time, S.Value as TsValue\n" +
				"FROM (" + atomSelStr + ") S \n" +
				"JOIN ("+timesStr+") T on (S.Time = T.Time)\n" +
				"WHERE S.CollectionID = "+seq.getCollectionID()+";";
		
		
		//Change the query if there aren't any conditions, making it go much faster
		if(conditionalSeqs.isEmpty()&&conditionStrs.isEmpty()){
			sqlStr = "SELECT T.Time as Time, T.Value AS TsValue \n" 
				+ "FROM " +	getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " T \n" +
				"JOIN (	SELECT M.AtomID as AtomID \n" +
				"		FROM AtomMembership M\n" +
				"		WHERE M.CollectionID = " + seq.getCollectionID()+") A " +
				"ON (T.AtomID = A.AtomID)" + " \n";
		}
		Hashtable<java.util.Date, Double> retData = new Hashtable<java.util.Date, Double>();
		
		try{
			Statement stmt = con.createStatement();
			ResultSet rs;
			
			System.out.println("Query:\n"+sqlStr);
			rs = stmt.executeQuery(sqlStr);
			
			while (rs.next()) {
				double retValues;
				retValues = rs.getDouble("TsValue");
				
				String dateTime = rs.getString("Time");
				if (dateTime != null)
					retData.put(parser.parse(dateTime), retValues);	
			}
			stmt.close();
			rs.close();
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retData;
	}
	
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromPercent(Collection collection,double magnitude, int minDuration){
		/*System.out.println("collection: |"+collection+"|");
		System.out.println("Collection ID: "+collection.getCollectionID());
		System.out.println("threshold: "+magnitude+"\nduration: "+minDuration);
		*/int parentCollection = this.getParentCollectionID(collection.getCollectionID());
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		String atomSelStr = "SELECT CollectionID, Time, Value" +
			"\n FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D \n" +
			"JOIN AtomMembership M ON (D.AtomID = M.AtomID)";
		
		
		String selectAtomsStr = "SELECT T.Time as Time, T.Value AS Value\n " +
				"FROM ("+atomSelStr+") T\n" +
				"WHERE T.CollectionID = "+collection.getCollectionID()+" ";
		
		String selectValuesStr = "SELECT DISTINCT T.Time as Time, V.Value as Value\n";
		
		String joinStr = 
		"FROM (SELECT CollectionID, Time, Value\n" +
		"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D\n" +
		" 	JOIN AtomMembership M on (D.AtomID = M.AtomID)) T\n" +
		"	JOIN CollectionRelationships CR on (CollectionID = CR.ChildID)";
		
		
		String selectAllTimesStr = 
			selectValuesStr + joinStr +
			"WHERE ParentID = " + parentCollection+"\n";
		
		String selectAllAtomsTimesStr = 
			selectValuesStr +
			joinStr +
		"	LEFT OUTER JOIN (" + selectAtomsStr + ") V ON (V.Time = T.Time)\n" +
		"	WHERE ParentID = " + parentCollection+"\n";
			
		String datesCountStr = 
			"SELECT COUNT(DISTINCT T.Time) as Count\n" +
			joinStr +
			"WHERE ParentID = " + parentCollection+"\n";
		
		String valuesCountStr = "SELECT COUNT(T.Time) as Count\n " +
		"FROM ("+atomSelStr+") T\n" +
		"WHERE T.CollectionID = "+collection.getCollectionID()+" \n";

		
		String orderedByTime = selectAllAtomsTimesStr + "     Order BY Time;\n";
		String orderedByValue =  selectAllAtomsTimesStr + "     Order BY Value;\n";
		
		ArrayList<TreeMap<Date,Double>> plumes = new ArrayList<TreeMap<Date,Double>>();
		TreeMap<Date,Double> curPlume = new TreeMap<Date,Double>();
		try{
			Statement stmt = con.createStatement();
			
			ResultSet rs;
			System.out.println(datesCountStr);
			rs = stmt.executeQuery(datesCountStr);
			boolean hasRows = rs.next();
			assert(hasRows);
			int numParticles = rs.getInt("Count");
			
			System.out.println(valuesCountStr);
			rs = stmt.executeQuery(valuesCountStr);
			hasRows = rs.next();
			assert(hasRows);
			int numPeaks = rs.getInt("Count");
			
			System.out.println(orderedByValue);
			rs = stmt.executeQuery(orderedByValue);
			System.out.println("numParticles: "+numParticles+"\nnumPeaks: "+numPeaks);
			System.out.println("skip to "+(int)(magnitude*numParticles));
			for(int i = 0; i < (int)(magnitude*numParticles); i++){
				hasRows = rs.next();
				//System.out.println("Next value: "+rs.getDouble("Value"));
				assert(hasRows);
			}
			double minValue = rs.getDouble("Value");
			plumes = createAndDetectPlumesFromValue(collection,minValue,minDuration);
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} 
		return plumes;
	}
	
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromMedian(Collection collection,double factor, int minDuration){
		/*System.out.println("collection: |"+collection+"|");
		System.out.println("Collection ID: "+collection.getCollectionID());
		System.out.println("threshold: "+magnitude+"\nduration: "+minDuration);
		*/int parentCollection = this.getParentCollectionID(collection.getCollectionID());
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		String atomSelStr = "SELECT CollectionID, Time, Value" +
			"\n FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D \n" +
			"JOIN AtomMembership M ON (D.AtomID = M.AtomID)";
		
		
		String selectAtomsStr = "SELECT T.Time as Time, T.Value AS Value\n " +
				"FROM ("+atomSelStr+") T\n" +
				"WHERE T.CollectionID = "+collection.getCollectionID()+" ";
		
		String selectValuesStr = "SELECT DISTINCT T.Time as Time, V.Value as Value\n";
		
		String joinStr = 
		"FROM (SELECT CollectionID, Time, Value\n" +
		"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D\n" +
		" 	JOIN AtomMembership M on (D.AtomID = M.AtomID)) T\n" +
		"	JOIN CollectionRelationships CR on (CollectionID = CR.ChildID)";
		
		
		String selectAllTimesStr = 
			selectValuesStr + joinStr +
			"WHERE ParentID = " + parentCollection+"\n";
		
		String selectAllAtomsTimesStr = 
			selectValuesStr +
			joinStr +
		"	LEFT OUTER JOIN (" + selectAtomsStr + ") V ON (V.Time = T.Time)\n" +
		"	WHERE ParentID = " + parentCollection+"\n";
			
		String datesCountStr = 
			"SELECT COUNT(DISTINCT T.Time) as Count\n" +
			joinStr +
			"WHERE ParentID = " + parentCollection+"\n";
		
		String valuesCountStr = "SELECT COUNT(T.Time) as Count\n " +
		"FROM ("+atomSelStr+") T\n" +
		"WHERE T.CollectionID = "+collection.getCollectionID()+" \n";

		
		String orderedByTime = selectAllAtomsTimesStr + "     Order BY Time;\n";
		String orderedByValue =  selectAllAtomsTimesStr + "     Order BY Value;\n";
		
		ArrayList<TreeMap<Date,Double>> plumes = new ArrayList<TreeMap<Date,Double>>();
		try{
			Statement stmt = con.createStatement();
			
			ResultSet rs;
			boolean hasRows = false;
			System.out.println(datesCountStr);
			rs = stmt.executeQuery(datesCountStr);
			hasRows = rs.next();
			assert(hasRows);
			int numParticles = rs.getInt("Count");
			
			System.out.println(valuesCountStr);
			rs = stmt.executeQuery(valuesCountStr);
			hasRows = rs.next();
			assert(hasRows);
			int numPeaks = rs.getInt("Count");
			
			System.out.println(orderedByValue);
			rs = stmt.executeQuery(orderedByValue);
			for(int i = 0; i < (int)(numParticles - .50*numPeaks); i++){
				hasRows = rs.next();
				//System.out.println("Next value: "+rs.getDouble("Value"));
				assert(hasRows);
			}
			double minValue = rs.getDouble("Value");
			minValue *= factor;
			plumes = createAndDetectPlumesFromValue(collection,minValue,minDuration);
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} 
		return plumes;
	}
	
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromValue(Collection collection,double minValue, int minDuration){
		/*System.out.println("collection: |"+collection+"|");
		System.out.println("Collection ID: "+collection.getCollectionID());
		System.out.println("threshold: "+magnitude+"\nduration: "+minDuration);
		*/int parentCollection = this.getParentCollectionID(collection.getCollectionID());
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		String atomSelStr = "SELECT CollectionID, Time, Value" +
			"\n FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D \n" +
			"JOIN AtomMembership M ON (D.AtomID = M.AtomID)";
		
		
		String selectAtomsStr = "SELECT T.Time as Time, T.Value AS Value\n " +
				"FROM ("+atomSelStr+") T\n" +
				"WHERE T.CollectionID = "+collection.getCollectionID()+" ";
		
		String selectValuesStr = "SELECT DISTINCT T.Time as Time, V.Value as Value\n";
		
		String joinStr = 
		"FROM (SELECT CollectionID, Time, Value\n" +
		"	FROM " + getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries") + " D\n" +
		" 	JOIN AtomMembership M on (D.AtomID = M.AtomID)) T\n" +
		"	JOIN CollectionRelationships CR on (CollectionID = CR.ChildID)";
		
		
		String selectAllTimesStr = 
			selectValuesStr + joinStr +
			"WHERE ParentID = " + parentCollection+"\n";
		
		String selectAllAtomsTimesStr = 
			selectValuesStr +
			joinStr +
		"	LEFT OUTER JOIN (" + selectAtomsStr + ") V ON (V.Time = T.Time)\n" +
		"	WHERE ParentID = " + parentCollection+"\n";
			
		String orderedByTime = selectAllAtomsTimesStr + "     Order BY Time;\n";
		
		
		ArrayList<TreeMap<Date,Double>> plumes = new ArrayList<TreeMap<Date,Double>>();
		TreeMap<Date,Double> curPlume = new TreeMap<Date,Double>();
		try{
			Statement stmt = con.createStatement();
			
			ResultSet rs;
			System.out.println("MinValue: "+minValue);
			System.out.println(orderedByTime);
			rs = stmt.executeQuery(orderedByTime);
			boolean more = true;
			more = rs.next();
			while(more){
				while(more && rs.getDouble("Value") >= minValue){
					String dateTime = rs.getString("Time");
					curPlume.put(parser.parse(dateTime),rs.getDouble("Value"));
					more = rs.next();
				}
				if(!curPlume.isEmpty()&&curPlume.lastKey().getTime() - curPlume.firstKey().getTime() 
						>= minDuration * 1000){
					plumes.add(curPlume);
					System.out.println("Plume: "+curPlume.values().toString());
				}
				curPlume = new TreeMap<Date,Double>();
				
				while(more && rs.getDouble("Value") < minValue){
					more = rs.next();
				}
			}
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving time series data.");
			System.err.println("Error retrieving time series data.");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return plumes;
	}
	
	public void syncWithIonsInDB(ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons) throws SQLException {
		try (Statement stmt = con.createStatement();) {

			stmt.executeUpdate("CREATE TEMPORARY TABLE sigs ( \n" +
							"    Name varchar(8000), \n" +
							"    IsPositive bit \n" +
							")");

			try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO temp.sigs VALUES(?,?)");) {
				con.setAutoCommit(false);
				for (LabelingIon ion : posIons) {
					pstmt.setString(1, ion.name);
					pstmt.setInt(2, 1);
				}
				for (LabelingIon ion : negIons) {
					pstmt.setString(1, ion.name);
					pstmt.setInt(2, 0);
				}
				pstmt.addBatch();
				con.setAutoCommit(true);
			}

			// Add any new ions from file into database
			stmt.executeUpdate("INSERT INTO IonSignature (Name, IsPositive) \n" +
							"select S.Name, S.IsPositive \n" +
							"from temp.sigs S \n" +
							"left outer join IonSignature IONS on (S.Name = IONS.Name and S.IsPositive = IONS.IsPositive) \n" +
							"where IONS.IonID IS NULL \n\n");

			ResultSet rs = stmt.executeQuery("select IONS.IonID, IONS.Name, IONS.IsPositive \n" +
							"from temp.sigs S \n" +
							"join IonSignature IONS on (S.Name = IONS.Name and S.IsPositive = IONS.IsPositive) \n");

			while (rs.next()) {
				ArrayList<LabelingIon> arrToLookThrough = rs.getBoolean("IsPositive") ? posIons : negIons;
				String ionName = rs.getString("Name");
				for (LabelingIon ion : arrToLookThrough) {
					if (ion.name.equals(ionName))
						ion.ionID = rs.getInt("IonID");
				}
			}

			rs.close();
		}
	}
	
	public void saveAtomRemovedIons(int atomID, ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons)
			throws SQLException {
		try (Statement stmt = con.createStatement();) {
			stmt.executeUpdate("delete from AtomIonSignaturesRemoved where AtomID = " + atomID);

			try (PreparedStatement pstmt = con.prepareStatement(
					"INSERT INTO AtomIonSignaturesRemoved (AtomID, IonID) VALUES (?, ?)");) {
				pstmt.setInt(1, atomID);
				for (LabelingIon ion : posIons)
					if (!ion.isChecked()) {
						pstmt.setInt(2, ion.ionID);
						pstmt.addBatch();
					}
				for (LabelingIon ion : negIons)
					if (!ion.isChecked()) {
						pstmt.setInt(2, ion.ionID);
						pstmt.addBatch();
					}
				con.setAutoCommit(false);
				pstmt.executeBatch();
				con.setAutoCommit(true);
			}
		}
	}
	
	public void buildAtomRemovedIons(int atomID, ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons) {
		Set<Integer> removedIDs = new HashSet<Integer>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select IonID from AtomIonSignaturesRemoved where AtomID = " + atomID);
			
			while (rs.next())
				removedIDs.add(rs.getInt("IonID"));
			
			rs.close();
			stmt.close();
		} catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving removed Ion data.");
			System.err.println("Error retrieving removed Ion data.");
			e.printStackTrace();
		}
		
		for (LabelingIon ion : posIons)
			ion.setChecked(!removedIDs.contains(ion.ionID));
		
		for (LabelingIon ion : negIons)
			ion.setChecked(!removedIDs.contains(ion.ionID));
		
	}
	
	/**
	 * Get all the datatypes currently entered in the database.
	 * 
	 * @return ArrayList<String> of the known datatypes.
	 */
	public ArrayList<String> getKnownDatatypes(){
		ArrayList<String> knownTypes = new ArrayList<String>();
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Datatype FROM MetaData");
			String currentType = "";
			while (rs.next()){
				if (! rs.getString(1).equalsIgnoreCase(currentType)){
					currentType = rs.getString(1);
					knownTypes.add(currentType);
				}
			}
			stmt.close();
		}
		catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving known datatypes.");
			System.err.println("Error getting the known datatypes.");
			e.printStackTrace();
		}
		
		return knownTypes;
	}
	
	/**
	 * Determine whether the database already contains a datatype.
	 * 
	 * @param type	The name of the datatype you seek.
	 * @return	True if the datatype is in the database, false otherwise.
	 */
	public boolean containsDatatype(String type){
		if (!removeReservedCharacters(type).equals(type))
			throw new IllegalArgumentException("Invalid database reserved characters in " +
					"querying name while checking for existence of datatype " + type);
		
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT Datatype FROM MetaData"
					+" WHERE Datatype = '" + type +"'");
			if (rs.next()){
				stmt.close();
				return true;
			}
			else {
				stmt.close();
				return false;
			}
		}
		catch (SQLException e){
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception checking for the existence of datatype "+type);
			System.err.println("problems checking datatype from SQLServer.");
			return false;
		}
		
	}
	
	/**
	 * Gets the atoms's original filename for display purposes.  ATOFMS-specific
	 * because it needs to grab a particular field from the dense data.
	 * @param atomID	the id of the atom whose filename we want
	 * @return			the filename for that atom
	 */
	public String getATOFMSFileName(int atomID){
		String fileName = "";
		Statement stmt;
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT OrigFilename FROM ATOFMSAtomInfoDense"
					+ " WHERE AtomID = " + atomID);
			rs.next();
			fileName = rs.getString(1);
			stmt.close();
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}

				
		return fileName;
	}
	
	/**
	 * @author steinbel
	 * Given a filename for an ATOFMS particle, returns the atomID associated 
	 * with it.
	 * @param ATOFMSFileName	The filename for the particle.
	 * @return					The atomID for the desired particle.
	 */
	public int getATOFMSAtomID(String ATOFMSFileName){
		int atomID = -99;
		boolean exists = false;
		Statement stmt;
		try{
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT AtomID FROM ATOFMSAtomInfoDense"
					+ " WHERE OrigFilename = '" + ATOFMSFileName + "'");
			if (rs.next()){
				exists = true;
				atomID = rs.getInt(1);
			} else {
				ErrorLogger.displayException(null, ATOFMSFileName + " was"
						+ " not found in your database.");
			}
			stmt.close();
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception getting atomID"
					+ " from filename.  Check for illegal characters like apostrophes.");
			e.printStackTrace();
		}
		return atomID;
	}
	
	/**
	 * @author steinbel
	 * Return true if the collection contains the given atomID.
	 * @param collectionID - the ID of the collection to check
	 * @param atomID -		 the ID of the target atom
	 * @return				 True if the collection contains the atom.
	 */
	public boolean collectionContainsAtom(int collectionID, int atomID){
		boolean contains = false;
		Statement stmt;
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * from InternalAtomOrder"
					+ " WHERE CollectionID = " + collectionID + " AND AtomID = "
					+ atomID);
			
			if (rs.next())
				contains = true;
			
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(), "SQL Exception checking"
					+ " atom membership.");
			e.printStackTrace();
		}
		
		return contains;
	}
	
	public CollectionCursor getMemoryClusteringCursor(Collection collection, ClusterInformation cInfo) {
		return new MemoryClusteringCursor(collection, cInfo);
	}
	
	public ArrayList<String> getPrimaryKey(String datatype, DynamicTable table) {
		ArrayList<String> strings = new ArrayList<String>();	
		Statement stmt;
		try {
			stmt = con.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT ColumnName FROM MetaData " +
					"WHERE PrimaryKey = 1 AND Datatype = '" + datatype + 
					"' AND TableID = " + table.ordinal());
			while (rs.next()) {
				if (!rs.getString(1).equalsIgnoreCase("AtomID")
						&& !rs.getString(1).equalsIgnoreCase("DatasetID"))
					strings.add(rs.getString(1));
			}
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 
		
		return strings;
	}
	
	/**
	 * Determines if the atom in question represents a cluster center, and if so,
	 * which cluster it represents.
	 * @param atomID - the potential cluster center atom
	 * @return	-1 if not a cluster center, the collectionID of the cluster if it is
	 */
	public int getRepresentedCluster(int atomID){
		int collectionID = -1;
		
		try{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT CollectionID FROM CenterAtoms"
					+ " WHERE AtomID = " + atomID);
			if (rs.next()){
				collectionID = rs.getInt("CollectionID");
			}
			
			assert !rs.next() : "Atom " + atomID + " should not be the center of multiple clusters.";			
			stmt.close();
		} catch (SQLException e){
			//TODO: figure out the error-logging procedures
			e.printStackTrace();
		}
		
		return collectionID;
	}
	
	/**
	 * Adds a new atom to InternalAtomOrder, assuming the atom is the next one
	 * in its collection.
	 * @param	atomID	- the atom to add
	 * @param	collectionID - the collection to which the atom is added
	 */
	void addInternalAtom(int atomID, int collectionID){
		try {
			Statement stmt = con.createStatement();

			String query = "INSERT INTO InternalAtomOrder " +
					"VALUES (" + atomID +", "+ collectionID+ ")";
			//System.out.println(query);//debugging
			stmt.execute(query);
			stmt.close();
			
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception adding atom.  Please check incoming data for correct format.");
			System.err.println("Exception adding particle to InternalAtomOrder.");
			e.printStackTrace();
		}
		
	}
	
	public void updateAllInternalAtomOrder(){

		// TopSort(G)
		ArrayList<Integer> sorted = new ArrayList<Integer>();
		HashSet<Integer> visited = new HashSet<Integer>();
		int place = 0;
		place = topologicalSort(0,place,sorted, visited);
		
		
		sorted = new ArrayList<Integer>();
		visited = new HashSet<Integer>();
		place = 0;
		place = topologicalSort(1,place,sorted, visited);
		
		
		
	}

	private int topologicalSort( int vertex, int place, ArrayList<Integer> sorted, HashSet<Integer> visited)
	{
		visited.add(vertex);
		ArrayList<Integer> children = getImmediateSubCollections(getCollection(vertex));
		for(Integer child : children)
			if (!visited.contains(child)) place = topologicalSort(child,place,sorted,visited);
		sorted.add(place,vertex);
		place++;
		return place;
	}
	
	
	/**
	 * @author steinbel - adapted to remove OrderNumber
	 * 
	 * internalAtomOrder updated by updating the collection itself,
	 * recursing through subcollections.  This ONLY updates the specified
	 * collection, and it works by traversing down to the leaves of the tree.
	 * It is a recursive algortihm, and will be used mostly for cut/copy/paste
	 * functionality.  Importing collections don't need this, since they have no
	 * children.
	 * 
	 * Implements the bulk insert method from Greg Cipriano, since the client
	 * and the server have to be on the same machine.
	 * 
	 * @param collection
	 */
	public void updateInternalAtomOrder(Collection collection) {
		File tempFile = null;
		int cID = collection.getCollectionID();
		if (cID == 0 || cID == 1) 
			return;
		try {
			Statement stmt = con.createStatement();
			con.setAutoCommit(false);

			//again, want to compare with existing IAO table to see if there
			//are any difference, if not, why delete? - steinbel
			stmt.execute("DELETE FROM InternalAtomOrder WHERE CollectionID = " + cID);
			
			//get all the AtomIDs from AtomMembership if the corresponding CollectionID was
			//the parent's or one of the children's.  We want the union of these so that there
			//are no overlaps.
			String query = "SELECT AtomID FROM AtomMembership WHERE CollectionID = " + cID;
			Iterator<Integer> subCollections = getAllDescendantCollections(cID,false).iterator();
			while (subCollections.hasNext())
				query += " union SELECT AtomID FROM AtomMembership WHERE CollectionID = " + subCollections.next();
			query += " ORDER BY AtomID";
			
			ResultSet rs = stmt.executeQuery(query);
			
			// Only bulk insert if client and server are on the same machine...
			String queryTemplate = "INSERT INTO InternalAtomOrder VALUES(?, ?)";
			PreparedStatement pstmt = con.prepareStatement(queryTemplate);
			while (rs.next()) {
				pstmt.setInt(1, rs.getInt(1));
				pstmt.setInt(2, cID);
				pstmt.addBatch();
			}

			pstmt.executeBatch();
			con.commit();
			con.setAutoCommit(true);
			stmt.close();
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
			}
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception inserting atom.  Please check incoming data for correct format.");
			System.err.println("Exception inserting particle.");
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * Percolate all atoms in the given collection up the collection hierarchy.
	 * This should be called whenever a new collection is created.  If it has one or more
	 * parent, it will cause the parent to contain all of the new collection's atoms
	 * 
	 * @param newCollection
	 */
	public void propagateNewCollection(Collection newCollection){
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("IF (OBJECT_ID('tempdb..#newCollection') " +
				"IS NOT NULL)\n" +
				"	DROP TABLE #newCollection\n");
			stmt.executeUpdate("CREATE TABLE #newCollection (AtomID int, CollectionID int)");
			String query = "INSERT INTO #newCollection (AtomID)" +
			" SELECT AtomID FROM InternalAtomOrder WHERE (CollectionID = "+newCollection.getCollectionID()+");";
			stmt.execute(query);
			propagateNewCollection(newCollection,newCollection.getParentCollection());
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void propagateNewCollection(Collection newCollection, Collection parentCollection){
		if (parentCollection == null || 
				parentCollection.getCollectionID() == 0 || 
				parentCollection.getCollectionID() == 1){
			try {
				Statement stmt = con.createStatement();
				String query = "DROP TABLE #newCollection;";
				stmt.execute(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return;
		}
		try {
			Statement stmt = con.createStatement();
			stmt.execute("UPDATE #newCollection SET CollectionID = " + parentCollection.getCollectionID());
			stmt.execute("INSERT INTO InternalAtomOrder (AtomID, CollectionID) SELECT * FROM #newCollection;");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		propagateNewCollection(newCollection,parentCollection.getParentCollection());
		
		
	}
	
	/**
	 * @author steinbel - removed OrderNumber
	 * @author jtbigwoo - use InternalAtomOrder for the children so that we get
	 * the contents from sub-sub collections
	 * TODO: still needs optimization
	 */
	public void updateAncestors(Collection collection) {
		// if you try to update a null collection or one of the root collections,
		// return.
		if (collection == null || 
				collection.getCollectionID() == 0 || 
				collection.getCollectionID() == 1) 
			return;
		int cID = collection.getCollectionID();
		try {
			Statement stmt = con.createStatement();
			con.setAutoCommit(false);
			
			/*get atomIDs for collection in IAO and in AtomMembership.  
			 * if difference, rectify it
			 *
			 * then, loop through all children and repeat procedure
			 */

			String query = "INSERT INTO InternalAtomOrder " +
					"(AtomID, CollectionID)" +
					"SELECT AtomID, CollectionID FROM AtomMembership " +
					"WHERE CollectionID = " + cID  +
					" AND (AtomID NOT IN (SELECT AtomID from InternalAtomOrder" +
					" WHERE CollectionID = " + cID + "))";
			ArrayList<Integer> subCollections = collection.getSubCollectionIDs();
			stmt.executeUpdate(query);
	
			if (subCollections.size() > 0){

				/*
				 * Because of SQL syntax, cannot combine SELECT with a constant,
				 * so we'll create a temp table and poplulate it with the AtomIDs
				 * of the children collection and the CollectionID of the 
				 * current collection.  Then we'll copy that info into IAO. - steinbel
				 */
//				stmt.executeUpdate("DROP TABLE IF EXISTS temp.children\n");
//				stmt.executeUpdate("CREATE temporary TABLE children (AtomID int, CollectionID int)");
				query = " SELECT AtomID FROM InternalAtomOrder WHERE (CollectionID = ";
				for (int i=0; i<subCollections.size(); i++){
					query += subCollections.get(i) + ")";
					if (i<subCollections.size()-1)
						query += "OR (CollectionID = ";
				}
				try (ResultSet atomsToAdd = stmt.executeQuery(query)) {
					PreparedStatement pstmt = con.prepareStatement(
							"INSERT OR IGNORE INTO InternalAtomOrder VALUES (?,?)");
					while (atomsToAdd.next()) {
						pstmt.setInt(1, atomsToAdd.getInt("AtomID"));
						pstmt.setInt(2, cID);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}


			con.commit();
			con.setAutoCommit(true);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// maybe make this conditional?  how?  temp table and replace it?
		updateAncestors(collection.getParentCollection());
	}
	


	/**
	 * Find the sum of values from a given column given a list of AtomIDs specifying which rows to use
	 * @param table Whether the table is static or dynamic
	 * @param string The name of the column to sum data from
	 * @param curIDs The IDs of the atoms from the column to be summed
	 * @param oldDatatype The type of data being accessed (ATOFMS, etc)
	 * @return A String representation of the sum.
	 */
	public String aggregateColumn(DynamicTable table, String string, ArrayList<Integer> curIDs, String oldDatatype) {
		double sum = 0;
		try {
			Statement stmt = con.createStatement();
			StringBuilder query = new StringBuilder("SELECT SUM("+string+") FROM "+
				getDynamicTableName(table,oldDatatype)+" WHERE AtomID IN (");
			for (int i = 0; i < curIDs.size(); i++) {
				query.append(curIDs.get(i));
				if (i!=curIDs.size()-1)
					query.append(",");
			}
			query.append(");");
			ResultSet rs = stmt.executeQuery(query.toString());
			rs.next();
			sum=rs.getDouble(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return ""+sum;
	}

	public BinnedPeakList getAveragePeakListForCollection(Collection coll) {
		BinnedPeakList bpl = null;
		try {
			Statement stmt = con.createStatement();
			StringBuilder query = new StringBuilder("SELECT PeakLocation, SUM(PeakArea) FROM " +
			getDynamicTableName(DynamicTable.AtomInfoSparse, coll.getDatatype()) + " a, InternalAtomOrder b WHERE " +
			"b.CollectionID = " + coll.getCollectionID() + " and a.AtomID = b.AtomID GROUP BY PeakLocation");
			ResultSet rs = stmt.executeQuery(query.toString());
			bpl = new BinnedPeakList();
			while (rs.next()) {
				bpl.add(rs.getInt(1), rs.getFloat(2));
			}
			query = new StringBuilder("SELECT COUNT(DISTINCT a.AtomID) FROM " + 
			getDynamicTableName(DynamicTable.AtomInfoSparse, coll.getDatatype()) + " a, InternalAtomOrder b WHERE " +
			"b.CollectionID = " + coll.getCollectionID() + " and a.AtomID = b.AtomID");
			rs = stmt.executeQuery(query.toString());
			rs.next();
			int numAtoms = rs.getInt(1);
			bpl.divideAreasBy(numAtoms);
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bpl;
	}
	
	public ArrayCursor getArrayCursor(Collection coll) throws SQLException {
		return new ArrayCursor(coll);
	}
	
	public ArrayCursor getArrayCursor(ArrayCursor other) {
		return new ArrayCursor(other);
	}

	// Cursor that stores mass spectra using primitive datatypes
	// Aux constructor allows multiple cursors to share data
	// Michael Murphy 2015
	public class ArrayCursor implements CollectionCursor {
		int[] atomIds;
		short[] peakLocations;
		short[] peakAreas;
		
		int pos;
		int numRows;
		
		ArrayCursor(Collection coll) throws SQLException {
			String query =  "SELECT AtomID, PeakLocation, PeakArea FROM ATOFMSAtomInfoSparse WHERE AtomID IN " +
							"(SELECT AtomID FROM InternalAtomOrder WHERE CollectionID = "+coll.getCollectionID()+") ORDER BY AtomID";
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = stmt.executeQuery(query);
			
			// retrieve row count
			rs.last();
			numRows = rs.getRow();
			rs.beforeFirst();
			
			atomIds = new int[numRows];
			peakLocations = new short[numRows];
			peakAreas = new short[numRows];
			
			// load rows into arrays
			int i = 0;
			while (rs.next()) {
//				if (Thread.interrupted()) {
//					close();
//					return;
//				}
				atomIds[i] = rs.getInt(1);
				peakLocations[i] = rs.getShort(2);
				peakAreas[i] = rs.getShort(3);
				i++;
			}
			
			pos = -1;
			
			// discard result set from memory
			stmt.close();
			rs.close();
			stmt = null;
			rs = null;
		}
		
		// Generate a cursor that shares other's dataset
		// Good for nested iteration
		ArrayCursor(ArrayCursor other) {
			atomIds = other.atomIds;
			peakLocations = other.peakLocations;
			peakAreas = other.peakAreas;
			
			pos = -1;
			numRows = other.numRows;
		}
		
		// self-explanatory
		public void align(ArrayCursor other) {
			pos = other.pos;
		}

		public boolean next() {
			if (pos >= numRows || numRows == 0) { // end of dataset
				return false;
			} else if (pos == -1 && numRows > 0) { // beginning of dataset
				pos++;
				return true;
			} else {
				int prevId = atomIds[pos];
				while (pos < numRows && atomIds[pos] == prevId)
					pos++;
				return (pos < numRows);
			}
		}

		public ParticleInfo getCurrent() {
			if (atomIds[pos] == -1)
				System.out.println("Missing atom at "+pos);
			if (-1 >= pos || pos >= numRows) {
				System.err.println("Invalid row index: "+pos+" ("+numRows+" rows)");
				return null;
			}
			BinnedPeakList bpl = new BinnedPeakList();
			int i = pos;
			while (i < numRows && atomIds[i] == atomIds[pos]) {
				bpl.add(peakLocations[i], peakAreas[i]);
				i++;
			}
			ParticleInfo p = new ParticleInfo();
			p.setID(atomIds[pos]);
			p.setBinnedList(bpl);
			return p;
		}

		public void close() {
			atomIds = null;
			peakLocations = null;
			peakAreas = null;
			pos = -1;
		}

		public void reset() {
			pos = -1;
		}

		public ParticleInfo get(int i) throws NoSuchMethodException {
			// TODO Auto-generated method stub
			return null;
		}

		public BinnedPeakList getPeakListfromAtomID(int id) {
			// TODO Auto-generated method stub
			return null;
		}
		
		public int getCurrentAtomID() {
			if (-1 >= pos || pos >= numRows) {
				System.err.println("Invalid row index: "+pos+" ("+numRows+" rows)");
				return -1;
			}
			return atomIds[pos];
		}
		
		public BinnedPeakList getCurrentPeakList() {
			if (-1 >= pos || pos >= numRows) {
				System.err.println("Invalid row index: "+pos+" ("+numRows+" rows)");
				return null;
			}
			BinnedPeakList bpl = new BinnedPeakList();
			int i = pos;
			while (i < numRows && atomIds[i] == atomIds[pos]) {
				bpl.add(peakLocations[i], peakAreas[i]);
				i++;
			}
			return bpl;
		}
	}
	
}