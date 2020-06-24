package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.gui.MainFrame;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * Makes database work with SQLite
 */
public class SQLiteDatabase extends Database {
    //This isn't used anymore. Safe for deletion.
    private static int instance = 0;
    // TODO: change path to something more permanent
    private static final Path dbPath =
        Paths.get(System.getProperty("user.home"), "enchilada", "sqlitedata");

    /**
     * Connect to the database using default settings, or overriding them with
     * the SQLServer section from config.ini if applicable.
     */
    public SQLiteDatabase()
    {

        // Verify that path for database exists
        if (!dbPath.toFile().exists()) {
            boolean success = dbPath.toFile().mkdirs();
            if (!success)
                throw new RuntimeException("Failed to make directory to store database.");
        }

        url = "localhost";
        port = "1433";
        database = MainFrame.dbname;
        loadConfiguration("SQLite");
    }

    public SQLiteDatabase(String dbName) {
        this();
        database = dbName;
    }

    /**
     * @see InfoWarehouse#isPresent()
     */
    public boolean isPresent() {
        return dbPath.resolve(database).toFile().exists();
    }

    public String getRebuildScriptFilename() {
        return "SQLiteRebuildDatabase.sql";
    }

    /**
     * Open a connection to a SQLite database:
     */
    public boolean openConnection() {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath.resolve(MainFrame.dbname),
                "SpASMS",
                "finally");
    }
    public boolean openConnection(String s) {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath.resolve(s),
                "SpASMS",
                "finally");
    }

    public boolean openConnectionNoDB() {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath.resolve(MainFrame.dbname),
                "SpASMS",
                "finally");
    }

    /**
     * @return the format to match how elsewhere in SQL Server expects it
     */
    public DateFormat getDateFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    protected class StringBatchExecuter extends BatchExecuter {
        private StringBuilder sb;
        public StringBatchExecuter(Statement stmt) {
            super(stmt);
            sb = new StringBuilder();
        }

        public void append(String sql) throws SQLException {
            if (sql.endsWith("\n"))
                sb.append(sql);
            else {
                sb.append(sql);
                sb.append("\n");
            }
        }

        public void execute() throws SQLException {
            System.out.println(sb);
            stmt.execute(sb.toString());
        }
    }

    //TODO-POSTGRES
    //Postgres doesn't have this type of bulk insert
    //But it does have something else
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
     * Specific commands to drop database.
     * @throws SQLException
     */
    public void dropDatabaseCommands() throws SQLException {
        File dbFile = dbPath.resolve(getDatabaseName()).toFile();
        if (!dbFile.exists()) {
            // easy: wasn't there anyway
            return;
        } else if (dbFile.isDirectory()) {
            // so it's there, but it's a directory. That should never happen,
            // but it's an error, so throw an exception so we know about it.
            // This isn't really an SQLException, but this file delete is taking
            // the place of an SQL command for SQLite.
            throw new SQLException("Tried to delete a database file that's actually a directory.");
        } else {
            // The file must be here, so delete it.
            boolean success = dbFile.delete();
            if (!success) {
                throw new SQLException("Failed to delete database file.");
            }
        }
    }

    public void createDatabaseCommands(String dbName)  {
        // For SQLite, there is no create necesary: it happens automatically as
        // soon as you use it.
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
            stmt.executeUpdate("DROP TABLE IF EXISTS newCollection\n");

            stmt.executeUpdate("CREATE TEMP TABLE newCollection (AtomID int, CollectionID int)");
            String query = "INSERT INTO newCollection (AtomID)" +
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
                String query = "DROP TABLE IF EXISTS newCollection;";
                stmt.execute(query);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return;
        }
        try {
            Statement stmt = con.createStatement();
            stmt.execute("UPDATE newCollection SET CollectionID = " + parentCollection.getCollectionID());
            stmt.execute("INSERT INTO InternalAtomOrder (AtomID, CollectionID) SELECT * FROM newCollection;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        propagateNewCollection(newCollection,parentCollection.getParentCollection());


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

        try {
            Statement stmt = con.createStatement();
            stmt.execute("INSERT INTO " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + " VALUES (" +
                    nextID + ", " + dense + ");");
            stmt.execute("INSERT INTO AtomMembership " +
                    "(CollectionID, AtomID) " +
                    "VALUES (" +
                    collection.getCollectionID() + ", " +
                    nextID + ");");
            stmt.execute("INSERT INTO DataSetMembers " +
                    "(OrigDataSetID, AtomID) " +
                    "VALUES (" +
                    datasetID + ", " +
                    nextID + ");");
            stmt.close();

            String tableName = getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype());

            String queryTemplate = "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?)";
            PreparedStatement pstmt = con.prepareStatement(queryTemplate);
            pstmt.setString(1, tableName);
            for (ATOFMSPeak peak : sparse) {
                pstmt.setInt(1, nextID);
                pstmt.setDouble(2, peak.massToCharge);
                pstmt.setInt(3, peak.area);
                pstmt.setFloat(4, peak.relArea);
                pstmt.setInt(5, peak.height);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

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
        InfoWarehouse blankDb = Database.getDatabase("");
        blankDb.createDatabaseCommands(dbName);

        Scanner in = null;
        Connection con = null;

        InfoWarehouse db = Database.getDatabase(dbName);
        try {
            db.openConnection(dbName);
            con = db.getCon();
            Statement stmt = con.createStatement();

            stmt.executeUpdate("CREATE TABLE DBInfo (Name VARCHAR(50) PRIMARY KEY, Value VARCHAR(7500))");
            // -- %version-next% Don't change the spacing of the version number, it gets parsed by the program as well as by SQL.  Also, don't put any lines between this one and the insertion of the version.  Versions are compared by STRING equality.
            stmt.executeUpdate("INSERT INTO DBInfo VALUES ('Version','Sept2006.1')");
            stmt.executeUpdate("CREATE TABLE Collections (CollectionID INT PRIMARY KEY, Name VARCHAR(8000), Comment VARCHAR(8000), Description TEXT, Datatype VARCHAR(8000))");
            stmt.executeUpdate("INSERT INTO Collections VALUES (0, 'ROOT', 'root for unsynchronized data','root', 'root')");
            stmt.executeUpdate("INSERT INTO Collections VALUES (1, 'ROOT-SYNCHRONIZED', 'root for synchronized data','root', 'root')");
            stmt.executeUpdate("CREATE TABLE AtomMembership (CollectionID INT, AtomID INT, PRIMARY KEY (CollectionID, AtomID))");
            stmt.executeUpdate("CREATE TABLE CollectionRelationships(ParentID INT, ChildID INT PRIMARY KEY)");
            stmt.executeUpdate("CREATE TABLE CenterAtoms (AtomID INT PRIMARY KEY, CollectionID INT UNIQUE)");
            stmt.executeUpdate("CREATE TABLE DataSetMembers (OrigDataSetID INT, AtomID INT PRIMARY KEY)");
            stmt.executeUpdate("CREATE TABLE MetaData (Datatype VARCHAR(8000), ColumnName VARCHAR(8000), ColumnType VARCHAR(8000), PrimaryKey BIT, TableID INT, ColumnOrder INT, PRIMARY KEY (Datatype, ColumnName, TableID))");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','DataSetID', 'INT', 1, 0, 1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','DataSet', 'VARCHAR(8000)', 0, 0, 2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','MassCalFile','VARCHAR(8000)', 0, 0, 3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','SizeCalFile','VARCHAR(8000)', 0, 0, 4)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','MinHeight','INT', 0,0,5)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','MinArea','INT', 0,0,6)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','MinRelArea','REAL',0,0,7)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','Autocal','BIT', 0,0,8)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','AtomID', 'INT', 1, 1,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','Time','DATETIME', 0,1,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','LaserPower','REAL', 0,1,3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','Size','REAL', 0,1,4)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','ScatDelay','INT', 0,1,5)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','OrigFilename','VARCHAR(8000)', 0,1,6)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','AtomID','INT', 1, 2, 1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','PeakLocation','REAL', 1,2,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','PeakArea','INT', 0,2,3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','RelPeakArea','REAL', 0,2,4)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('ATOFMS','PeakHeight','INT', 0,2,5)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','DataSetID','INT', 1,0,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','DataSet','VARCHAR(8000)', 0,0,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','OrigCollectionID','INT', 0,0,3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','IsSynchronized','BIT', 0,0,4)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','AtomID','INT', 1,1,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','Time','DATETIME', 0,1,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('TimeSeries','Value','REAL', 0,1,3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','DataSetID','INT', 1,0,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','DataSet','VARCHAR(8000)', 0,0,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','OrigFilename','VARCHAR(8000)', 0,0,3)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','TimeSeriesFile','VARCHAR(8000)', 0,0,4)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','MassToChargeFile','VARCHAR(8000)', 0,0,5)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','AtomID','INT', 1,1,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','Time','DATETIME', 0,1,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','AtomID','INT', 1,2,1)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','PeakLocation','REAL', 1,2,2)");
            stmt.executeUpdate("INSERT INTO MetaData VALUES ('AMS','PeakHeight','REAL', 0,2,3)");
            stmt.executeUpdate("CREATE TABLE ATOFMSDataSetInfo (DataSetID INT, DataSet VARCHAR(8000), MassCalFile VARCHAR(8000), SizeCalFile VARCHAR(8000), MinHeight INT, MinArea INT, MinRelArea REAL, Autocal BIT,  PRIMARY KEY (DataSetID))");
            stmt.executeUpdate("CREATE TABLE ATOFMSAtomInfoDense (AtomID INT, Time TIMESTAMP, LaserPower REAL, Size REAL, ScatDelay INT, OrigFilename VARCHAR(8000),  PRIMARY KEY (AtomID))");
            stmt.executeUpdate("CREATE TABLE ATOFMSAtomInfoSparse (AtomID INT, PeakLocation REAL, PeakArea INT, RelPeakArea REAL, PeakHeight INT, PRIMARY KEY (AtomID, PeakLocation))");
            stmt.executeUpdate("CREATE TABLE TimeSeriesDataSetInfo(DataSetID INT, DataSet VARCHAR(8000), OrigCollectionID INT NULL, IsSynchronized bit, PRIMARY KEY (DataSetID))");
            stmt.executeUpdate("CREATE TABLE TimeSeriesAtomInfoDense(AtomID INT, Time TIMESTAMP, Value REAL, PRIMARY KEY (AtomID))");
            stmt.executeUpdate("CREATE TABLE AMSDataSetInfo (DataSetID INT, DataSet VARCHAR(8000), OrigFilename VARCHAR(8000), TimeSeriesFile VARCHAR(8000), MassToChargeFile VARCHAR(8000), PRIMARY KEY (DataSetID))");
            stmt.executeUpdate("CREATE TABLE AMSAtomInfoDense (AtomID INT, Time TIMESTAMP, PRIMARY KEY (AtomID))");
            stmt.executeUpdate("CREATE TABLE AMSAtomInfoSparse (AtomID INT, PeakLocation REAL, PeakHeight REAL, PRIMARY KEY (AtomID,PeakLocation))");
            stmt.executeUpdate("CREATE TABLE ValueMaps(ValueMapID INTEGER PRIMARY KEY AUTOINCREMENT, Name VARCHAR(100))");
            stmt.executeUpdate("CREATE TABLE ValueMapRanges(ValueMapID INT, Value INT, Low INT, High INT, FOREIGN KEY (ValueMapID) REFERENCES ValueMaps(ValueMapID))");
            stmt.executeUpdate("CREATE TABLE IonSignature(IonID INTEGER PRIMARY KEY, IsPositive BIT(1), Name VARCHAR(7000))");
            stmt.executeUpdate("CREATE TABLE AtomIonSignaturesRemoved(AtomID INT, IonID INT, PRIMARY KEY (AtomID, IonID), FOREIGN KEY (IonID) REFERENCES IonSignature(IonID))");
            stmt.executeUpdate("CREATE TABLE InternalAtomOrder(AtomID INT, CollectionID INT, PRIMARY KEY (CollectionID, AtomID))");

        } catch (SQLException e) {
            throw new ExceptionAdapter(e);
        } finally {
            if (db != null)
                db.closeConnection();
            if (in != null)
                in.close();

        }
        return true;
    }



}
