package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.MainFrame;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

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


}
