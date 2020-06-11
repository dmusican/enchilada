package edu.carleton.enchilada.database;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.MainFrame;

import java.io.File;
import java.sql.Connection;
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
    private static final String dbPath = "sqlitedata/";

    /**
     * Connect to the database using default settings, or overriding them with
     * the SQLServer section from config.ini if applicable.
     */
    public SQLiteDatabase()
    {
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
     * @see InfoWarehouse.java#isPresent()
     */
    public boolean isPresent() {
        return isPresentImpl(
                "SELECT name FROM sqlite_master\n" +
                        "WHERE type='table'\n" +
                        "ORDER BY name");
    }

    public String getRebuildScriptFilename() {
        return "SQLiteRebuildDatabase.sql";
    }

    /**
     * Open a connection to a SQLite database:
     */
    public boolean openConnection() {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath + MainFrame.dbname,
                "SpASMS",
                "finally");
    }
    public boolean openConnection(String s) {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath + s,
                "SpASMS",
                "finally");
    }

    public boolean openConnectionNoDB() {
        return openConnectionImpl(
                "jdbc:sqlite:" + dbPath + MainFrame.dbname,
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
        return new SQLiteDatabase.StringBatchExecuter(stmt);
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
        File dbFile = new File(dbPath + getDatabaseName());
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

    public void createDatabaseCommands(String dbName) throws SQLException {
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
    public int insertParticle(String dense, ArrayList<String> sparse,
                              Collection collection,
                              int datasetID, int nextID, boolean importing) {

        //System.out.println("next AtomID: "+nextID);
        try {
            Statement stmt = con.createStatement();
            //System.out.println("Adding batches");
//            BatchExecuter sql = getBatchExecuter(stmt);
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

            String tableName = getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype());


//            Inserter bi = getBulkInserter(sql, tableName);
            for (int j = 0; j < sparse.size(); ++j) {
                String insertQuery = "INSERT INTO " + tableName + " VALUES (" + nextID + ", " + sparse.get(j) + ");\n";
                stmt.execute(insertQuery);
//                        bi.append(nextID + "," + sparse.get(j));
            }
//            bi.close();


            stmt.close();
//            bi.cleanUp();
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
