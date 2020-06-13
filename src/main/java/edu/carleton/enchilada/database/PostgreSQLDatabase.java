package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.MainFrame;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Makes database work with PostgreSQL
 */
public class PostgreSQLDatabase extends Database {
    //This isn't used anymore. Safe for deletion.
    private static int instance = 0;

    /**
     * Connect to the database using default settings, or overriding them with
     * the SQLServer section from config.ini if applicable.
     */
    public PostgreSQLDatabase()
    {
        url = "localhost";
        port = "1433";
        database = MainFrame.dbname;
        loadConfiguration("PostgreSQL");
    }

    public PostgreSQLDatabase(String dbName) {
        this();
        database = dbName;
    }

    /**
     * @see InfoWarehouse.java#isPresent()
     */
    public boolean isPresent() {
        return isPresentImpl("SELECT datname FROM pg_database");
    }

    public String getRebuildScriptFilename() {
        return "PostgreSQLRebuildDatabase.txt";
    }

    /**
     * Open a connection to a MySQL database:
     * uses the jtds driver from jtds-*.jar
     * TODO: change security model
     */
    public boolean openConnection() {
        return openConnectionImpl(
                "jdbc:postgresql://localhost/"+MainFrame.dbname,
                //Use this string to connect to a SQL Server Express instance
                //"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;databaseName=SpASMSdb;SelectMethod=cursor;",
                "postgres",
                "finally");
    }
    public boolean openConnection(String s) {
        return openConnectionImpl(
                "jdbc:postgresql://localhost/"+s+"",
                //Use this string to connect to a SQL Server Express instance
                //"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;databaseName="+s+";SelectMethod=cursor;",
                "postgres",
                "finally");
    }
    /**
     * Open a connection to a MySQL database:
     * uses the jtds driver from jtds-*.jar
     * TODO: change security model
     */
    public boolean openConnectionNoDB() {
        return openConnectionImpl(
                "jdbc:postgresql://localhost/",
                //Use this string to connect to a SQL Server Express instance
                //"jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;SelectMethod=cursor;",
                "postgres",
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
        return new PostgreSQLDatabase.StringBatchExecuter(stmt);
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

    public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
                              Collection collection,
                              int datasetID, int nextID) {
        throw new UnsupportedOperationException();
    }

}
