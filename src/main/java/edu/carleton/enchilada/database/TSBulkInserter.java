package edu.carleton.enchilada.database;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;

import java.text.SimpleDateFormat;
import java.sql.*;

/**
 * This class makes it fairly simple to insert a lot of time series data quickly.
 * <p>
 * It is not synchronized or anything, so only use one at a time!  
 * Otherwise you'll get conflicting AtomIDs.
 * 
 * @author smitht
 * @author jtbigwoo
 * @author dmusicant
 *
 */
public class TSBulkInserter {
	private PreparedStatement vals, membership, iao, dataset;
	private String tabName;
	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private boolean started = false;
	private int collectionID, datasetID, nextID, firstID;
	// number of inserts to batch up before executing, to avoid filling up too much memory before doing so
	private final int maxBufferSize = 1000;
	private int numRowsAddedInBatch;
	
	
	private InfoWarehouse db;
	private Connection con;

	/**
	 * Create a new TSBulkInserter with an already-connected database.
	 * @param db
	 */
	public TSBulkInserter(InfoWarehouse db) {
		this.db = db;
		setUp();
	}

	private void setUp() {
		try {
			tabName = db.getDynamicTableName(DynamicTable.AtomInfoDense, "TimeSeries");
			nextID = firstID = collectionID = datasetID = -1;
			con = db.getCon();
			con.setAutoCommit(false);
			vals = con.prepareStatement("INSERT INTO " + tabName + " VALUES (?, ?, ?)");
			membership = con.prepareStatement("INSERT INTO AtomMembership (CollectionID, AtomID) VALUES (?, ?)");
			iao = con.prepareStatement("INSERT INTO InternalAtomOrder (CollectionID, AtomID) VALUES (?, ?)");
			dataset = con.prepareStatement("INSERT INTO DataSetMembers (OrigDatasetID, AtomID) VALUES (?, ?)");
			numRowsAddedInBatch = 0;
		} catch (SQLException e) {
			throw new ExceptionAdapter(e);
		}
	}
	
	/**
	 * Creates the {@link Collection} that the time series info will go into,
	 * and sets everything up for adding the data.
	 * 
	 * @param collName name of the time series column
	 * @return an array containing the collectionID (index 0) and the datasetID (1).
	 */
	public int[] startDataset(String collName) {
		if (started) throw new Error("Bad order of calls to TSBulkInserter");
		int[] collectionInfo = db.createEmptyCollectionAndDataset(
				"TimeSeries",
				0,
				collName,
				"",
				"-1,0");
		collectionID = collectionInfo[0];
		datasetID = collectionInfo[1];
		nextID = firstID = db.getNextID();
		started = true;
		return collectionInfo;
	}

	/**
	 * Add a time, value pair to be inserted.  If there are enough pairs built up,
	 * this method may run some of the queries.
	 * 
	 * @param time the time at which the observation was made
	 * @param val the value of the observation
	 * @throws SQLException
	 */
	public void addPoint(java.util.Date time, float val) throws SQLException {
		if (!started) {
			throw new Error("Haven't called startDataset() before adding a point.");
		}
		vals.setInt(1, nextID);
		vals.setString(2, df.format(time));
		vals.setFloat(3, val);
		vals.addBatch();

		membership.setInt(1, collectionID);
		membership.setInt(2, nextID);
		membership.addBatch();

		iao.setInt(1, collectionID);
		iao.setInt(2, nextID);
		iao.addBatch();

		dataset.setInt(1, datasetID);
		dataset.setInt(2, nextID);
		dataset.addBatch();
		
		nextID++;
		numRowsAddedInBatch++;
		
		if (numRowsAddedInBatch >= maxBufferSize) {
			interimCommit();
		}
	
	}
	
	/**
	 * commits the particles that are currently queued up in the StringBuffers.
	 */
	private void interimCommit() throws SQLException {
		if (db.getNextID() != firstID) {
			throw new IllegalStateException("Database has changed under a batch insert.. you can't do that!");
		}

		vals.executeBatch();
		membership.executeBatch();
		iao.executeBatch();
		dataset.executeBatch();
		con.commit();
		numRowsAddedInBatch = 0;
		firstID = nextID = db.getNextID();
	}
	
	/**
	 * Put all the time, value pairs that have been added using addPoint into the database.
	 * <p>
	 * Note that some may have been put into the database before you call this method.
	 * 
	 * @return the collectionID of the collection which holds the observations.
	 */
	public int commit() throws SQLException {
		interimCommit();
		started = false;
		
		db.propagateNewCollection(db.getCollection(collectionID));
		
		int ret = collectionID;
		
		collectionID = -1;
		datasetID = -1;
		nextID = firstID = -1;
		
		return ret;
	}
}
