package edu.carleton.enchilada.dataImporters;

import java.awt.Window;
import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.swing.SwingUtilities;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.*;
import edu.carleton.enchilada.gui.AMSTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

public class AMSDataSetImporter {
	private AMSTableModel table;
	private Window mainFrame;
	private boolean parent;
	
	//Table values - used repeatedly.
	protected int rowCount;
	protected String datasetName = "";
	protected String timeSeriesFile, massToChargeFile;
	protected ArrayList<Date> timeSeries = null;
	protected ArrayList<Double> massToCharge = null;
	
	private String dense;
	private ArrayList<String> sparse;
	
	// Progress Bar variables
	public static final String TITLE = "Importing AMS Dataset";
	protected ProgressBarWrapper progressBar;
	protected int particleNum;
	protected int totalParticles;
	
	/* contains the collectionID and particleID */
	private int[] id;
	protected int positionInBatch, totalInBatch;
	
	/* Database object */
	Database db;
	
	/* for time conversion */
	private final Calendar startCalendar = new GregorianCalendar(1904,1,1,0,0,0);
	private Calendar convertedCalendar;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final float SEC_PER_YEAR  = 31556926;

	private Scanner readData; // for reading the dataset;
	
	//the parent collection to import into
	private int parentID = 0;
		
	/**
	 * 
	 * Constructor.  Sets the particle table for the importer.
	 * @param amsTableModel - particle table model.
	 */
	public AMSDataSetImporter(AMSTableModel t, Window mf, Database db) {
		startCalendar.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
		table = t;
		mainFrame = mf;
		this.db = db;
	}
	
	public AMSDataSetImporter(AMSTableModel t, Window mf, Database db, ProgressBarWrapper pbar) {
		this(t, mf, db);
		progressBar = pbar;
	}
	
	/**
	 * Loops through each row, collects the information, and processes the
	 * datasets row by row.
	 * @throws WriteException 
	 */
	public void collectTableInfo() throws DisplayException, WriteException {

		rowCount = table.getRowCount()-1;
		totalInBatch = rowCount;
		//Loops through each dataset and creates each collection.
		for (int i=0;i<rowCount;i++) {
			try {
				// Indicate progress for this dataset
				progressBar.reset();
				progressBar.setTitle(TITLE+": "+(i+1)+" of "+rowCount);
				
				// Table values for this row.
				datasetName = (String)table.getValueAt(i,1);
				timeSeriesFile = (String)table.getValueAt(i,2);
				massToChargeFile = (String)table.getValueAt(i,3);
				
				//System.out.println(datasetName);
				//System.out.println(timeSeriesFile);
				//System.out.println(massToChargeFile);
				
				positionInBatch = i + 1;
				// Call relevant methods
				processDataSet();

			} catch (DisplayException e) {
				throw new DisplayException(datasetName + " failed to import. Exception: " + e.toString());
			} catch (WriteException e) {
				throw new WriteException(datasetName + " failed to import.  Exception: " + e.toString());
			}
		}
	}
	
	/**
	 * Dumps data from datasetInfo, timeSeriesFile, and massToChargeFile
	 * into database.
	 * 
	 * // NOTE: Datatype is already in the db.
	 */
	public void processDataSet() throws DisplayException, WriteException {
		boolean skipFile = false;
		String[] AMS_tables = {"AMSAtomInfoDense", "AtomMembership", "DataSetMembers", "AMSAtomInfoSparse"};
		Database.BulkInserter bulkInserter = db.new BulkInserter(AMS_tables);

		progressBar.setIndeterminate(true);
		progressBar.setText("Reading time series file");
		//put time series file and mz file into an array, since they will
		//be accessed in the same way for every atom.
		Scanner readTimeSeries;
		try {
			readTimeSeries = new Scanner(new File(timeSeriesFile));
		} catch (FileNotFoundException e1) {
			throw new WriteException(timeSeriesFile + " was not found.");
		}
		readTimeSeries.next(); // skip name
		timeSeries = new ArrayList<Date>();
		BigInteger maxInt = new BigInteger("" + Integer.MAX_VALUE);
		BigInteger bigInt;
		String tempStr;
		BigInteger prevBigInt = null, temp = null;
		while (readTimeSeries.hasNext()) {
			tempStr = readTimeSeries.next();
			if (tempStr.indexOf('.') != -1) {
				tempStr = tempStr.substring(0, tempStr.indexOf('.'));
				bigInt = new BigInteger(tempStr);
				bigInt = bigInt.add(new BigInteger("1"));
			} else
				bigInt = new BigInteger(tempStr);
			//if this is the first time, then calculate it using the while loop.
			if (prevBigInt == null) {
				prevBigInt = bigInt;
				convertedCalendar = (Calendar) startCalendar.clone();
				while (bigInt.compareTo(maxInt) == 1) {
					convertedCalendar.add(Calendar.SECOND, Integer.MAX_VALUE);
					bigInt = bigInt.subtract(maxInt);
				}
				convertedCalendar.add(Calendar.SECOND, bigInt.intValue());
			}
			// else, subtract it from previous time to calculate.
			else {
				temp = bigInt.subtract(prevBigInt);
				convertedCalendar.add(Calendar.SECOND, temp.intValue());
				prevBigInt = bigInt;
			}
			timeSeries.add(convertedCalendar.getTime());
		}
		readTimeSeries.close();

		progressBar.setText("Reading m/z file");
		Scanner readMZ;
		try {
			readMZ = new Scanner(new File(massToChargeFile));
		} catch (FileNotFoundException e1) {
			throw new WriteException(massToChargeFile + " was not found.");
		}
		readMZ.next(); // skip name
		massToCharge = new ArrayList<Double>();
		while (readMZ.hasNext()) {
			massToCharge.add(readMZ.nextDouble());
		}
		readMZ.close();

		// create empty collection.
		try {
			id = db.createEmptyCollectionAndDataset("AMS", parentID, getName(), "AMS import",
					"'" + datasetName + "','" + timeSeriesFile + "','" + massToChargeFile + "'");
		} catch (FileNotFoundException e1) {
			throw new WriteException("Attempt to get name for collection not" +
					" found because the file was not found.");
		}


		// get total number of particles for progress bar.
		progressBar.setText("Finding number of items");
		try {
			readData = new Scanner(new File(datasetName));
		} catch (FileNotFoundException e1) {
			throw new WriteException(datasetName + " was not found.");
		}
		readData.next();//skip name
		int tParticles = 0;
		while (readData.hasNextLine()) {
			//Items are stored in matrix format: one line per item, with
			//	massToCharge.size() number of space-separated values per line
			readData.nextLine();
			tParticles++;
		}
		readData.close();
		final int totalParticles = tParticles - 1;
		System.out.println("total items: " + totalParticles);
		progressBar.setMaximum((totalParticles / 10) + 1);
		progressBar.setIndeterminate(false);

		try {
			Collection destination = db.getCollection(id[0]);

			readData = new Scanner(new File(datasetName));
			readData.next(); // skip name

			//for skipped particles with no sparse information
			Vector<Integer> nodataParticles = new Vector<>();
			particleNum = 0;
			int nextID = db.getNextID();
			while (readData.hasNext()) { // repeat until end of file.
				if (particleNum % 10 == 0 && particleNum >= 10) {
					String barText = "Importing Item # " + particleNum + " out of "
							+ totalParticles;
					if (!nodataParticles.isEmpty())
						barText += ", " + nodataParticles.size() + " have no data";
					progressBar.increment(barText);
				}
				read(particleNum, nextID);
				if (sparse != null && !sparse.isEmpty()) {
					bulkInserter.queueInsertion(dense, sparse, destination, id[1], nextID);
					nextID++;
				} else {
					nodataParticles.add(particleNum);
				}
				particleNum++;
			}
			progressBar.setIndeterminate(true);
			progressBar.setText("Inserting Items...");
			bulkInserter.executeBatch();
			db.updateInternalAtomOrder(destination);
			//write information on no-data particles to Collection Information tab
			if (!nodataParticles.isEmpty()) {
				StringBuilder desc =
						new StringBuilder(db.getCollectionDescription(destination.getCollectionID()));
				desc.append("\n");
				desc.append(nodataParticles.size());
				desc.append(" items had no associated m/z spectrum data and were " +
						"skipped during import. Their original indices are:\n");
				Integer cur = null;
				Iterator<Integer> i = nodataParticles.iterator();
				while (i.hasNext()) {
					desc.append(i.next().intValue());
					if (i.hasNext())
						desc.append(", ");
				}
				db.setCollectionDescription(destination, desc.toString());
			}

			//percolate possession of new atoms up the hierarchy
			progressBar.setText("Updating Ancestors...");
			db.propagateNewCollection(destination);
			readData.close();

		} catch (Exception e) {

			if (e instanceof ExceptionAdapter && ((ExceptionAdapter) e).originalException instanceof SQLException) {
				throw (ExceptionAdapter) e;
			}
			try {
				e.printStackTrace();
				final String exception = e.toString();
				SwingUtilities.invokeAndWait(new Runnable() {
					public void run() {
						// don't throw an exception here because we want to keep going:
						ErrorLogger.writeExceptionToLogAndPrompt("Importing", "Corrupt datatset file or particle: " + exception);
					}
				});
			} catch (Exception e2) {
				e2.printStackTrace();
				// don't throw exception here because we want to keep going:
				ErrorLogger.writeExceptionToLogAndPrompt("Importing", "ParticleException: " + e2.toString());
			}
		}
	}
	
	/**
	 * This method loops through the table and checks to make sure that there is a
	 * are non-null values for all datasets.
	 */
	public void errorCheck() throws DisplayException{
		String name, timeFile, mzFile;
		File d,m,t;
		for (int i=0;i<table.getRowCount()-1;i++) {
			name = (String)table.getValueAt(i,1);
			timeFile = (String)table.getValueAt(i,2);
			mzFile = (String)table.getValueAt(i,3);
			//Check to make sure that .par and .cal files are present.
			if (name.equals("data file") || name.equals("") 
					|| timeFile.equals("time series file") 
					|| timeFile.equals("")
					|| mzFile.equals("mass to charge file") 
					|| mzFile.equals("")) {
				throw new DisplayException("You must enter a data file, a time series file," +
						" and a mass to charge file at row # " + (i+1) + ".");
			}
			
			// check to make sure all files are valid:
			d = new File(name);
			t = new File(timeFile);
			m = new File(mzFile);
			if (!d.exists() || !t.exists() || !m.exists()) {
				throw new DisplayException("One of the files does not exist at row # " + (i+1) +".");
			}
		}
	}
	
	/**
	 * Removes the file extension from datasetName and returns the 
	 * collection name.
	 * @return
	 * @throws FileNotFoundException 
	 */
	public String getName() throws FileNotFoundException {
		Scanner scan = new Scanner(new File(datasetName));
		String toReturn = scan.next();
		scan.close();
		System.out.println("Importing: " + toReturn);
		return toReturn;

	}
	
	/** 
	 * constructs a dense list and a sparse list for the atom at the given
	 * position in the data file.  These are global variables.
	 * @return
	 */
	public void read(int particleNum, int nextID) {
		// populate dense string
		//dense = "'"+dateFormat.format(timeSeries.get(particleNum))+"'";
		dense = dateFormat.format(timeSeries.get(particleNum));
		sparse = new ArrayList<String>();
		double tempNum;
		for (int i = 0; i < massToCharge.size(); i++) {
			tempNum = readData.nextDouble();
			//System.out.println(tempNum);
			if (tempNum != 0.0 && tempNum != -999.0)
				sparse.add(massToCharge.get(i)+","+tempNum);
		}	
	}
	
	/**
	 * Change the parent collection for incoming datasets from its default of 0
	 * @param id the parent collection ID
	 * @author shaferia
	 */
	public void setParentID(int id) {
		parentID = id;
	}
}
