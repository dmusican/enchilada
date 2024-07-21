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

package edu.carleton.enchilada.dataImporters;

import java.awt.Window;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.InputMismatchException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import javax.swing.SwingUtilities;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.*;
import edu.carleton.enchilada.gui.PALMSTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**Imports a PALMS type data file into the database as
 *  ATOFMS data points. 
 * 
 * @author rzeszotj with credit to AMS and ATOFMS import authors
 *
 */

public class PALMSDataSetImporter {
	private PALMSTableModel table;
	private Window mainFrame;
	private boolean parent;
	
	//Table values - used repeatedly.
	protected int rowCount;
	protected String datasetName = "";
	//header sizes, dense and sparse data strings/lists;
	//This is a fix for peak area being represented as an integer while
	//  PALMS represents peak relatively in decimal
	//HACK FIX THIS NOW  -  Right now it just bumps up X sigfigs to integers and rounds
	private final int significantFiguresToKeep = 6;
	
	private String dense;
	private ArrayList<String> sparse;
	
	private String comments;	
	private int decimalScalar;
	private Date fullDate;
	private String missionDate;
	
	private int numPeaks;
	private int peakScalar;
	private int header2Length;
	
	
	// Progress Bar variables
	public static final String TITLE = "Importing PALMS Dataset";
	protected ProgressBarWrapper progressBar;
	protected int particleNum;
	protected int totalParticles;
	
	/* contains the collectionID and particleID */
	private int[] id;
	protected int positionInBatch, totalInBatch;
	
	/* Database object */
	Database db;
	
	/* Lock to make sure database is only accessed in one batch at a time */
	private static Integer dbLock = new Integer(0);
	
	/* for time conversion */
	private ParsePosition p = new ParsePosition(0);
	private SimpleDateFormat secondsFormat = new SimpleDateFormat("HH mm ss SSS");
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy MM dd HH mm ss SSS");
	private final float SEC_PER_YEAR  = 31556926;
	
	private Scanner readData; // for reading the dataset;
	
	//the parent collection to import into
	private int parentID = 0;
		
	/**
	 * Constructor.  Sets the particle table for the importer.
	 * @param PALMSTableModel - particle table model.
	 */
	public PALMSDataSetImporter(PALMSTableModel t, Window mf, Database db) {
		table = t; //The gui table with multiple datasets
		mainFrame = mf;
		this.db = db;
	}
	
	public PALMSDataSetImporter(PALMSTableModel t, Window mf, Database db, ProgressBarWrapper pbar) {
		this(t, mf, db);
		progressBar = pbar;
	}
	
	/**
	 * Loops through each gui row, collects the information, and processes the
	 * _datasets_ row by row.
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
				
				System.out.println(datasetName);
				
				positionInBatch = i + 1;
				// Call relevant method to read PALMS.txt
				processDataSet(i);

			} catch (DisplayException e) {
				throw new DisplayException(datasetName + " failed to import. Exception: " + e.toString());
			} catch (WriteException e) {
				throw new WriteException(datasetName + " failed to import.  Exception: " + e.toString());
			} catch (InputMismatchException e) {
				throw new InputMismatchException(getName() + " failed to import. It is an invalid PALMS data file.");
			}
				
			
		}
	}
	
	/**
	 * Dumps data from particles in .txt as ATOFMS into database.
	 * 
	 * // NOTE: Datatype is already in the db.
	 * @author rzeszotj with credit to AMSimport author
	 */
	public void processDataSet(int index) throws DisplayException, WriteException, InputMismatchException {
		boolean skipFile = false;
		final String[] ATOFMS_tables = {"ATOFMSAtomInfoDense", "AtomMembership", 
				   "DataSetMembers", "ATOFMSAtomInfoSparse","InternalAtomOrder"};
		Database.BulkInserter bulkInserter = db.new BulkInserter(ATOFMS_tables);

			//Begin reading data file
			progressBar.setIndeterminate(true);
			progressBar.setText("Reading data headers");
			try {
				readData = new Scanner(new File(datasetName));//.useDelimiter("\t");
			} catch (FileNotFoundException e1) {
				throw new WriteException(datasetName+" was not found.");
			}

			//Read in header block, adding to comments
			String versionNumber1 = readData.nextLine();
			comments = "Comments: ";
			comments = comments.concat(readData.nextLine() + ", ");
			comments = comments.concat(readData.nextLine() + ", ");
			//Relying on a string here sounds like a terrible idea...
			String negPosDetermine = readData.nextLine();
			if(negPosDetermine.contains("Positive"))
			{
				System.out.println("Spectrum is positive");
				peakScalar = 1;
			}
			else if(negPosDetermine.contains("Negative"))
			{
				System.out.println("Spectrum is negative");
				peakScalar = -1;
			}
			else
			{
				System.out.println("Unknown whether spectrum is positive or negative.");
				System.out.println("Assuming spectrum is positive");
				peakScalar = 1;
			}
			comments = comments.concat(negPosDetermine + ", ");
			comments = comments.concat(readData.nextLine());
			System.out.println(comments);
			String versionNumber2 = readData.nextLine();
			//Get the date of mission for use during particle read
			String temptimes = readData.nextLine();
			missionDate = temptimes.substring(0,10);

			readData.nextLine();
			readData.nextLine();

			numPeaks = readData.nextInt() - 4;
			System.out.println(numPeaks+" peaks exist.");

			//Skip the (numpeaks+4)*2+4 worth of garbage info
			for(int i = 0; i <= ((numPeaks + 4)*2+4); i++)
				readData.nextLine();

			//Skip the meaningless mass names
			for(int i = 0; i < numPeaks; i++)
				readData.nextLine();

			//Get length of header 2 and skip the 0s, maxINTs, and header text
			header2Length = readData.nextInt();
			for (int i = 0; i <= header2Length*3; i++)
				readData.nextLine();
			//Skip 2 dead lines
			readData.nextLine();
			readData.nextLine();
			//Finally no more garbage data, we are at the right place
			//Leave readData hanging around so read() can grab it

			//Calculate peak height scalar here - no place better for it
			decimalScalar = (int)Math.pow(10, significantFiguresToKeep);

			//Create empty ATOFMS collection
			id = db.createEmptyCollectionAndDataset("ATOFMS",parentID,getName(),
					comments,
					"'" + "PALMS" + "','" + "PALMS" + "'," +
							"0" + "," + "0"  + "," + "0" + ",0");

			progressBar.setText("Reading particle data");

			Collection destination = db.getCollection(id[0]);

			//Loop through particles in file
			particleNum = 0;
			int nextID = db.getNextID();
			while (readData.hasNext()) {
				//Announce
				if (particleNum % 100 == 0)
					System.out.println("Reading particle #"+particleNum);

				read(particleNum, nextID); //READ IN PARTICLE DATA HERE

				//Only copy in particles with peaks
				if (sparse != null && !sparse.isEmpty()) {
					bulkInserter.queueInsertion(dense,sparse,
							destination,id[1],nextID);
					nextID++;
				}
				particleNum++;
			}
			progressBar.setText("Inserting Items...");
			bulkInserter.executeBatch();
			db.updateInternalAtomOrder(destination);
			//percolate possession of new atoms up the hierarchy
			progressBar.setText("Updating Ancestors...");
			db.propagateNewCollection(destination);
			readData.close();
	}

	
	/** 
	 * constructs a dense list and a sparse list for the atom at the given
	 * position in the data file.  These are global variables.
	 * @author rzeszotj
	 */
	public void read(int particleNum, int nextID) {
		//Import dense data
		long dateS = (long)readData.nextDouble();
		readData.nextDouble();
		int acqNumber = readData.nextInt();
		
		//Skip MORE useless garbage, awful hack etc.
		for (int i = 0; i < header2Length-2; i++)
			readData.nextLine();
		//Skip top 4 lines of mass data
		for (int i = 0; i <= 4; i++)
			readData.nextLine();
		
		//Format date correctly	- add seconds to mission date (stripping off last seconds if need to)
		Date seconds = new Date(dateS);
		String sec = secondsFormat.format(seconds);
		missionDate = missionDate.substring(0,10);
		missionDate = missionDate.concat(" "+sec);
		p.setIndex(0);
		fullDate = dateFormat.parse(missionDate,p);
		DateFormat dbDF = db.getDateFormat();
		String newD = dbDF.format(fullDate);		
		
		//ATOFMSDense = Time, Laser Power, Size, Scatter Delay, File Name
		dense = (newD + ", 0.0, 0.0, " + acqNumber + "," + datasetName);
		//ATOFMSSparse = Location, Height, Area, Relative Area
		sparse = new ArrayList<String>();
		for (int i = 1; i <= numPeaks; i++) {
			double t = readData.nextDouble();
			if (t != 0.0)
			{
				//Scale the result and write it
				int temp = (int)Math.round(t*decimalScalar);
				sparse.add((i*peakScalar)+", "+temp+", "+"0"+", "+"0");
			}
		}
		//System.out.println(sparse);
	}
	
	/**
	 * Removes the file extension and directory information 
	 * from datasetName and returns the collection name. 
	 * @return String
	 * @author rzeszotj
	 */
	public String getName(){
		String s = "PALMS file";
		Character c1 = '.';
		Character c2 = '\\';
		
		if (!(datasetName == null)){
			int begin = 0;
			int end = datasetName.length() - 1;
			for (int i = (datasetName.length()-1); i >= 0; i--) {
				Character c3 = datasetName.charAt(i);
				if (c1.equals(c3))
					end = i;
				if (c2.equals(c3)){
					begin = i+1;
					return datasetName.substring(begin,end);
				}
			}
			return datasetName;//datasetName had no extension etc.
		}
		//For some reason datasetName doesn't exist
		return "PALMS data";
	}
	
	/**
	 * Change the parent collection for incoming datasets from its default of 0
	 * @param id the parent collection ID
	 * @author shaferia
	 */
	public void setParentID(int id) {
		parentID = id;
	}
	
	public void errorCheck() throws DisplayException{
		String name;
		File d;
		for (int i=0;i<table.getRowCount()-1;i++) {
			name = (String)table.getValueAt(i,1);
			//Check to make sure that .txt files are present.
			if (name.equals(".txt file") || name.equals("")) 
				throw new DisplayException("You must enter a data file at row # " + (i+1) + ".");
			
			// check to make sure all files are valid:
			d = new File(name);
			if (!d.exists()) {
				throw new DisplayException("The file " + name +" does not exist.");
			}
		}
	}
}
