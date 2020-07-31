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
 * The Original Code is EDAM Enchilada's DatasetImporter class.
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
 * Created on Aug 3, 2004
 *
 */
package edu.carleton.enchilada.dataImporters;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import edu.carleton.enchilada.ATOFMS.*;

import java.awt.Window;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.zip.DataFormatException;
import java.util.Date;


import edu.carleton.enchilada.collection.Collection;


/**
 * Creates a new DataSetProcessor object for each dataset that processes the spectra and
 * creates the particle.  It passes a file name, a CalInfo object and a PeakParams object 
 * to DataSetProcessor.
 * 
 * 
 * @author ritza, Jamie Olson
 *
 */
public class ATOFMSDataSetImporter {
	
	private ParTable table;
	private Window mainFrame;
	private boolean parent;
	
	//Table values - used repeatedly.
	private String name = "";
	private String massCalFile, sizeCalFile;
	private int height, area;
	private float relArea;
	private float peakError;
	private boolean autoCal;
	
	// Progress Bar variables
	protected ProgressBarWrapper progressBar;
	public static final String title = "Importing ATOFMS DataSet";
	
	/* '.par' file */
	protected File parFile;
	
	/* contains the collectionID and particleID */
	protected int[] id;
	protected int collectionIndex, numCollections;
	protected Integer nextAtomID;
	protected Integer particleNumber;
	protected int[] numParticles;
	protected Collection[] collections;
	private final ATOFMSDataSetImporter thisRef;
	/* Database object */
	Database db;
	
	/* Lock to make sure database is only accessed in one batch at a time */
	//private static Integer dbLock = new Integer(0);
	
	/* the parent collection */
	private int parentID = 0;

	
	/**
	 * 
	 * Constructor.  Sets the particle table for the importer.
	 * @param t - particle table model.
	 */
	public ATOFMSDataSetImporter(ParTable t, Window mf, ProgressBarWrapper progressBar) {
		table = t;
		mainFrame = mf;
		this.progressBar = progressBar;
		nextAtomID = new Integer(-1);
		particleNumber = 0;
		thisRef = this;
	}
	
	public ATOFMSDataSetImporter(ParTable t, Window mf, Database db, ProgressBarWrapper progressBar) {
		this(t,mf,progressBar);
		this.db = db;
	}
	
	/**
	 * Collects table-wide information
	 */
	public void collectTableInfo(){
		
		numCollections = table.getRowCount()-1;
		collections = new Collection[numCollections];
		try {
			numParticles = getNumParticles();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		progressBar.setMaximum(numParticles[numCollections]);
		collectionIndex = 0;	
	}
	
	/**
	 * Loops through each row, collects the information, and processes the
	 * datasets row by row.
	 */
	public void collectRowInfo() throws InterruptedException, DisplayException{
		
		//Loops through each dataset and creates each collection.
		if(this.collectionIndex<numCollections){
			if(progressBar.wasTerminated()){
				throw new InterruptedException();
			}
			progressBar.setTitle(title+": "+(collectionIndex+1)+" of "+numCollections);
			// Table values for this row.
			setVariablesFromTable(collectionIndex);
			// Call relevant methods
			try {
				processDataSet();
				readParFileAndCreateEmptyCollection();
				readSpectraAndCreateParticle();

			} catch (IOException e) {
				//e.printStackTrace();
				ErrorLogger.writeExceptionToLog("Importing","File "+name+
						" failed to import: \n\tMessage : "+e.toString()+","+e.getMessage()+"\n\t"+Arrays.toString(e.getStackTrace()));
				throw new DisplayException("Error importing data: Import aborted.  Check Error Log.");
			}catch (DataFormatException e) {
				//e.printStackTrace();
				ErrorLogger.writeExceptionToLog("Importing","File "+name+
						" failed to import: \n\tMessage : "+e.toString()+","+e.getMessage()+"\n\t"+Arrays.toString(e.getStackTrace())); 
				throw new DisplayException("Error importing data: Import aborted.  Check Error Log.");
			}catch (ParseException e) {
				//e.printStackTrace();
				ErrorLogger.writeExceptionToLog("Importing","File "+name+
						" failed to import: \n\tMessage : "+e.toString()+","+e.getMessage()+"\n\t"+Arrays.toString(e.getStackTrace())); 
				throw new DisplayException("Error importing data: Import aborted.  Check Error Log.");
			}
			collectionIndex++;
		}
	}
	
	
	public void setVariablesFromTable(int i){
		int nextCol = 1;
		name = (String)table.getValueAt(i,nextCol++);
		massCalFile = (String)table.getValueAt(i,nextCol++);
		sizeCalFile = (String)table.getValueAt(i,nextCol++);
		height= ((Integer)table.getValueAt(i,nextCol++)).intValue();
		area = ((Integer)table.getValueAt(i,nextCol++)).intValue();
		relArea = ((Float)table.getValueAt(i,nextCol++)).floatValue();
		if(table.getColumnCount() == 9){
			peakError = ((Float)table.getValueAt(i,nextCol++)).floatValue();
		}else{
			peakError = .50f;
		}
		autoCal = ((Boolean)table.getValueAt(i,nextCol++)).booleanValue();
		
	}
	
	/**
	 * Does some preprocessing before importing a datset.
	 * Sets the currCalInfo and currPeakParam fields of ATOFMSParticle,
	 * creates an empty collection and fills that collection with the dataset's 
	 * particles.
	 * @throws DisplayException 
	 */
	public void processDataSet()
	throws IOException, DataFormatException, DisplayException {
		boolean skipFile = false;
		
		//Create CalInfo Object.
		CalInfo calInfo = null;
		try {
			if (sizeCalFile.equals(".noz file") || sizeCalFile.equals("")) 
				calInfo = new CalInfo(massCalFile, autoCal);
			else 
				calInfo = new CalInfo(massCalFile,sizeCalFile, autoCal);
		} catch (Exception e) {
			ErrorLogger.writeExceptionToLog("Importing","Corrupt calibration file : " +
					"\n\tMessage: "+e.getMessage()+"\n\t"+Arrays.toString(e.getStackTrace()));
			throw new IOException();
		}
		if (!skipFile) { // If we don't have to skip this row due to an error...
			
			// Create PeakParam Object.
			PeakParams peakParams = new PeakParams(height,area,relArea,peakError);
			
			//Read '.par' file and create collection to fill.
			parFile = new File(name);
			if(parFile==null){
				ErrorLogger.writeExceptionToLog("Importing","Could not open file: "+name+".");
				throw new FileNotFoundException();
			}
			ATOFMSParticle.currCalInfo = calInfo;
			ATOFMSParticle.currPeakParams = peakParams;
			
			// NOTE: Datatype is already in the db.
		}
	}
	
	/**
	 * This method loops through the table and checks to make sure that there is a
	 * .par file and a .cal file for every row, as well as non-zeros for the params.
	 * @return returns if there are no null rows, throws exception if there are.
	 */
	public void checkNullRows() throws DisplayException {
		String name, massCalFile;
		int height, area;
		float relArea;
		for (int i=0;i<table.getRowCount()-1;i++) {
			name = (String)table.getValueAt(i,1);
			massCalFile = (String)table.getValueAt(i,2);
			//Check to make sure that .par and .cal files are present.
			if (name.equals(".par file") || name.equals("") 
					|| massCalFile.equals(".cal file") 
					|| massCalFile.equals("")) {
				throw new DisplayException("You must enter a '.par' file and a " +
						"'.cal' file at row # " + (i+1) + ".");
			}
			height= ((Integer)table.getValueAt(i,4)).intValue();
			if (height == 0)
				throw new DisplayException("The Min. Height must be greater than 0 in row # " + (i+1) + ".");
			area = ((Integer)table.getValueAt(i,5)).intValue();
			if (area == 0)
				throw new DisplayException("The Min. Area must be greater than 0 in row # " + (i+1) + ".");
			relArea = ((Float)table.getValueAt(i,6)).floatValue();
			if (relArea == 0.0)
				throw new DisplayException("The Min. Rel. Area must be greater than 0 in row # " + (i+1) + ".");
		}
	}
	
	/**
	 * Reads the pertinent information from the '.par' file and creates
	 * an empty collection ready to populate with that dataset's particles.
	 *
	 */
	public void readParFileAndCreateEmptyCollection() 
	throws IOException, NullPointerException, DataFormatException {
		//Read '.par' info.
		String[] data = parVersion();
		//CreateEmptyCollectionandDataset
		if (db == null) {
			db = MainFrame.db;
		}
		if (db == null) { // still
			db = Database.getDatabase();
			db.openConnection();
		}
		id = new int[2];
		System.out.println(data[0]);
		System.out.println(data[2]);
		System.out.println(massCalFile);
		System.out.println(sizeCalFile);
		int bool = -1;
		if (ATOFMSParticle.currCalInfo.autocal)
			bool = 1;
		else bool = 0;
		String dSet = parFile.toString();
		dSet = dSet.substring(dSet.lastIndexOf(File.separator)+1, dSet.lastIndexOf("."));
		
		//if datasets are imported into a parent collection
		//pass parent's id in as second parameter, else parentID is root (0)
//		if (ipd.parentExists())
//			parentID = ipd.getParentID();
		//begin a transaction
		
		id = db.createEmptyCollectionAndDataset("ATOFMS",parentID,data[0],data[2],
				"'" + massCalFile + "', '" + sizeCalFile + "', " +
				ATOFMSParticle.currPeakParams.minHeight + ", " + 
				ATOFMSParticle.currPeakParams.minArea  + ", " + 
				ATOFMSParticle.currPeakParams.minRelArea + ", " + 
				bool);
		
	}
	
	public int[] getNumParticles() throws IOException{
		int[] numParticles = new int[numCollections+1];
		for (int i=0;i<numCollections;i++) {
			String fileName = (String)table.getValueAt(i,1);
			File parFile = new File(fileName);

			File canonical = parFile.getAbsoluteFile();
			File parent = canonical.getParentFile();
			File grandParent = parent.getParentFile();
			//System.out.println("Data set: " + parent.toString());
			final ATOFMSDataSetImporter thisref = this;

			String name = parent.getName();
			name = parent.toString()+ File.separator + name + ".set";
			if (new File(name).isFile()) {
				Scanner countSet = new Scanner(new File(name));
				countSet.useDelimiter("\r\n");
		        
				int tParticles = 0;
				int particleNum = 0;
				while(countSet.hasNextLine())
				{
					countSet.nextLine();
					tParticles++;
				}
				countSet.close();
				numParticles[i] = tParticles;
				numParticles[numCollections] += tParticles;
			}else{
				numParticles[i] = 0;
			}
		}
		return numParticles;

	}
	
	/**
	 * Reads the filename of each spectrum from the '.set' file, finds that file, reads 
	 * the file's information, and creates the particle.
	 * 
	 */
	
	// ***SLH
	public void readSpectraAndCreateParticle() 
	throws IOException, NumberFormatException, InterruptedException, ParseException{
		//Read spectra & create particle.
		File canonical = parFile.getAbsoluteFile();
		File parent = canonical.getParentFile();
		File grandParent = parent.getParentFile();
		//System.out.println("Data set: " + parent.toString());
		final ATOFMSDataSetImporter thisref = this;

		//***SLH
		final String[] ATOFMS_tables = {"ATOFMSAtomInfoDense", "AtomMembership", "DataSetMembers", "ATOFMSAtomInfoSparse","InternalAtomOrder"};
		Database.Data_bulkBucket ATOFMS_buckets = ((Database)db).getDatabulkBucket(ATOFMS_tables) ;
		String name = parent.getName();
		name = parent.toString()+ File.separator + name + ".set";
		if (new File(name).isFile()) {
			
			progressBar.setMaximum(numParticles[collectionIndex]);
			progressBar.reset();

			int particleNum = 0;
			Collection destination = db.getCollection(id[0]);
			collections[collectionIndex] = destination;
			ATOFMSParticle currentParticle;
			Date d;
			DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			Scanner readSet = new Scanner(new File(name));
			readSet.useDelimiter("\r\n");

			StringTokenizer token;
			String particleFileName;
			//int doDisplay = 4;
			int nextID = db.getNextID();
			Collection curCollection = db.getCollection(id[0]);
			while (readSet.hasNextLine()) { // repeat until end of file.

				if (progressBar.wasTerminated()) {
					throw new InterruptedException();
				}

				token = new StringTokenizer(readSet.nextLine(), ",");
				// .set files are sometimes made with really strange line delims,
				// so we ignore empty lines.
				if (!token.hasMoreTokens()) {
					continue;
				}

				token.nextToken();
				String particleName = token.nextToken().replace('\\', File.separatorChar);
				particleFileName = grandParent.toString() + File.separator + particleName;

				for (int i = 0; i < 3; i++) {
					token.nextToken();
				}

				String time = token.nextToken();
				d = df.parse(time);

				try {
					ReadSpec read = new ReadSpec(particleFileName, d);

					currentParticle = read.getParticle();

					((Database) db).insertParticle(
							currentParticle.particleInfoDenseString(db.getDateFormat()),
							currentParticle.getSparsePeakMap().values(),
							destination, id[1], nextID, true);

					nextID++;
					particleNum++;
					if (particleNum > 0) {
						progressBar.setValue(particleNum);
						progressBar.setText(
								"Importing Particle # " + particleNum + " out of " + numParticles[collectionIndex]);

					}
				} catch (FileNotFoundException e) {
					ErrorLogger.writeExceptionToLogAndPrompt("Importing",
															 "Particle listed in .set file but file is missing.");

				}
			} //***SLH
			//Percolate new atoms upward
			db.propagateNewCollection(curCollection);
			readSet.close();
		} else {
			ErrorLogger.displayException(progressBar, 
					"Dataset has no hits because " +name+" does not exist.");
		}

	}

	// tests for .par version (.ams,.amz)
	// String[] returned is Name, Comment, and Description.
	public String[] parVersion() throws IOException, DataFormatException {
		//if(parFile==null)throw new FileNotFoundException();
		BufferedReader readPar = new BufferedReader(new FileReader(parFile));
		String test = readPar.readLine();
		String[] data = new String[3];
		if (test.equals("ATOFMS data set parameters")) {
			data[0] = readPar.readLine();
			data[1] = readPar.readLine();
			data[2] = "";
			while (readPar.ready()) {
				data[2] = data[2] + readPar.readLine() + " ";
			}
			readPar.close();
			return data;
		}
		else if (test.equals("[ATOFMS PARFile]")){
			StringTokenizer token = new StringTokenizer(readPar.readLine(),"=");
			token.nextToken();
			data[0] = token.nextToken();
			token = new StringTokenizer(readPar.readLine(), "=");
			token.nextToken();
			String time = token.nextToken();
			token = new StringTokenizer(readPar.readLine(), "=");
			token.nextToken();
			data[1] = token.nextToken() + " " + time;
			// Skip inlet type:
			readPar.readLine();
			token = new StringTokenizer(readPar.readLine(), "=");
			token.nextToken();
			//TODO:  This only takes the first line of comments.
			if (token.hasMoreTokens()) {
				data[2] = token.nextToken();
			}
			else {
				// no comment?  just add blank - jtbigwoo
				data[2] = "";
			}
			return data;
		}
		else {
			throw new DataFormatException
			("Corrupt data in " + parFile.toString() + " file.");
		}
	}
	
	private synchronized Integer getNextAtomID(){
		if(nextAtomID < 0)nextAtomID = db.getNextID();
		nextAtomID++;
		return nextAtomID -1;
	}
	public void setParentID(int parentID) {
		this.parentID = parentID;
	}

	public int getNumCollections() {
		return numCollections;
	}
}