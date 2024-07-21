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
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import javax.swing.SwingUtilities;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.*;
import edu.carleton.enchilada.gui.SPLATTableModel;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**Imports a SPLAT type data file into the database as
 *  ATOFMS data points. 
 * 
 * @author SPLAT importer modified by Michael Murphy 2014
 *
 */

public class SPLATDataSetImporter {
	private SPLATTableModel table;
	private Window mainFrame;
	private boolean parent;
	
	//Table values - used repeatedly.
	protected int rowCount;
	protected String datasetName = "";
	//protected massToCharge list, dense and sparse data strings/lists;
	protected ArrayList<Double> massToCharge = null;
	private String dense;
	private ArrayList<String> sparse;
	
	// Progress Bar variables
	public static final String TITLE = "Importing SPLAT Dataset";
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
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	//private final float SEC_PER_YEAR  = 31556926;
	
	private Scanner readData; // for reading the dataset;
	
	//the parent collection to import into
	private int parentID = 0;
		
	/**
	 * Constructor.  Sets the particle table for the importer.
	 * @param SPLATTableModel - particle table model.
	 */
	public SPLATDataSetImporter(SPLATTableModel t, Window mf, Database db) {
		table = t; //The gui table with multiple datasets
		mainFrame = mf;
		this.db = db;
	}
	
	public SPLATDataSetImporter(SPLATTableModel t, Window mf, Database db, ProgressBarWrapper pbar) {
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
				// Call relevant method to read SPLAT.txt
				processDataSet(i);

			} catch (DisplayException e) {
				throw new DisplayException(datasetName + " failed to import. Exception: " + e.toString());
			} catch (WriteException e) {
				throw new WriteException(datasetName + " failed to import.  Exception: " + e.toString());
			}
		}
	}
	
	/**
	 * Dumps data from particles in .txt as ATOFMS into database.
	 * 
	 * // NOTE: Datatype is already in the db.
	 * @author rzeszotj with credit to AMSimport author, modified by Michael Murphy 2014
	 */
	public void processDataSet(int index) throws DisplayException, WriteException {
		boolean skipFile = false;
		final String[] ATOFMS_tables = {"ATOFMSAtomInfoDense", "AtomMembership", 
				   "DataSetMembers", "ATOFMSAtomInfoSparse","InternalAtomOrder"};
		Database.BulkInserter bulkInserter = db.new BulkInserter(ATOFMS_tables);

		//get total number of particles for progress bar.
		progressBar.setIndeterminate(true);
		progressBar.setText("Finding number of items");
		try {
			readData = new Scanner(new File(datasetName));//.useDelimiter(" ");
		} catch (FileNotFoundException e1) {
			throw new WriteException(datasetName+" was not found.");
		}

		//Count # particles by counting lines after header
		int tParticles = 0;
		while (readData.hasNextLine()) {
			readData.nextLine();
			tParticles++;
		}
		readData.close();
		final int totalParticles = tParticles;
		int progressTextStep = tParticles/20;
		System.out.println("total items: " + totalParticles);

		//Generate m/z labels - SPLAT ranges from m/z=1 to m/z=450
		massToCharge = new ArrayList<Double>();
		double mzNum = 0;
		while (mzNum < 450) {
			mzNum += 1;
			massToCharge.add(mzNum);
		}

		//Create empty ATOFMS collection
		id = db.createEmptyCollectionAndDataset("ATOFMS",parentID,getName(),
				"SPLAT Import dummy ATOFMS",
				"'" + "SPLAT" + "','" + "SPLAT" + "'," +
						"0" + "," + "0"  + "," + "0" + ",0");

		progressBar.setMaximum((totalParticles/10)+1);
		progressBar.setIndeterminate(false);

		Collection destination = db.getCollection(id[0]);

        try {
            readData = new Scanner(new File(datasetName));
        } catch (FileNotFoundException e) {
            throw new ExceptionAdapter(e);
        }

        //Loop through particles in file
		particleNum = 0;
		int nextID = db.getNextID();
		while (readData.hasNext()) { // repeat until end of file.
			if(particleNum % 10 == 0 && particleNum >= 10) {
				String barText =
						"Importing Item # " + particleNum + " out of "
								+ totalParticles;
				progressBar.increment(barText);
			}
			if ((particleNum % progressTextStep) == 0)
				System.out.println("Processing particle #" + particleNum);

			read(particleNum, nextID); //READ IN PARTICLE DATA HERE

			//Only copy in particles with peaks
			if (sparse != null && !sparse.isEmpty()) {
				bulkInserter.queueInsertion(dense,sparse,
						destination,id[1],nextID);
				nextID++;
			}
			particleNum++;
		}

		progressBar.setIndeterminate(true);
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
	 * @author rzeszotj, modified by Michael Murphy 2014
	 */
	public void read(int particleNum, int nextID) {
		//Import dense data
		int acqNumber = readData.nextInt();
		acqNumber = 0;
		String fName = readData.next();
		String dateS = readData.next();
		double diameterV = Double.parseDouble(readData.next()) / 1000; // convert nm to um
		int totalArea = readData.nextInt();
		
		//Format date correctly	
		DateFormat dbDF = db.getDateFormat();
		ParsePosition p = new ParsePosition(0);
		Date currentDate = dateFormat.parse(dateS, p);
		String newD = dbDF.format(currentDate);
		
		//ATOFMSDense = Time, Laser Power, Size, Scatter Delay, File Name
		dense = (newD + ", 0.0, " + diameterV + ", " + acqNumber + "," + fName);
		
		//ATOFMSSparse = Location, Area, Relative Area, Height
		// should populate the area and relative area cells - how?
		sparse = new ArrayList<String>();
		List<Double> pkAreas = new ArrayList<Double>();
		
		int pkHeight;
		int pkArea;
		double relArea;
		
		for (int i = 0; i < massToCharge.size(); i++) {
			pkHeight = readData.nextInt();
			pkArea = pkHeight; // doubt width = 1 is accurate assumption?
			relArea = ((double) pkArea) / totalArea;
			if (pkHeight != 0.0)
			{
				sparse.add(massToCharge.get(i)+", "+pkArea+", "+relArea+", "+pkHeight);
//				pkAreas.add((double) pkArea); // for normalization
//				sumArea += pkArea;
			}
		}
		
		// populate RPAs
//		String temp;
		
//		for (int i = 0; i < sparse.size(); i++) {
//			relArea = (pkAreas.get(i) / sumArea);
//			temp = sparse.get(i).replace("$",String.valueOf(relArea));
//			sparse.set(i, temp);
//			//System.out.println(temp);
//		}
	}
	
	/**
	 * Removes the file extension and directory information 
	 * from datasetName and returns the collection name. 
	 * @return String
	 * @author rzeszotj
	 */
	public String getName(){
		String s = "SPLAT file";
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
		return "SPLAT data";
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
