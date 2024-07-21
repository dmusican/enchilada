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

import java.io.File;
import java.io.IOException;

import edu.carleton.enchilada.ATOFMS.*;

import java.awt.Window;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;

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
public class FlatFileATOFMSDataSetImporter {
	
	private Window parent;
	
	//Table values - used repeatedly.
	private String name = "";
	private int height, area;
	private float relArea;
	private float peakError;
	
	// Progress Bar variables
	protected ProgressBarWrapper progressBar;
	public static final String title = "Importing ATOFMS DataSet";
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	/* '.par' file */
	protected File spassFile;
	protected String spassFilename;
	
	/* contains the collectionID and particleID */
	protected int[] id;
	protected ArrayList<Integer> mz;
	int[] posSpec;
	int[] negSpec;
	protected Integer nextAtomID;
	protected Integer particleNumber;
	protected Collection collection;
	private final FlatFileATOFMSDataSetImporter thisRef;
	/* Database object */
	Database db;
	
	/* Lock to make sure database is only accessed in one batch at a time */
	//private static Integer dbLock = new Integer(0);
	
	/* the parent collection */
	private int parentID = 0;

	

	
	public FlatFileATOFMSDataSetImporter(Window mf, Database db,  ProgressBarWrapper progressBar) {
		parent = mf;
		this.progressBar = progressBar;
		nextAtomID = new Integer(-1);
		particleNumber = 0;
		thisRef = this;
		this.db = db;
	}
	
	

	
	/**
	 * Loops through each row, collects the information, and processes the
	 * datasets row by row.
	 * @throws InterruptedException 
	 * @throws DisplayException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public void processFile(String filename) throws InterruptedException, DisplayException{
		spassFilename = filename;
		spassFile = new File(filename);
		//Loops through each dataset and creates each collection.
			if(progressBar.wasTerminated()){
				throw new InterruptedException();
			}
			try {
				
				createNewCollection();
					readFileAndCreateParticles();
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new DisplayException("message");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new DisplayException("message");
				}
				
			
				particleNumber++;
		
	}



	/**
	 * Creates the parent collection.
	 *
	 */
	public void createNewCollection()  {
		
		
		//if datasets are imported into a parent collection
		//pass parent's id in as second parameter, else parentID is root (0)
//		if (ipd.parentExists())
//			parentID = ipd.getParentID();
		//begin a transaction
		
		id = db.createEmptyCollectionAndDataset("ATOFMS",parentID,spassFilename,"comment","'" + ".par" + "', '" + ".noz" + "', " +
				10 + ", " + 
				20  + ", " + 
				0.005 + ", " + 
				1);
		
	}
	
	public int getNumParticles() throws IOException{
		if(!spassFile.isFile())return 0;
		Scanner input = new Scanner(spassFile);
		
		int numParticles = 0;
		while(input.hasNext()){
			numParticles++;
			input.nextLine();
		}
		return numParticles;

	}
	
	/**
	 * Reads the filename of each spectrum from the '.set' file, finds that file, reads 
	 * the file's information, and creates the particle.
	 * @throws DisplayException 
	 * 
	 */
	
	// ***SLH
	public void readFileAndCreateParticles() 
	throws IOException, NumberFormatException, InterruptedException, DisplayException{
		//Read spectra & create particle.
		
		//***SLH
		final String[] ATOFMS_tables = {"ATOFMSAtomInfoDense", "AtomMembership", "DataSetMembers", "ATOFMSAtomInfoSparse","InternalAtomOrder"};
		Database.BulkInserter bulkInserter = db.new BulkInserter(ATOFMS_tables);

		int numParticles = this.getNumParticles();
		if (spassFile.isFile()) {

			progressBar.setMaximum(numParticles);
			progressBar.reset();

			int particleNum = 0;
			collection = db.getCollection(id[0]);
			FlatFileATOFMSParticle currentParticle;
			Scanner readSet = new Scanner(spassFile);
			readSet.useDelimiter("\r\n");
			String header = readSet.nextLine();
			setParameters(header);


			StringTokenizer token;
			String particleFileName;
			//int doDisplay = 4;
			int nextID = db.getNextID();
			Collection curCollection = db.getCollection(id[0]);
			while (readSet.hasNextLine()) { // repeat until end of file.

				if (progressBar.wasTerminated()) {
					throw new InterruptedException();
				}
				String line = readSet.nextLine();

				currentParticle = this.getParticle(line);
				bulkInserter.queueInsertion(
						currentParticle.particleInfoDenseStr(db.getDateFormat()),
						currentParticle.particleInfoSparseString(),
						collection, id[1], nextID);

				nextID++;
				particleNum++;
				if (particleNum % 10 == 0 && particleNum > 0) {
					progressBar.setValue(particleNum);
					progressBar.setText("Importing Particle # " + particleNum + " out of " + numParticles);

				}
			} //***SLH
			bulkInserter.executeBatch();
			//Percolate new atoms upward
			db.propagateNewCollection(curCollection);
			readSet.close();

		} else {
			ErrorLogger.displayException(progressBar,
					"Dataset has no hits because " + name + " does not exist.");
		}
	}
	
	private void setParameters(String header) throws DisplayException {
		Scanner scan = new Scanner(header);
		scan.useDelimiter("\t");
		
		scan.next();
		scan.next();
		scan.next();
		scan.next();
		mz = new ArrayList<Integer>();
		while(scan.hasNextInt()){
			mz.add(scan.nextInt());
		}
		if(scan.hasNext()){
			throw new DisplayException("Error processing the file header.  MZ values must be integers.  Check an example file.");
		}
		int numPos = 0;
		int numNeg = 0;
		if(mz.get(0)<0){
			numNeg = -1*mz.get(0);
		}
		if(mz.get(mz.size()-1)>0){
			numPos = mz.get(mz.size()-1);
		}
		//System.out.println(numPos+"positive, "+numNeg + " negative: "+mz.toString());
		posSpec = new int[numPos+1];
		negSpec = new int[numNeg+1];
		
		
	}

	private FlatFileATOFMSParticle getParticle(String line) throws DisplayException {
		// TODO Auto-generated method stub
		Scanner scan = new Scanner(line);
		scan.useDelimiter("\t");
		
		String fileName = scan.next();
		String dateString = scan.next();
		
		Integer acquisitionNumber = scan.nextInt();
		Date date = null;
		try {
			date = dateFormat.parse(dateString);
		} catch (ParseException e) {
			throw new DisplayException("Error importing particle number " + acquisitionNumber+
					": error parsing the date.");
		}
		Double diameter = scan.nextDouble();
		Arrays.fill(posSpec, 0);
		Arrays.fill(negSpec, 0);
		int index = -1;
		while(scan.hasNextInt()){
			index++;
			if(index>=mz.size()){
				throw new DisplayException("Error importing particle number " + acquisitionNumber+
						": row contains more columns than the file header.");
			}
			int mzVal = mz.get(index);
			int newVal = scan.nextInt(); 
			if(mzVal<0){
				negSpec[-1*mzVal] = newVal; 
			}else{
				posSpec[mzVal] = newVal;
			}
			
		}
		if(index < mz.size()-1){
			throw new DisplayException("Error importing particle number " + acquisitionNumber+"" +
			": row contains fewer columns than the file header.");
		}
		if(scan.hasNext()){
			throw new DisplayException("Error importing particle number " + acquisitionNumber+
					": incorrect file format.");
		}
		FlatFileATOFMSParticle newParticle = new FlatFileATOFMSParticle(fileName, diameter, date, posSpec, negSpec);
		return newParticle;
	}

	
	
	private synchronized Integer getNextAtomID(){
		if(nextAtomID < 0)nextAtomID = db.getNextID();
		nextAtomID++;
		return nextAtomID -1;
	}
	public void setParentID(int parentID) {
		this.parentID = parentID;
	}
}