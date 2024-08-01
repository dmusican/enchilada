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
 * The Original Code is EDAM Enchilada's ClusterK class.
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
 * Created on Aug 19, 2004
 *
 */
package edu.carleton.enchilada.analysis.clustering;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.CollectionDivider;
import edu.carleton.enchilada.analysis.PeakTransform;
import edu.carleton.enchilada.analysis.SubSampleCursor;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.NonZeroCursor;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.externalswing.SwingWorker;

/**
 * An intermediate class to implement if you are using an 
 * algorithm which produces a user specified number of clusters.  
 * Appends this number to the name of the spectrum and sets a 
 * variable(k) to this value.
 * 
 * @author other people
 * @author jtbigwoo
 * @version 2.0 October 15, 2009 - sped up farthest point initial centroids
 * at the expense of using more memory, added support for kmeans++ and random
 * initial centroids
 */
public abstract class ClusterK extends Cluster {

	public enum CentroidsApproach {
		REFINED_CENTROIDS,
		RANDOM_CENTROIDS,
		FARTHEST_DIST_CENTROIDS,
		KMEANS_PLUS_PLUS_CENTROIDS,
		USER_DEFINED_CENTROIDS
	}

	public static final int DEFAULT_RANDOM = 90125;
	/* Declared Class Variables */
	/**
	 * sets how we want to pick initial centroids
	 * REFINED_CENTROIDS to refine centroids, RANDOM_CENTROIDS for
	 * random centroids, FARTHEST_DIST_CENTROIDS for farthest distance.
	 */
	private CentroidsApproach initialCentroids; //
	private ArrayList<String> centroidFilenames;
	
	protected int k; // number of centroids desired.
	private int numParticles; // number of particles in the collection.
	private Random random;
	private boolean createCentroids = true;

	private static float error = 0.01f;
	private static int numSamples = 50;
	private static int randomNumber = DEFAULT_RANDOM;
	protected NonZeroCursor curs;
	private int returnThis;
	private JFrame parentContainer;
	private int curInt;
	private double difference;
	private ArrayList<BinnedPeakList> tempArray;
	
	private JDialog errorUpdate;
	private JLabel errorLabel;
	private JFrame container;
	
	//private static boolean stationary = false;
	
	//Testing time
	public static long timeTaken = 0;
	
	
	/**
	 * Constructor; calls the constructor of the Cluster class.
	 * @param cID - collection ID
	 * @param database - database interface
	 * @param k - number of centroids desired
	 * @param name - collection name
	 * @param comment - comment to insert
	 * 
	 */
	public ClusterK(int cID, Database database, int k,
					String name, String comment, CentroidsApproach initialCentroids, ClusterInformation c)
	{
		super(cID, database,name.concat(",K=" + k),comment, c.normalize);
		this.k = k;
		this.initialCentroids = initialCentroids;
		collectionID = cID;
		parameterString = name.concat(",K=" + k + super.folderName);
		totalDistancePerPass = new ArrayList<Double>();
		super.clusterInfo = c;//set inherited variable
	}
	
	/**
	* innerDivide() contains all the code that happens within 
	* the progress bar in divide().  It's seperated out for
	* ease of testing
	* @param interactive indicates whether we are in interactive
	* mode (normal mode) or testing mode
	* @author christej
	* 
	*/
	public int innerDivide(boolean interactive) {
		ArrayList<Centroid> centroidList = 
			new ArrayList<Centroid>();
		numParticles = db.getCollectionSize(collectionID);
		// If refineCentroids is true, randomize the db and cluster subsamples.
		if (initialCentroids == CentroidsApproach.REFINED_CENTROIDS) {
			centroidList = chooseRefinedCentroids(interactive);
		}
		else if (initialCentroids == CentroidsApproach.RANDOM_CENTROIDS) {
			centroidList = chooseRandomCentroids();
		}
		else if (initialCentroids == CentroidsApproach.KMEANS_PLUS_PLUS_CENTROIDS) {
			centroidList = chooseKmeansPPCentroids();
		}
		else if (initialCentroids == CentroidsApproach.USER_DEFINED_CENTROIDS) {
			centroidList = chooseUserDefinedCentroids();
		}
		
		if(interactive){
			try {
				SwingUtilities.invokeAndWait(new Runnable() {
					public void run() {
						updateErrorDialog("Clustering particles...");
					}
				});
			} catch (Exception e) {	e.printStackTrace(); }
		}
		
//		if (stationary) {
//			System.out.println("Holding centroids stationary");
//			if (initialCentroids == FARTHEST_DIST_CENTROIDS) { // would get called in processPart otherwise
//				centroidList = chooseFarthestDistanceCentroids();
//			}
//			System.out.println(centroidList);
//		} else {
//		}
		centroidList = processPart(centroidList, curs);

		returnThis =
			assignAtomsToNearestCentroid(centroidList, curs, createCentroids);
		curs.close();

		if(interactive){
			try {
				SwingUtilities.invokeAndWait(new Runnable() {
					public void run() {
						if(true){
						errorUpdate.setVisible(false);
						}
						errorUpdate = null;
					
					}
				});
			} catch (Exception e) {	e.printStackTrace(); }
		}		
		return returnThis;
	}
	
	
	/**
	 * Divide refines the centroids if needed and calls the clustering method.
	 * In the end, it finalizes the clusters by calling a method to report 
	 * the centroids.
	 * TODO:  The max number of subsamples clustered when we refine centroids is 
	 * 50.  We need a way to either validate this or a way to change it from the
	 * application.  
	 * 
	 * (non-Javadoc)
	 * @see analysis.CollectionDivider#divide()
	 */	
	public int divide() {
		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				int returnThis = innerDivide(true);	
				return returnThis;
			}
		};
		
		errorUpdate = new JDialog((JFrame)container,"Clustering",true);
		errorLabel = new JLabel("Clusters stabilize when change in error = 0");
		errorLabel.setSize(100,250);
		errorUpdate.add(errorLabel);
		errorUpdate.pack();
		errorUpdate.validate();
		// XXX: still a race condition!  Not a really bad one, though.  		System.out.println("HERE");
		worker.start();
		errorUpdate.setVisible(true);
		
		return returnThis;
	}
	
	public void updateErrorDialog(String str) {
		if (errorUpdate != null) {
		errorLabel.setText(str);
		errorUpdate.validate();
		}
	}
	
	/**
	 * Sets the cursor type; clustering can be done using either by 
	 * disk or by memory.
	 * 
	 * (non-Javadoc)
	 * @see analysis.CollectionDivider#setCursorType(int)
	 */
	
	// Added support for random subsampling -- MM 2015
	public boolean setCursorType(int type, double frac) 
	{

		switch (type) {
		case CollectionDivider.RANDOM_SUBSAMPLE :
			System.out.println("RANDOM_SUBSAMPLE");
			try {
				double sampleSize = db.getCollection(collectionID).getCollectionSize() * frac;
				// might wanna throw an exception here if sampleSize = 0
				curs = new NonZeroCursor(new SubSampleCursor(
						db.getRandomizedCursor(db.getCollection(collectionID)), 
						0, 
						(int)sampleSize));
				System.out.println("Sample size: "+(int)sampleSize);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return true;
		default :
			return false;
		}
	}
	
	// why weren't these methods defined in Cluster?
	public boolean setCursorType(int type) 
	{
		switch (type) {
		case CollectionDivider.DISK_BASED :
			System.out.println("DISK_BASED");
			try {
					curs = new NonZeroCursor(db.getBPLOnlyCursor(db.getCollection(collectionID)));
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		return true;
		case CollectionDivider.STORE_ON_FIRST_PASS : 
		    System.out.println("STORE_ON_FIRST_PASS");
			curs = new NonZeroCursor(db.getMemoryClusteringCursor(db.getCollection(collectionID), clusterInfo));
		return true;
		default :
			return false;
		}
	}

	private static class OutlierData implements Comparable<OutlierData> {
		private BinnedPeakList peakList;
		private double distance;
		public OutlierData(BinnedPeakList pl, double d) {
			peakList = pl;
			distance = d;
		}
		public int compareTo(OutlierData o) {
			if (distance > o.distance)
				return 1;
			else if (distance < o.distance)
				return -1;
			else
				return 0;
		}
	}

	/**
	 * ProcessPart is the method that does the actual clustering.  For K-Means and
	 * K-Medians, this is the exact same method.
	 * 
	 * @param centroidList - list of centroids - enter a null list on first pass.
	 * @param curs - cursor to loop through the particles in the db.
	 * @return the new list of centroids. Centroid list may be empty if operation is not doable.
	 */
	private ArrayList<Centroid> processPart(ArrayList<Centroid> centroidList,
											  NonZeroCursor curs) {
			
		long beginning = System.currentTimeMillis();
		
		boolean isStable = false;
		
		// Create an arrayList of particle peaklists for each centroid. 
		int arrayStartingSize = numParticles/k;
		ArrayList<ArrayList<Integer>> particlesInCentroids = 
			new ArrayList<ArrayList<Integer>>(k);
		for (int i = 0; i < k; i++) 
			particlesInCentroids.add(new ArrayList<Integer>(arrayStartingSize));
		
		
		// If there are the same number of particles as centroids, 
		// assign each particle to a centroid and you're done.
		if (numParticles == k) {
			// TODO:  numParticles here includes particles with zero peaks.  That could cause problems when assigning particles to centroids.
			// See bug #2788156
			centroidList.clear();
			for (int j = 0; j < k; j++) {
				// because the cursor is a non-zero cursor, you can run
				// out of particles before you get to K.  If that happens,
				// clear the centroid list and show an error.
				if (curs.next()) {
					centroidList.add(getCurrentParticleAsCentroid(curs));
				}
				else {
					ErrorLogger.writeExceptionToLogAndPrompt("KCluster","Not enough particles to cluster " +
							"with this many centroids. (You requested more" +
					"centroids than there are particles");
					centroidList.clear();
					return centroidList;
				}
			}
			curs.reset();
			return centroidList;
		}
		
		// If there are fewer centroids than particles, display an error. 
		if (k > numParticles) {
			ErrorLogger.writeExceptionToLogAndPrompt("KCluster","Not enough particles to cluster " +
					"with this many centroids. (You requested more" +
			"centroids than there are particles");
			return centroidList;
		}

		// If the centroid list contains some centroids, but not the right
		// amount, display an error.
		if (centroidList.size() > 0 && centroidList.size() != k) {
			ErrorLogger.writeExceptionToLogAndPrompt("KCluster","The system was able to" +
					" create some initial centroids, but" +
					" not the right amount of them.");
			return centroidList;
		}
		
		// If the centroidList has no centroids in it, choose k 
		// centroids. Only choose peaks where at least one particle
		// has a peak at that key.
		if (centroidList.size() == 0) {
			centroidList = chooseFarthestDistanceCentroids();
		}
		
		// Since TreeSet does not allow dups, insert distinct but
		// small initial values.
		TreeSet<OutlierData> outliers = new TreeSet<OutlierData>();
		
		//clear totalDistancePerPass array.
		totalDistancePerPass.clear(); 
		double accumDistance = 0.0;
		curs.reset();

		// Get centroid magnitudes for efficiency
		float[] centroidMags = new float[k];
		for (int i=0; i < k; i++) {
			centroidMags[i] = centroidList.get(i).peaks.getMagnitude(distanceMetric);
			//centroidList.get(i).peaks.printPeakList();
		}
		while (!isStable) {
			for (ArrayList<Integer> array : particlesInCentroids){
				array.clear();
			}

			outliers.clear();
			for (int i=0; i < k; i++)
				outliers.add(new OutlierData(null,i*Double.MIN_VALUE));
			double smallestOutlierDistance = Double.MIN_VALUE;
			// Make array to store sum of all the cluster centroids
			BinnedPeakList[] cumulativeCentroids = new BinnedPeakList[k];
			for (int i=0; i < k; i++)
				cumulativeCentroids[i] = new BinnedPeakList();
			
			ArrayList<float[]> tempCentroidList =
				Cluster.generateCentroidArrays(centroidList,Cluster.ARRAYOFFSET);
			
			int particleNumber = 0;
			BinnedPeakList thisBinnedPeakList;
			while(curs.next())
			{ // while there are particles remaining
				particleNumber++;
				if (particleNumber % 10000 == 0)
					System.out.println("Particle number = " + particleNumber);
				ParticleInfo p = curs.getCurrent();
				// safe to use original if not transforming
				if (peakTransform != PeakTransform.NONE) {
					thisBinnedPeakList = p.getBinnedList().copyOf();
					thisBinnedPeakList.transformAreas(peakTransform);
				} else {
					thisBinnedPeakList = p.getBinnedList();
				}
				thisBinnedPeakList.normalize(distanceMetric,posNegNorm);
				double nearestDistance = Double.MAX_VALUE;
				int nearestCentroid = -1;
				for (int curCent = 0; curCent < k; curCent++)
				{// for each centroid
					
					//first parameter was centroidList.get(curCent).peaks
					//change it back in order to use the original data structure
					//(instead of float arrays)
					//also, don't pass 4th argument (arrayoffset)
					// - benzaids
					double distance = thisBinnedPeakList.getDistance(
							tempCentroidList.get(curCent),centroidMags[curCent],
							distanceMetric, Cluster.ARRAYOFFSET);
					//If nearestDistance hasn't been set or is larger 
					//than found distance, set the nearestCentroid index.
					if (distance < nearestDistance){
						nearestCentroid = curCent;
						nearestDistance = distance;
					}
				}// end for each centroid
				
				// catch bad particles. think these are means of other solutions...? -- MM 2014
				if (nearestCentroid == -1) {
					//System.out.println("Bad AtomID: "+p.getID());
					//zeroPeakListParticleCount++;
					continue;
				}
				
				// TreeSets do not allow duplicates. Therefore, we make small
				// distinctions in the distances for the outliers that we add (if necessary).
				// Making the distance smaller here (not bigger) is crucial. This ensures that
				// if a whole series of atoms have the same distance, they do not keep bouncing
				// each other out.
				if (nearestDistance > smallestOutlierDistance) {
					OutlierData outlier = new OutlierData(thisBinnedPeakList,nearestDistance);
					while (outlier.distance > smallestOutlierDistance && outliers.contains(outlier))
						outlier.distance -= 1e-5;

					//	If distance is still an outlier, add it to the outlier array.
					if (nearestDistance > smallestOutlierDistance) {
						outliers.add(outlier);			
						if (outliers.size() > k)
							outliers.remove(outliers.first());						
						smallestOutlierDistance = outliers.first().distance;
					}
				}					
				
				// Put atomID assigned to curCent in particlesInCentroids array, and increment
				// appropriately.  
				particlesInCentroids.get(nearestCentroid).add(new Integer(
						p.getID()));
				centroidList.get(nearestCentroid).numMembers++;
				accumDistance += nearestDistance;
				cumulativeCentroids[nearestCentroid].addAnotherParticle(thisBinnedPeakList);

			}// end while there are particles remaining
			// IMPORTANT TO FIX: ZERO ISSUE
			zeroPeakListParticleCount = 0; //curs.getZeroCount();
			totalDistancePerPass.add(new Double(accumDistance));

			// reset centroid list.  The averageCluster method is overwritten
			// in K-Means and K-Medians.
			for (int i = 0; i < k; i++) {
				Centroid newCent;
				if (this instanceof KMeans) {
					// we have the sums - divide by the particle number to get mean.
					cumulativeCentroids[i].divideAreasBy(centroidList.get(i).numMembers);
					//Create and return a centroid with the new list and 0 members.
					// don't want to do pos/neg normalization here, we've already done
					// pos/neg normalization on the peaks for the particles
					cumulativeCentroids[i].normalize(distanceMetric);
					newCent = new Centroid(cumulativeCentroids[i],0);
				}
				else if (this instanceof KMedians) {
					newCent = averageCluster(centroidList.get(i),
							particlesInCentroids.get(i));
				}
				else
					throw new UnsupportedOperationException("Undefined clustering type.");
				centroidList.set(i, newCent);
			}
			//accumDistance:
			accumDistance = 0.0;
			// cursor:
			curs.reset();
			

			if (outliers.last().distance < 1E-4) {
				System.out.println("Particles are perfectly clustered!");
				return centroidList;
			}

			// If there is one (or more) empty centroids, replace them 
			ArrayList<Integer> emptyCentIndex = new ArrayList<Integer>();
			isStable = stableCentroids(totalDistancePerPass);
			for (int i = 0; i < k; i++) {
				if (particlesInCentroids.get(i).size() == 0) {
					OutlierData outlier = outliers.last();
					centroidList.set(i,new Centroid(outlier.peakList,0));
					outliers.remove(outlier);
					isStable = false;
				}
			}
		} // end while loop
			
		// Remove the last pass in the total distance array,
		// since these are duplicates.
		//totalDistancePerPass.remove(totalDistancePerPass.size()-1);
		//totalDistancePerPass.remove(totalDistancePerPass.size()-1);
		

		// IMPORTANT: FIX ZERO COUNT
		//System.out.println("Zero count = " + curs.getZeroCount());
		
		//Timing stuff
        System.out.println("Time taken for getDistance (ms): " + BinnedPeakList.distTime);
        System.out.println("Time taken for other getDistance (ms): " + BinnedPeakList.dist2Time);
        System.out.println("Total time taken for getDistance methods (ms): " + (BinnedPeakList.dist2Time + BinnedPeakList.distTime));
		
        timeTaken += System.currentTimeMillis() - beginning;
		System.out.println("Time taken for processPart (clustering): " + timeTaken);
        
		return centroidList;
	}
	
	/**
	 * Determines whether the centroids are stable or not.  It does this by
	 * determining how much the centroids moved on the last pass and comparing
	 * it to a pre-determined error.
	 * 
	 * @param totDist - total distance array
	 * @param error - pre-determined error
	 * @return - true if stable, false otherwise.
	 */
	public boolean stableCentroids(ArrayList<Double> totDist) {
		if (totDist.size() == 1)
			return false;
		int lastIndex = totDist.size() - 1;
		difference = 
			totDist.get(lastIndex-1).doubleValue() - 
			totDist.get(lastIndex).doubleValue();
		try {
			SwingUtilities.invokeAndWait(new Runnable() {
				public void run() {
					updateErrorDialog("Change in error = " + (difference));
				}
			});
		} catch (Exception e) {	e.printStackTrace(); }
		//difference = Math.abs(difference);
		System.out.println("Error: " + totDist.get(lastIndex).doubleValue());
		System.out.println("Change in error: " + difference);
		System.out.flush();
		assert (difference >= -1f) : "increased error!";
		if (difference > error) 
			return false;
		return true;
	}

	/**
	 * This method splits the data into 50 sub-samples and then clusters each 
	 * sub-sample to get fifty sets of k centroids.  Then it clusters all the 
	 * centroids 50 times using each set of centroids as the starting
	 * centroids.  The set of initial centroids that result in the lowest error
	 * in this second clustering is the one we return to use for the real 
	 * clustering.
	 * @param interactive
	 * @return the best set of centroids we could find
	 */
	private ArrayList<Centroid> chooseRefinedCentroids(boolean interactive) {
		ArrayList<Centroid> centroidList = 
			new ArrayList<Centroid>();
		int sampleSize;
		if (numSamples*4 > numParticles) 
			sampleSize = numParticles/(numSamples*2);
		else 
			sampleSize = numParticles/numSamples - 1;
		db.seedRandom(randomNumber);
		CollectionCursor randCurs = 
			db.getRandomizedCursor(db.getCollection(collectionID));
		NonZeroCursor partCurs = null;
		System.out.println("clustering subSamples:");
		System.out.println("number of samples: " + numSamples);
		System.out.println("sample size: " + sampleSize);
		ArrayList<ArrayList<Centroid>> allCentroidLists =
		    new ArrayList<ArrayList<Centroid>>(numSamples);
		sampleIters = 0;
		for(int i = 0; i < numSamples; i++)
		{
			curInt = i+1;
			
			if(interactive){
				try {
					SwingUtilities.invokeAndWait(new Runnable() {
						public void run() {
							updateErrorDialog("Clustering subsample #" + (curInt));
						}
					});
				} catch (Exception e) {	e.printStackTrace(); }
				partCurs = new NonZeroCursor(new SubSampleCursor(
						randCurs, 
						i*sampleSize, 
						sampleSize));
			}
			
			allCentroidLists.add(processPart(new ArrayList<Centroid>(),
			        				partCurs));
			centroidList.addAll(allCentroidLists.get(i));

		
			sampleIters += totalDistancePerPass.size();
		}
		
		// Of the various centroids that are found, try clustering all
		// the centroids together using each set of centroids as a starting
		// point. For each of the centroids that result, choose those
		// that result in the least error.
		System.out.println("clustering Centroids:");
		double bestDistance = Double.POSITIVE_INFINITY;
		ArrayList<Centroid> bestStartingCentroids = null;
		int bestIndex = -1;
		clusterCentroidIters = 0;
		for (int i=0; i < numSamples; i++) {
			curInt = i+1;
			if(interactive){
				try {
					SwingUtilities.invokeAndWait(new Runnable() {
						public void run() {
							updateErrorDialog("Clustering centroid #" + (curInt));
						}
					});
				} catch (Exception e) {	e.printStackTrace(); }
			}

			ArrayList<Centroid> centroids = 
				processPart(allCentroidLists.get(i),
						new NonZeroCursor(
								new CentroidListCursor(centroidList)));
			double distance = (totalDistancePerPass.
		              get(totalDistancePerPass.size()-1).doubleValue());
			clusterCentroidIters += totalDistancePerPass.size();
		    if (distance < bestDistance) {
		        bestDistance = distance;
		        bestStartingCentroids = centroids;
		        bestIndex = i;
		    }
		}
		System.out.println("Centroid clustering iterations: " +
		        clusterCentroidIters);
		centroidList = processPart(bestStartingCentroids,
				new NonZeroCursor(new CentroidListCursor(centroidList)));
		partCurs.close();
		randCurs.close();
		return centroidList;
	}

	/**
	 * Chooses k particles at random from the collection identified by
	 * collectionID.
	 * @return k centroids derived from random particles from the collection
	 */
	private ArrayList<Centroid> chooseRandomCentroids() {
		// jtbigwoo
		ArrayList<Centroid> centroidList = new ArrayList<Centroid>();
		db.seedRandom(randomNumber);
		CollectionCursor randCurs = 
			db.getRandomizedCursor(db.getCollection(collectionID));
		NonZeroCursor partCurs = new NonZeroCursor(randCurs);
		for (int i = 0; i < k; i++) {
			partCurs.next();
			centroidList.add(getCurrentParticleAsCentroid(partCurs));
		}
		partCurs.close();
		return centroidList;
	}

	/**
	 * Chooses centroids from the collection identified by collectionID using
	 * the Kmeans++ algorithm.  It picks the first centroid as a random point
	 * in the collection.  It chooses the next particles randomly, but giving
	 * more weight to particles farther away from the particles we've already 
	 * selected.
	 * @return k centroids derived from a sort-of random selection of particles
	 */
	private ArrayList<Centroid> chooseKmeansPPCentroids() {
		//jtbigwoo
		ArrayList<Centroid> centroidList = new ArrayList<Centroid>();
		ArrayList<Double> distances = new ArrayList<Double>();
		int cursIndex;
		double totalDistanceThisPass, randomIndex;
		db.seedRandom(randomNumber);
		CollectionCursor randCurs = 
			db.getRandomizedCursor(db.getCollection(collectionID));
		NonZeroCursor partCurs = new NonZeroCursor(randCurs);
		partCurs.next();
		Centroid newCent = getCurrentParticleAsCentroid(partCurs);
		centroidList.add(newCent);
		
		partCurs.close();
		
		random = new Random(43291);
		for (int i = 1; i < k; i++) {
			curs.reset();
			cursIndex = 0;
			totalDistanceThisPass = 0f;

			// build a list of distances from centroids
			// for each particle, we want the distance to its closest centroid
			BinnedPeakList thisBinnedPeakList;
			while (curs.next()) {
				ParticleInfo p = curs.getCurrent();
				// safe to use original if not transforming
				if (peakTransform != PeakTransform.NONE) {
					thisBinnedPeakList = p.getBinnedList().copyOf();
					thisBinnedPeakList.transformAreas(peakTransform);
				} else {
					thisBinnedPeakList = p.getBinnedList();
				}
				thisBinnedPeakList.normalize(distanceMetric,posNegNorm);
				double distance = newCent.peaks.getDistance(thisBinnedPeakList, distanceMetric);
				if (distances.size() >= cursIndex) { 
					// if we don't have a distance for this one, add it
					distances.add(new Double(distance));
				}
				else if (distance < distances.get(cursIndex)) {
					// if the current distance is smaller than the last, add it
					distances.set(cursIndex, new Double(distance));
				}
				totalDistanceThisPass += distances.get(cursIndex);
				cursIndex++;
			}
			
			// get a random number  
			randomIndex = random.nextDouble() * totalDistanceThisPass;
			curs.reset();
			cursIndex = 0;
			while (curs.next()) {
				if (randomIndex <= distances.get(cursIndex) && distances.get(cursIndex) != 0d) {
					newCent = getCurrentParticleAsCentroid(curs);
					centroidList.add(newCent);
					break;
				}
				else {
					randomIndex -= distances.get(cursIndex);
				}
				cursIndex++;
			}
		}
		return centroidList;
	}

	/**
	 * A set of centroids for the given cursor by taking the first particle
	 * and then for each succeeding point, take the one that is furthest away 
	 * from the closest of the points chosen so far.
	 * The centroids will be generated out of the cursor curs.  The number of
	 * centroids we'll generate is k.
	 * @return
	 */
	private ArrayList<Centroid> chooseFarthestDistanceCentroids()
	{
		ArrayList<Centroid> centroidList = new ArrayList<Centroid>(k);
		ArrayList<Double> distances = new ArrayList<Double>(numParticles);
		int cursIndex;
		Centroid newCent;
	    // Take the first point as the first centroid. For each succeeding
	    // point, take the one that is furthest away from the closest
	    // of the centroids chosen so far.
		
		if (randomNumber == DEFAULT_RANDOM) {
			// for backward compatibility, the default should be the first.  we used to ignore the random seed--jtbigwoo
			curs.reset();
		    boolean status = curs.next();
		    assert status : "Cursor is empty.";
		    newCent = getCurrentParticleAsCentroid(curs);
		}
		else {
			db.seedRandom(randomNumber);
			try (CollectionCursor randCurs =
				db.getRandomizedCursor(db.getCollection(collectionID));
				 NonZeroCursor partCurs = new NonZeroCursor(randCurs)) {
				partCurs.next();
				newCent = getCurrentParticleAsCentroid(partCurs);
			}
		}

		assert (newCent != null) : "Error adding centroid";
	    assert (newCent.peaks != null) : "New centroid has no peaklist!";
	    centroidList.add(newCent);
	    for (int i=1; i < k; i++) {
			curs.reset();
			cursIndex = 0;
			BinnedPeakList furthestPeakList = null;
			BinnedPeakList thisBinnedPeakList;
			double furthestGlobalDistance = (double) 0;
	        while (curs.next()) {
				ParticleInfo p = curs.getCurrent();
				// safe to use original if not transforming
				if (peakTransform != PeakTransform.NONE) {
					thisBinnedPeakList = p.getBinnedList().copyOf();
					thisBinnedPeakList.transformAreas(peakTransform);
				} else {
					thisBinnedPeakList = p.getBinnedList();
				}
				thisBinnedPeakList.normalize(distanceMetric,posNegNorm);
				//thisBinnedPeakList.printPeakList();
				//System.out.println("***");
				
				double distance = newCent.peaks.getDistance(thisBinnedPeakList, distanceMetric);
				if (distances.size() <= cursIndex) { 
					// if we don't have a distance for this one, add it
					distances.add(new Double(distance));
				}
				else if (distance < distances.get(cursIndex)) {
					// if the current distance is smaller than the last, add it
					distances.set(cursIndex, new Double(distance));
				}
				//System.out.println("nearestDist: " + nearestDistance);
				if (distances.get(cursIndex) >= furthestGlobalDistance) {
				    furthestGlobalDistance = distances.get(cursIndex);
				    furthestPeakList = thisBinnedPeakList;
				}
				cursIndex++;
	        } // end while curs.next()
	        if (furthestPeakList == null) {
	        	// XXX this would be better as a dialog box, we should
	        	// do something about that.
	        	throw new RuntimeException("No furthest particle: probably "
	        		+"ran out of particles to make into centroids!");
	        }
	        newCent = new Centroid(furthestPeakList,0);
	        centroidList.add(newCent);
	    } // for i:1 to k    
	    return centroidList;
	}
	
	// Use initial centroids provided by user; largely transplanted from ClusterQuery
	// File format appears to be header first line, then: peakLocation, peakArea on each line
	// Michael Murphy 2014
	
	private ArrayList<Centroid> chooseUserDefinedCentroids() {
		Scanner scanner;
		BinnedPeakList peakList;
		String[] peak;
		Centroid centroid;
		ArrayList<Centroid> centroidList = new ArrayList<Centroid>(k);
		for (int i = 0; i < centroidFilenames.size(); i++){
			try{
				System.out.println();
				System.out.println("Centroid "+(i+1));
				scanner = new Scanner(new File(centroidFilenames.get(i)));
				peakList = new BinnedPeakList();
				String header = scanner.nextLine();
				while(scanner.hasNextLine()){
					peak = scanner.nextLine().split(",");
					// hack to skip over labels
					try {
						Integer.parseInt(peak[0]);
					} catch (NumberFormatException e) {
						continue;
					}
					if(peak.length>1 && Double.parseDouble(peak[1])>0) { // omit empty peaks
						peakList.add(Float.valueOf(peak[0]), Float.valueOf(peak[1]));
						for(int j=0;j<peak.length;j++){
							System.out.print(peak[j]+" ");
						}
						System.out.println();
					}
				}
				// peak-area transforms -- Michael Murphy 2014
				peakList.transformAreas(peakTransform);
				peakList.normalize(distanceMetric,posNegNorm);
				centroidList.add(new Centroid(peakList, 0));
				scanner.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		return centroidList;
	}

	/**
	 * Uses the particle identified by getCurrent from the supplied cursor to
	 * make a Centroid.
	 * @param curs 
	 * @return
	 */
	private Centroid getCurrentParticleAsCentroid(CollectionCursor curs) {
		ParticleInfo p = curs.getCurrent();
		BinnedPeakList thisBinnedPeakList = p.getBinnedList().copyOf();
		thisBinnedPeakList.preProcess(power);
		// peak-area transforms -- Michael Murphy 2014
		thisBinnedPeakList.transformAreas(peakTransform);
		thisBinnedPeakList.normalize(distanceMetric,posNegNorm);
	    return new Centroid(thisBinnedPeakList,0);
	}

	/**
	 * The following four get and set methods are used in the Advanced dialog box
	 * for the user to input specifications.
	 */
	
	public static float getError() {
		return error;
	}
	
	public static int getNumSamples() {
		return numSamples;
	}
	
	public static void setError(float err) {
		error = err;
	}
	
	public static void setNumSamples(int num) {
		numSamples = num;
	}
	
	/**
	 * Set this to false if you want to cluster without creating centroids 
	 * in the database.  Useful if you're only interested in 
	 * saving the clustered particles in the db.  (i.e. when pre-clustering for 
	 * hierarchical clustering.)
	 * @param move
	 */
	public void setCreateCentroids(boolean create) {
		createCentroids = create;
	}
	
	/**
	 * Sets the seed for the random number generator so we can generate 
	 * different initial centroids
	 * @param randomSeed
	 */
	public static void setRandomSeed(int randomSeed) {
		randomNumber = randomSeed;
	}
	
//	public static void setStationary(boolean stn) {
//		stationary = stn;
//	}
	
	/**
	 * Abstract method averageCluster that is overwritten in the children classes.
	 * 
	 * @param thisCentroid - centroid to average
	 * @param particlesInCentroid - list of atomIDs for this centroid
	 * @param curs - the cursor; used to get binned peak lists, not for looping 
	 * through particles.
	 * @return - a new centroid.
	 */
	public abstract Centroid averageCluster(
			Centroid thisCentroid,
			ArrayList<Integer> particlesInCentroid);

	// method to load initial centroids from file -- MM 2014
	// transplanted from ClusterQuery
	public void setCentroidFilenames(ArrayList<String> filenames) {
		this.centroidFilenames = filenames;
	}
}
