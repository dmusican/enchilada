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
 * The Original Code is EDAM Enchilada's Clusters class.
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
 * Tom Bigwood tom.bigwood@nevelex.com
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

package edu.carleton.enchilada.analysis.clustering;

import java.sql.SQLException;
import java.util.*;

import javax.swing.JFrame;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.CollectionDivider;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.SubSampleCursor;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.NonZeroCursor;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.externalswing.SwingWorker;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

/**
 * @author jtbigwoo
 * @version 1.0 April 23, 2009 Yesterday was the 15th anniversary of Nixon's death.
 */

public class ClusterHierarchical extends Cluster {

	private int numInitialClusters; // number of particles in the collection.
	private int returnThis;

	private ProgressBarWrapper progressBar;
	private JFrame container;
	private int numCollections = 0;

	private boolean preClustered = false;
	private int cursorType = CollectionDivider.DISK_BASED;
	
	/**
	 * Constructor.  Calls the constructor for ClusterK.
	 * @param cID - collection ID
	 * @param database - database interface
	 * @param k - number of centroids desired
	 * @param name - collection name
	 * @param comment -comment to enter
	 * @param mainFrame - the parent frame so we can create a progress bar
	 */
	public ClusterHierarchical(int cID, Database database,
							   String name, String comment, ClusterInformation c, JFrame mainFrame)
	{
		super(cID, database, name.concat("Hierarchical, Clusters Ward's"), comment, c.normalize);
		collectionID = cID;
		clusterInfo = c;//set inherited variable
		totalDistancePerPass = new ArrayList<Double>();
		parameterString = name.concat("Hierarchical, Clusters Ward's " + super.folderName);
		container = mainFrame;
	}
	
	/** 
	 * method necessary to extend from Cluster.  Begins the clustering
	 * process.
	 * @param - interactive or testing mode
	 * @return - new collection id.
	 */
	public int cluster(boolean interactive) {
		if(interactive)
			return divide();
		else
			return innerDivide(interactive);
	}

	/**
	 * Calls the clustering method.
	 * In the end, it finalizes the clusters by calling a method to report 
	 * the centroids.
	 * @see analysis.CollectionDivider#divide()
	 */
	public int divide() {
		if (preClustered) {
			numInitialClusters = db.getImmediateSubCollections(collection).size();
		}
		else {
			numInitialClusters = db.getCollectionSize(collectionID);
		}
	    progressBar = new ProgressBarWrapper(container, "Hierarchical Clustering", numInitialClusters);
		progressBar.constructThis();
		progressBar.setIndeterminate(true);

		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				int returnThis = innerDivide(true);	
				return returnThis;
			}
		};

		worker.start();

		return returnThis;
	}

	public int innerDivide(boolean interactive) {
		long timeInMillis = new Date().getTime();
		processPart(interactive);
		returnThis = newHostID;
		System.out.println("Milliseconds elapsed: " + (new Date().getTime() - timeInMillis));

		if(interactive){
			progressBar.disposeThis();
		}		
		return returnThis;
	}

	private void processPart(boolean interactive)
	{
		ArrayList<ClusterPair> clusterPairs = new ArrayList<ClusterPair>((numInitialClusters * numInitialClusters) / 2);
		HashMap<Integer, ClusterContents> clusterContentsMap = new HashMap<Integer, ClusterContents>((numInitialClusters * 4) / 2);
		sampleIters = 0; // this is the number of passes
		clusterCentroidIters = 0; // also the number of passes

		if (interactive) {
			progressBar.setText("Building distance matrix");
			progressBar.setMaximum((numInitialClusters * numInitialClusters)/2);
			progressBar.setIndeterminate(false);
		}

		// set up distance matrix
		if (preClustered) {
			// set up a ClusterContents for each collection
			ArrayList<Integer> subCollectionIDs = db.getImmediateSubCollections(collection);
			for (int subCollectionID : subCollectionIDs) {
				ClusterContents currentCluster = new ClusterContents(subCollectionID);
				for (ClusterContents otherCluster : clusterContentsMap.values()) {
					float distance = currentCluster.getDistance(otherCluster, distanceMetric);
					clusterPairs.add(new ClusterPair(currentCluster.clusterCollectionID, otherCluster.clusterCollectionID, distance));
					if (interactive) {
						progressBar.increment("Building Distance Matrix");
					}
				}
				clusterContentsMap.put(currentCluster.clusterCollectionID, currentCluster);
			}
		}
		else {
			// set up a ClusterContents for each particle
			curs = getCursor(collectionID);
			while (curs.next()) {
				ParticleInfo info = curs.getCurrent();
				info.getBinnedList().posNegNormalize(distanceMetric);
				ClusterContents currentCluster = new ClusterContents(info);
				// after we've constructed our cluster object, get the distances
				// to all other cluster objects
				for (ClusterContents otherCluster : clusterContentsMap.values()) {
					float distance = currentCluster.getDistance(otherCluster, distanceMetric);
					clusterPairs.add(new ClusterPair(currentCluster.clusterCollectionID, otherCluster.clusterCollectionID, distance));
					if (interactive) {
						progressBar.increment("Building Distance Matrix");
					}
				}
				// this next bit is silly, but by waiting 1/10 of a second, we give the garbage collector time to run
				// the whole process runs faster by waiting because we don't take up so much memory.
				// in an ideal future, we could the next two lines out -- jtbigwoo
//				Runtime.getRuntime().gc();
//				try {Thread.sleep(100);} catch (Exception e) {}
//				System.out.println(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
				clusterContentsMap.put(currentCluster.clusterCollectionID, currentCluster);
			}
		}
		
		if (interactive) {
			System.err.println("done!");
			progressBar.setMaximum(clusterContentsMap.size());
			progressBar.reset();
			progressBar.setText("Number of Clusters Remaining: " + clusterContentsMap.size());
		}
		while (clusterContentsMap.size() > 1)
		{
			// the first pair element in the sorted list has the smallest distance, merge them
			// name the clusters A and B.  Cluster B will be merged into Cluster A and removed.
			// Sorry, cluster B.
			Collections.sort(clusterPairs);
			int clusterAID = clusterPairs.get(0).getFirstClusterID();
			int clusterBID = clusterPairs.get(0).getSecondClusterID();
			totalDistancePerPass.add(new Double(clusterPairs.get(0).getDistance()));
			float aToBDistance = clusterPairs.get(0).getDistance();
			int clusterASize = clusterContentsMap.get(clusterAID).getAtomIDList().size();
			int clusterBSize = clusterContentsMap.get(clusterBID).getAtomIDList().size();
			clusterPairs.remove(0);
			clusterContentsMap.get(clusterAID).merge(clusterContentsMap.get(clusterBID));
			clusterContentsMap.remove(clusterBID);

			// set the size to try to avoid rehashes
			HashMap<Integer, ClusterPair> removedPairs = new HashMap<Integer, ClusterPair>((clusterContentsMap.size() * 4) / 3);
			// because we're going to be removing elements, 
			// it's easier to go backwards through the list
			for (int i = clusterPairs.size() - 1; i >= 0; i--) {
				// We're trying to get the distance from the new cluster to each 
				// existing cluster.  We'll call the existing clusters Cluster Q.
				int clusterQID = -1;
				ClusterPair currentPair = clusterPairs.get(i);
				if (clusterAID == currentPair.getFirstClusterID() || clusterBID == currentPair.getFirstClusterID()) {
					clusterQID = currentPair.getSecondClusterID();
				}
				else if (clusterAID == currentPair.getSecondClusterID() || clusterBID == currentPair.getSecondClusterID()) {
					clusterQID = currentPair.getFirstClusterID();
				}
				if (clusterQID != -1) {
					if (removedPairs.get(clusterQID) == null) {
						// if this is the first time we've encountered Cluster Q, delete the pair
						// from the list, but save it for calculations below.
						removedPairs.put(clusterQID, currentPair);
						clusterPairs.remove(i);
					}
					else {
						// if this is the second time we've encountered Cluster Q, recalculate
						// distance and put a new pair in the list.
						ClusterPair removedPair = removedPairs.get(clusterQID);
						int clusterQSize = clusterContentsMap.get(clusterQID).getAtomIDList().size();
						float aToQDistance, bToQDistance, newDistance;
						if (clusterAID == removedPair.getFirstClusterID() || clusterAID == removedPair.getSecondClusterID()) {
							aToQDistance = removedPair.getDistance();
							bToQDistance = currentPair.getDistance();
						}
						else
						{
							bToQDistance = removedPair.getDistance();
							aToQDistance = currentPair.getDistance();
						}
						float distance = ((clusterASize + clusterQSize) * aToQDistance) / (clusterASize + clusterBSize + clusterQSize) + 
							((clusterBSize + clusterQSize) * bToQDistance) / (clusterASize + clusterBSize + clusterQSize) - 
							((clusterQSize) * aToBDistance) / (clusterASize + clusterBSize + clusterQSize);
						clusterPairs.set(i, new ClusterPair(clusterAID, clusterQID, distance));
					}
				}
			}
			sampleIters++;
			clusterCentroidIters++;
			if (interactive) {
				progressBar.increment("Number of Clusters Remaining: " + clusterContentsMap.size());
			}

		}

		return;
	}

	/**
	 * Holds the id's for a pair of clusters and the distance between them.
	 */
	class ClusterPair implements Comparable<ClusterPair> {
		
		int firstClusterID;
		int secondClusterID;
		float distance;
		
		public ClusterPair(int firstID, int secondID, float dist) {
			firstClusterID = firstID;
			secondClusterID = secondID;
			distance = dist;
		}

		public int getFirstClusterID() {
			return firstClusterID;
		}
		
		public int getSecondClusterID() {
			return secondClusterID;
		}
		
		public float getDistance() {
			return distance;
		}

		public int compareTo(ClusterPair otherOne) {
			if (otherOne.distance > this.distance) {
				return -1;
			}
			else if (otherOne.distance < this.distance) {
				return 1;
			}
			else {
				return 0;
			}
		}
	}

	/**
	 * Holds the atom id's in a cluster and the average peaklist
	 */
	class ClusterContents {
		
		int clusterCollectionID;
		ArrayList<Integer> atomIDList;
		BinnedPeakList peaks;

		/**
		 * Creates a new cluster from a single particle.  Makes a new
		 * collection in the database and puts the particle in the
		 * collection.
		 * @param particle
		 */
		public ClusterContents(ParticleInfo particle) {
			atomIDList = new ArrayList<Integer>();
			atomIDList.add(particle.getID());
			peaks = particle.getBinnedList();
			this.clusterCollectionID = createNewCollection();
			addAtomsToCollection();
		}

		/**
		 * Creates a new cluster from a collection.  Doesn't have to
		 * make anything new or move anything in the database.
		 * @param collID
		 */
		public ClusterContents(int collID) {
			this.clusterCollectionID = collID;
			atomIDList = new ArrayList<Integer>();
            try (CollectionCursor curs = getCursor(collID)) {
				assert curs != null;
                while (curs.next()) {
                    ParticleInfo particle = curs.getCurrent();
                    atomIDList.add(particle.getID());
                    if (peaks == null) {
                        peaks = particle.getBinnedList();
                    } else {
                        peaks.addAnotherParticle(particle.getBinnedList());
                    }
                }
            }
            if (peaks != null) {
				peaks.divideAreasBy(atomIDList.size());
			}
		}
		
		public void merge(ClusterContents otherOne) {
			peaks.multiply(atomIDList.size());
			otherOne.peaks.multiply(otherOne.getAtomIDList().size());
			peaks.addAnotherParticle(otherOne.getPeaks());
			atomIDList.addAll(otherOne.atomIDList);
			peaks.divideAreasBy(atomIDList.size());
			int superCollectionID = createNewCollection();
			Collection newColl = db.getCollection(superCollectionID);
			db.moveCollection(db.getCollection(otherOne.clusterCollectionID), newColl);
			db.moveCollection(db.getCollection(clusterCollectionID), newColl);
			clusterCollectionID = superCollectionID;
		}
		
		public ArrayList<Integer> getAtomIDList() {
			return atomIDList;
		}
		
		public BinnedPeakList getPeaks() {
			return peaks;
		}

		private int createNewCollection() {
			numCollections++;
			return db.createEmptyCollection(collection.getDatatype(), newHostID,
					Integer.toString(numCollections), 
					Integer.toString(numCollections),"");
		}

		private void addAtomsToCollection() {
			try {
				ArrayList<Database.BulkInsertAtomRow> atomRows = new ArrayList<>();
				System.out.println("AtomIDList = " + atomIDList);
				for (int atomID : atomIDList) {
					atomRows.add(new Database.BulkInsertAtomRow(atomID, clusterCollectionID));
				}
				db.bulkInsertAtom(atomRows);
			}
			catch (Exception e) {
				throw new ExceptionAdapter(e);
			}
		}
		
		/**
		 * Gets the distance from this cluster to another cluster.  Right now
		 * it's just the distance between the average peak lists which is 
		 * useful for Ward's and centroid approaches to clustering.  We could
		 * add other more complicated distance calculations for other 
		 * hierarchical clustering approaches like single link, complete link,
		 * group average, or centroid.
		 * @param otherCluster
		 * @param metric
		 * @return
		 */
		public float getDistance(ClusterContents otherCluster, DistanceMetric metric) {
			return peaks.getDistance(otherCluster.getPeaks(), metric);
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
	
	public boolean setCursorType(int type) 
	{
		switch (type) {
		case CollectionDivider.DISK_BASED :
		case CollectionDivider.STORE_ON_FIRST_PASS : 
			cursorType = type;
			return true;
		default :
			return false;
		}
	}

	private CollectionCursor getCursor (int collID) {
		switch (cursorType) {
		case CollectionDivider.DISK_BASED :
			System.out.println("DISK_BASED");
			try {
					curs = new NonZeroCursor(db.getBPLOnlyCursor(db.getCollection(collID)));
				} catch (SQLException e) {
					e.printStackTrace();
				}
		return curs;
		case CollectionDivider.STORE_ON_FIRST_PASS : 
		    System.out.println("STORE_ON_FIRST_PASS");
			curs = new NonZeroCursor(db.getMemoryClusteringCursor(db.getCollection(collID), clusterInfo));
		return curs;
		default :
			return null;
		}
	}
	/**
	 * Tells us whether we have preclustered the data already.  If we have, we
	 * don't create new collections to put everything in--we just use the ones
	 * that are already under the main collection
	 */
	public void setPreClustered() {
		preClustered = true;
		// this is a bit of a hack...
		// if we're preclustered, we don't want to make another collection
		// to hold the results in--jtbigwoo
		if (preClustered) {
			db.removeEmptyCollection(db.getCollection(newHostID));
			newHostID = collectionID;
			db.renameCollection(db.getCollection(newHostID), "Hierarchical, Clusters Ward\'s" + folderName);
		}
	}
}
