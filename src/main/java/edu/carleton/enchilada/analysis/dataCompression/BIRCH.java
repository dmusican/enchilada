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
 * The Original Code is EDAM Enchilada's BIRCH class.
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

package edu.carleton.enchilada.analysis.dataCompression;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.database.*;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.BinnedPeak;

/**
 * BIRCH is a scalable clustering algorithm that fits all the particles into
 * the largest height-balanced tree that fits in memory, using summary 
 * information about particles to create cluster feature.  These cluster
 * features are then clustered to produced more refined clusters.
 * 
 * @author ritza
 * @author christej
 */
public class BIRCH extends CompressData{
	/* Class Variables */
	private int branchingFactor; // Number of cluster features each node is allowed to have.
	private int collectionID;
	private CFTree curTree;
	
	//keep track of the time it takes to build, to rebuild, and to do everything
	private long buildStart, buildEnd, rebuildStart, rebuildEnd, realStart, realEnd;
	private long buildTotal = 0; 
	private long rebuildTotal = 0;
	
	//keep track of how many times you rebuild the tree
	private int rebuildCount;
	
	//variables to choose the next threshold
	private float lastThreshold;
	private float curThreshold;
	private int lastNumParticles;
	private int curNumParticles;
	private int size;
	private int numParticles;
	/**
	 * MEM_THRESHOLD is a new way of calculating memory predictably.  This threshold
	 * should ultimately be the max threshold of the system that Enchilada is being
	 * run on, but for now I manually set it to test smaller datasets.  I think, for example.
	 * that running Tester works well if MEM_THRESHOLD = 1000.
	 * 
	 * This should be an advanced option that the user can change in the GUI.
	 * 
	 * memory is in bytes - janara
	 */
	//ThinkPad claims it has 1GB RAM - 1073741824 bytes
	//1000 for test data, 150118 for i
	//private final float MEM_THRESHOLD = 150000;
	private final float MEM_THRESHOLD = 150000;

	public static long buildTime = 0;
	
	/*
	 * Constructor.  Calls the CompressData Class's constructor.
	 */
	public BIRCH(Collection c, InfoWarehouse database, String name, String comment, DistanceMetric d) 
	{
		super(c,database,name,comment,d);
		// set parameters.
		branchingFactor = 3; // should be an advanced option in the GUI
		collectionID = oldCollection.getCollectionID();
		assignCursorType();
		rebuildCount = 0; 
		size = 0;
		curThreshold = 0;
		numParticles = database.getCollectionSize(c.getCollectionID());
		System.out.println("numParticles = " + numParticles);
	}
	
	/**
	 * Builds the tree in memory. Inserts the particles one at a time, 
	 * increasing the threshold if we run out of memory.
	 * @param threshold - initial threshold for tree.
	 */
	public void buildTree(float threshold) {
		System.out.println("\n**********BUILDING THE PRELIMINARY TREE**********");
		realStart = new Date().getTime();
		buildStart = new Date().getTime();
		threshold = (float)0.0;
		curTree = new CFTree(threshold, branchingFactor, distanceMetric); ; 
		
		// Insert particles one by one.
		ParticleInfo p;
		BinnedPeakList peakList;
		Integer atomID;
		CFNode changedNode, lastSplitNode;
		while(curs.next()) {
			p = curs.getCurrent();
			peakList = p.getBinnedList();
			atomID = p.getID();
			peakList.posNegNormalize(distanceMetric);//necessary?
			System.out.println("inserting particle " + atomID);
			assert!peakList.containsZeros():"zero present";
			// insert the entry
			changedNode = curTree.insertEntry(peakList, atomID);
			// if it's possible to split the node, do so.
			lastSplitNode = curTree.splitNodeIfPossible(changedNode);
			// If there has been a split above the leaves, refine the split
			if (!lastSplitNode.isLeaf() || 
					!changedNode.equals(lastSplitNode)) {
				curTree.refineMerge(lastSplitNode);
			}	
			// if we have run out of memory, rebuild the tree.
			if (curTree.getMemory()> MEM_THRESHOLD) {
				
				//tester stuff
				buildEnd = new Date().getTime();
				buildTotal = buildTotal + (buildEnd-buildStart);
				rebuildStart = new Date().getTime();
				int[] counts = {0,0,0,0,0};
				int leafNum = curTree.countNodesRecurse(curTree.root,counts)[1];
				
				/**
				 * If there is only one leaf node, then that means that there is only
				 * enough memory to clump all particles into one collection, therefore
				 * making BIRCH pointless.
				 */
				if (curTree.threshold >= 2 && leafNum == 1) {
					System.err.println("Out of memory at max threshold with 1 " +
							"leaf in tree.\nAll points will be in the same leaf," +
							" so the clustering is pointless.\nExiting.");
					System.exit(0);
				}
				rebuildCount++;
				System.out.println("OUT OF MEMORY @ "+ curTree.getMemory() + "\n");
				
				//tester stuff
				curTree.countNodes();
				System.out.println("interval: " + buildTotal);
				
			//	System.exit(1);
				System.out.println("*****************REBUILDING TREE*****************\n");
			
				//  rebuild tree.
				size = curTree.getSize();
				curNumParticles = size;
				rebuildTree();
				lastNumParticles = curTree.getSize();
				
				rebuildEnd = new Date().getTime();
				rebuildTotal = rebuildTotal + (rebuildEnd-rebuildStart);
				buildStart = new Date().getTime();
				curTree.countNodes();
			}
		}	
		buildEnd = new Date().getTime();
		realEnd = new Date().getTime();
		buildTotal = buildTotal + (buildEnd-buildStart);
		System.out.println("\nFINAL TREE:");
		curTree.printTree();
		
		//make sure each particle was inserted, and that there are no duplicates
		//just tester code
		ArrayList<Integer> array = curTree.getArrayOfParticles();
		array = sort(array);
		for(int i = 1; i<array.size(); i++){
			if( array.get(i) != (array.get(i-1) + 1))
				System.out.println("OH NO--duplicate particle!");
		}
		System.out.println("array.size() "+ array.size());
		System.out.println("interval: " + (realEnd-realStart));
		System.out.println("buildTotal : " + buildTotal);
		System.out.println("rebuildtotal : " + rebuildTotal);
		System.out.println("rebuildCount : " + rebuildCount);

		System.out.println("Build time = " + buildTime);

	}
	/**
	 * Sorts an array of all the atom ids
	 * just a tester method
	 */
	public ArrayList<Integer> sort(ArrayList<Integer> array){
		ArrayList<Integer> array2 = new ArrayList<Integer>();
		int j = 0;
		while(array.size()>0){
			int min = array.get(0);
			int pos = 0;
			for(int i = 0; i<array.size(); i++){
				if(array.get(i)<min){
					min = array.get(i);
					pos = i;
				}
			}
			array2.add(min);
			array.remove(pos);
		}
		return array2;
	}
	/**
	 * finds the next threshold needed to fit two times as
	 * many particles as currently are in the tree unless
	 * two times that number is more than the number of particles
	 * in the database--if it is use the number of particles
	 * in the database
	 * 
	 * uses the last two thresholds and the number of particles
	 * that fit in the tree at those thresholds
	 */
	public float linearThreshold(){
		float deltaThreshold = curThreshold-lastThreshold;
		float deltaSize = curNumParticles - lastNumParticles;
		float slope = deltaSize/deltaThreshold;
		float intcpt = curNumParticles-slope*curThreshold;
		float goal;
		if(size<.5*numParticles)
			goal = 2*size;
		else
			goal = numParticles;
		float t = (goal - intcpt)/slope;
		System.out.println("newThreshold " +curThreshold);
		System.out.println("lastThreshold " +lastThreshold);
		System.out.println("deltaThreshold " +deltaThreshold);
		System.out.println("lastFit " +lastNumParticles);
		System.out.println("newFit " +curNumParticles);
		System.out.println("deltaSize " + deltaSize);
		System.out.println("intcpt " + intcpt);
		System.out.println("goal " + goal);
		System.out.println("slope " + slope);
		System.out.println("t " + t);
		return t;
	}
	/**
	 * Rebuilds the tree if we run out of memory.  Calls rebuildTreeRecurse,
	 * then removes all the empty nodes in the new tree.  Sets the current
	 * tree to the new one at the end of the method.
	 */
	public void rebuildTree() {
		float t = 0;
		if(rebuildTotal>1)
			t = linearThreshold();
		lastThreshold = curThreshold;
		// predict the next best threshold.
		curTree.assignLeaves();
		if(rebuildTotal<1 || t > 2.0)
			curThreshold = curTree.nextThreshold();
		else
			curThreshold = t;

		System.out.println("new THRESHOLD: " + curThreshold);
		
		CFTree newTree = new CFTree(curThreshold, branchingFactor, distanceMetric);
		newTree = rebuildTreeRecurse(newTree, newTree.root, curTree.root, null);
		System.out.println("*** DONE REBUILDING ***");
		newTree.assignLeaves();
		
		// remove all the nodes with count = 0;
		CFNode curLeaf = newTree.getFirstLeaf();
		
		//get rid of all the empty clusterfeatures in the new tree
		while (curLeaf != null) {
			// TODO: make this more elegant
			if (curLeaf.getSize() == 0 || 
					(curLeaf.getSize() == 1 && curLeaf.getCFs().get(0).getCount() == 0)) {
				CFNode emptyNode = curLeaf;
				ClusterFeature emptyCF;
				while (emptyNode.parentCF != null && emptyNode.parentCF.getCount() == 0) {
					emptyNode.parentNode.removeCF(emptyNode.parentCF);
					emptyNode = emptyNode.parentNode;
				}
			}
			curLeaf = curLeaf.nextLeaf;
		}
		newTree.numDataPoints = curTree.numDataPoints;
		newTree.assignLeaves();
		
		// reassign memory allocation to each node.
		newTree.findTreeMemory(newTree.root, true);
		curTree = newTree;
		curTree.setSize(size);
		System.out.println("end of rebuildTree(), threshold = " + curTree.getThreshold());
	}
	
	/**
	 * Recursive method for rebuilding the tree.  Returns the new tree.
	 * @param newTree - new tree that's being built
	 * @param newCurNode - current node in the new tree
	 * @param oldCurNode - corresponding node in the old tree
	 * @param lastLeaf - most recent leaf
	 * @return the final new tree.
	 */
	public CFTree rebuildTreeRecurse(CFTree newTree, CFNode newCurNode, 
			CFNode oldCurNode, CFNode lastLeaf) {
		newTree.assignLeaves();
		//if it's a leaf, we want to insert each cluster feature
		//into the new tree, merging as many cluster features
		//as possible
		if (oldCurNode.isLeaf()) {
			if (lastLeaf == null)
				lastLeaf = newTree.getFirstLeaf();
			else if (lastLeaf.nextLeaf == null)
				lastLeaf.nextLeaf = newCurNode;
			
			boolean reinserted;
			/*for (int i = 0; i < oldCurNode.getSize(); i++) {
				ClusterFeature thisCF = oldCurNode.getCFs().get(i);
				//try to reinsert the cf
				reinserted = newTree.reinsertEntry(thisCF);
				//if reinserting it would have resulted in too many cfs for that node
				if (!reinserted) {
					//make a new cluster feature and add it to newCurNode
					ClusterFeature newLeaf = new ClusterFeature(
							newCurNode, thisCF.getCount(), thisCF.getSums(), 
							thisCF.getSumOfSquares(), thisCF.getAtomIDs(),
							thisCF.getMagnitude());				
					newCurNode.addCF(newLeaf);
					//update everything
					newTree.updateNonSplitPath(newLeaf.curNode);
					for (int j = 0; j < newLeaf.curNode.getCFs().size(); j++){
						newLeaf.curNode.getCFs().get(j).updateCF();
					}
				}*/
			for (int i = 0; i < oldCurNode.getSize(); i++) {
				ClusterFeature thisCF = oldCurNode.getCFs().get(i);
				//try to reinsert the cf
				reinserted = newTree.reinsertEntry(thisCF);
				//if reinserting it would have resulted in too many cfs for that node
				if (!reinserted) {
					//make a new cluster feature and add it to newCurNode
					ClusterFeature newLeaf = new ClusterFeature(
							newCurNode, thisCF.getCount(), thisCF.getSums(), 
							thisCF.getNonNormalizedSums(),
							thisCF.getSumOfSquares(), thisCF.getAtomIDs(),
							thisCF.getMagnitude());				
					newCurNode.addCF(newLeaf);
					//update everything
					newTree.updateNonSplitPath(newLeaf.curNode);
					for (int j = 0; j < newLeaf.curNode.getCFs().size(); j++){
						newLeaf.curNode.getCFs().get(j).updateCF();
					}
				}
			}
		}
		//if it's not a leaf, we just want to build the correct path
		else {
			for (int i = 0; i < oldCurNode.getSize(); i++) {
				//keep making empty cfs till there are the same number in newCurNode as in oldCurNode
				while (newCurNode.getSize() <= i) {
					ClusterFeature newCF = new ClusterFeature(newCurNode, newCurNode.dMetric);
					newCurNode.addCF(newCF);
				}
				//if the child of that cf is null, make a new child and update the pointers
				if (newCurNode.getCFs().get(i).child == null) {
					CFNode newChild = new CFNode(newCurNode.getCFs().get(i), distanceMetric);
					newCurNode.getCFs().get(i).updatePointers(
							newChild, newCurNode);
				}
				//rebuild the tree using the child as the new node
				rebuildTreeRecurse(newTree, newCurNode.getCFs().get(i).child, 
						oldCurNode.getCFs().get(i).child, lastLeaf);
			}
		}
		oldCurNode = null;
		return newTree;
	}
	/**
	 * @Override
	 * 
	 * sets the cursor type to memory binned cursor, since the whole point
	 * of this algorithm is that it's in memory.
	 */
	public boolean assignCursorType() {
		Collection collection = db.getCollection(collectionID);
	//	curs = new NonZeroCursor(db.getBinnedCursor(collection));
		try {
			curs = db.getBPLOnlyCursor(collection);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public void compress() {
		long b = System.currentTimeMillis();
		buildTree(0.0f);
		System.out.println("Total time = " + (System.currentTimeMillis()-b));
		System.out.println("Distance time = " + BinnedPeakList.distTime);
		System.out.println();
		curTree.countNodes();
		System.out.println(curTree.getSize());
		putCollectionInDB();
	}

	@Override
	/**
	 * This method is for ATOFMS particles only.  The SQL stmts. are MUCH
	 * easier to write, and we're not looking to generalize this method until
	 * we've restructured the db to accomodate generalizations. - AR
	 */
	protected void putCollectionInDB() {	
		// Create new collection and dataset:
		ArrayList<ArrayList<Integer>> centroidList = new ArrayList<ArrayList<Integer>>();
		String compressedParams = getDatasetParams(oldDatatype);
		int[] IDs = db.createEmptyCollectionAndDataset(newDatatype,0,name,comment,""); 
		int newCollectionID = IDs[0];
		int newDatasetID = IDs[1];
		
		// insert each CF as a new atom.
		int atomID;
		Collection collection  = new Collection(newDatatype, newCollectionID, db);
		
		CFNode leaf = curTree.getFirstLeaf();
		ArrayList<ClusterFeature> curCF;
		ArrayList<Integer> curIDs;
		ArrayList<ATOFMSPeak> sparseArray = new ArrayList<>();
		ArrayList<Integer> cfAtomIDs;
		// Enter the CFS of each leaf
		while (leaf != null) {
			curCF = leaf.getCFs();
			for (int i = 0; i < curCF.size(); i++) {
				cfAtomIDs = curCF.get(i).getAtomIDs();
				centroidList.add(cfAtomIDs);
				
				atomID = db.getNextID();
				
				// create denseAtomInfo string.
				String denseStr = "";
				curIDs = curCF.get(i).getAtomIDs();
				ArrayList<String> denseNames = db.getColNames(newDatatype, DynamicTable.AtomInfoDense);
				
				// start at 1 to skip AtomID column.
				for (int j = 1; j < denseNames.size()-1; j++) {
					denseStr += db.aggregateColumn(DynamicTable.AtomInfoDense,denseNames.get(j),curIDs,oldDatatype);
					denseStr += ", ";
				}
				denseStr+=curCF.get(i).getCount();
				
				// create sparseAtomInfo string arraylist.
				sparseArray = new ArrayList<>();
				Iterator iter = curCF.get(i).getSums().iterator();
				while (iter.hasNext()) {
					BinnedPeak p=(BinnedPeak) iter.next();
					sparseArray.add(new ATOFMSPeak(0, Math.round(p.getValue()), p.getValue(), p.getKey()));
				}
				
				//insert particle
				db.insertParticle(denseStr,sparseArray,collection,newDatasetID,atomID);
			}
			leaf=leaf.nextLeaf;
		}
		
		ArrayList<String> list = new ArrayList<String>();
		db.updateInternalAtomOrder(collection);

		//This bit is for testing, all it does is prints out the 
		//atomID and thefilename for all 
		//atoms in each new clusterfeature, this is nice for when
		//you use experimental data and you want to make sure each 
		//clusterfeature contains only the particles derived from
		//the same original particle
		try {
			Statement stmt = db.getCon().createStatement();

			for (int i = 0; i<centroidList.size(); i++) {
				
				Integer j = new Integer(i);
				System.out.println("Set: " + i);
				for (int k = 0; k<centroidList.get(i).size(); k++) {
					String query = "SELECT *\n" +
					" FROM ATOFMSAtomInfoDense" +
					" WHERE AtomID = " + centroidList.get(i).get(k);
					ResultSet rs = stmt.executeQuery(query);
					rs.next();
					System.out.println("AtomID: " + centroidList.get(i).get(k) + " file: " + rs.getString(6));
					
				}
			}
			stmt.close();
		} catch (SQLException e) {
			System.err.println("Exception creating the dataset entries:");
			e.printStackTrace();
		}

		System.out.println("Done inserting BIRCH into DB.");
	}
}
