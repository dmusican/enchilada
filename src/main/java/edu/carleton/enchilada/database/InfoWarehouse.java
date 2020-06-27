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
 * The Original Code is EDAM Enchilada's InfoWarehouse class.
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
 * Created on Jul 16, 2004
 *
 */
package edu.carleton.enchilada.database;

import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.gui.ProgressBarWrapper;
import edu.carleton.enchilada.gui.LabelingIon;
import java.sql.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.clustering.ClusterInformation;

import edu.carleton.enchilada.ATOFMS.Peak;


import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database.ArrayCursor;
import edu.carleton.enchilada.database.Database.BPLOnlyCursor;


/**
 * The InfoWarehouse interface describes an interface for interacting 
 * with long term storage, typically in the form of a database, but in
 * theory it could be anything, flat files, a custom binary format, etc.
 * @author andersbe
 * @author jtbigwoo
 */
public interface InfoWarehouse {
	/**
	 * Opens a connection to the database, flat file, memory structure,
	 * or whatever you're working with.  
	 * @return true on success
	 */
	public boolean openConnection();

	/**
	 * Opens a connection to the database, flat file, memory structure,
	 * or whatever you're working with.  
	 * @return true on success
	 */
	public boolean openConnectionNoDB();

	/**
	 * Closes existing connection
	 * @return true on success.
	 */
	public boolean closeConnection();

	/**
	 * @return true if this resource is available for use
	 */
	public boolean isPresent();

	/**
	 * Creates an empty collection with no atomic analysis units in it.
	 *
	 * @param parent		The key to add this collection under (0
	 * 						to add at the root).
	 * @param name			What to call this collection in the interface
	 * @param comment		A comment for this collection
	 * @param description	The description of this collection to display
	 * 						in the interface
	 * @return				The collectionID of the resulting collection
	 */
	public int createEmptyCollection( String datatype,
									  int parent,
									  String name,
									  String comment,
									  String description);

	/**
	 * Create a new collection from an array list of atomIDs which 
	 * have yet to be inserted into the database.  
	 *
	 * @param parentID	The key of the parent to insert this
	 * 					collection (0 to insert at root level)
	 * @param name		What to call this collection
	 * @param comment	What to leave as the comment
	 * @param atomType	The type of atoms you are inserting ("ATOFMSParticle" most likely
	 * @param atomList	An array list of atomID's to insert into the 
	 * 					database
	 * @return			The CollectionID of the new collection
	 *//*
	public int createCollectionFromAtoms(String datatype,
			int parentID,
			String name,
			String comment,
			ArrayList<String> atomList);
	*/
	/**
	 * Moves a collection and all its children from one parent to 
	 * another.  If the subcollection was the only child of the parent
	 * containing a particular atom, that atom will be removed from 
	 * the parent, if there are other existing subcollections of the 
	 * parent containing particles also belonging to this collection, 
	 * those particles will then exist both in the current collection and
	 * its parent.  <br><br>
	 *
	 * To avoid removing particles, use copyCollection instead.
	 * @param collectionID The collection id of the collection to move.
	 * @param toParentID The collection id of the new parent.
	 * @return True on success. 
	 */
	public boolean moveCollection(Collection collection,
								  Collection toCollection);

	/**
	 * Similar to moveCollection, except instead of removing the 
	 * collection and its unique children, the original collection 
	 * remains with original parent and a duplicate with a new id is 
	 * assigned to the new parent.  
	 * @param collectionID The collection id of the collection to move.
	 * @param toParentID The collection id of the new parent.  
	 * @return The collection id of the copy.  
	 */
	public int copyCollection(Collection collection, Collection toCollection);

	/**
	 * orphanAndAdopt() essentially deletes a collection and assigns 
	 * the ownership of all its children (collections and atoms) to 
	 * their grandparent collection.  
	 * @param collectionID The ID of the collection to remove. 
	 * @return true on success.
	 */
	public boolean orphanAndAdopt(Collection collection);

	/**
	 * Percolate all atoms in the given collection up the collection hierarchy.
	 * This should be called whenever a new collection is created.  If it has one or more
	 * parent, it will cause the parent to contain all of the new collection's atoms
	 *
	 * @param newCollection
	 */
	public void propagateNewCollection(Collection newCollection);

	/**
	 * Deletes a collection and unlike orphanAndAdopt() also recursively
	 * deletes all direct descendents. 
	 *
	 * @param collectionID The id of the collection to delete
	 * @return true on success. 
	 */
	public boolean recursiveDelete(Collection collection);

	/**
	 * Renames a collection.
	 * @param collection the collection to rename
	 * @param newName the new name for the collection
	 * @return true on success
	 * @author atlasr
	 */
	public boolean renameCollection(Collection collection, String newName);

	/**
	 * Returns the id's of the immediate subchildren of the input
	 * collection
	 * @param collectionID 	ID of the collection you wish to see the 
	 * 						subchildren of
	 * @return	an array of subchildren
	 */
	public ArrayList<Integer> getImmediateSubCollections(Collection collection);
	public ArrayList<Integer> getImmediateSubCollections(ArrayList<Integer> collections);

	/**
	 * Returns every Atom that exists in the collection identified
	 * by collectionID, and every Atom that is a child of 
	 * descendent collections of collectionID's collection.
	 * @param collectionID 	The ID of the collection to find 
	 * 						descended atoms of.
	 * @return	An ArrayList of Integers containing the Atom IDs 
	 * 			of the descended atoms (Atom IDs in the list 
	 * 			<em>are</em> distinct).
	 */
	public ArrayList<Integer> getAllDescendedAtoms(Collection collection);

	/**
	 * Get the string representation of the given collection's name
	 * @param collectionID	CollectionID of the collection whose name
	 * 						you would like
	 * @return				A string representing the name of the collection
	 */
	public String getCollectionName(int collectionID);

	/**
	 * Returns the number of atoms in a collection
	 * @param collection
	 * @return
	 */
	public int getCollectionSize(int collectionID);

	public ArrayList<Integer> getCollectionIDsWithAtoms(java.util.Collection<Integer> collectionIDs);

	/**
	 * Returns an array list of ATOFMSAtomFromDB's describing
	 * every particle descending from the given collection.
	 * @param collectionID	The id of the collection you want
	 * 						to find particles descended from
	 * @return An array list of particle info.
	 *
	 *DEPRECIATED 12/05 - AR
	public ArrayList<GeneralAtomFromDB> getCollectionParticles(Collection collection);
	 */

	public Set<Integer> getAllDescendantCollections(int collectionID, boolean includeTopLevel);

	public Date exportToMSAnalyzeDatabase(Collection collection, String newName, String sOdbcConnection, String fileName, ProgressBarWrapper progressBar);

	public String getCollectionComment(int collectionID);

	public boolean moveAtom(int atomID, int fromParentID, int toCollectionID);

	//public boolean moveAtomBatch(int atomID, int fromParentID, int toCollectionID);

	public boolean checkAtomParent(int atomID, int isMemberOf);

	public boolean addAtom(int atomID, int parentID);

	public boolean addAtomBatch(int atomID, int parentID);

	public boolean deleteAtomsBatch(String atomIDs, Collection collection);

	public boolean deleteAtomBatch(int atomID, Collection collection);

	public void atomBatchExecute();

	public String getCollectionDescription(int collectionID);

	public boolean setCollectionDescription(Collection collection,
											String description);

	public ArrayList<Peak> getPeaks(String datatype, int atomID);

	public void atomBatchInit();

	public Collection getCollection(int collectionID);

	public CollectionCursor getAtomInfoOnlyCursor(Collection collection);
	public CollectionCursor getSQLAtomIDCursor(Collection collection,  String where);
	public CollectionCursor getSQLCursor(Collection collection, String where);

	public CollectionCursor getPeakCursor(Collection collection);

	public CollectionCursor getBinnedCursor(Collection collection);
	public BPLOnlyCursor getBPLOnlyCursor(Collection collection) throws SQLException;

	public CollectionCursor getClusteringCursor(Collection collection, ClusterInformation cInfo);

	public CollectionCursor getMemoryClusteringCursor(Collection collection, ClusterInformation cInfo);
	public CollectionCursor getMemoryBinnedCursor(Collection collection);

	public CollectionCursor getRandomizedCursor(Collection collection);

	public void seedRandom(int seed);

	public ArrayList<String> getColNames(String datatype, DynamicTable table);
	public Connection getCon();
	public Vector<Vector<Object>> updateParticleTable(Collection collection, Vector<Vector<Object>> particleTable, int lowAtomID, int hightAtomID);

	public int saveMap(String name, Vector<int[]> mapRanges);
	public Hashtable<Integer, String> getValueMaps();
	public Vector<int[]> getValueMapRanges();

	public int applyMap(String mapName, Vector<int[]> map, Collection collection);
	public void createTempAggregateBasis(Collection c, Collection basis);
	public void createTempAggregateBasis(Collection c, Calendar start, Calendar end, Calendar interval);
	public void deleteTempAggregateBasis();
	public void getMaxMinDateInCollections(Collection[] collections, Calendar minDate, Calendar maxDate);

	public boolean createAggregateTimeSeries(ProgressBarWrapper progressBar, int rootCollectionID,
											 Collection curColl, int[] mzValues) throws InterruptedException;
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromPercent(Collection collection,double magnitude, int minDuration);
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromMedian(Collection collection,double magnitude, int minDuration);
	public ArrayList<TreeMap<Date,Double>> createAndDetectPlumesFromValue(Collection collection,double magnitude, int minDuration);

	public int[] getValidSelectedMZValuesForCollection(Collection collection, Date startDate, Date endDate);

	public ArrayList<Date> getCollectionDates(Collection seq1, Collection seq2);
	public ArrayList<Date> getCollectionDates(Collection[] collections);

	public Hashtable<Date, Double> getConditionalTSCollectionData(Collection seq,
																  ArrayList<Collection> conditionalSeqs, ArrayList<String> conditionStrs);

	public void syncWithIonsInDB(ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons);
	public void saveAtomRemovedIons(int atomID, ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons);
	public void buildAtomRemovedIons(int atomID, ArrayList<LabelingIon> posIons, ArrayList<LabelingIon> negIons);

	public String getDynamicTableName(DynamicTable table, String datatype);

	public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table);
	public int getNextID();
	public int[] createEmptyCollectionAndDataset(String datatype, int parent, String datasetName, String comment, String params);
	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse, Collection collection,int datasetID, int nextID);
	public boolean isDirty();
	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse, Collection collection, int nextID);
	public int getParentCollectionID(int collectionID);
	public void updateAncestors(Collection collection);
	public void updateInternalAtomOrder(Collection collection);
	public String aggregateColumn(DynamicTable atomInfoDense, String string, ArrayList<Integer> curIDs, String oldDatatype);
	public ArrayList<String> getPrimaryKey(String datatype, DynamicTable atomInfoSparse);
	public String getCollectionDatatype(int subCollectionNum);
	public boolean addCenterAtom(int centerAtomID, int centerCollID);
	public HashMap<Integer,ArrayList<Integer>> getSubCollectionsHierarchy(Collection collection);
	public boolean beginTransaction();
	public boolean commitTransaction();
	public boolean rollbackTransaction();

	public int getRepresentedCluster(int atomID);
	public ArrayList<String> getKnownDatatypes();
	public String getVersion();
	public String getAtomDatatype(int atomID);
	public boolean containsDatatype(String type);
	public String getATOFMSFileName(int atomID);
	public boolean removeEmptyCollection(Collection collection);

	/**
	 * Get the date format used by this InfoWarehouse
	 * @return a DateFormat object that can format Dates inserted into this InfoWarehouse.
	 */
	public DateFormat getDateFormat();

	/**
	 * @return the name of the backing store this InfoWarehouse uses (SQLServer, MySQL, FlatFile, etc.)
	 */
	public String getName();

	/**
	 * Return true if the collection contains the given atomID.
	 * @param collectionID - the ID of the collection to check
	 * @param atomID -		 the ID of the target atom
	 * @return				 True if the collection contains the atom.
	 */
	public boolean collectionContainsAtom(int collectionID, int atomID);

	/**
	 * Given a filename for an ATOFMS particle, returns the atomID associated 
	 * with it.
	 * @param ATOFMSFileName	The filename for the particle.
	 * @return					The atomID for the desired particle.
	 */
	public int getATOFMSAtomID(String ATOFMSFileName);
	public boolean openConnection(String dbName);
	public void bulkInsertInit() throws Exception;
	public void bulkInsertAtom(int newChildID, int newHostID) throws Exception;
	public void bulkInsertExecute();

	public String getDatabaseName();

	public void clearCache();

	/**
	 * Given a collection, returns the average peak list of all non-zero particles
	 * @param coll		The collection to average
	 */
	public BinnedPeakList getAveragePeakListForCollection(Collection coll);

	public CollectionCursor getCentroidCursor(Collection collection) throws SQLException;

	public ArrayCursor getArrayCursor(Collection parent) throws SQLException;

	public ArrayCursor getArrayCursor(ArrayCursor other);

	public void dropDatabaseCommands() throws SQLException;
	public void createDatabaseCommands(String dbName);
	public String getRebuildScriptFilename();

	public int insertParticle(String dense, java.util.Collection<ATOFMSPeak> sparse,
							  Collection collection,
							  int datasetID, int nextID, boolean importing);

	public boolean rebuildDatabase(String dbName);

}