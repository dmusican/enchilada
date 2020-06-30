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
 * The Original Code is EDAM Enchilada's Collection class.
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
 * Created on Jul 15, 2004
 *
 */
package edu.carleton.enchilada.collection;

import edu.carleton.enchilada.database.*;
import edu.carleton.enchilada.database.Database.CentroidCursor;

import java.sql.SQLException;
import java.util.*;

/**
 * @author gregc
 * @author andersbe
 *
 */
public class Collection implements Comparable{
	private int collectionID;
	private Collection parentCollection;
	private boolean parentCollectionInitialized = false;
	
	private Database db = null;
	private String datatype;
	private AggregationOptions aggregationOptions;

	private String cachedName, cachedComment, cachedDescription;
	private ArrayList<Integer> cachedSubCollectionIDs = null;
	private Set<Integer> cachedCollectionIDSubTree = null;
	private Collection[] cachedSubCollections;
	private int cachedContainsData = -1;
	private int cachedSize = -1;
	
	public Collection(String type, int cID, Database database)
	{
		collectionID = cID;
		datatype = type;
		db = database;
		
	}
	
	public Collection(String name, String type, int cID, Database database)
	{
		cachedName = name;
		collectionID = cID;
		datatype = type;
		db = database;
	}
	
	public void clearCachedChildren() {
		cachedSubCollectionIDs = null;
		cachedCollectionIDSubTree = null;
		cachedSubCollections = null;
		parentCollectionInitialized = false;
	}
	
	/**
	 * @guru Jamie Olson
	 * @return the list of CollectionIDs for all child collections
	 */
	public ArrayList<Integer> getSubCollectionIDs()
	{
		if (cachedSubCollectionIDs == null){
			//cachedSubCollectionIDs = db.getImmediateSubCollections(this);
			HashMap<Integer,ArrayList<Integer>> hierarchy = db.getSubCollectionsHierarchy(this);
			ArrayList<Integer> allSubChildren = new ArrayList<Integer>();
			HashMap<Integer,Collection> collections = new HashMap<Integer,Collection>();
			allSubChildren.add(collectionID);
			collections.put(collectionID,this);
			//while there are more collection to go through
			while (!allSubChildren.isEmpty()) {
				//this is the parent
				int nextParent = allSubChildren.get(0);
				//if it has children, setup the children
//				get the collection object for the current collection
				Collection collection = collections.get(nextParent);
				if (hierarchy.containsKey(nextParent)) {
					//add the children to the list of collections to be setup
					allSubChildren.addAll(hierarchy
							.get(new Integer(nextParent)));
					//set it's collectionID cache
					collection.cachedSubCollectionIDs = null;
					collection.cachedSubCollectionIDs = hierarchy.get(nextParent);
					//set it's collection cache
					collection.cachedSubCollections = new Collection[collection.cachedSubCollectionIDs
							.size()];
					//populate it's collection cache
					for (int index = 0; index < collection.cachedSubCollectionIDs
							.size(); index++) {
						Collection childCollection;
						int childID = collection.cachedSubCollectionIDs.get(index);
						//create the collection
						if (collection.datatype.contains("root")){
							childCollection = db.getCollection(childID);
						}else{
							childCollection = new Collection(collection.datatype,
									childID, db);
						}
						//set its parent
						childCollection.setParentCollection(collection);
						collection.cachedSubCollections[index] = childCollection;
						//add it to the map of collections
						collections.put(collection.cachedSubCollectionIDs
											.get(index), childCollection);
					}
					
				}else{
					collection.cachedSubCollectionIDs = new ArrayList<Integer>();
				}
				allSubChildren.remove(0);
			}
			
		}
		return cachedSubCollectionIDs;
	}
	
	public boolean equals(Object o) {
		if (o instanceof Collection) {
			return ((Collection) o).collectionID == collectionID;
		}
		
		return false;
	}
	
	public Collection getChildAt(int index) {
		ArrayList<Integer> subCollectionIDs = getSubCollectionIDs();
		
		if (cachedSubCollections == null)
			cachedSubCollections = new Collection[subCollectionIDs.size()];
		
		if (cachedSubCollections[index] == null) {
			int collectionID = subCollectionIDs.get(index).intValue();
			
			// Make sure children have correct datatype...
			// Should only share from parent nodes that aren't root
			if (datatype.contains("root"))
				cachedSubCollections[index] = db.getCollection(collectionID);
			else
				cachedSubCollections[index] = new Collection(datatype, collectionID, db);
			
			cachedSubCollections[index].setParentCollection(this);
		}
		
		return cachedSubCollections[index];
	}
	 private void setChildren(ArrayList<Collection> children){
		 ;
	 }
	
	
	/**
	 * Since parent collection might not have been initialized, you must 
	 * make sure that it doesn't have a parent in the db.  Then boolean
	 * is set to true, and the parentCollection is in the cache.
	 * @return
	 */
	public Collection getParentCollection() {
		if (!parentCollectionInitialized) {
			int temp = db.getParentCollectionID(collectionID);
			if (temp != -1)
				parentCollection = db.getCollection(temp);
			parentCollectionInitialized = true;
		}
		return parentCollection;
	}
	
	public void setParentCollection(Collection c) {
		parentCollection = c;
	}
	
	public int getCollectionID()
	{
		return collectionID;
	}
	
	public String getDatatype() {
		return datatype;
	}
	
	public boolean containsData() {
		if (cachedContainsData == -1)
			cachedContainsData = db.getCollectionIDsWithAtoms(getCollectionIDSubTree()).size();
	
		return cachedContainsData > 0;
	}
	
	public Set<Integer> getCollectionIDSubTree() {
		if (cachedCollectionIDSubTree == null) {
			cachedCollectionIDSubTree = db.getAllDescendantCollections(collectionID, true); 
		}
		
		return cachedCollectionIDSubTree;
	}
	
	public String getName(){
		if (cachedName == null)
			cachedName = db.getCollectionName(collectionID);
		return cachedName;
	}
	
	//Hack - not good
	public int compareTo(Object o){
		Collection c = (Collection)o;
		return (((Integer)this.getCollectionID()).compareTo(c.getCollectionID()));
	}
	
	public void setName(String newName){
		cachedName=newName;
	}
	
	public String getComment() {
		if (cachedComment == null)
			cachedComment = db.getCollectionComment(collectionID);
		
		return cachedComment;
	}
	
	public String getDescription() {
		if (cachedDescription == null)
			cachedDescription = db.getCollectionDescription(collectionID);
		
		return cachedDescription;
	}
	
	public String toString()
	{
		return getName() == null ? "" : getName().trim();
	}
	
	public ArrayList<Integer> getParticleIDs()
	{
		return db.getAllDescendedAtoms(this);
	}
	
	public void setAggregationOptions(AggregationOptions ao) {
		aggregationOptions = ao;
	}
	
	public AggregationOptions getAggregationOptions() { 
		return aggregationOptions;
	}
	
	public void updateParent(Collection p) {
		parentCollection = p;
	}
	
	public int getCollectionSize() {
		if (cachedSize == -1)  {
			cachedSize = db.getCollectionSize(collectionID);
		}
		return cachedSize;
	}
	
	// check if there is a center atom associated with this collection; if so, it's a cluster
	// this doesn't work for cluster query results... why? answer: CQ doesn't add to CenterAtoms
	// Michael Murphy 2014
	public boolean isCluster() throws SQLException {
		CentroidCursor curs = (CentroidCursor) db.getCentroidCursor(this);
		boolean isCluster = !curs.isEmpty();
		curs.close();
		return isCluster;
	}
	
	// check if any of the immediate subcollections have center atoms associated
	public boolean isClusterParent() throws SQLException {
		ArrayList<Integer> childrenIds = this.getSubCollectionIDs();
		boolean isClusterParent = true;
		for (int cid : childrenIds) {
			isClusterParent &= db.getCollection(cid).isCluster();
		}
		return isClusterParent;
	}
}
