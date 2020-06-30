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
 * The Original Code is EDAM Enchilada's CollectionModel class.
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
 * Created on Aug 9, 2004
 */
package edu.carleton.enchilada.gui;

import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;
import java.util.ArrayList;
import java.util.Hashtable;
import edu.carleton.enchilada.collection.*;
import edu.carleton.enchilada.database.*;
import javax.swing.event.EventListenerList;

/**
 * @author andersbe
 *
 */
public class CollectionModel implements TreeModel {
	private Collection root = null;
	private Database db = null;
	private boolean forSynchronized;
	
	private EventListenerList listenerList;
	private Hashtable<Integer, Collection> collectionLookup;
	
	/**
	 * 
	 */
	public CollectionModel(Database database, boolean sync) {
		super();
		db = database;
		forSynchronized = sync;
		
		listenerList = new EventListenerList();
		collectionLookup = new Hashtable<Integer, Collection>();
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#getRoot()
	 */
	public Object getRoot() {
		if (root == null) {
			if (forSynchronized)
				root = new Collection("root-synchronized", 1, db);
			else
				root = new Collection("root", 0, db);
			
			collectionLookup.put(0, root);
		}
		
		return root;
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#getChild(java.lang.Object, int)
	 */
	public Object getChild(Object parent, int index) {
		Collection child = ((Collection) parent).getChildAt(index);
		if (child != null && !collectionLookup.containsKey(child.getCollectionID())) {
			collectionLookup.put(child.getCollectionID(), child);
		}
		
		return child;
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#getChildCount(java.lang.Object)
	 */
	public int getChildCount(Object parent) {
		ArrayList<Integer> subChildren = 
			((Collection)parent).getSubCollectionIDs();
		if (subChildren != null)
			return subChildren.size();
		return 0;
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#isLeaf(java.lang.Object)
	 */
	public boolean isLeaf(Object node) {
		return getChildCount(node) == 0;
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#valueForPathChanged(javax.swing.tree.TreePath, java.lang.Object)
	 */
	public void valueForPathChanged(TreePath path, Object newValue) {
		// TODO Implement this sometime.

	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#getIndexOfChild(java.lang.Object, java.lang.Object)
	 */
	public int getIndexOfChild(Object parent, Object child) {
		ArrayList<Integer> subCollections = 
			((Collection)parent).getSubCollectionIDs();
		Integer childID = 
			new Integer(((Collection)child).getCollectionID());
		
		for (int i = 0; i < subCollections.size(); i++)
		{
			if (childID.equals(subCollections.get(i)))
			{
				return i;
			}
		}
		return -1;
	}
	
	public Collection findCollectionNode(int collectionID) {
		if (collectionLookup.containsKey(collectionID))
			return collectionLookup.get(collectionID);
		else 
			return root;
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#addTreeModelListener(javax.swing.event.TreeModelListener)
	 */
	public void addTreeModelListener(TreeModelListener l) {
		listenerList.add(TreeModelListener.class, l);
	}

	/* (non-Javadoc)
	 * @see javax.swing.tree.TreeModel#removeTreeModelListener(javax.swing.event.TreeModelListener)
	 */
	public void removeTreeModelListener(TreeModelListener l) {
		listenerList.remove(TreeModelListener.class, l);
	}

	protected void fireTreeStructureChanged(Collection[] nodePathList)
	{
		collectionLookup.clear();
		
		Collection[] newNodeList = new Collection[nodePathList.length - 1];
		for (int i = 0; i < newNodeList.length; i++)
			newNodeList[i] = nodePathList[i];
		
		TreeModelEvent e;
		
		// Erase cached subtree... and construct node path...
		if (newNodeList.length == 0) {
			root = null;
			e = new TreeModelEvent(this, nodePathList);
		} else {
			newNodeList[newNodeList.length - 1].clearCachedChildren();
			e = new TreeModelEvent(this, newNodeList);
			retouchCache(newNodeList[newNodeList.length - 1]);
		}
		

		TreeModelListener[] listeners = 
			listenerList.getListeners(TreeModelListener.class);
	     // Process the listeners last to first, notifying
	     // those that are interested in this event
	     for (int i = 0; i < listeners.length; i++) 
	         listeners[i].treeStructureChanged(e);
	}
	
	private void retouchCache(Collection c) {
		for (int i = 0; i < getChildCount(c); i++)
			retouchCache((Collection) getChild(c, i));
	}
}
