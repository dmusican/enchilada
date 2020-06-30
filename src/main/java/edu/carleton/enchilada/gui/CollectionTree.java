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
 * The Original Code is EDAM Enchilada's CollectionTree class.
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
 * Created on Jul 20, 2004
 *
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;
import javax.swing.tree.*;
import javax.swing.event.*;

import java.awt.BorderLayout;
import java.util.ArrayList;

import edu.carleton.enchilada.collection.*;
import edu.carleton.enchilada.database.*;

/**
 * @author ritza
 *
 * CollectionTree contains the JTree of Collections.  Eventually, 
 * its functionality will include drag-and-drop.  It accesses all 
 * of the necessary information through a data model, talking to 
 * the database.
 * 
 * see http://java.sun.com/docs/books/tutorial/uiswing/
 * 											  components/tree.html
 * 
 */
public class CollectionTree extends JPanel 
	implements TreeSelectionListener 
{ 
	private JTree tree; //Collection tree
	private CollectionModel treeModel;

	private Database db;
	
	private MainFrame parentFrame = null;
	public CollectionTree(Database database, MainFrame pFrame, boolean forSynchronized) {
        super(new BorderLayout());
        
        String treeTitle = forSynchronized ? "Synchronized Time Series" : "Collections";
        int selectionMode = forSynchronized ? TreeSelectionModel.DISCONTIGUOUS_TREE_SELECTION 
        									: TreeSelectionModel.DISCONTIGUOUS_TREE_SELECTION;
        db = database;
        
        //Create a tree that allows one selection at a time.
        tree = new JTree(treeModel = new CollectionModel(db, forSynchronized));
        tree.setRootVisible(false); // hides the root.
        tree.setShowsRootHandles(true); // shows +/- expand handles on 
        								// root level nodes
        tree.getSelectionModel().setSelectionMode(selectionMode);
        tree.addTreeSelectionListener(this);
        parentFrame = pFrame;
        
        DefaultTreeCellRenderer r = new DefaultTreeCellRenderer();
        r.setLeafIcon(r.getClosedIcon());
        tree.setCellRenderer(r);
        
        add(new JLabel(treeTitle, SwingConstants.CENTER), BorderLayout.NORTH);
        add(new JScrollPane(tree), BorderLayout.CENTER);
	}
	
    
    /* TreeSelectionListener Interface Implementation */
    
    public void valueChanged(TreeSelectionEvent e) {
        Collection node = 
        	(Collection)tree.getLastSelectedPathComponent();
        if (node == null) return;
        parentFrame.collectionSelected(this, node);

    }
    
    public void clearSelection() {
    	tree.clearSelection();
    }
    
    // Fallback... update from root...
    public void updateTree() { 
    	int collectionID = ((Collection) treeModel.getRoot()).getCollectionID();
    	updateTree(collectionID); 
    }
    
    public void updateTree(int collectionID)
    {
    	Collection c = treeModel.findCollectionNode(collectionID);
    	treeModel.fireTreeStructureChanged(getCollectionsUpFrom(c));
    }
    
    /**
     * Switch which collection is being displayed in the main window.
     * @param cID	The target collection to display.
     */
    public void switchCollections(int cID){
		Collection[] path = getCollectionsUpFrom(db.getCollection(cID));
		TreePath newPath = new TreePath(path);
		tree.getSelectionModel().setSelectionPath(newPath);
    }
    
    public ArrayList<Collection> getCollectionsInTreeOrderFromRoot(int depthToStart, Collection collection) {
    	Collection[] collectionList = getCollectionsUpFrom(collection);
    	
    	ArrayList<Collection> collections = new ArrayList<Collection>();
    	
    	return getCollectionsInTreeOrderFromRootRec("", collectionList[depthToStart], collections, true);
    }
    
    private ArrayList<Collection> getCollectionsInTreeOrderFromRootRec(String prefixSoFar, 
    		Collection rootNode, ArrayList<Collection> curCollectionList, boolean isRoot) {

    	String newPrefix = "";
    	
    	if (!isRoot) {
	    	newPrefix = (prefixSoFar == "") ? rootNode.getName() : 
	    									  prefixSoFar + " : " + rootNode.getName();
	    	
	    	curCollectionList.add(new Collection(newPrefix, rootNode.getDatatype(), rootNode.getCollectionID(), db));
    	}
    	
    	for (int i = 0; i < treeModel.getChildCount(rootNode); i++) {
    		Collection node = (Collection) treeModel.getChild(rootNode, i);
    		
    		getCollectionsInTreeOrderFromRootRec(newPrefix, node, curCollectionList, false);
    	}
    	
    	return curCollectionList;
    }
    
    
    public Collection[] getCollectionsUpFrom(Collection collection) {
    	Collection root = (Collection) treeModel.getRoot();
    	
    	ArrayList<Collection> foundCollections = new ArrayList<Collection>();
    	while (collection.getCollectionID() != root.getCollectionID()) {
    		foundCollections.add(collection);
    		collection = collection.getParentCollection();
    	}
    	foundCollections.add(root);
    	
    	Collection[] ret = new Collection[foundCollections.size()];
    	for (int i = 0; i < foundCollections.size(); i++)
    		ret[i] = foundCollections.get(foundCollections.size() - i - 1);

    	return ret;
    }
    
    public Collection[] getSelectedCollections() {
    	TreePath[] tps = tree.getSelectionPaths();
    	if (tps != null) {
	    	Collection[] selected = new Collection[tps.length];
	    	for (int i = 0; i < tps.length; i++)
	    		selected[i] =  (Collection) tps[i].getLastPathComponent();
	    	
	    	return selected;
    	} else 
    		return null;
    }
    
    
    public Collection getSelectedCollection(){
    	TreePath tp = tree.getSelectionPath();
    	
    	return tp == null ? null : (Collection) tp.getLastPathComponent();
    }
}
