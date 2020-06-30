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
 * The Original Code is EDAM Enchilada's MainFrame class.
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
 * Jonathan Sulman sulmanj@carleton.edu
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


/*
 * Created on June 5, 2010
 *
 */
package edu.carleton.enchilada.chartlib;

import java.util.ArrayList;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.uci.ics.jung.graph.DelegateTree;
import edu.uci.ics.jung.graph.Tree;

public class GraphBuilder {

	Collection collection;
	Database db;
	
	public GraphBuilder(Database db, Collection coll) {
		this.collection = coll;
		this.db = db;
	}
	
	public Tree<Collection, String> getFullGraph() {
		DelegateTree<Collection, String> tree;

		tree = new DelegateTree<Collection, String>();
		
		tree.setRoot(collection);
		
		addCollectionAndAllChildrenToGraph(tree, collection, 0, 0);

		return tree;
	}
	
	public Tree<Collection, String> getSubGraph(int levels) {
		DelegateTree<Collection, String> tree;

		tree = new DelegateTree<Collection, String>();
		
		tree.setRoot(collection);
		
		addCollectionAndAllChildrenToGraph(tree, collection, levels, 0);

		return tree;
	}

	public Tree<Collection, String> getSubGraph(Collection coll, int levels) {
		collection = coll;
		return getSubGraph(levels);
	}

	private void addCollectionAndAllChildrenToGraph(DelegateTree tree, Collection parentColl, int maxLevels, int currentLevel) {
		ArrayList<Integer> collectionIDs;

		collectionIDs = db.getImmediateSubCollections(parentColl);
		for (int childCollectionID : collectionIDs) {
			Collection childColl = db.getCollection(childCollectionID);
			StringBuilder edgeLabel = new StringBuilder();
			edgeLabel.append(parentColl.getCollectionID());
			edgeLabel.append(":");
			edgeLabel.append(childCollectionID);
			tree.addChild(edgeLabel.toString(), parentColl, childColl);
			if (maxLevels == 0 || maxLevels > currentLevel) {
				addCollectionAndAllChildrenToGraph(tree, childColl, maxLevels, currentLevel + 1);
			}
		}
	}
}
