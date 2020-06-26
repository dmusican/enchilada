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
 * The Original Code is EDAM Enchilada's KMeans class.
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
 * TODO: May need to create tables in Database to store the atom
 * id's of the particles belonging to each centroid to avoid 
 * blowing out memory
 */
package edu.carleton.enchilada.analysis.clustering;

import java.util.ArrayList;
import edu.carleton.enchilada.database.InfoWarehouse;

/**
 * KMeans uses the mean to determine the new centroids.  
 * 
 * @author andersbe
 *
 */
public class KMeans extends ClusterK 
{
	/**
	 * Constructor.  Calls the constructor for ClusterK.
	 * @param cID - collection ID
	 * @param database - database interface
	 * @param k - number of centroids desired
	 * @param name - collection name
	 * @param comment -comment to enter
	 * @param initialCentroids - how to pick the initial centroids
	 */
	public KMeans(int cID, InfoWarehouse database, int k,
			String name, String comment, ClusterK.CentroidsApproach initialCentroids, ClusterInformation c)
			{
				super(cID, database, k, 
						name.concat("KMeans"), comment, initialCentroids, c);
	}

	/** 
	 * method necessary to extend from ClusterK.  Begins the clustering
	 * process.
	 * @param - interactive or testing mode - christej
	 * @return - new collection int.
	 */
	public int cluster(boolean interactive) {
		if(interactive)
			return divide();
		else
			return innerDivide(interactive);
	}

	/**
	 * This code has been removed; it is much more efficient to accumulate
	 * a cluster as you go.
	 * @author dmusican
	 * 
	 */
	public Centroid averageCluster(
			Centroid thisCentroid,
			ArrayList<Integer> particlesInCentroid) {
		throw new UnsupportedOperationException("Averaging a k-means cluster is inefficient.");
	}
}
