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
 * The Original Code is EDAM Enchilada's KMedians class.
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

import java.util.*;

import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.NonZeroCursor;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.PeakTransform;
import edu.carleton.enchilada.analysis.MedianFinder;
import edu.carleton.enchilada.analysis.Normalizer;
import edu.carleton.enchilada.analysis.DummyNormalizer;

/**
 * @author andersbe
 */
public class KMedians extends ClusterK {
	
	
	/**
	 * @param cID
	 * @param database
	 * @param initialCentroids - how to pick the initial centroids
	 */
	public KMedians(int cID, InfoWarehouse database, int k,
			String name, String comment, ClusterK.CentroidsApproach initialCentroids, ClusterInformation c)
	{
		super(cID, database, k, 
				name.concat("KMedians"), comment, initialCentroids, c);
	}

	public int cluster(boolean interactive) {
		if(interactive)
			return divide();
		else
			return innerDivide(interactive);
	}

	public Centroid averageCluster(
			Centroid origCentroids,
			ArrayList<Integer> particlesInCentroid)
	{
		CollectionCursor curs = new NonZeroCursor(db.getClusteringCursor(db.getCollection(collectionID), clusterInfo));

		ArrayList<BinnedPeakList> medianThis = 
			new ArrayList<BinnedPeakList>(particlesInCentroid.size());
		int atomID = 0;
//		 Loop through the particles in the centroid and add the areas together.
		BinnedPeakList temp;
		for (int i = 0; i < particlesInCentroid.size(); i++) 
		{
			// Using the atomID, find the atom's peak list.
			atomID = particlesInCentroid.get(i).intValue();
			// safe to use original if not transforming
			if (peakTransform != PeakTransform.NONE) {
				temp = curs.getPeakListfromAtomID(atomID).copyOf();
				temp.transformAreas(peakTransform);
			} else {
				temp = curs.getPeakListfromAtomID(atomID);
			}
			temp.normalize(distanceMetric,posNegNorm);
			medianThis.add(temp);
		}
		Centroid returnThis = null;
		MedianFinder mf = null;
		if (medianThis.size() == 0)
		{
			System.out.println("Centroid contains no particles");
			if (isNormalized)
				returnThis = new Centroid(new BinnedPeakList(new Normalizer()),
					0,origCentroids.subCollectionNum);
			else
				returnThis = new Centroid(new BinnedPeakList(new DummyNormalizer()),
						0,origCentroids.subCollectionNum);
		}
		else
		{
			mf = new MedianFinder(medianThis, isNormalized);

			// Experimental code to generate MPS file so that problem
			// can be solved as an LP instead. Runs MUCH slower this way.
//			System.out.println("Number of particles = " + medianThis.size());
//			long start = System.currentTimeMillis();
//			BinnedPeakList median = mf.getMedianSumToOne();
//			long stop = System.currentTimeMillis();
//			System.out.println("Elapsed time = " + (stop-start)/1000. + " seconds.");
//			mf.displayError(median);
//			median.printPeakList();
//			mf.makeMPS();
//			System.exit(0);
			
			//TODO: Simply use getMedian for this to work the old way
			returnThis = new Centroid(mf.getMedianSumToOne(),
					0,origCentroids.subCollectionNum);
		}
		curs.close();
		return returnThis;
	}
}