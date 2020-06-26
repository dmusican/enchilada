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
 * The Original Code is EDAM Enchilada's KMeans unit test.
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


package edu.carleton.enchilada.analysis.clustering;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.analysis.CollectionDivider;

import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.Database;
import junit.framework.TestCase;

/**
 * @author jtbigwoo
 * @version 1.0 April 23, 2009
 * @version 1.1 August 31, 2009 - added tests for preclustering
 */
public class ClusterHierarchicalTest extends TestCase {

    private ClusterHierarchical clusterer;
    private InfoWarehouse db;
    String dbName = "TestDB";
    
    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        
		new CreateTestDatabase();
		db = Database.getDatabase("TestDB");
		db.openConnection("TestDB");
		
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
		db.closeConnection();
		System.runFinalization();
		System.gc();
	    Database.dropDatabase(dbName);
    }

    // TEST DISABLED FOR NOW, as it seems as though it was never working
    public void disabledTestHierarchicalWithPreClustering() throws Exception {
    	Connection con = db.getCon();
    	Statement stmt = con.createStatement();
    	stmt.executeUpdate("update atommembership set collectionid = 2 where collectionid = 3");
    	stmt.executeUpdate("update internalatomorder set collectionid = 2 where collectionid = 3");

    	// precluster
    	int cID = 2;
        int k = 4;
        String name = "";
        String comment = "Test comment";
        ArrayList<String> list = new ArrayList<String>();
        list.add("ATOFMSAtomInfoSparse.PeakArea");
    	ClusterInformation cInfo = new ClusterInformation(list, "ATOFMSAtomInfoSparse.PeakLocation", null, false, true);
    	KMeans kmeans = new KMeans(cID,db,k,name,comment,ClusterK.CentroidsApproach.FARTHEST_DIST_CENTROIDS, cInfo);
    	kmeans.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
		kmeans.setCreateCentroids(false);
    	int dividedParticleCollectionId = kmeans.cluster(false);

    	// real clustering
    	clusterer = new ClusterHierarchical(dividedParticleCollectionId,db,name,comment,cInfo, null);
    	clusterer.setPreClustered();
    	clusterer.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
    	int collectionID = clusterer.cluster(false);

    	assertTrue(collectionID == 7);

    	Collection clusterParent = db.getCollection(7);

    	Collection cluster = db.getCollection(8);
    	assertTrue(cluster.containsData());
    	assertEquals("1", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("1", cluster.getName());
    	assertEquals(12, cluster.getParentCollection().getCollectionID()); 
    	ArrayList<Integer> particles = cluster.getParticleIDs();
       	assertEquals(2, particles.get(0).intValue());
       	assertEquals(3, particles.get(1).intValue());
       	assertEquals(9, particles.get(2).intValue());
       	assertEquals(10, particles.get(3).intValue());
       	assertEquals(4, particles.size());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(9);
    	assertTrue(cluster.containsData());
    	assertEquals("2", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("2", cluster.getName());
    	assertEquals(12, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(4, particles.get(0).intValue());
       	assertEquals(1, particles.size());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(10);
    	assertTrue(cluster.containsData());
    	assertEquals("3", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("3", cluster.getName());
    	assertEquals(14, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(5, particles.get(0).intValue());
       	assertEquals(6, particles.get(1).intValue());
       	assertEquals(7, particles.get(2).intValue());
       	assertEquals(3, particles.size());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(11);
    	assertTrue(cluster.containsData());
    	assertEquals("4", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("4", cluster.getName());
    	assertEquals(13, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(8, particles.get(0).intValue());
       	assertEquals(1, particles.size());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(12);
    	assertTrue(cluster.containsData());
    	assertEquals("1", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("1", cluster.getName());
    	assertEquals(13, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(2, particles.get(0).intValue());
       	assertEquals(3, particles.get(1).intValue());
       	assertEquals(4, particles.get(2).intValue());
       	assertEquals(9, particles.get(3).intValue());
       	assertEquals(10, particles.get(4).intValue());
       	assertEquals(5, particles.size());
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);
    	
    	cluster = db.getCollection(13);
    	assertTrue(cluster.containsData());
    	assertEquals("2", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("2", cluster.getName());
    	assertEquals(14, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);
    	
    	cluster = db.getCollection(14);
    	assertTrue(cluster.containsData());
    	assertEquals("3", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("3", cluster.getName());
    	assertEquals(7, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);

    	/* how the collection hierarchy should look now:
    	 * 7
    	 * |-14
    	 *   |-13
    	 *   | |-10 (particles 5, 6, 7)
    	 *   | |-12
    	 *   |   |-8 (particles 2, 3, 9, 10)
    	 *   |   |-9 (particle 4)
    	 *   |-11 (particle 8)
    	 */

    }
    
    public void testHierarchicalClustering() throws Exception {
        int cID = 2;
        String name = "";
        String comment = "Test comment";
        ArrayList<String> list = new ArrayList<String>();
        list.add("ATOFMSAtomInfoSparse.PeakArea");
    	ClusterInformation cInfo = new ClusterInformation(list, "ATOFMSAtomInfoSparse.PeakLocation", null, false, true);
    	clusterer = new ClusterHierarchical(cID,db,name,comment,cInfo, null);
    	
    	clusterer.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
    	int collectionID = clusterer.cluster(false);
    	
    	assertTrue(collectionID == 7);
    	
    	Collection clusterParent = db.getCollection(7);
    	assertEquals("Name: Hierarchical, Clusters Ward's,CLUST Comment: Test comment", clusterParent.getDescription());
    	
    	Collection cluster = db.getCollection(8);
    	assertTrue(cluster.containsData());
    	assertEquals("1", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("1", cluster.getName());
    	assertEquals(12, cluster.getParentCollection().getCollectionID()); 
    	ArrayList<Integer> particles = cluster.getParticleIDs();
       	assertEquals(2, particles.get(0).intValue());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(9);
    	assertTrue(cluster.containsData());
    	assertEquals("2", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("2", cluster.getName());
    	assertEquals(12, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(3, particles.get(0).intValue());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(10);
    	assertTrue(cluster.containsData());
    	assertEquals("3", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("3", cluster.getName());
    	assertEquals(13, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(4, particles.get(0).intValue());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(11);
    	assertTrue(cluster.containsData());
    	assertEquals("4", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("4", cluster.getName());
    	assertEquals(13, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(5, particles.get(0).intValue());
    	assertTrue(cluster.getSubCollectionIDs().isEmpty());
    	
    	cluster = db.getCollection(12);
    	assertTrue(cluster.containsData());
    	assertEquals("5", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("5", cluster.getName());
    	assertEquals(14, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(2, particles.get(0).intValue());
       	assertEquals(3, particles.get(1).intValue());
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);
    	
    	cluster = db.getCollection(13);
    	assertTrue(cluster.containsData());
    	assertEquals("6", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("6", cluster.getName());
    	assertEquals(14, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(4, particles.get(0).intValue());
       	assertEquals(5, particles.get(1).intValue());
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);
    	
    	cluster = db.getCollection(14);
    	assertTrue(cluster.containsData());
    	assertEquals("7", cluster.getComment());
    	assertEquals("ATOFMS", cluster.getDatatype());
    	assertEquals("7", cluster.getName());
    	assertEquals(7, cluster.getParentCollection().getCollectionID()); 
    	particles = cluster.getParticleIDs();
       	assertEquals(2, particles.get(0).intValue());
       	assertEquals(3, particles.get(1).intValue());
       	assertEquals(4, particles.get(2).intValue());
       	assertEquals(5, particles.get(3).intValue());
    	assertTrue(cluster.getSubCollectionIDs().size() == 2);
    }
}
