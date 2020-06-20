package edu.carleton.enchilada.analysis.clustering;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import junit.framework.TestCase;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.CollectionDivider;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.Normalizer;


import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.Database;

public class Art2ATest extends TestCase{
	  private Art2A art2a;
	  private Art2A art2a2;
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
			
	        int cID = 2;
	        String name = "Test clustering";
	        String comment = "Test comment";
	        ArrayList<String> list = new ArrayList<String>();
	        list.add("ATOFMSAtomInfoSparse.PeakArea");
	    	ClusterInformation cInfo = new ClusterInformation(list, "ATOFMSAtomInfoSparse.PeakLocation", null, false, true);
	    	art2a = new Art2A(cID,db,1.0f, 0.005f,25,DistanceMetric.CITY_BLOCK,comment,cInfo);
	    }

	    /*
	     * @see TestCase#tearDown()
	     */
	    protected void tearDown() throws Exception {
	        super.tearDown();
			db.closeConnection();
			System.runFinalization();
			System.gc();
		    //Database.dropDatabase(dbName);
	    }

	    public void testGetDistance() {
	        BinnedPeakList list1 = new BinnedPeakList(new Normalizer());
	        BinnedPeakList list2 = new BinnedPeakList(new Normalizer());
	        list1.add(1,0.1f);
	        list1.add(2,0.2f);
	        list2.add(1,0.3f);
	        list2.add(3,0.3f);

	        art2a.setDistanceMetric(DistanceMetric.CITY_BLOCK);
	        assertTrue(Math.round(list1.getDistance(list2,DistanceMetric.CITY_BLOCK)*100)/100. == 0.7);
	        art2a.setDistanceMetric(DistanceMetric.EUCLIDEAN_SQUARED);
	        assertTrue(Math.round(list1.getDistance(list2,DistanceMetric.EUCLIDEAN_SQUARED)*100)/100.
	                == 0.17);
	        art2a.setDistanceMetric(DistanceMetric.DOT_PRODUCT);
	        assertTrue(Math.round(list1.getDistance(list2,DistanceMetric.DOT_PRODUCT)*100)/100.
	                == 0.97);
	    }
	    
	    public void testName() {
	    	assertTrue(art2a.parameterString.equals("Art2A,V=1.0,LR=0.005,Passes=25,DMetric=" +
	    			"CITY_BLOCK,Test comment"));
	    }
	    
	    public void testArt2A() {
	    	art2a.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
	    	int collectionID = art2a.cluster();
	    	
	    	assertTrue(collectionID == 7);
	    	
	    	Collection cluster1 = db.getCollection(8);
	    	Collection cluster2 = db.getCollection(9);
	    	Collection cluster3 = db.getCollection(10);
	    	
	    	assertTrue(cluster1.containsData());
	    	assertTrue(cluster1.getComment().equals("1"));
	    	assertTrue(cluster1.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster1.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster1.getName().equals("1"));
	    	assertTrue(cluster1.getParentCollection().getCollectionID() == 7);
	    	ArrayList<Integer> particles = cluster1.getParticleIDs();
	       	assertTrue(particles.get(0) == 2);
	    	assertTrue(particles.get(1) == 3);
	    	assertTrue(cluster1.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster2.containsData());
	    	assertTrue(cluster2.getComment().equals("2"));
	    	assertTrue(cluster2.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster2.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster2.getName().equals("2"));
	    	assertTrue(cluster2.getParentCollection().getCollectionID() == 7);
	    	particles = cluster2.getParticleIDs();
	    	assertTrue(particles.get(0) == 4);
	    	assertTrue(cluster2.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster3.containsData());
	    	assertTrue(cluster3.getComment().equals("3"));
	    	assertTrue(cluster3.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster3.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster3.getName().equals("3"));
	    	assertTrue(cluster3.getParentCollection().getCollectionID() == 7);
	    	particles = cluster3.getParticleIDs();
	    	assertTrue(particles.get(0) == 5);
	    	assertTrue(cluster3.getSubCollectionIDs().isEmpty());
	    	
	    	/** Output:
				Clustering Parameters: 
				Art2A,V=1.0,LR=0.0050,Passes=25,DMetric=CITY_BLOCK,Test comment
				
				
				Number of ignored particles with zero peaks = 1
				Total clustering passes during sampling = 0
				Total number of centroid clustering passes = 0
				Total number of passes = 13
				Average distance of all points from their centers at each iteration:
				0.125
				0.125003129534889
				0.125006218906492
				0.12500928447116166
				0.12501231208443642
				0.12501531769521534
				0.12501828651875257
				0.12502123322337866
				0.12502414314076304
				0.12502703070640564
				0.12502990197390318
				0.1250327043235302
				0.12500003073364496
				average distance of all points from their centers on final assignment:
				0.12500003073364496
				
				Peaks in centroids:
				Centroid 1: Number of particles = 2
				Centroid 2: Number of particles = 1
				Centroid 3: Number of particles = 1
				
				Centroid 1:
				Number of particles in cluster: 2
				Mean size: 0.25 Std dev: +/-0.05000005
				Geometric mean size: 0.24494898
				Key:	Value:
				-30	5000.0005
				30	4857.9614
				45	142.03822
				Centroid 2:
				Number of particles in cluster: 1
				Mean size: 0.4 Std dev: +/-0.0
				Geometric mean size: 0.4
				Key:	Value:
				-30	1666.6667
				-20	1666.6667
				-10	1666.6667
				20	5000.0
				Centroid 3:
				Number of particles in cluster: 1
				Mean size: 0.5 Std dev: +/-0.0
				Geometric mean size: 0.5
				Key:	Value:
				-300	1666.6667
				-30	1666.6667
				-20	1666.6667
				6	2500.0
				30	2500.0
	    	 */
	    	
	    }
	    
	    /**
	     * This one is set up as a test that breaks if you don't normalize the
	     * positive and negative sections of the original peak lists separately
	     * (Output of this cluster should be the same as the one above if
	     * pos/neg normalization is working right.)
	     * @author jtbigwoo
	     */
	    public void testArt2APosNeg() throws Exception {
	    	Connection con = db.getCon();
	    	Statement stmt = con.createStatement();
	    	
	    	stmt.executeUpdate("UPDATE ATOFMSAtomInfoSparse set peakarea = 1 " + 
	    			"where atomid in (select atomid from atommembership where collectionid = 2) and " +
	    			" peaklocation > 0");
	    	
	    	art2a.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
	    	int collectionID = art2a.cluster();
	    	
	    	assertTrue(collectionID == 7);
	    	
	    	Collection cluster1 = db.getCollection(8);
	    	Collection cluster2 = db.getCollection(9);
	    	Collection cluster3 = db.getCollection(10);
	    	
	    	assertTrue(cluster1.containsData());
	    	ArrayList<Integer> particles = cluster1.getParticleIDs();
	       	assertTrue(particles.get(0) == 2);
	    	assertTrue(particles.get(1) == 3);
	    	assertTrue(cluster1.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster2.containsData());
	    	particles = cluster2.getParticleIDs();
	    	assertTrue(particles.get(0) == 4);
	    	assertTrue(cluster2.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster3.containsData());
	    	particles = cluster3.getParticleIDs();
	    	assertTrue(particles.get(0) == 5);
	    	assertTrue(cluster3.getSubCollectionIDs().isEmpty());
	    	
	    }

	    /**
		 * @author rzeszotj
	     * Tests whether Art2A can cluster the centers of a previously clustered collection,
	     * Results should mimic what happens with K-Means/Medians          -rzeszotj
	     */
	    public void testClusterCenters() {
	    	art2a.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
	    	int collectionID = art2a.cluster();
	    	assertTrue(collectionID == 7);
	    	
	    	System.out.println("First Clusters Generated!");
	    	
	    	int centersID = 11;
	        
	        String comment = "Test comment";
	        ArrayList<String> list = new ArrayList<String>();
	        list.add("ATOFMSAtomInfoSparse.PeakArea");
	    	ClusterInformation cInfo = new ClusterInformation(list, "ATOFMSAtomInfoSparse.PeakLocation", null, false, true);
	    	art2a2 = new Art2A(centersID,db,1.0f, 0.005f,25,DistanceMetric.CITY_BLOCK,comment,cInfo);
	    	art2a2.setCursorType(CollectionDivider.STORE_ON_FIRST_PASS);
	    	
	    	int centerClusterID = art2a2.cluster();
	    	assertTrue(centerClusterID == 12);
	    	
	    	Collection cluster1 = db.getCollection(13);
	    	Collection cluster2 = db.getCollection(14);
	    	Collection cluster3 = db.getCollection(15);
	    	
	    	assertTrue(cluster1.containsData());
	    	assertTrue(cluster1.getComment().equals("1"));
	    	assertTrue(cluster1.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster1.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster1.getName().equals("1"));
	    	assertTrue(cluster1.getParentCollection().getCollectionID() == 12);
	    	assertTrue(cluster1.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster2.containsData());
	    	assertTrue(cluster2.getComment().equals("2"));
	    	assertTrue(cluster2.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster2.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster2.getName().equals("2"));
	    	assertTrue(cluster2.getParentCollection().getCollectionID() == 12);
	    	assertTrue(cluster2.getSubCollectionIDs().isEmpty());
	    	
	    	assertTrue(cluster3.containsData());
	    	assertTrue(cluster3.getComment().equals("3"));
	    	assertTrue(cluster3.getDatatype().equals("ATOFMS"));
	    	assertTrue(cluster3.getDescription().startsWith("Key:\tValue:"));
	    	assertTrue(cluster3.getName().equals("3"));
	    	assertTrue(cluster3.getParentCollection().getCollectionID() == 12);
	    	assertTrue(cluster3.getSubCollectionIDs().isEmpty());
	    }
}
