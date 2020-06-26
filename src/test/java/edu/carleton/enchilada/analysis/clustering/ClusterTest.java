package edu.carleton.enchilada.analysis.clustering;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.InfoWarehouse;
import junit.framework.TestCase;

/**
 * It's the 173rd anniversary of the battle at the Alamo.  
 * Why not celebrate by writing a test?
 * 
 * @author jtbigwoo
 *
 */
// TODO: Add a test or two for createCenterAtoms
public class ClusterTest extends TestCase{

	private InfoWarehouse db;
	private Cluster classToTest;
    String dbName = "TestDB";
	
	public void setUp() throws Exception {
        super.setUp();
        
		new CreateTestDatabase();
		db = Database.getDatabase("TestDB");
		db.openConnection("TestDB");
		
        int cID = 2;
        int k = 2;
        String name = "";
        String comment = "Test comment";
        boolean refine = false;
        ArrayList<String> list = new ArrayList<String>();
        list.add("ATOFMSAtomInfoSparse.PeakArea");
    	ClusterInformation cInfo = new ClusterInformation(list, "ATOFMSAtomInfoSparse.PeakLocation", null, false, true);
    	classToTest = new KMeans(cID,db,k,name,comment,ClusterK.CentroidsApproach.FARTHEST_DIST_CENTROIDS, cInfo);
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

	public void testGenerateCentroidArrays() {
		ArrayList<float[]> resultList;
		// just try it with two centroids
		BinnedPeakList peakList = new BinnedPeakList();
		peakList.add(12, 57.0f);
		peakList.add(-12, 57.5f);
		Centroid center = new Centroid(peakList, 1);
		ArrayList<Centroid> list = new ArrayList<Centroid>();
		list.add(center);
		peakList = new BinnedPeakList();
		peakList.add(13, .58f);
		peakList.add(26, .42f);
		center = new Centroid(peakList, 1);
		list.add(center);
		resultList = Cluster.generateCentroidArrays(list, Cluster.ARRAYOFFSET);
		float[] resultArray = resultList.get(0);
		assertEquals(57.5f, resultArray[-12 + Cluster.ARRAYOFFSET]);
		assertEquals(57.0f, resultArray[12 + Cluster.ARRAYOFFSET]);
		resultArray = resultList.get(1);
		assertEquals(0f, resultArray[-13 + Cluster.ARRAYOFFSET]);
		assertEquals(.58f, resultArray[13 + Cluster.ARRAYOFFSET]);
		assertEquals(.42f, resultArray[26 + Cluster.ARRAYOFFSET]);
	}
	
	public void testcreateCenterAtoms() throws Exception {
		// fake some data in the db
		makeFakeClusters();
		
		// put in the coll id's that contain the particles
		ArrayList<Integer> clusterIds = new ArrayList<Integer>(2);
		clusterIds.add(new Integer(8));
		clusterIds.add(new Integer(9));

		// set up dummy cluster centers
		ArrayList<Centroid> centers = new ArrayList<Centroid>(2);
		BinnedPeakList peakList = new BinnedPeakList();
		peakList.add(12, 5000.0f);
		peakList.add(-12, 4000.5f);
		peakList.add(18, 999.5f);
		Centroid center = new Centroid(peakList, 3, 1);
		centers.add(center);
		peakList = new BinnedPeakList();
		peakList.add(13, .58f);
		peakList.add(26, .42f);
		center = new Centroid(peakList, 1, 2);
		centers.add(center);

		classToTest.createCenterAtoms(centers, clusterIds);

		// check center atoms table
    	Statement stmt = db.getCon().createStatement();
    	int atomId = 0;
    	ResultSet rs = stmt.executeQuery("SELECT AtomId FROM CenterAtoms WHERE CollectionId = 8");
    	if (rs.next())
    		atomId = rs.getInt("AtomId");
    	else
    		fail("Failed to create an entry in the CenterAtoms table for the first cluster center");
    	// check sparse info table
    	CollectionCursor curs = db.getAtomInfoOnlyCursor(db.getCollection(8));
    	BinnedPeakList peaks = curs.getPeakListfromAtomID(atomId);
    	assertEquals("Adding the centroid to the db should not have changed the number of peaks", 3, peaks.getPeaks().size());
    	assertEquals(5000f, peaks.getAreaAt(12));
    	assertEquals(4000f, peaks.getAreaAt(-12));
    	assertEquals(999f, peaks.getAreaAt(18));
    	rs = stmt.executeQuery("SELECT RelPeakArea FROM ATOFMSAtomInfoSparse WHERE AtomId = " + atomId + " ORDER BY PeakLocation");
    	rs.next();
    	assertEquals(.40005f, rs.getFloat("RelPeakArea"));
    	rs.next();
    	assertEquals(.5f, rs.getFloat("RelPeakArea"));
    	rs.next();
    	assertEquals(.09995f, rs.getFloat("RelPeakArea"));
    	// check dense info table
    	rs = stmt.executeQuery("SELECT * FROM ATOFMSAtomInfoDense WHERE AtomId = " + atomId);
    	if (rs.next()) {
    		assertEquals(10.0f/3.0f, rs.getFloat("LaserPower"));
    		assertEquals(.033333333f, rs.getFloat("Size"));
    		assertEquals(3, rs.getInt("ScatDelay"));
    	}

    	// second center
    	// check center atoms table
    	rs = stmt.executeQuery("SELECT AtomId FROM CenterAtoms WHERE CollectionId = 9");
    	if (rs.next())
    		atomId = rs.getInt("AtomId");
    	else
    		fail("Failed to create an entry in the CenterAtoms table for the second cluster center");
    	// check sparse info table
    	curs = db.getAtomInfoOnlyCursor(db.getCollection(9));
    	peaks = curs.getPeakListfromAtomID(23);
    	assertEquals("Adding the centroid to the db should not have changed the number of peaks", 2, peaks.getPeaks().size());
    	assertEquals(0f, peaks.getAreaAt(13));
    	assertEquals(0f, peaks.getAreaAt(26));
    	rs = stmt.executeQuery("SELECT RelPeakArea FROM ATOFMSAtomInfoSparse WHERE AtomId = " + atomId + " ORDER BY PeakLocation");
    	rs.next();
    	assertEquals(.58f, rs.getFloat("RelPeakArea"));
    	rs.next();
    	assertEquals(.42f, rs.getFloat("RelPeakArea"));
    	// check dense info table
    	rs = stmt.executeQuery("SELECT * FROM ATOFMSAtomInfoDense WHERE AtomId = " + atomId);
    	if (rs.next()) {
    		assertEquals(4.0f, rs.getFloat("LaserPower"));
    		assertEquals(.04f, rs.getFloat("Size"));
    		assertEquals(4, rs.getInt("ScatDelay"));
    	}
    	
    	stmt.close();
	}
	
	private void makeFakeClusters() throws SQLException{
		//write clusters directly to the db (in order to isolate the
		//method we're testing here)
		Statement stmt = db.getCon().createStatement();
		stmt.addBatch("INSERT INTO Collections \n"
			+ " VALUES (8,'1','1','dummy first cluster','ATOFMS')");
		stmt.addBatch("INSERT INTO Collections \n"
			+ " VALUES (9,'2','2','dummy second cluster','ATOFMS')");
		stmt.addBatch("INSERT INTO AtomMembership \n"
			+ " VALUES (8,2)");
		stmt.addBatch("INSERT INTO AtomMembership \n"
			+ " VALUES (8,3)");
		stmt.addBatch("INSERT INTO AtomMembership \n"
			+ " VALUES (8,5)");
		stmt.addBatch("INSERT INTO AtomMembership \n"
			+ " VALUES (9,4)");
		stmt.addBatch("INSERT INTO CollectionRelationships \n"
			+ " VALUES (7,8)");
		stmt.addBatch("INSERT INTO CollectionRelationships \n"
			+ " VALUES (7,9)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (2,7)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (3,7)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (4,7)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (5,7)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (2,8)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (3,8)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (5,8)");
		stmt.addBatch("INSERT INTO InternalAtomOrder \n"
			+ " VALUES (4,9)");
		stmt.executeBatch();
		stmt.close();
	}

	public void testIntersperse(){
		
		assertTrue(Cluster.intersperse("22.4", "").
				equals("22.4"));
		assertTrue(Cluster.intersperse("multiple words", "").
				equals("'multiple words'"));
		assertTrue(Cluster.intersperse("13.1", "22.4").
				equals("22.4, 13.1"));
		assertTrue(Cluster.intersperse("more words", "'words'").
				equals("'words', 'more words'"));
		assertTrue(Cluster.intersperse("mixed numbers and words", "77.7, 'string'").
				equals("77.7, 'string', 'mixed numbers and words'"));
	}

}
