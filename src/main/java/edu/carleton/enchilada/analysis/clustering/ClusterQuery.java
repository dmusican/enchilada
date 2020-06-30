package edu.carleton.enchilada.analysis.clustering;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.CollectionDivider;
import edu.carleton.enchilada.analysis.DistanceMetric;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.NonZeroCursor;

/**
 * ClusterQuery clusters a collection around a list of centroids chosen by the user
 * @author christej
 * @author turetske
 *
 */
public class ClusterQuery extends Cluster {

	private ArrayList<Centroid> centroids; //the list of centroids
	Float distance;  //the minimum distance a particle can be from a centroid before it's assigned to it
	int collectionID;
	private ArrayList<String> centroidFilenames; //original filenames of the centroids
	DistanceMetric distanceMetric = DistanceMetric.EUCLIDEAN_SQUARED;
	public ClusterQuery(int cID, Database database, String name, String comment, boolean norm) {
		super(cID, database, name, comment, norm);
		// TODO Auto-generated constructor stub
	}
	public ClusterQuery(int cID, Database database, String name, String comment, boolean norm, ArrayList<String> filenames, Float d) {
		super(cID, database, name, comment, norm);
		distance = d;
		centroids = new ArrayList<Centroid>();
		centroidFilenames = new ArrayList<String>();
		collectionID = cID;
		parameterString = name.concat(super.folderName);
		readFiles(filenames);
	}
	
	/**
	 * Reads particle files and adds them to the list of centroids
	 * @param filenames
	 */
	private void readFiles(ArrayList<String> filenames) {
		Scanner scanner;
		BinnedPeakList peakList;
		String[] peak;
		Centroid centroid;
		for (int i = 0; i < filenames.size(); i++){
			try{
				scanner = new Scanner(new File(filenames.get(i)));
				peakList = new BinnedPeakList();
				if(scanner.hasNextLine()){
					centroidFilenames.add(scanner.nextLine());
				}
				while(scanner.hasNextLine()){
				//	peakList.add(scanner.next)
				//	System.out.println(scanner.next());
					peak = scanner.nextLine().split(",");
					for(int j=0;j<peak.length;j++){
						System.out.println(j + " " + peak[j]);
					}
					if(peak.length>1)
						peakList.add(Float.valueOf(peak[0]), Float.valueOf(peak[1]));
				}
				peakList.normalize(distanceMetric);
				centroids.add(new Centroid(peakList, 0));
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	@Override
	public int divide() {
		super.assignAtomsToNearestCentroid(centroids, curs, (double)distance, true, false, true);
		// ClusterQuery now saves its center atoms -- MM 2014
		// this seems redundant, but it didn't save centroids until this line was added...
		super.createCenterAtoms(centroids, subCollectionIDs);
		return 0;
	}
	public boolean setCursorType(int type) 
	{
		System.out.println("collectionID " + collectionID);
		//System.out.println("keyColumn " + clusterInfo.keyColumn);
		switch (type) {
		case CollectionDivider.DISK_BASED :
			System.out.println("DISK_BASED");
			try {
					curs = new NonZeroCursor(db.getBPLOnlyCursor(db.getCollection(collectionID)));
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		return true;
		case CollectionDivider.STORE_ON_FIRST_PASS : 
		    System.out.println("STORE_ON_FIRST_PASS");
			curs = new NonZeroCursor(db.getMemoryClusteringCursor(db.getCollection(collectionID), clusterInfo));
		return true;
		default :
			return false;
		}
	}
	
}
