package edu.carleton.enchilada.experiments;
import edu.carleton.enchilada.database.*;

import java.sql.*;

/**
 * Something to make a histogram of particle size.
 * 
 * @author smitht
 *
 */

public class SizeHistogrammer {
//	private static final int collID = 24; // a 50,000 particle atofms collection
	private static final int collID = 839;
	
	
	public SizeHistogrammer() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws SQLException {
		Database db = Database.getDatabase();
		if (!db.openConnection()) throw new RuntimeException();
		
		Connection conn = db.getCon();
		Statement s = conn.createStatement();
//		s.execute("SELECT SIZE FROM ATOFMSAtomInfoDense WHERE AtomID IN " +
//				"( SELECT AtomID FROM InternalAtomOrder WHERE " +
//				"CollectionID = "+collID+" )");
		s.execute("SELECT Value FROM TimeSeriesAtomInfoDense WHERE AtomID IN "+
				"( Select AtomID FROM InternalAtomOrder WHERE " +
				"CollectionID = "+ collID+" )");
		ResultSet set = s.getResultSet();
		
		HistList histogram = new HistList(2f);
		
		while (set.next()) {
			histogram.addPeak(set.getFloat(1));
		}
		
		for (int i = 0; i < histogram.size(); i++) {
			System.out.println(histogram.getIndexMiddle(i)
					+ "\t" + histogram.get(i));
			if (histogram.getIndexMiddle(i) > 50) break;
		}
		
		db.closeConnection();
	}

}
