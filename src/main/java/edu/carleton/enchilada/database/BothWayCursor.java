/**
 * 
 */
package edu.carleton.enchilada.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeakList;

/**
 * @author steinbel
 * Trying a cursor that will go both forward and backwards.  (Possibilities for
 * use in actual code if previous() works out - then implement other methods.)
 */
public class BothWayCursor implements CollectionCursor {

	ResultSet rs;
	
	/**
	 * @throws SQLException 
	 * 
	 */
	public BothWayCursor(Connection con, int collID) throws SQLException {
		
		Statement stmt = con.createStatement();
		rs = stmt.executeQuery("USE TestDB2 SELECT AtomID FROM AtomMembership WHERE CollectionID = "
				+ collID);
		//can't figure out how to set this to go backwards . . . 
		//dump it into an array???  But that can't be any good because then
		//we've got it in memory . . .
		System.out.println(rs.getType() == ResultSet.TYPE_FORWARD_ONLY);
	
		
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#next()
	 */
	public boolean next() {
		try {
			rs.setFetchDirection(ResultSet.FETCH_FORWARD);
			return rs.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#getCurrent()
	 */
	public ParticleInfo getCurrent() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getAtomID(){
		int atomID = -9999;
		
		try {
			atomID = rs.getInt(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return atomID;
	}
	
	public boolean previous(){
		try {
			rs.setFetchDirection(ResultSet.FETCH_REVERSE);
			return rs.previous();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	
	/* (non-Javadoc)
	 * @see database.CollectionCursor#close()
	 */
	public void close() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#reset()
	 */
	public void reset() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#get(int)
	 */
	public ParticleInfo get(int i) throws NoSuchMethodException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#getPeakListfromAtomID(int)
	 */
	public BinnedPeakList getPeakListfromAtomID(int id) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new CreateTestDatabase2();

		System.out.println("created db.");
		
		InfoWarehouse iw = Database.getDatabase("TestDB2");
		try {
			iw.getCon();
			iw.openConnection();
			BothWayCursor curs = new BothWayCursor(iw.getCon(), 4);
			
			while (curs.next())
				System.out.println(curs.getAtomID());
			
			System.out.println("----------Now backwards---------");
			
			while (curs.previous())
				System.out.println(curs.getAtomID());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
