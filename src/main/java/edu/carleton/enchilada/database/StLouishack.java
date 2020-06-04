package edu.carleton.enchilada.database;

import edu.carleton.enchilada.gui.MainFrame;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author steinbel
 * Hack to remove IOA.OrderNumber from the existing St.Louis db in order to avoid
 * reimporting the whole damn thing.  This class could potentially be used in
 * similiar future database-change situations, but if we think that's unlikely
 * we can just drop it.  (I was having permissions trouble with SQLServerManager,
 * which is why I did it this way.  It was faster and easier for me.)
 */
public class StLouishack {

	public StLouishack() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Database db = (Database) Database.getDatabase(MainFrame.dbname);
		db.openConnection();
		Connection con = db.getCon();
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("DROP INDEX InternalAtomOrder.Order_Index");
			stmt.executeUpdate("ALTER TABLE InternalAtomOrder DROP COLUMN OrderNumber");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db.closeConnection();
	}

}
