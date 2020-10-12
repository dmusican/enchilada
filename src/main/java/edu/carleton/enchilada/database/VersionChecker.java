package edu.carleton.enchilada.database;

/**
 * VersionChecker - see whether the program's expectations about database structure are well-founded.
 * <p>
 * Rather than relying on version strings, as this does, you might want to
 * put more rows into the DBInfo table.  Then, you can see whether the DB
 * has a specific feature, and if not, add it, if it's something addable.
 * This seems like a more reasonable test for upgradeability than one based
 * just on numbers.  Methods that test for the existence of features should
 * get added to this class.
 * 
 * @author smitht
 */


import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionChecker {
	private Database db;
	private String progVers;
	private String dbVers;
	
	/**
	 * This constructor will open a new connection to the database.
	 *
	 */
	public VersionChecker() {
		db = Database.getDatabase();
		db.openConnection();
	}

	/**
	 * This constructor uses your existing connection to the database.
	 * @param db an OPEN database connection
	 */
	public VersionChecker(Database db) {
		this.db = db;
	}
	
	/**
	 * Tests whether the database and the program have EXACTLY the same version.
	 * 
	 * @return whether the db was built by a version of the program with current expectations about the database's structure.
	 * @throws SQLException if the database is munged somehow
	 * @throws IOException if the database rebuilding file is lost or misformatted
	 */
	public boolean isDatabaseCurrent() throws SQLException, IOException {
		return dbVersion().equals(programVersion());
	}
	
	/**
	 * Returns the version number corresponding to the structure of the
	 * database.
	 * 
	 * @return the version given by the database
	 * @throws SQLException
	 */
	public String dbVersion() throws SQLException {
		if (dbVers == null) {
			dbVers = db.getVersion();
		}
		return dbVers;
	}
	
	/**
	 * Returns the version number corresponding to the expectations the program
	 * has about the database.
	 * 
	 * @return the version given by the program.
	 * @throws IOException
	 */
	public String programVersion() throws IOException {
		if (progVers == null) {
			progVers = "Sept2006.1";
		}
		return progVers;
		
	}
	
	
}
