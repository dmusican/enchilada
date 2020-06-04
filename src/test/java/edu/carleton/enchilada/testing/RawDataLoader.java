package edu.carleton.enchilada.testing;

import java.io.*;
import java.sql.*;
import java.util.*;
import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.gui.MainFrame;

/**
 * A utility for importing and exporting the entire database without using SQL Server backups
 * Useful for transferring data quickly between Enchilada running SQL Server 2000 and SQL Server 2005.
 * Data is stored as flat-text files in a specified directory.
 * 
 * See the constructor for usage details
 * @author shaferia
 */
public class RawDataLoader {
	//The database to import/export from
	private InfoWarehouse db;
	//a list of Enchilada's tables, created at run-time
	private ArrayList<String> tables;
	//the extension to add to saved bulk files
	private String extension = ".txt";
	
	public static boolean LOAD = true;
	public static boolean SAVE = false;
	
	/**
	 * Test using the RawDataLoader to import a database from C:\test
	 * @param args not used
	 */
	public static void main(String[] args) {
		InfoWarehouse database = Database.getDatabase();
		database.openConnection();
		RawDataLoader load = new RawDataLoader(database, "C:\\test", LOAD);
	}
	
	/**
	 * Use RawDataLoader to either import or export the database from the specified location
	 * @param db the database to import from / export to
	 * @param location the directory location to use
	 * @param load the requested action: load or save the database?
	 */
	public RawDataLoader(InfoWarehouse db, String location, boolean load) {
		this.db = db;
		tables = getTables();
		
		System.out.println("Found tables:");
		for (String table : tables)
			System.out.println("\t" + table);
		System.out.println();
		
		File f = new File(location);
		if (!f.exists())
			f.mkdir();
		
		if (load)
			load(f.getPath());
		else
			save(f.getPath());
		
		System.out.println();
		System.out.println("Process complete.");
	}
	
	/**
	 * Load the database from the given location
	 * @param location a directory containing text files with database contents
	 */
	private void load(String location) {
		File f = new File(location);
		
		if (f.isDirectory()) {
			for (String table : tables) {
				try {
					//first remove all existing data
					db.getCon().createStatement().execute("delete from " + table);
				}
				catch (SQLException ex) {
					ex.printStackTrace();
				}
				
				String curf = f.getPath() + File.separator + table + extension;
				System.out.println("Loading from " + curf);
				loadFile(table, curf);
				System.out.println("\t...done.");
			}
		}
		else {
			System.err.println("Load location must be a directory!");
		}
	}
	
	/**
	 * Save the database to the given location
	 * @param location a directory to save text files with database contents
	 */
	private void save(String location) {
		File f = new File(location);
		
		if (f.isDirectory()) {
			for (String table : tables) {
				String curf = f.getPath() + File.separator + table + extension;
				System.out.println("Saving to " + curf);
				saveFile(table, curf);
				System.out.println("\t...done.");
			}
		}
		else {
			System.err.println("Save location must be a directory!");
		}
	}
	
	/**
	 * Old, not used: does not save dates in the correct format for a BULK INSERT
	 * @param table the table to save from
	 * @param fname the filename to write to
	 */
	private void saveFileManually(String table, String fname) {
		File file = new File(fname);
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			
			String query = "select * from " + table;
			ResultSet rs = db.getCon().createStatement().executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			
			while (rs.next()) {
				for (int i = 0; i < rsmd.getColumnCount() - 1; ++i) {
					bw.write(rs.getString(i + 1));
					bw.write("\t");
				}
				bw.write(rs.getString(rsmd.getColumnCount()));
				bw.newLine();
			}
			
			bw.close();
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Bulk write a database table to a flat-text file
	 * @param table the table to collect data from
	 * @param fname the filename to write to
	 */
	private void saveFile(String table, String fname) {
		try {
			Process proc = Runtime.getRuntime().exec(
					"bcp \"USE " + MainFrame.dbname + "; SELECT * FROM " + table + "\" queryout " + fname + " -c -U SpASMS -P finally");
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Bulk load a database table from a flat-text file
	 * @param table the table to write data to
	 * @param fname the filename to gather data from
	 */
	private void loadFile(String table, String fname) {
		try {
			String query = "bulk insert " + table + " from \'" + fname + "\'";
			db.getCon().createStatement().execute(query);
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Get the list of database tables used by Enchilada that require saving/restoring
	 * @return a list of Strings corresponding to Enchilada's tables
	 */
	public ArrayList<String> getTables() {
		ArrayList<String> tables = new ArrayList<String>();
	
		try {
			//SQL Server 2005
			
			//SQL Server 2000
			String query = "select name from sysobjects where type=\'u\'";
			ResultSet rs = db.getCon().createStatement().executeQuery(query);
			
			while (rs.next()) {
				tables.add(rs.getString(1));
			}
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		return tables;
	}
}
