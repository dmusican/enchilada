package edu.carleton.enchilada.experiments;

import java.sql.*;

import edu.carleton.enchilada.database.*;

import edu.carleton.enchilada.analysis.*;

/**
 * Prints out a CSV representation of the peaks at the first 100 m/z values for each particle.
 * @author smitht
 *
 */

public class ATOFMSToCSV {

	public ATOFMSToCSV() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws SQLException {
		Database db = Database.getDatabase();
		if (!db.openConnection()) throw new RuntimeException();
		
		CollectionCursor b = db.getBinnedCursor(db.getCollection(7));
		BinnedPeakList particle;
		int partnum = 0;
		
		int maxMZ = 100;
		
		System.out.print("partnum");
		for (int i = 1; i < maxMZ; i++) {
			System.out.print("," + i);
		}
		System.out.println();
		
		while (b.next()) {
			particle = b.getCurrent().getBinnedList();
			System.out.print(partnum++);
			for (int i = 1; i < maxMZ; i++) {
				System.out.print("," + particle.getAreaAt(i));
			}
			System.out.println();
		}
		
	}

}
