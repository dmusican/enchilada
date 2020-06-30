/*
 * Created on Feb 14, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package edu.carleton.enchilada.experiments;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

import edu.carleton.enchilada.database.Database;

/**
 * @author andersbe
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class HomogeneityCalculator {
	public static void main(String args[])
	{
		switch(Integer.parseInt(args[0])) {
		case 0:
			calculateParticles();
			break;
		case 1: 
			calculateDocuments();
		}
	}
	
	public static void calculateParticles() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter collectionID: ");
		int cID = sc.nextInt();
		
		Database db = Database.getDatabase();
		db.openConnection();
		
		Connection con = db.getCon();
		
		try {
			Statement stmt = con.createStatement();
			ArrayList<String> filenames = new ArrayList<String>();
			ArrayList<Integer> counts = new ArrayList<Integer>();
			
			ResultSet rs = stmt.executeQuery(
					"SELECT OrigFilename\n" +
					"FROM ATOFMSAtomInfoDense, AtomMembership\n" +
					"	WHERE AtomMembership.CollectionID = " + cID + "\n" +
					"	AND AtomMembership.AtomID = ATOFMSAtomInfoDense" +
					".AtomID");
			while (rs.next())
			{
				String filename = rs.getString(1);
				boolean nameFound = false;
				for (int i = 0; i < filenames.size(); i++)
				{
					if (filename.equals(filenames.get(i)))
					{
						counts.set(i, new Integer(counts.get(i).intValue()+1));
						nameFound = true;
					}
				}
				if (!nameFound)
				{
					filenames.add(filename);
					counts.add(new Integer(1));
				}
			}
			/*
			for (int i = 0; i < filenames.size(); i++)
			{
				rs = stmt.executeQuery("(SELECT COUNT " +
						"(AtomID)\n" +
						"		FROM AtomMembership, AtomInfo\n" +
						"			WHERE AtomMembership.CollectionID = " + cID + "\n" +
						"			AND AtomMembership.AtomID = AtomInfo.AtomID" + "\n" +
						"			AND OrigFilename = \"" + filenames.get(i) + "\"");
				System.out.println("Filename " + filenames.get(i) + 
						" occurs " + rs.getInt(1) + "times");
			}*/
			for (int i = 0; i < filenames.size(); i++)
			{
				System.out.println("Filename: " + filenames.get(i) + " = " +
						counts.get(i));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db.closeConnection();
	}
	
	public static void calculateDocuments() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter collectionID: ");
		int cID = sc.nextInt();
		
		Database db = Database.getDatabase();
		db.openConnection();
		
		Connection con = db.getCon();
		
		try {
			Statement stmt = con.createStatement();
			ArrayList<String> filenames = new ArrayList<String>();
			ArrayList<Integer> counts = new ArrayList<Integer>();
			
			ResultSet rs = stmt.executeQuery(
					"SELECT OrigFilename\n" +
					"FROM AtomInfo, AtomMembership\n" +
					"	WHERE AtomMembership.CollectionID = " + cID + "\n" +
					"	AND AtomMembership.AtomID = AtomInfo.AtomID");
			while (rs.next())
			{
				String filename = rs.getString(1);
				int truncate = filename.lastIndexOf("/");
				filename = filename.substring(0,truncate);
				boolean nameFound = false;
				for (int i = 0; i < filenames.size(); i++)
				{
					if (filename.equals(filenames.get(i)))
					{
						counts.set(i, new Integer(counts.get(i).intValue()+1));
						nameFound = true;
					}
				}
				if (!nameFound)
				{
					filenames.add(filename);
					counts.add(new Integer(1));
				}
			}
			for (int i = 0; i < filenames.size(); i++)
			{
				System.out.println("Filename: " + filenames.get(i) + " = " +
						counts.get(i));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db.closeConnection();
	}
	
}
