package edu.carleton.enchilada.experiments;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import edu.carleton.enchilada.database.Database;

/**
 * Creates a time series CSV file with any number of rows.
 * Files created are stored in C:\temp\.  Cannot be used unless
 * this is a valid directory.
 * 
 * @author ritza
 *
 **/
public class CreateDummyCSV {
	Calendar calendar;
	Random random;
	Database db;
	String filename;
	PrintWriter timeseries;
	SimpleDateFormat dateFormat;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new CreateDummyCSV();
		System.out.println("done");
	}
	
	public CreateDummyCSV() {
		calendar =  new GregorianCalendar(2004,8,4,10,0,0);
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		random = new Random(87654321);
		filename = "C:\\temp\\timeseries.csv";
		System.out.println("Writing to file: " + filename.toString());
		timeseries = null;
		try {
			timeseries = new PrintWriter(new FileWriter(filename));
		} catch (IOException e) {
			System.err.println("Trouble creating " + filename);
			e.printStackTrace();
		}
		timeseries.println("Time, Value");
		
		/***TODO Swap the parameter here to change importing particles:***/
		createTS(150);
		timeseries.close();
	}

	public void createTS(int num) {
		for (int i = 1; i <= num; i++) {
			calendar.add(Calendar.SECOND, 1);
			timeseries.println(dateFormat.format(calendar.getTime())+","+random.nextInt(1000));
		}
	}
	
	
	

}
