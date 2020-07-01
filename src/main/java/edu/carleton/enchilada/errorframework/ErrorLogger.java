package edu.carleton.enchilada.errorframework;

import com.sun.tools.javac.Main;
import edu.carleton.enchilada.gui.MainFrame;

import java.awt.Component;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import javax.swing.JOptionPane;

/**
 * ErrorLogger is the class that handles all errors and exceptions that
 * the user should be aware of.  To write an error to the log, use the 
 * writeExceptionToLog method.  To write an error to a message dialog box,
 * use the displayException method.  To alert the user to any errors that have 
 * been written to the error log recently, use the flushLog method.  All methods
 * are static.
 * 
 * @author ritza
 *
 */
public class ErrorLogger {
	public static boolean testing;  /* Used for disposing of modal dialogs when
									 * simply running tests.  Set to false in
									 * Mainframe for normal use, but can be
									 * set to true in any tests that 
									 * would provoke a modal dialog. - steinbel
									 */
	public static File dir; 
	public static File file; 
	public static boolean error = false;
	public static final Path errorPath = MainFrame.userDataLocation.resolve("ErrorLogs");
	
	/**
	 * Writes the exception to the log timestamped with that particular day's 
	 * date.  The logs are in errorframework/ErrorLogs directory.
	 * @param type - type of error (SQLServer, Importing, etc.)
	 * @param message - message to write to log.
	 * @return whether or not it succeeded
	 */
	public static boolean writeExceptionToLog(String type, String message){
		file = ErrorLogger.errorPath.resolve("ErrorLog"+constructDate()+".txt").toFile();
//		file = new File("errorframework"+File.separator+"ErrorLogs"+File.separator+"ErrorLog"+constructDate()+".txt");

		try {
			if (!file.exists())
				initializeLog();
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(file, true)));
			writer.write(constructTime()+" : "+type+" Error: "+message+"\n");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Writes the exception to the log timestamped with that particular day's 
	 * date.  The logs are in errorframework/ErrorLogs directory.
	 * @param type - type of error (SQLServer, Importing, etc.)
	 * @param message - message to write to log.
	 * @return whether or not write to log succeeded
	 */
	public static boolean writeExceptionToLogAndPrompt(String type, String message){
		error=true;
		return writeExceptionToLog(type,message);
	}

	/**
	 * There are only 10 log files at a time, representing the errors collected
	 * over the past 10 days' worth of working with Enchilada.  A new error
	 * log is created when the date changes, and the oldest log is erased.
	 * @throws IOException
	 */
	private static void initializeLog() throws IOException {
		System.out.println("initializing log and deleting the oldest file.");

		dir = errorPath.toFile();

		// Verify that path for error log exists
		if (!dir.exists()) {
			boolean success = dir.mkdirs();
			if (!success)
				throw new RuntimeException("Failed to make directory to store database.");
		}

		File[] files = dir.listFiles();
		if (files != null && files.length == 10) {
			File oldestFile = files[0];
			long oldestTime = files[0].lastModified();
			for (int i=1;i<files.length;i++) {
				if (files[i].lastModified() < oldestTime) {
					oldestFile = files[i];
					oldestTime = files[i].lastModified();
				}
			}
			oldestFile.delete();
		}
		
		file = errorPath.resolve("ErrorLog"+constructDate()+".txt").toFile();
		System.out.println("Generating new error log: " + file.toString());
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write("ErrorLog: started @ "+constructDate()+"\n");
		writer.close();
	}
	
	/**
	 * Simple wrapper method to display an exception using the JOptionPane.
	 * Used for wrong user input, etc.
	 * @param parent - parent Container
	 * @param message - message to output.
	 */
	public static void displayException(Component parent, String message) {
		//unit tests don't show the dialogs because they require a response - steinbel
		if (!testing)
			JOptionPane.showMessageDialog(parent, message);
	}
	
	/**
	 * When flushLog is called, if any errors have been written since the last
	 * flushLog method call then a message is displayed alerting the user to 
	 * take a look at the error log.
	 * @param parent - parent Container
	 * @return true if errors are identified; false otherwise.
	 */
	public static boolean flushLog(Component parent) {
		if (error) {
			displayException(parent, "One or more errors has occurred.  Please" +
					" check the ErrorLog timestamped as " +constructDate()+".");
			error = false;
			return true;
		}
		return false;
		
	}
	
	/**
	 * Constructs the date (year, month and day) for timestamping the 
	 * error log files.
	 * @return the date
	 */
	private static String constructDate() {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("CST"));	
		String DATE_FORMAT = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		return sdf.format(cal.getTime());
	}
	
	/**
	 * Constructs the time (hour, minute, second) for timestamping each
	 * individual error that gets written to the log.
	 * @return the time
	 */
	private static String constructTime() {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("CST"));	
		String DATE_FORMAT = "HH:mm:ss:SSS";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		return sdf.format(cal.getTime());
	}
}