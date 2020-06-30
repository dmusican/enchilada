package edu.carleton.enchilada.dataImporters;

import java.awt.Frame;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.*;
import java.util.*;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;


import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.TSBulkInserter;

import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.errorframework.WriteException;
import edu.carleton.enchilada.externalswing.ProgressTask;
import edu.carleton.enchilada.gui.MainFrame;

/**
 * TSImport.java - Import a list of CSV files to the database.
 * @author smitht
 * @author jtbigwoo
 */

/*
 * Complete list of needed SQLServerDatabase methods:
 * getNextID - get new AtomID
 * createEmptyCollectionAndDataset
 * insertParticle
 * getCollection? what's that?
 */

public class TSImport{
	// variables to keep track of the state of the importing
	public static final int NORMAL = 0;
	public static final int INTERRUPTED = 1;
	public static final int FAILED = 2;
	public static final int PARSEERROR = 3;
	private int status = TSImport.NORMAL;
	public int choice;
	
	private Database db;
	
	private Frame parent;
	
	private ProgressTask convTask;
	
	public static final String defaultDFString = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat defaultDateFormatter = new SimpleDateFormat(defaultDFString);
    
    public static final String excelWithSecondsDFString = "MM/dd/yyyy HH:mm:ss";
    private static final SimpleDateFormat excelWithSecondsDateFormatter = new SimpleDateFormat(excelWithSecondsDFString);

    public static final String excelDFString = "MM/dd/yyyy HH:mm";
    private static final SimpleDateFormat excelDateFormatter = new SimpleDateFormat(excelDFString);
    private SimpleDateFormat dateFormatter = null;
    
    private static final SimpleDateFormat humanFormatter = new SimpleDateFormat("MMM d, yyyy, hh:mm:ss a");

    public boolean interactive;
    public File task;
	public String prefix;
	public BufferedReader in;
	//the task file
	public String tf;
    
    public TSImport(Database db, Frame parent, boolean interactive) {
    	super();
    	this.parent = parent;
    	this.db = db;
    	this.interactive = interactive;
	}
    
	/*
	* this is the non gui version of readTaskFile()
	* it's used for testing purposes
	* @author christej
	*/
    public void readTaskFileNonGui(BufferedReader in){
    	int line_no = 0;
		String line;
		try {
			// Made it so if a "" is encountered, while loop ends 
			// (i.e. lines at the end of the 
			System.out.println(in);
			while((line = in.readLine()) != null){
				line = line.trim();
				if (line.equals("")) continue;
				if(line.charAt(0) == '#') continue;
				line_no++;
				String[] args = line.split("\\s*,\\s*");
				System.out.println("args " + args);
				System.out.println("tf " + tf);
				System.out.println("line_no " + line_no);
				System.out.println("prefix " + prefix);
				process(args, tf, line_no, prefix);
			}
		} catch (ParseException e){
			//this message needs to get passed back to the gui, but run can't throw an Exception,
			// so instead just set the status
			status = TSImport.PARSEERROR;
		} catch (InterruptedException e) {
			status = TSImport.INTERRUPTED;
		} catch (Exception e) {
			status = TSImport.FAILED;
			System.err.println(e.toString());
			System.err.println("Exception while converting data!");
			e.printStackTrace();
			ErrorLogger.writeExceptionToLogAndPrompt("TSImport","Exception while converting data: " +e.toString());
		}
    	
    }
    public boolean readTaskFile(String task_file) throws DisplayException, WriteException,  
    UnsupportedFormatException{
    	final String tf = task_file;
    	if (! tf.endsWith("task")) {
    		// They haven't given us a task file!
    		throw new DisplayException("Currently, to import any CSV" +
    				" file, a .task file must be used." +
    				"  See 'importation files\\demo.task' in the installation " +
    				"directory.");
    	}
    	
    	task = new File(task_file);
    	prefix = task.getParent();
    	final BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(task_file));
		} catch (FileNotFoundException e1) {
			throw new WriteException(task_file+" is not found.  Please check the file name");
		}
    	
		// interactive mode--i.e. normal mode
		if(interactive){
			convTask = new ProgressTask(parent, 
					"Importing CSV Files", true) {
				public void run() {  			
					pSetInd(true);
					this.pack();
					readTaskFileNonGui(in);
				}
			};
			// Since we called ProgressTask as a modal dialog, this call to .start()
			// does not return until the task is completed, but the GUI gets 
			// redrawn as needed anyway.  
			convTask.start();
		}
		
		//for non interactive mode, i.e. testing mode
		else{
			readTaskFileNonGui(in);			
		}
		
    	// throw an error all the way back to the gui if there was a Date format error
    	if(status==TSImport.PARSEERROR){
    		throw new UnsupportedFormatException("Improper Date Format");
    	}
    	return status==TSImport.NORMAL;
    }
    
    public boolean readCSVFile(final String csvFile)throws UnsupportedFormatException{
    	convTask = new ProgressTask(parent, 
    			"Importing CSV Files", true) {
    		public void run(){  			
    			pSetInd(true);
    			this.pack();
    			int line_no = 0;
    			String line;
    			try {
    				// Made it so if a "" is encountered, while loop ends 
    				// (i.e. lines at the end of the 
    					setStatus(("CSV :                           ")  // 26 spaces
    							.substring(0,25)+"...");
    					process(csvFile);
    				
    			} catch (ParseException e){
//    				this message needs to get passed back to the gui, but run can't throw an Exception,
    				// so instead just set the status
    				System.out.println("parse error caught");
    				status = TSImport.PARSEERROR;
    			} catch (InterruptedException e) {
    				status = TSImport.INTERRUPTED;
    			}catch (Exception e) {
    				status = TSImport.FAILED;
    				System.err.println(e.toString());
    				System.err.println("Exception while converting data!");
    				e.printStackTrace();
    				ErrorLogger.writeExceptionToLogAndPrompt("TSImport","Exception while converting data: " +e.toString());
    			}
    		}
    	};
    	// Since we called ProgressTask as a modal dialog, this call to .start()
    	// does not return until the task is completed, but the GUI gets 
    	// redrawn as needed anyway.  
    	convTask.start();
    	
//    	 throw an error all the way back to the gui if there was a Date format error
    	if(status==TSImport.PARSEERROR){
    		throw new UnsupportedFormatException("Improper Date Format");
    	}
    	return status==TSImport.NORMAL;
    }

    // args[0]: file name
    // args[1]: time-series column
    // args[2 ...]: value columns
    private void process(String[] args, String task_file, int line_no, String prefix)
    throws Exception{
        System.out.println("Processing "+args[0]+" ...");
		if(interactive){
			if (convTask.terminate) throw new InterruptedException("Inter");
		}
        if(args.length < 3)
            throw new Exception("Error in "+task_file+" at line "+line_no+": The correct format is FileName, TimeColumn, ValueColumn1, ...\n");
        final BufferedReader in = new BufferedReader(
        		new FileReader(prefix+File.separator+args[0]));
        String line = in.readLine();
        if(line == null || line.trim().equals(""))
            throw new Exception("Error in "+args[0]+" at line 1: The first line should be the list of column names\n");
        
        String[] column = line.split("\\s*,\\s*");
        int[] colIndex = new int[args.length];
        
        for(int i=1; i<args.length; i++){
            boolean found = false;
            for(int j=0; j<column.length; j++){
                if(args[i].equals(column[j])){
                    colIndex[i] = j; found = true; break;
                }
            }
            if(!found) throw new Exception("Error in "+args[0]+" at line 1: Cannot find column name "+args[i]+", which is defined in "+task_file+" at line "+line_no+"\n");
        }
        
	    final ArrayList<String>[] values = new ArrayList[args.length];
        for(int i=1; i<values.length; i++)
        	values[i] = new ArrayList<String>(1000);
        try {
        	while((line = in.readLine()) != null){
        		if(line.trim().equals("")) continue;
        		//System.out.println(line.trim());
        		String[] v = line.split("\\s*,\\s*");
        		for(int i=1; i<values.length; i++)
        			values[i].add(v[colIndex[i]]);
        	}
        } catch (IOException i) {
        	System.out.println(i.getMessage());
        }
        
//      Check date formatting
        final String testString = (String)values[1].get(0);
        verifyDate(testString);
        //If they say it's wrong, it's wrong so throw an exception
        if(choice == 1){
        	throw new ParseException("Invalid Date",0);
        }else if(choice == 2){ //if they cancel, cancel the whole process
        	throw new InterruptedException("Date Confirmation cancelled");
        }
        
    	for(int i=2; i<values.length; i++){
   	 		if (interactive && convTask.terminate) throw new InterruptedException("dialog closed, probably");
        	putDataset(args[i],values[1],values[i]);
    	}
    }

    // process a csv file
    private void process(String file) throws Exception{
        System.out.println("Processing "+file+" ...");
		
		if (convTask.terminate) throw new InterruptedException("Inter");
		
        final BufferedReader in = new BufferedReader(
        		new FileReader(file));
        String line = in.readLine();
        if(line == null || line.trim().equals(""))
            throw new Exception("Error in "+file+" at line 1: The first line should be the list of column names\n");
        
        String[] column = line.split("\\s*,\\s*");
        
        final ArrayList[] values = new ArrayList[column.length];
        for(int i=0; i<values.length; i++)
        	values[i] = new ArrayList<String>(1000);
        try {
        	while((line = in.readLine()) != null){
        		if(line.trim().equals("")) continue;
        		//System.out.println(line.trim());
        		String[] v = line.split("\\s*,\\s*");
        		for(int i=0; i<values.length; i++)
        			values[i].add(v[i]);
        	}
        } catch (IOException i) {
        	System.out.println(i.getMessage());
        }
        
        //Check date formatting
        final String testString = (String)values[0].get(0);
        verifyDate(testString);
        //If they say it's wrong, it's wrong so throw an exception
        if(choice == 1){
        	throw new ParseException("Invalid Date",0);
        }else if(choice == 2){ //if they cancel, cancel the whole process
        	throw new InterruptedException("Date Confirmation cancelled");
        }
        
        for(int i=1; i<values.length; i++){
        	if (convTask.terminate) throw new InterruptedException("dialog closed, probably");
            putDataset(column[i],values[0],values[i]);
        }
    }
    
    private void verifyDate(final String testString) throws ParseException{
    	Date testDate=null;
    	try{
       	testDate = defaultDateFormatter.parse(testString);
    	dateFormatter = defaultDateFormatter;
		}catch(ParseException e){
	    try{
	       	testDate = excelWithSecondsDateFormatter.parse(testString);
        	dateFormatter = excelWithSecondsDateFormatter;
        }catch(ParseException f){
		try{
			testDate = excelDateFormatter.parse(testString);
			dateFormatter = excelDateFormatter;
		}catch(ParseException g){
			dateFormatter = null;
		}
	    }
		}
		if(interactive){
			if(testDate!=null){
				final String output = humanFormatter.format(testDate);
				final TSImport thisref = this;
				try {
					SwingUtilities.invokeAndWait(new Runnable(){
						public void run(){
							thisref.choice = JOptionPane.showConfirmDialog(parent,"The date "+testString+" was interpreted as "+
								output+".  Is this correct?","Verify Date Format",JOptionPane.YES_NO_CANCEL_OPTION);
						}
					});
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				throw new ParseException("Invalid Date",0);
			}
		}
		
    }

    private void putDataset(String name, ArrayList<String> time, ArrayList<String> value)
    throws SQLException, UnsupportedFormatException, InterruptedException, ParseException
    {
    	System.out.println("Putting a dataset: " +name);
    	TSBulkInserter ins = new TSBulkInserter(db);
    	ins.startDataset(name);
    	
    	TreeMap<String, ArrayList<String>> noSparseTables = new TreeMap<String, ArrayList<String>>(); 
    	
    	for(int i=0; i<time.size(); i++) {
    		if (interactive && convTask.terminate) throw new InterruptedException("Time for the task to terminate!");
    		float nextValue = 0;
    		try{
    			Date nextDate = dateFormatter.parse(time.get(i));
    			if(nextDate==null)throw new ParseException("Invalid Date",0);
    			if(!value.get(i).equals("")){
    				nextValue = Float.parseFloat(value.get(i));
    			}
    			ins.addPoint(nextDate, nextValue);
    		}catch(NumberFormatException e){
    			System.err.println("Invalid Value: "+ value.get(i) +" was skipped at timestamp "+time.get(i));
    		} 
    		
    	
    	}
    	ins.commit();
    }

    public static void main(String[] args) {
    	Database db = Database.getDatabase(MainFrame.dbname);
    	db.openConnection();
    	
    	TSImport t = new TSImport(db, null, true);
    	
    	ArrayList<String> times = new ArrayList<String>();
    	times.add("2005-07-04 13:00:00");
    	
    	ArrayList<String> values = new ArrayList<String>();
    	values.add("37");
    	
    	try {
    		t.putDataset("WOOT", times, values);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    }
 
	public class UnsupportedFormatException extends IOException {
		public UnsupportedFormatException(String message) {
			super(message);
		}
	}
}
