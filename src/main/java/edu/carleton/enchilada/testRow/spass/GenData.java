/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is EDAM Enchilada's Database class.
 *
 * The Initial Developer of the Original Code is
 * The EDAM Project at Carleton College.
 * Portions created by the Initial Developer are Copyright (C) 2005
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * Ben J Anderson andersbe@gmail.com
 * David R Musicant dmusican@carleton.edu
 * Anna Ritz ritza@carleton.edu
 * Greg Cipriano gregc@cs.wisc.edu
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */
package testRow.spass;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Used to generate SPASS test data
 * @author rzeszotj based off of shaferia
 */
public class GenData {
	//should messages be output when files are overwritten?
	private boolean warn_overwrite;
	//the location to save data
	public static final int[] peakVals = {10,20,30,40,50,60,70,80,99};
	private static String THISLOC = "testRow/spass/";
	private SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	private File f;
	
	/**
	 * Use for one-time generation of data
	 * @param args	not used.
	 */
	public static void main(String[] args) {
		GenData d = new GenData();
		d.warn_overwrite = true;

		int items = 20;
		int mzmin = -10;
		int mzmax = 10;
		long tstart = 3114199800l;
		long tdelta = 600;
		
		d.writeData("Test", items, mzmin, mzmax, tstart, tdelta, new int[]{2, 5, 6});
	}
	
	/**
	 * Generate sample SPASS data
	 * @param items	the number of SPASS items to write
	 * @param mzmin the minimum in the range of m/z valued to consider
	 * @param mzmax	the maximum in the range of m/z values to consider
	 * @param peaks	a list containing the m/z values at which SPASS items should have peaks (in range 1..mzlen)
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the change in time per particle
	 * @return relative pathnames of the files created: {datasetname, timeseriesname, mzname}
	 */
	public static String[] generate(
			String[] fnames, int items, int mzmin, int mzmax, int[] peaks, long tstart, long tdelta) {
		GenData d = new GenData();
		d.warn_overwrite = false;
		
		d.writeData(fnames[0], items, mzmin, mzmax, tstart, tdelta, peaks);

		for (int i = 0; i < fnames.length; ++i)
			fnames[i] = THISLOC + fnames[i] + ".txt";
		
		return fnames;
	}
	
	/**
	 * Get a PrintWriter that outputs to the specified file
	 * @param fname the name of the file
	 * @return a PrintWriter on file fname
	 */
	private PrintWriter getWriter(String fname) {
		f = new File(THISLOC + fname);
		
		if (warn_overwrite && f.exists())
			System.out.printf("Warning: file %s already exists; overwriting.\n", fname);
		
		try {
			return new PrintWriter(new BufferedWriter(new FileWriter(f)));
		}
		catch (IOException ex) {
			System.out.printf("Error: Could not create file %s\n", fname);
			ex.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Write the 2D matrix data file for the SPASS test data
	 * @param name	the dataset name (name and first line of file)
	 * @param items	the number of SPASS items to write to the file
	 * @param mzmin	the minimum of m/z values in the data
	 * @param mzmax the maximum of m/z values in the data
	 * @param tstart the starting time for particles
	 * @param tdelta the change in time between particles
	 * @param peaks	the m/z values at which to write non-negative values
	 * 
	 */
	public void writeData(String name, int items, int mzmin, int mzmax, long tstart, long tdelta, int[] peaks) {
		PrintWriter file = getWriter(name + ".txt");
		//File header
		
		file.print("Filename\t");
		file.print("Date\t");
		file.print("Acquisition #\t");
		file.print("Diameter\t");
		//Peak positions
		for (int i = mzmin; i <= mzmax; i++)
			file.print(i+"\t");
		file.println();
		
		// the value to write for no peak
		int nothing = 0;
		
		String fileLoc = "C:/abcd/"+name+".txt\t";
		Date tempDate;
		for (int i = 0; i < items; i++) {//Loop through particles
			tempDate = new Date(tstart);
			tstart += tdelta;
			//Fill in particle header
			file.print(fileLoc);
			file.print(dateFormat.format(tempDate) + "\t");
			file.print(i+1+"\t");
			double t = (double)(i)/10;
			file.print(t+"\t");
			
			//Fill in peak values
			boolean peaked = false;
			for (int k = mzmin; k <= mzmax; k++){
				for (int j = 0; j < peaks.length && !peaked; j++) {
					if (k == peaks[j]) {
						file.print(peakVals[j%peakVals.length]+"\t");
						peaked = true;
					}	
				}
				if (!peaked){
					if (k == mzmax)
						file.print(nothing);
					else
						file.print(nothing+"\t");
				}					
				peaked = false;
			}
			//End the particle line
			file.println();
		}
		
		try{Scanner test = new Scanner(f);
			while(test.hasNext())
			{System.out.println(test.nextLine());}
			System.out.println("test");
		}
		catch(Exception e){}
		file.close();
	}
}
