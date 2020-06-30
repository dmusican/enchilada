
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
package edu.carleton.enchilada.testRow.palms;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Used to generate PALMS test data
 * @author rzeszotj based off of shaferia
 */
public class GenData {
	//should messages be output when files are overwritten?
	private boolean warn_overwrite;
	//the location to save data
	private static Path THISLOC;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	private File f;

	/**
	 * Generate sample PALMS data
	 * @param items	the number of PALMS items to write
	 * @param mzmin the minimum in the range of m/z valued to consider
	 * @param mzmax	the maximum in the range of m/z values to consider
	 * @param peaks	a list containing the m/z values at which PALMS items should have peaks (in range 1..mzlen)
	 * @param tstart	the time at which to start the particle timeseries
	 * @param tdelta	the change in time per particle
	 * @return relative pathnames of the files created: {datasetname, timeseriesname, mzname}
	 */
	public static String[] generate(
			String[] fnames, int items, int mznum, int mzscale, int[] peaks, long tstart, long tdelta) {

		try {
			THISLOC = Files.createTempDirectory("PALMS");

			GenData d = new GenData();
			d.warn_overwrite = false;
			d.writeData(fnames[0], items, mznum, mzscale, tstart, tdelta, peaks);

			for (int i = 0; i < fnames.length; ++i)
				fnames[i] = THISLOC.resolve(fnames[i] + ".txt").toString();

			return fnames;
		} catch (IOException e) {
			throw new ExceptionAdapter(e);
		}
	}

	/**
	 * Get a PrintWriter that outputs to the specified file
	 * @param fname the name of the file
	 * @return a PrintWriter on file fname
	 */
	private PrintWriter getWriter(String fname) {
		f = THISLOC.resolve(fname).toFile();

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
	 * Write the 2D matrix data file for the PALMS test data
	 * @param name	the dataset name (name and first line of file)
	 * @param items	the number of PALMS items to write to the file
	 * @param mzmin	the minimum of m/z values in the data
	 * @param mzmax the maximum of m/z values in the data
	 * @param tstart the starting time for particles
	 * @param tdelta the change in time between particles
	 * @param peaks	the m/z values at which to write non-negative values
	 *
	 */
	public void writeData(String name, int items, int mznum, int mzscale, long tstart, long tdelta, int[] peaks) {
		PrintWriter file = getWriter(name + ".txt");
		//File header - Time to write a ton of garbage
		file.println("999 9999");
		file.println("Doe, John");
		file.println("TEST Lab");
		if (mzscale == 1)
			file.println("PALMS Positive Ion Data");
		else if (mzscale == -1)
			file.println("PALMS Negative Ion Data");
		else
			file.println("PALMS GJIFJIGJ Ion Data");
		file.println("TEST Mission");
		file.println("1 1");
		file.println("1970 01 01 2008 07 09");
		file.println("0");
		file.println("TIME (UT SECONDS)");
		file.println(mznum+4);
		for(int i = 0; i < mznum+4; i++)
			file.println("1.0");
		for(int i = 0; i < mznum+4; i++)
			file.println("9.9E29");
		file.println("TOTION total MCP signal (electron units)");
		file.println("HMASS high mass integral (fraction)");
		file.println("UNLIST (unlisted low mass peaks (fraction)");
		file.println("UFO unidentified peaks (fraction)"); //Proof of alien life
		for(int i = 1; i <= mznum; i++)
			file.println("MS"+i+" (fraction)");
		int header2length = 13;
		file.println(header2length);
		for(int i = 0; i < header2length; i++)
			file.println("1.0");
		for(int i = 0; i < header2length; i++)
			file.println("9.9E29");
		//Prepare yourself for an onslaught of refuse
		file.println("AirCraftTime aircraft time (s)");
		file.println("INDEX index ()");
		file.println("SCAT scatter (V)");
		file.println("JMETER joule meter ()"); //boo
		file.println("ND neutral density (fraction)");
		file.println("SCALEA Mass scale intercept (us)");
		file.println("SCALEB mass scale slope (us)");//boo
		file.println("NUMPKS number of peaks ()");
		file.println("CONF confidence (coded)");
		file.println("CAT preliminary category ()");//boo
		file.println("AeroDiam aerodynamic diameter (um)");
		file.println("AeroDiam1p7 aero diam if density=1.7 (um)");
		file.println("TOTBACK total background subtracted (electron units)");
		file.println("0");
		file.println("0");
		// the value to write for no peak
		String nothing = "0.000000";
		//Loop through particles
		for(int i = 0; i < items; i++)
		{
			//Fill in particle header
			file.println(tstart + (tdelta*i));
			file.println(tstart + (tdelta*i) - 3);
			file.println(i+1);
			//The garbage keeps on coming
			for(int j = 0; j < 15; j++)
				file.println(Math.random());
			//Fill in peak values
			boolean peaked = false;
			for (int k = 1; k <= mznum; k++){
				for (int j = 0; j < peaks.length && !peaked; j++)
					if (k == peaks[j]){
						double randData = (int)(1000000*(j+1));
						file.println(randData/1000000);
						peaked = true;
					}
				if (!peaked)
					file.println(nothing);
				peaked = false;
			}
			//End the particle line
		}
		try{Scanner test = new Scanner(f);
			while(test.hasNext())
			{
				test.nextLine();
			}
			System.out.println("test");
		}
		catch(Exception e){}
		file.close();
	}
}
