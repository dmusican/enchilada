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
 * The Original Code is EDAM Enchilada's ClusterDialog class.
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

package edu.carleton.enchilada.dataExporters;

import java.awt.Window;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.atom.ATOFMSAtomFromDB;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

public class MSAnalyzeDataSetExporter {

	/* window that spawned this process so we can send messages, etc. */
	Window mainFrame;

	/* Database object */
	Database db;

	ProgressBarWrapper progressBar;

	public static final String TITLE = "Exporting Data Set to MS-Analyze";

	public MSAnalyzeDataSetExporter(Window mf, Database db, ProgressBarWrapper pbar) {
		mainFrame = mf;
		this.db = db;
		progressBar = pbar;
	}

	/**
	 * Exports the data in the given collection to the given par file and the
	 * given MS-Analyze access database file.  If null is suppled for the ms
	 * analyze file, it will use the default system data source for MS-Anazyle
	 * @param coll the collection to export
	 * @param parFileName the name of the file where we'll put the basic 
	 * collection info
	 * @param msAnalyzeFileName a valid access database file or null if you
	 * want to use the default
	 * @return true if it worked
	 * @throws DisplayException
	 */
	public boolean exportToPar(Collection coll, String parFileName, String msAnalyzeFileName) throws DisplayException
	{
		String name;
		File setFile = null;
		
		if (parFileName == null) {
			return false;
		} else if (! parFileName.endsWith(".par")) {
			parFileName = parFileName + ".par";
		}
		if (! coll.getDatatype().equals("ATOFMS")) {
			throw new DisplayException("Please choose a ATOFMS collection to export to MSAnalyze.");
		}

		if (msAnalyzeFileName == null) {
			throw new DisplayException("Please specify the Access database.");
		}

		parFileName = parFileName.replaceAll("'", "");
		// parFileName is an absolute pathname to the par file.
		// the set file goes in the same directory
		File parFile = new File(parFileName);
		System.out.println(parFile.getAbsolutePath());
		
		// String name is the basename of the par file---the dataset name.
		// I've done trivial testing---spaces and periods seem to be ok
		// in this name.  I'm sure that single quotes aren't... so I remove
		// them above.
		File noExt = new File(parFileName.replaceAll("\\.par$", ""));
		name = noExt.getName(); // name has no path

		progressBar.setText("Updating MS-Analyze database");
		progressBar.setIndeterminate(true);

		Date date = null;
		try {
			date = db.exportToMSAnalyzeDatabase(coll, name, msAnalyzeFileName);
		} catch (IOException e) {
			ErrorLogger.writeExceptionToLog("Access","Error exporting to Access database.");
			e.printStackTrace();
			throw new DisplayException("Error exporting to Access database.");
		} catch (SQLException e) {
			ErrorLogger.writeExceptionToLog("Access","Error exporting to Access database.");
			e.printStackTrace();
			throw new DisplayException("Error accessing Enchilada database.");
		}
		if (date == null)
		{
			return false;
		}

		try {
			// make the par file first, then the set file
			writeParFile(parFile, name, date, coll.getComment());
			
			setFile = new File(parFileName.replaceAll("\\.par$", ".set"));
			int particleCount = 1;
			DateFormat dFormat = 
				new SimpleDateFormat("MM/dd/yyyy kk:mm:ss");
			PrintWriter out = new PrintWriter(new FileOutputStream(setFile, false));

			ATOFMSAtomFromDB particle = null;
			// The Atom iterator is essentially a thin wrapper to a 
			// Result set and thus you must call hasNext to get to the
			// next element of the interator as it links to 
			// ResultSet.next()
			CollectionCursor curs = db.getAtomInfoOnlyCursor(coll);
			
			NumberFormat nFormat = NumberFormat.getNumberInstance();
			nFormat.setMaximumFractionDigits(6);
			nFormat.setMinimumFractionDigits(6);
			
			int totalParticles = db.getCollectionSize(coll.getCollectionID());
			progressBar.setIndeterminate(false);
			progressBar.setMaximum(totalParticles);
			while (curs.next())
			{
				particle = curs.getCurrent().getATOFMSParticleInfo();

			// the number in the string (65535) is 
			// somewhat meaningless for our purposes, this is
			// the busy 
			// delay according to the MS-Analyze manual.  So I 
			// just put in a dummy value.  I looked at a dataset
			// and it had 65535 for the busy time for every single
			// particle anyway, which leads me to believe it's 
			// actually the max value of a bin in the data (2^16)
			// TODO: Test to make sure size matches the
			// scatter delay MSA produces for the same dataset
				out.println(particleCount + "," + 
						// better would be to copy all the .amz files around
						// and use relative pathnames in a non-abusive way.
//						particle.getFilename().substring(0,1) + 
//						File.separator +
						
						//XXX: hack.  MSA only understands relative pathnames:
						"..\\..\\..\\..\\..\\..\\..\\..\\..\\..\\" +
						// .substring(3) : the part of the path after "c:\".
						// How to make this platform-independent?
						// well, MS-Analyze doesn't work on linux, so there!
						particle.getFilename().substring(3) + 

						// if MS-Analyze is ever smart enough to recognize
						// absolute pathnames, get rid of the stuff above
						// and use this line.
//						particle.getFilename() + 
						", " + particle.getScatDelay() + 
						", 65535, " +
						nFormat.format(particle.getLaserPower()) + 
						", " + 
						dFormat.format(particle.getTimeStamp()));
				particleCount++;
				if(particleCount % 50 == 0 && particleCount >= 50) {
					String barText = "Exporting Item # " + particleCount + " out of " 
						+ totalParticles + " to set file";
					progressBar.increment(barText);
				}
			}
			curs.close();
			out.close();
			System.out.println("Finished exporting to MSA");
		} catch (IOException e) {
			if (setFile == null) {
				ErrorLogger.writeExceptionToLogAndPrompt("MSAAnalyze Exporter","Error writing .par file please ensure the applicaiton can write to the specified par file.");
			}
			else {
				ErrorLogger.writeExceptionToLogAndPrompt("MSAAnalyze Exporter","Error writing .set file please ensure the applicaiton can write to the specified par file.");
			}
			System.err.println("Problem writing file: ");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private void writeParFile(File parFile, String name, java.util.Date date, String comment)
		throws java.io.FileNotFoundException {
		progressBar.setText("Writing .par file");
		progressBar.setIndeterminate(true);

		DateFormat dFormat = 
			new SimpleDateFormat("MM/dd/yyyy kk:mm:ss");
		PrintWriter out = new PrintWriter(new FileOutputStream(parFile, false));
		out.println("ATOFMS data set parameters");
		out.println(name);
		out.println(dFormat.format(date));
		out.println(comment);
		out.close();
	}
}
