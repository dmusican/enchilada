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
 * Tom Bigwood tom.bigwood@nevelex.com
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

import com.opencsv.CSVWriter;
import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeak;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.NonZeroCursor;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.gui.ExportHierarchyCSVDialog;
import edu.carleton.enchilada.gui.ProgressBarWrapper;

import java.io.*;
import java.awt.Window;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Exports the peak list for a particle or set of particles to a file as comma-
 * separated values.
 * 2009-03-12
 * @author jtbigwoo
 */

public class CSVDataSetExporter {

	/* window that spawned this process so we can send messages, etc. */
	Window mainFrame;

	/* Database object */
	Database db;

	ProgressBarWrapper progressBar;
	
	private boolean onePerFile = false;

	public static final String TITLE = "Exporting Data Set to File";

	public CSVDataSetExporter(Window mf, Database db, ProgressBarWrapper pbar) {
		mainFrame = mf;
		this.db = db;
		progressBar = pbar;
	}

	/**
	 * Exports the peak list data for the supplied collection
	 * @param coll the collection of the particles we want to 
	 * export
	 * @param fileName the path to the file that we're going to create
	 * @param maxMZValue this is the maximum mass to charge value to export
	 * (we often filter out the largest and smallest mass to charge values
	 * @return true if it worked
	 */
	public boolean exportToCSV(Collection coll, String fileName, int maxMZValue)
		throws DisplayException {
		double mzConstraint = new Double(maxMZValue);
		boolean showingNegatives;
		CollectionCursor atomInfoCur;
		ParticleInfo particleInfo;
		
		if (fileName == null) {
			return false;
		} else if (! fileName.endsWith(ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION)) {
			fileName = fileName + "." + ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION;
		}
		if (! coll.getDatatype().equals("ATOFMS")) {
			throw new DisplayException("Please choose a ATOFMS collection to export.");
		}

		fileName = fileName.replaceAll("'", "");

		try {
	
			progressBar.setText("Exporting peak data");
			progressBar.setIndeterminate(true);
			
			atomInfoCur = db.getAtomInfoOnlyCursor(coll);

			ArrayList<ParticleInfo> particleList = new ArrayList<ParticleInfo>();
			int fileIndex = 0;
			while (atomInfoCur.next()) {
				particleInfo = atomInfoCur.getCurrent();
				particleInfo.setBinnedList(atomInfoCur.getPeakListfromAtomID(particleInfo.getID()));
				particleList.add(particleInfo);
				if (onePerFile) {
					writeOutSingleParticle(particleInfo, fileName, fileIndex++);
				} else if (particleList.size() == 127) {
					writeOutParticlesToFile(particleList, fileName, fileIndex++, maxMZValue);
					particleList.clear();
				}
			}
			if (particleList.size() > 0 && !onePerFile)
			{
				writeOutParticlesToFile(particleList, fileName, fileIndex++, maxMZValue);
			}
		} catch (IOException e) {
			ErrorLogger.writeExceptionToLogAndPrompt("CSV Data Exporter","Error writing file please ensure the application can write to the specified file.");
			System.err.println("Problem writing file: ");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	// Writes out a single particle in sparse list format; suitable for Cluster Query
	// Michael Murphy 2014
	private void writeOutSingleParticle(ParticleInfo particle, String fileName, int fileIndex) throws IOException {
		PrintWriter out = null;
		File csvFile;
		DecimalFormat formatter = new DecimalFormat("0.00");

		if (fileIndex == 0)
		{
			csvFile = new File(fileName);
		}
		else
		{
			csvFile = new File(fileName.replace(".csv", "_" + fileIndex + ".csv"));
		}
		
		out = new PrintWriter(new FileOutputStream(csvFile, false));
		
		StringBuffer sbHeader = new StringBuffer();
		sbHeader.append("AtomID: "+particle.getID());
		out.println(sbHeader.toString());
		
		StringBuffer sbPeaks = new StringBuffer();
		
		Map.Entry<Integer, Float> entry;
		Iterator<Map.Entry<Integer, Float>> iterator;

		iterator = particle.getBinnedList().getPeaks().entrySet().iterator();
		while (iterator.hasNext()) {
			entry = iterator.next();
			sbPeaks.append(entry.getKey()+","+formatter.format(entry.getValue()));
			out.println(sbPeaks.toString());
			sbPeaks.setLength(0);
		}
		out.close();
	}

	/**
	 * Takes a list of ParticleInfo objects and writes it out to a file.
	 * You probably want to limit your list to 127 items since that's all that
	 * Excel will display.  If you have more than 127 items, call this method
	 * repeatedly using increasing file indices.
	 * @param particleList the list of particles to write out
	 * @param fileName the name of the file to write out.  please name it 
	 * &lt;filename&gt;.csv
	 * @param fileIndex the index if we need to name more than one file.  
	 * e.g. fileIndex = 0 would be filename.csv, fileIndex = 1 would be 
	 * filename_1.csv
	 * @param maxMZValue the largest (and smallest) mz value to export, e.g.
	 * if you choose maxMZValue=200, m/z = +201 and m/z = -201 will not be included
	 * @throws IOException
	 */
	private void writeOutParticlesToFile(ArrayList<ParticleInfo> particleList, String fileName, int fileIndex, int maxMZValue)
		throws IOException {
		PrintWriter out = null;
		File csvFile;
		DecimalFormat formatter = new DecimalFormat("0.00");
		ArrayList<BinnedPeak> currentPeakForAllParticles;
		ArrayList<Iterator<BinnedPeak>> peakLists;

		if (fileIndex == 0)
		{
			csvFile = new File(fileName);
		}
		else
		{
			csvFile = new File(fileName.replace(".csv", "_" + fileIndex + ".csv"));
		}

		out = new PrintWriter(new FileOutputStream(csvFile, false));
		currentPeakForAllParticles = new ArrayList<BinnedPeak>(particleList.size());
		peakLists = new ArrayList<Iterator<BinnedPeak>>(particleList.size());
		StringBuffer sbHeader = new StringBuffer();
		StringBuffer sbSubHeader1 = new StringBuffer();
		StringBuffer sbSubHeader2 = new StringBuffer();
		StringBuffer sbNegLabels = new StringBuffer();
		StringBuffer sbPosLabels = new StringBuffer();
		for (ParticleInfo particleInfo : particleList) {
			sbHeader.append(particleInfo instanceof AverageParticleInfo ? "****** Collection: " : "****** Particle: ");
			String choppedName = particleInfo instanceof AverageParticleInfo ? ((AverageParticleInfo)particleInfo).getCollection().getName() : particleInfo.getATOFMSParticleInfo().getFilename(); 
			choppedName = choppedName.indexOf('\\') > 0 ? choppedName.substring(choppedName.lastIndexOf('\\') + 1) : choppedName;
			sbHeader.append(choppedName);
			sbHeader.append(" ******,,");
			if (particleInfo instanceof AverageParticleInfo) {
				sbSubHeader1.append("Collection ID: ");
				sbSubHeader1.append(((AverageParticleInfo)particleInfo).getCollection().getCollectionID());
				sbSubHeader1.append(",,");
				sbSubHeader2.append("Parent Collection ID: ");
				sbSubHeader2.append(((AverageParticleInfo)particleInfo).getCollection().getParentCollection().getCollectionID());
				sbSubHeader2.append(",,");
			}
			Iterator<BinnedPeak> peaks = particleInfo.getBinnedList().iterator();
			BinnedPeak peak = null;
			while (peaks.hasNext()) {
				peak = peaks.next();
				if (peak.getKey() >= -maxMZValue)
					break;
			}
			if (peak == null || peak.getKey() < -maxMZValue)
				peak = null;
			currentPeakForAllParticles.add(peak);
			peakLists.add(peaks);
			sbNegLabels.append("Negative Spectrum,,");
			sbPosLabels.append("Positive Spectrum,,");
		}
		out.println(sbHeader.toString());
		if (sbSubHeader1.length() > 0) {
			out.println(sbSubHeader1.toString());
			out.println(sbSubHeader2.toString());
		}
		out.println(sbNegLabels.toString());

		boolean showingNegatives = true;
		StringBuffer sbValues = new StringBuffer();
		
		for (int location = -maxMZValue; location <= maxMZValue; location++) {
			if (showingNegatives && location >= 0) {
				showingNegatives = false;
				out.println(sbPosLabels.toString());
			}
			for (int particleIndex = 0; particleIndex < peakLists.size(); particleIndex++) {
				BinnedPeak peak = currentPeakForAllParticles.get(particleIndex);
				if (peak == null || location < peak.getKey()) {
					sbValues.append(location);
					sbValues.append(",0.00,");
				}
				else {
					// write it out
					sbValues.append(new Double(peak.getKey()).intValue());
					sbValues.append(",");
					sbValues.append(formatter.format(peak.getValue()));
					sbValues.append(",");
					currentPeakForAllParticles.set(particleIndex, peakLists.get(particleIndex).hasNext() ? peakLists.get(particleIndex).next() : null);
				}
			}
			out.println(sbValues.toString());
			sbValues.setLength(0);
		}
		out.close();
	}

	/**
	 * Exports the peak list data for the supplied collection
	 * @param coll the collection of the particles we want to 
	 * export
	 * @param fileName the path to the file that we're going to create
	 * @param maxMZValue this is the maximum mass to charge value to export
	 * (we often filter out the largest and smallest mass to charge values
	 * @return true if it worked
	 */
	public boolean exportHierarchyToCSV(Collection coll, String fileName, int maxMZValue)
		throws DisplayException {
		double mzConstraint = new Double(maxMZValue);
		boolean showingNegatives;
		ArrayList<Integer> collectionIDList;
		
		if (fileName == null) {
			return false;
		} else if (! fileName.endsWith(ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION)) {
			fileName = fileName + "." + ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION;
		}
		if (! coll.getDatatype().equals("ATOFMS")) {
			throw new DisplayException("Please choose a ATOFMS collection to export.");
		}

		fileName = fileName.replaceAll("'", "");

		try {
	
			progressBar.setText("Exporting peak data");
			progressBar.setIndeterminate(true);

			collectionIDList = new ArrayList<Integer>();
			collectionIDList.add(coll.getCollectionID());

			ArrayList<ParticleInfo> particleList = new ArrayList<ParticleInfo>();
			ArrayList<Integer> collectionIDs = new ArrayList<Integer>();
			collectionIDs.addAll(db.getAllDescendantCollections(coll.getCollectionID(), true));
			int fileIndex = 0;
			for (int collectionID : collectionIDs) {
				Collection collection = db.getCollection(collectionID);
				particleList.add(new AverageParticleInfo(collection, getNormalizedAverageBPL(collection)));
				if (particleList.size() == 127)
				{
					writeOutParticlesToFile(particleList, fileName, fileIndex++, maxMZValue);
					particleList.clear();
				}
			}
			if (particleList.size() > 0)
			{
				writeOutParticlesToFile(particleList, fileName, fileIndex++, maxMZValue);
			}
		} catch (IOException e) {
			ErrorLogger.writeExceptionToLogAndPrompt("CSV Data Exporter","Error writing file please ensure the application can write to the specified file.");
			System.err.println("Problem writing file: ");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private BinnedPeakList getNormalizedAverageBPL(Collection collection) {
		BinnedPeakList returnList = new BinnedPeakList();
		int particleCount = 0;
		try {
			NonZeroCursor curs = new NonZeroCursor(db.getBPLOnlyCursor(collection));
			while (curs.next()) {
				particleCount++;
				BinnedPeakList peakList = curs.getCurrent().getBinnedList();
				peakList.normalize(DistanceMetric.EUCLIDEAN_SQUARED);
				for (BinnedPeak peak : peakList) {
					returnList.add(peak);
				}
			}
		}
		catch (SQLException sqle) {
			return new BinnedPeakList();
		}
		if (particleCount > 0) {
			returnList.divideAreasBy(particleCount);
		}
		return returnList;
	}
	
	public void setOnePerFile(boolean onePerFile) {
		this.onePerFile = onePerFile;
	}
	
	class AverageParticleInfo extends ParticleInfo {
		Collection collection;
		
		public AverageParticleInfo(Collection coll, BinnedPeakList peakList) {
			this.setBinnedList(peakList);
			this.collection = coll;
		}
		
		public Collection getCollection() {
			return collection;
		}
		
		
	}


	@SuppressWarnings("StringConcatenationInLoop")
	public void exportHistogramToCSV(Collection[] collections, String csvFileNameRoot, String qtype,
			String ltime, String utime, int timeres, String choice, List<Double> bins) throws SQLException, IOException {

		if (ltime.equals(""))
			ltime = "1753-01-01 00:00:00"; // minimum SQL date
		if (utime.equals(""))
			utime = "9999-12-31 23:59:59"; //maximum SQL date

		String hf, mf, sf;
		int hl, ml, sl;
		if (timeres >= 3600) {
			hf = "/" + (timeres / 3600);
			hl = timeres / 3600;
			mf = "*0";
			ml = 1;
			sf = "*0";
			sl = 1;
		} else if (timeres >= 60) {
			hf = "";
			hl = 1;
			mf = "/" + (timeres / 60);
			ml = timeres / 60;
			sf = "*0";
			sl = 1;
		} else {
			hf = "";
			hl = 1;
			mf = "";
			ml = 1;
			sf = "/" + (timeres);
			sl = timeres;
		}


		String cols, join;
		if (qtype.equals("size count")) {
			cols = "d.AtomID, d.Time, d.Size ";
			join = ", ";
			if (choice.equals("16")) {
				bins = List.of(0., 0.010, 0.012, 0.013, 0.015, 0.018, 0.021, 0.024, 0.027, 0.032, 0.037, 0.042, 0.049, 0.056, 0.065, 0.075, 0.087, 0.100, 0.115,
							   0.133, 0.154, 0.178, 0.205, 0.237, 0.274, 0.316, 0.365, 0.422, 0.487, 0.562, 0.649, 0.750, 0.866, 1.000, 1.155, 1.334, 1.540,
							   1.778, 2.054, 2.371, 2.738, 3.162, 3.652, 4.217, 4.870, 5.623, 6.494, 7.499, 8.660, 10.000);
			} else if (choice.equals("32")) {
				bins = List.of(0., 0.010, 0.011, 0.012, 0.012, 0.013, 0.014, 0.015, 0.017, 0.018, 0.019, 0.021, 0.022, 0.024, 0.025, 0.027, 0.029, 0.032,
							   0.034, 0.037, 0.039, 0.042, 0.045, 0.049, 0.052, 0.056, 0.060, 0.065, 0.070, 0.075, 0.081, 0.087, 0.093, 0.100, 0.107, 0.115,
							   0.124, 0.133, 0.143, 0.154, 0.165, 0.178, 0.191, 0.205, 0.221, 0.237, 0.255, 0.274, 0.294, 0.316, 0.340, 0.365, 0.392, 0.422,
							   0.453, 0.487, 0.523, 0.562, 0.604, 0.649, 0.698, 0.750, 0.806, 0.866, 0.931, 1.000, 1.075, 1.155, 1.241, 1.334, 1.433, 1.540,
							   1.655, 1.778, 1.911, 2.054, 2.207, 2.371, 2.548, 2.738, 2.943, 3.162, 3.398, 3.652, 3.924, 4.217, 4.532, 4.870, 5.233, 5.623,
							   6.043, 6.494, 6.978, 7.499, 8.058, 8.660, 9.306, 10.000);
			} else if (choice.equals("64")) {
				bins = List.of(0., 0.010000, 0.010366, 0.010746, 0.011140, 0.011548, 0.011971, 0.012409, 0.012864, 0.013335, 0.013824, 0.014330, 0.014855,
							   0.015399, 0.015963, 0.016548, 0.017154, 0.017783, 0.018434, 0.019110, 0.019810, 0.020535, 0.021288, 0.022067, 0.022876,
							   0.023714, 0.024582, 0.025483, 0.026416, 0.027384, 0.028387, 0.029427, 0.030505, 0.031623, 0.032781, 0.033982, 0.035227,
							   0.036517, 0.037855, 0.039242, 0.040679, 0.042170, 0.043714, 0.045316, 0.046976, 0.048697, 0.050481, 0.052330, 0.054247,
							   0.056234, 0.058294, 0.060430, 0.062643, 0.064938, 0.067317, 0.069783, 0.072339, 0.074989, 0.077737, 0.080584, 0.083536,
							   0.086596, 0.089769, 0.093057, 0.096466, 0.100000, 0.103663, 0.107461, 0.111397, 0.115478, 0.119709, 0.124094, 0.128640,
							   0.133352, 0.138237, 0.143301, 0.148551, 0.153993, 0.159634, 0.165482, 0.171544, 0.177828, 0.184342, 0.191095, 0.198096,
							   0.205353, 0.212875, 0.220673, 0.228757, 0.237137, 0.245824, 0.254830, 0.264165, 0.273842, 0.283874, 0.294273, 0.305053,
							   0.316228, 0.327812, 0.339821, 0.352269, 0.365174, 0.378552, 0.392419, 0.406794, 0.421697, 0.437144, 0.453158, 0.469759,
							   0.486968, 0.504807, 0.523299, 0.542469, 0.562341, 0.582942, 0.604296, 0.626434, 0.649382, 0.673170, 0.697831, 0.723394,
							   0.749894, 0.777365, 0.805842, 0.835363, 0.865964, 0.897687, 0.930572, 0.964662, 1.000000, 1.036633, 1.074608, 1.113974,
							   1.154782, 1.197085, 1.240938, 1.286397, 1.333521, 1.382372, 1.433013, 1.485508, 1.539927, 1.596339, 1.654817, 1.715438,
							   1.778279, 1.843423, 1.910953, 1.980957, 2.053525, 2.128752, 2.206734, 2.287573, 2.371374, 2.458244, 2.548297, 2.641648,
							   2.738420, 2.838736, 2.942727, 3.050528, 3.162278, 3.278121, 3.398208, 3.522695, 3.651741, 3.785515, 3.924190, 4.067944,
							   4.216965, 4.371445, 4.531584, 4.697589, 4.869675, 5.048066, 5.232991, 5.424691, 5.623413, 5.829415, 6.042964, 6.264335,
							   6.493816, 6.731704, 6.978306, 7.233942, 7.498942, 7.773650, 8.058422, 8.353625, 8.659643, 8.976871, 9.305720, 9.646616,
							   10.000000);
			} else if (choice.equals("128")) {
				bins = List.of(0., 0.010000, 0.010182, 0.010366, 0.010554, 0.010746, 0.010941, 0.011140, 0.011342, 0.011548, 0.011757, 0.011971, 0.012188,
							   0.012409, 0.012635, 0.012864, 0.013097, 0.013335, 0.013577, 0.013824, 0.014075, 0.014330, 0.014590, 0.014855, 0.015125, 0.015399,
							   0.015679, 0.015963, 0.016253, 0.016548, 0.016849, 0.017154, 0.017466, 0.017783, 0.018106, 0.018434, 0.018769, 0.019110, 0.019456,
							   0.019810, 0.020169, 0.020535, 0.020908, 0.021288, 0.021674, 0.022067, 0.022468, 0.022876, 0.023291, 0.023714, 0.024144, 0.024582,
							   0.025029, 0.025483, 0.025946, 0.026416, 0.026896, 0.027384, 0.027881, 0.028387, 0.028903, 0.029427, 0.029961, 0.030505, 0.031059,
							   0.031623, 0.032197, 0.032781, 0.033376, 0.033982, 0.034599, 0.035227, 0.035866, 0.036517, 0.037180, 0.037855, 0.038542, 0.039242,
							   0.039954, 0.040679, 0.041418, 0.042170, 0.042935, 0.043714, 0.044508, 0.045316, 0.046138, 0.046976, 0.047829, 0.048697, 0.049581,
							   0.050481, 0.051397, 0.052330, 0.053280, 0.054247, 0.055232, 0.056234, 0.057255, 0.058294, 0.059352, 0.060430, 0.061527, 0.062643,
							   0.063780, 0.064938, 0.066117, 0.067317, 0.068539, 0.069783, 0.071050, 0.072339, 0.073653, 0.074989, 0.076351, 0.077737, 0.079148,
							   0.080584, 0.082047, 0.083536, 0.085053, 0.086596, 0.088168, 0.089769, 0.091398, 0.093057, 0.094746, 0.096466, 0.098217, 0.100000,
							   0.101815, 0.103663, 0.105545, 0.107461, 0.109411, 0.111397, 0.113419, 0.115478, 0.117574, 0.119709, 0.121881, 0.124094, 0.126346,
							   0.128640, 0.130975, 0.133352, 0.135773, 0.138237, 0.140746, 0.143301, 0.145902, 0.148551, 0.151247, 0.153993, 0.156788, 0.159634,
							   0.162531, 0.165482, 0.168485, 0.171544, 0.174658, 0.177828, 0.181056, 0.184342, 0.187688, 0.191095, 0.194564, 0.198096, 0.201691,
							   0.205353, 0.209080, 0.212875, 0.216739, 0.220673, 0.224679, 0.228757, 0.232910, 0.237137, 0.241442, 0.245824, 0.250287, 0.254830,
							   0.259455, 0.264165, 0.268960, 0.273842, 0.278813, 0.283874, 0.289026, 0.294273, 0.299614, 0.305053, 0.310590, 0.316228, 0.321968,
							   0.327812, 0.333762, 0.339821, 0.345989, 0.352269, 0.358664, 0.365174, 0.371803, 0.378552, 0.385423, 0.392419, 0.399542, 0.406794,
							   0.414178, 0.421697, 0.429351, 0.437144, 0.445079, 0.453158, 0.461384, 0.469759, 0.478286, 0.486968, 0.495807, 0.504807, 0.513970,
							   0.523299, 0.532798, 0.542469, 0.552316, 0.562341, 0.572549, 0.582942, 0.593523, 0.604296, 0.615265, 0.626434, 0.637804, 0.649382,
							   0.661169, 0.673170, 0.685390, 0.697831, 0.710497, 0.723394, 0.736525, 0.749894, 0.763506, 0.777365, 0.791476, 0.805842, 0.820470,
							   0.835363, 0.850526, 0.865964, 0.881683, 0.897687, 0.913982, 0.930572, 0.947464, 0.964662, 0.982172, 1.000000, 1.018152, 1.036633,
							   1.055450, 1.074608, 1.094114, 1.113974, 1.134194, 1.154782, 1.175743, 1.197085, 1.218814, 1.240938, 1.263463, 1.286397, 1.309747,
							   1.333521, 1.357727, 1.382372, 1.407465, 1.433013, 1.459024, 1.485508, 1.512473, 1.539927, 1.567879, 1.596339, 1.625315, 1.654817,
							   1.684855, 1.715438, 1.746576, 1.778279, 1.810558, 1.843423, 1.876884, 1.910953, 1.945640, 1.980957, 2.016915, 2.053525, 2.090800,
							   2.128752, 2.167392, 2.206734, 2.246790, 2.287573, 2.329097, 2.371374, 2.414418, 2.458244, 2.502865, 2.548297, 2.594553, 2.641648,
							   2.689599, 2.738420, 2.788127, 2.838736, 2.890264, 2.942727, 2.996143, 3.050528, 3.105900, 3.162278, 3.219678, 3.278121, 3.337625,
							   3.398208, 3.459892, 3.522695, 3.586638, 3.651741, 3.718027, 3.785515, 3.854229, 3.924190, 3.995421, 4.067944, 4.141785, 4.216965,
							   4.293510, 4.371445, 4.450794, 4.531584, 4.613840, 4.697589, 4.782858, 4.869675, 4.958068, 5.048066, 5.139697, 5.232991, 5.327979,
							   5.424691, 5.523158, 5.623413, 5.725488, 5.829415, 5.935229, 6.042964, 6.152654, 6.264335, 6.378044, 6.493816, 6.611690, 6.731704,
							   6.853896, 6.978306, 7.104974, 7.233942, 7.365250, 7.498942, 7.635061, 7.773650, 7.914755, 8.058422, 8.204696, 8.353625, 8.505258,
							   8.659643, 8.816831, 8.976871, 9.139817, 9.305720, 9.474635, 9.646616, 9.821719, 10.000000);
			}
		} else {
			cols = "d.AtomID, d.Time, d.Size, s.PeakHeight, s.PeakLocation, s.PeakArea, s.RelPeakArea ";
			join = "JOIN (SELECT * FROM ATOFMSAtomInfoSparse) AS s ON d.AtomID = s.AtomID, ";
		}

		for (Collection collection : collections) {
			int cid = collection.getCollectionID();
			String cn = collection.getName();

			String select = "";
			ArrayList<String> labels = new ArrayList<String>();
			labels.add("Date");
			labels.add("StartTime");

			if (qtype.equals("size count")) {
				for (int j = 0; j < bins.size() - 1; j++) {
					select += "SUM(CAST((CASE WHEN Size BETWEEN " + bins.get(j) + " AND " + bins.get(j + 1) +
							" THEN 1 ELSE 0 END) AS FLOAT)) AS bin" + (j + 1) + ", ";
					labels.add("" + (bins.get(j)) + "-" + (bins.get(j + 1)));

				}

			} else if (qtype.equals("peak count")) {
				for (double jd : bins) {
					int j = (int)jd;
					select += "SUM(CAST((CASE WHEN PeakLocation = " + j +
							" THEN 1 ELSE 0 END) AS FLOAT)) AS bin" + j + ", ";
					labels.add("" + j);

				}
			} else {
				for (double jd : bins) {
					int j = (int)jd;
					String qt;
					switch (qtype) {
						case "height sum":
							qt = "PeakHeight";
							break;
						case "rel area sum":
							qt = "RelPeakArea";
							break;
						case "area sum":
							qt = "PeakArea";
							break;
						default:
							throw new IllegalArgumentException("Bad query type");
					}
					select += "SUM(CAST(" + qt + " * (CASE WHEN PeakLocation = " + j +
							" THEN 1 ELSE 0 END) AS FLOAT)) AS bin" + j + ", ";
					labels.add("" + j);
				}
			}

			// select = select.rstrip(', ')
			while (select.endsWith(",") || select.endsWith(" ")) {
				select = select.substring(0, select.length() - 1);
			}

			String query = String.join(
					System.getProperty("line.separator"),
					"SELECT",
					"  CAST(strftime('%Y', Time) AS integer) as y,",
					"  CAST(strftime('%m', Time) AS integer) as m,",
					"  CAST(strftime('%d', Time) AS integer) as d,",
					"  CAST(strftime('%H', Time) AS INTEGER)" + hf + " AS h,",
					"  CAST(strftime('%M', Time) AS INTEGER)" + mf + " AS mi,",
					"  CAST(strftime('%S', Time) AS INTEGER)" + sf + " AS s, ",
					"  " + select,
					"  FROM (",
					"    SELECT " + cols,
					"    FROM ATOFMSAtomInfoDense AS d " + join,
					"      InternalAtomOrder AS i",
					"    WHERE i.CollectionID = " + cid + " AND d.AtomID = i.AtomID",
					"    AND d.Time BETWEEN '" + ltime + "' AND '" + utime + "'",
					"  ) AS data",
					"GROUP BY y, m, d, h, mi, s",
					"ORDER BY y, m, d, h, mi, s"
			);

			int dataCols;
			ArrayList<LocalDateTime> datetimes = new ArrayList<>();
			ArrayList<ArrayList<String>> data = new ArrayList<>();

			try (Statement stmt = db.getCon().createStatement();
				 ResultSet rs = stmt.executeQuery(query);) {
				dataCols = rs.getMetaData().getColumnCount();
				while (rs.next()) {
					int yyyy = rs.getInt(1);
					int mm = rs.getInt(2);
					int dd = rs.getInt(3);
					int hh = rs.getInt(4) * hl;
					int mi = rs.getInt(5) * ml;
					int ss = rs.getInt(6) * sl;
					datetimes.add(LocalDateTime.of(yyyy, mm, dd, hh, mi, ss));
					ArrayList<String> row = new ArrayList<>();
					for (int col = 7; col <= dataCols; col++) {
						row.add("" + rs.getFloat(col));
					}
					data.add(row);
				}
			}

			int l = 0;

			ArrayList<String> zrow = new ArrayList<>();
			for (int i = 7; i <= dataCols; i++) {
				zrow.add("0.0");
			}

			while (datetimes.get(l).compareTo(datetimes.get(datetimes.size() - 1)) < 0) {
				Duration delta = Duration.between(datetimes.get(l), datetimes.get(l + 1));
				if (delta.getSeconds() != timeres) {
					LocalDateTime thistime = datetimes.get(l).plusSeconds(timeres);
					datetimes.add(l + 1, thistime);
					data.add(l + 1, zrow);
				}
				l += 1;
			}

			ArrayList<String> datelabels = new ArrayList<>();
			ArrayList<String> timelabels = new ArrayList<>();

			for (LocalDateTime x : datetimes) {
				datelabels.add(x.format(DateTimeFormatter.ISO_LOCAL_DATE));
				timelabels.add(x.format(DateTimeFormatter.ISO_LOCAL_TIME));
			}

			// For the file name, add on the first word of the query type.
			String csvFileName = Path.of(csvFileNameRoot)
					.resolve("histogram" + "_" + qtype.split(" ")[0] + "_"
									 + collection.getName() + ".csv").toString();
			try (CSVWriter writer = new CSVWriter(new FileWriter(csvFileName),
												  ',',
												  CSVWriter.NO_QUOTE_CHARACTER)) {
				writer.writeNext(labels.toArray(new String[0]));

				for (int i = 0; i < datelabels.size(); i++) {
					ArrayList<String> row = new ArrayList<>();
					row.add(datelabels.get(i));
					row.add(timelabels.get(i));
					row.addAll(data.get(i));
					writer.writeNext(row.toArray(new String[0]));
				}
			}
		}
	}
}
