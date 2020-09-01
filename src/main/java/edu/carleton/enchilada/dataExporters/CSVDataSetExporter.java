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
			double Dmin = 0.01;
			double Dmax = 10;
			double Dtemp = Math.pow(10, (int)Math.log10(Dmin));
			int nstart = 0;
			int Dres;
			int nNumSize;

			try {
				bins = new ArrayList<>();
				bins.add(0.);

				Dres = Integer.parseInt(choice);
				nNumSize = (int) Math.log10(Dmax / Dtemp) * Dres - nstart + 1;
				for (int i=0; i < nNumSize; i++) {
					int precision = 100000;
					double bin = Dtemp*Math.pow(10, (i + nstart)/(double)Dres);
					bin = Math.round(bin*precision)/(double)precision;
					bins.add(bin);
				}
			} catch (NumberFormatException e) {
				// Choice is non-numeric in this case, which means none of below will run
			};

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
					select += "SUM(CAST((CASE WHEN Size >= " + bins.get(j) + " AND SIZE <" + bins.get(j + 1) +
							" THEN 1 ELSE 0 END) AS FLOAT)) AS bin" + (j + 1) + ", ";
					labels.add("" + (bins.get(j)));

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
