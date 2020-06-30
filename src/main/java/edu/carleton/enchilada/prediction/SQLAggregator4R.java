package edu.carleton.enchilada.prediction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

import java.util.Date;
import java.util.Scanner;

import edu.carleton.enchilada.database.Database;

/*
 * Note: St.Louis min & max m/z values are (respectively) -1585 and 1691, so we
 * could boost by 1700 on attribute numbering and include all peaks.
 * Query run on 12.5.06 by steinbel
 *	 
 */

public class SQLAggregator4R {
	
	// Set maxAtomId to 0 to run the whole thing
	public static final int maxAtomId = 0;

	private static Database db;
	private static Connection con;
	private static int maxAtt = 604;          //the maximum number of attributes
	private static int peakRange = 300;       //the abs of the max/min peak values
	private static int numAttributesB4MZ = 4; //number of attributes before mz
	private static double density = 1;        //this will be given us by the atmo. scientists
	private static final boolean PRETTY = true;
	//Minimum number of particles in an acceptable bin
	private static final int minParticles = 200;
		
	/**
	 * @author steinbel
	 * @author dmusican
	 * Gathers and formats the ATOFMS data appropriately.
	 * @param outputFile - name of the output file
	 */
	public static void process(PrintWriter out) throws IOException{
		//send incoming string to create a temp table
		createTempTable();

		System.out.println("table created");//TESTING

		try {
			// Calculate total mass for each time bin, and connect it with
			// EC data. Throw away any bins with too few particles
			String denseQuery =
				"SELECT roundedTime, mass, value1, cnt FROM\n" +
				"(SELECT roundedTime, SUM(1/de) as cnt,\n" +
				"	SUM(size*size*size*" + density + "*(1/de)) as mass\n" +
				"   FROM RoundedDense\n";
			if (maxAtomId > 0)
				denseQuery += " WHERE atomID <= " + maxAtomId + "\n"; 
			denseQuery +=
				"   GROUP BY roundedTime\n" +
				"   HAVING COUNT(*) > " + minParticles + ") Masses, AggData\n" +
				"WHERE Masses.roundedTime = AggData.Timestamp\n" +
				"AND value1 <> 0\n" +
				"ORDER BY roundedTime";

			System.out.println(denseQuery);
			Statement denseStmt = con.createStatement();
			ResultSet denseSet = denseStmt.executeQuery(denseQuery); 

			/* Sum together the sizes of the particles and the areas of the
			 * peaks, both adjusted for detection efficiency. 
			 */
			String sparseQuery =
				"SELECT roundedTime, peaklocation,\n" +
				"	SUM(peakarea/de) as adjustedpeak\n" +
				"FROM RoundedDense d, ATOFMSAtomInfosparse s\n" +
				//relpeakarea > .0001 has been added; remove it later
				"WHERE d.atomid = s.atomid and relpeakarea > .0001\n" +
				"AND roundedTime is not NULL\n" ;
			if (maxAtomId > 0)
				sparseQuery += "AND d.atomID <= " + maxAtomId + "\n"; 
			sparseQuery +=
				"GROUP BY roundedTime, peaklocation\n" +
				"ORDER BY roundedTime, peaklocation";
			System.out.println(sparseQuery);
			Statement sparseStmt = con.createStatement();
			ResultSet sparseSet = sparseStmt.executeQuery(sparseQuery);

			boolean denseRead = denseSet.next();
			boolean sparseRead = sparseSet.next();
			boolean newMass = true;

			//Make sure dense and sparse sets are not empty.
			if (!denseRead)
				throw new RuntimeException("Dense set empty");
			if (!sparseRead)
				throw new RuntimeException("Sparse set empty");
			
			//Execute as long as there is another row in denseSet
			
			
//			created for line numbers
			
			int linecount = 1;
			
			while (denseRead) {
				Timestamp denseTime = denseSet.getTimestamp("roundedTime");
				Timestamp sparseTime = sparseSet.getTimestamp("roundedTime");

				// If denseTime is greater than sparseTime, we may have thrown
				// away that dense row because mass was 0. Skim through sparse
				// time until we match
				while (denseTime.compareTo(sparseTime) > 0) {
					sparseRead = sparseSet.next();
					if (sparseRead) {
						sparseTime = sparseSet.getTimestamp("roundedTime");
					} else {
						throw new RuntimeException("No more sparse data");
					}
				}
				
				if (!denseTime.equals(sparseTime)) {
					// Should never happen, this is error checking
					throw new RuntimeException("Dense time does not equal sparse time");
				}
				
				float value = denseSet.getFloat("value1");
				float mass = denseSet.getFloat("mass");
				float count = denseSet.getFloat("cnt");
				
				// Starting new dense, write out the header. Chop off the ".0"
				// at the end of the time that seems to give Weka trouble.
				String wekaTime = (denseTime.toString().split("\\."))[0];
				//StringBuffer wekaTime2 = new StringBuffer(wekaTime);
				//wekaTime2.replace(10,10,"_");

				// Adjust the units of mass to fit a smaller scale:
				// this makes tools such as Weka happier. Also divide by
				// count to normalize for the number of particles seen.
								
				//arbitrary scaling factors that are NOT hex
				float massScaleFactor = 1e8f;
				float countScaleFactor = 1e7f;
				
				//replace spaces with #
				//added count for R
				if(PRETTY){
					out.print(linecount + " " //+ wekaTime2 + " " 
							+ value);// + " " +
						//mass/massScaleFactor + " " +
						//count/countScaleFactor);
				}
				else{
					out.print("{0 \"" + wekaTime + "\"" +
						",1 " + value +
						",2 " + mass/massScaleFactor +
						",3 " + count/countScaleFactor);
				}
				linecount++;
				int lastLoc = -301;
				// Loop over all sparse data with matching time
				while (sparseRead && denseTime.equals(sparseTime)) {

					// Write out the sparse data. Adjust the units
					// of adjustpeak to put it on a smaller scale:
					// this makes tools such as Weka happier.
					
					//more not hex
					float peakScaleFactor = 1e8f;
					int location = sparseSet.getInt("peaklocation");
					if ((location >= -(peakRange)) && (location <= (peakRange))){
						
						
//						replace spaces with #
						if(PRETTY){
							while((lastLoc + 1)<location){
								//out.print(" " + (lastLoc+1) + " $");
								out.print(" " + "0");
								lastLoc++;
							}
							//out.print(" " + location + " " + (sparseSet.getFloat("adjustedpeak") /
									//peakScaleFactor));
							out.print(" " + (sparseSet.getFloat("adjustedpeak") / peakScaleFactor));
							lastLoc = location;
						}
						else{
							out.print("," + (location+peakRange+numAttributesB4MZ) + " " +
									sparseSet.getFloat("adjustedpeak") /
									peakScaleFactor);
						}
					}

					// Grab next sparse row
					sparseRead = sparseSet.next();
					if (sparseRead) {
						sparseTime = sparseSet.getTimestamp("roundedTime");
					}
				}

				// Write out end of row
				
//				replace spaces with #
				if(PRETTY){
					while(lastLoc<300){
						//out.print(" " + (lastLoc+1)+ " $");
						out.print(" 0");
						lastLoc++;
					}
					out.println();
				}
				else
					out.println("}");

				// Grab next dense row
				denseRead = denseSet.next();
			}
			denseSet.close();
			sparseSet.close();
			denseStmt.close();
			sparseStmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @author steinbel
	 * Opens the connection to the database.
	 */
	public static void openAndConnect(){
		db = Database.getDatabase();
		db.openConnection();
		con = db.getCon();
	}
	
	/**
	 * @author steinbel
	 * @author dmusican
	 * 
	 * Create a temp table with atomID, detection efficiency, roundedTime 
	 * (initially populated with the size of the particles) during the
	 * hour-long timebin around the given time.
	 */
	private static void createTempTable(){
		try {
			//Drop the temp table, in case one already exists.
			dropTempTable();
			
			//Piece together the query to create the temp table (RoundedDense)
			System.out.println("Creating temp table.");
			String order = 
				"CREATE TABLE RoundedDense (\n" +
				"	atomid int,\n" +
				"	roundedTime DATETIME,\n" +
				"	size REAL,\n" +
				"	de REAL)\n" +
				"INSERT INTO RoundedDense (atomid, roundedTime, size, de)\n" +
				"SELECT atomid,\n" +
				
				//SQL Server-specific queries for appropriately rounding time values (nearest hour)
				
				//Rounds based on half-past the hour
				//"	DATEADD(hour, DATEDIFF(hour, '20000101', DATEADD(minute, 30, time)), '20000101') as roundedTime,\n" +
				//Rounds down to the nearest hour (truncates)
				"	DATEADD(hour, DATEDIFF(hour, '20000101', time), '20000101') as roundedTime,\n" +
				//Rounds up to the nearest hour
				//"	DATEADD(hour, DATEDIFF(hour, '20000101', DATEADD(minute, 60, time)), '20000101') as roundedTime,\n" +
								
				"	size, NULL\n" +
				"FROM ATOFMSAtomInfoDense\n"; //+
				
				//Size range is limited, as we do not have formulas for sizes outside of this range.
				//"WHERE size >= 0.1 AND size <= 2.5\n";
			
			if (maxAtomId > 0)
				order += "AND atomID <= " + maxAtomId; 
			
			System.out.println(order);//TESTING
			Statement stmt = con.createStatement();
			stmt.executeUpdate(order);

			System.out.println("Updates 1,2, and 3");
			stmt.executeUpdate("update RoundedDense " +
									"set de = " +
										"case " +
											"when (size < .1) then (select power(1*1000, 2.8574)*exp(-27.16))"+
											"when (size >= .1 and size <= .75) " +
												"then (select power(size*1000, 2.8574)*exp(-27.16)) " +
											"when (size > .75 and size < 1.) " +
												"then (select power(size*1000, -.58272)*exp(-4.803)) " +
											"when (size >= 1. and size <= 2.5) " +
												"then (select power(size*1000, -7.52)*exp(42.031)) " +
												"when (size > 2.5) then (select power(1*1000, 2.8574)*exp(-27.16))"+
										"end;");
			
			// Verify that all de rows have been calculated
			ResultSet rs = 
				stmt.executeQuery("SELECT * FROM RoundedDense WHERE de IS NULL");
			if (rs.next())
				throw new RuntimeException("de not calculated for all values");
			rs.close();
			
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @author steinbel
	 * Drops the temporary table RoundedDense from the database SpASMSdb.
	 */
	private static void dropTempTable(){
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("IF (OBJECT_ID('RoundedDense') " +
					"IS NOT NULL)\n" +
					" DROP TABLE RoundedDense\n");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @author steinbel
	 * @author dmusican
	 * Writes the .arff header info for this file, which includes naming
	 * the m/z values as attributes so we can find them later. 
	 * @param relationName - the name of the relation
	 * @param predictThis - the attribute to be predicted (e.g., "ec" or "bc")
	 * @return the header for the .arff file in the form of a string
	 */
	public static String assembleAttributes(String relationName, String predictThis){
		String attributeNames = "@relation " + relationName +"\n"
				+"@attribute time date \"yyyy-MM-dd HH:mm:ss\" \n"
				+"@attribute " + predictThis + " numeric \n"
				+"@attribute mass numeric \n"
				+"@attribute count numeric \n";
		
		//start at 3 because of the attributes named above
		for (int i=-peakRange; i<=peakRange; i++){
			attributeNames+="@attribute mz" + (i) + " numeric \n";
		}
					
		attributeNames+="@data \n";
		return attributeNames;
	}
	
	/**
	 * @author dmusican
	 * Import the filter data
	 */
	private static void importFilterData(String filename) {
		try {
			Statement stmt = con.createStatement();

			stmt.executeUpdate(
				"IF (OBJECT_ID('AggData') IS NOT NULL) DROP TABLE AggData"
			);
			
			//count the number of Value columns needed
			int numColumns = 0;
			
			try {
				Scanner tempScanner = new Scanner(new File(filename));
				String[] tempArray = tempScanner.nextLine().split(",");
				numColumns = tempArray.length - 1;
			}
			catch (IOException e) {
				System.out.println("Something is wrong with the .csv file.");
			}
				
			//build and execute query to create a table with appropriate number of Value columns
			String valueColumns = "";
			for (int i = 0; i < numColumns; i++) {
				valueColumns += ", Value" + (i+1) + " FLOAT";
			}
			
			String tableQuery = "CREATE TABLE AggData (TimeStamp DATETIME" + valueColumns + ")";
			stmt.executeUpdate(tableQuery);
			
			//change this
			//stmt.executeUpdate(
			//	"CREATE TABLE AggData (TimeStamp DATETIME, Value1 FLOAT, Value2 FLOAT)"
			//);
			String q = "BULK INSERT AggData\n" +
			"FROM '" + filename + "'\n" + 
			"WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')";
			System.out.println(q);
			stmt.executeUpdate(q);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	take in file of ec/bc/whatever data in csv format
	public static void main(String[] args){
		Date start = new Date();
		openAndConnect();
		try {
			//Create .arff file for output.
			String location = (new File(".")).getCanonicalPath();
			String outputfilename;
			if(PRETTY)
				outputfilename = location + "/prediction/swiss370R.csv";
			else
				outputfilename = location + "/prediction/testrun2.arff";
			PrintWriter out = new PrintWriter(outputfilename);   
			if (PRETTY)
				out.println("bap mz-300 mz-299 mz-298 mz-297 mz-296 mz-295 mz-294 mz-293 mz-292 mz-291 mz-290 mz-289 mz-288 mz-287 mz-286 mz-285 mz-284 mz-283 mz-282 mz-281 mz-280 mz-279 mz-278 mz-277 mz-276 mz-275 mz-274 mz-273 mz-272 mz-271 mz-270 mz-269 mz-268 mz-267 mz-266 mz-265 mz-264 mz-263 mz-262 mz-261 mz-260 mz-259 mz-258 mz-257 mz-256 mz-255 mz-254 mz-253 mz-252 mz-251 mz-250 mz-249 mz-248 mz-247 mz-246 mz-245 mz-244 mz-243 mz-242 mz-241 mz-240 mz-239 mz-238 mz-237 mz-236 mz-235 mz-234 mz-233 mz-232 mz-231 mz-230 mz-229 mz-228 mz-227 mz-226 mz-225 mz-224 mz-223 mz-222 mz-221 mz-220 mz-219 mz-218 mz-217 mz-216 mz-215 mz-214 mz-213 mz-212 mz-211 mz-210 mz-209 mz-208 mz-207 mz-206 mz-205 mz-204 mz-203 mz-202 mz-201 mz-200 mz-199 mz-198 mz-197 mz-196 mz-195 mz-194 mz-193 mz-192 mz-191 mz-190 mz-189 mz-188 mz-187 mz-186 mz-185 mz-184 mz-183 mz-182 mz-181 mz-180 mz-179 mz-178 mz-177 mz-176 mz-175 mz-174 mz-173 mz-172 mz-171 mz-170 mz-169 mz-168 mz-167 mz-166 mz-165 mz-164 mz-163 mz-162 mz-161 mz-160 mz-159 mz-158 mz-157 mz-156 mz-155 mz-154 mz-153 mz-152 mz-151 mz-150 mz-149 mz-148 mz-147 mz-146 mz-145 mz-144 mz-143 mz-142 mz-141 mz-140 mz-139 mz-138 mz-137 mz-136 mz-135 mz-134 mz-133 mz-132 mz-131 mz-130 mz-129 mz-128 mz-127 mz-126 mz-125 mz-124 mz-123 mz-122 mz-121 mz-120 mz-119 mz-118 mz-117 mz-116 mz-115 mz-114 mz-113 mz-112 mz-111 mz-110 mz-109 mz-108 mz-107 mz-106 mz-105 mz-104 mz-103 mz-102 mz-101 mz-100 mz-99 mz-98 mz-97 mz-96 mz-95 mz-94 mz-93 mz-92 mz-91 mz-90 mz-89 mz-88 mz-87 mz-86 mz-85 mz-84 mz-83 mz-82 mz-81 mz-80 mz-79 mz-78 mz-77 mz-76 mz-75 mz-74 mz-73 mz-72 mz-71 mz-70 mz-69 mz-68 mz-67 mz-66 mz-65 mz-64 mz-63 mz-62 mz-61 mz-60 mz-59 mz-58 mz-57 mz-56 mz-55 mz-54 mz-53 mz-52 mz-51 mz-50 mz-49 mz-48 mz-47 mz-46 mz-45 mz-44 mz-43 mz-42 mz-41 mz-40 mz-39 mz-38 mz-37 mz-36 mz-35 mz-34 mz-33 mz-32 mz-31 mz-30 mz-29 mz-28 mz-27 mz-26 mz-25 mz-24 mz-23 mz-22 mz-21 mz-20 mz-19 mz-18 mz-17 mz-16 mz-15 mz-14 mz-13 mz-12 mz-11 mz-10 mz-9 mz-8 mz-7 mz-6 mz-5 mz-4 mz-3 mz-2 mz-1 mz0 mz1 mz2 mz3 mz4 mz5 mz6 mz7 mz8 mz9 mz10 mz11 mz12 mz13 mz14 mz15 mz16 mz17 mz18 mz19 mz20 mz21 mz22 mz23 mz24 mz25 mz26 mz27 mz28 mz29 mz30 mz31 mz32 mz33 mz34 mz35 mz36 mz37 mz38 mz39 mz40 mz41 mz42 mz43 mz44 mz45 mz46 mz47 mz48 mz49 mz50 mz51 mz52 mz53 mz54 mz55 mz56 mz57 mz58 mz59 mz60 mz61 mz62 mz63 mz64 mz65 mz66 mz67 mz68 mz69 mz70 mz71 mz72 mz73 mz74 mz75 mz76 mz77 mz78 mz79 mz80 mz81 mz82 mz83 mz84 mz85 mz86 mz87 mz88 mz89 mz90 mz91 mz92 mz93 mz94 mz95 mz96 mz97 mz98 mz99 mz100 mz101 mz102 mz103 mz104 mz105 mz106 mz107 mz108 mz109 mz110 mz111 mz112 mz113 mz114 mz115 mz116 mz117 mz118 mz119 mz120 mz121 mz122 mz123 mz124 mz125 mz126 mz127 mz128 mz129 mz130 mz131 mz132 mz133 mz134 mz135 mz136 mz137 mz138 mz139 mz140 mz141 mz142 mz143 mz144 mz145 mz146 mz147 mz148 mz149 mz150 mz151 mz152 mz153 mz154 mz155 mz156 mz157 mz158 mz159 mz160 mz161 mz162 mz163 mz164 mz165 mz166 mz167 mz168 mz169 mz170 mz171 mz172 mz173 mz174 mz175 mz176 mz177 mz178 mz179 mz180 mz181 mz182 mz183 mz184 mz185 mz186 mz187 mz188 mz189 mz190 mz191 mz192 mz193 mz194 mz195 mz196 mz197 mz198 mz199 mz200 mz201 mz202 mz203 mz204 mz205 mz206 mz207 mz208 mz209 mz210 mz211 mz212 mz213 mz214 mz215 mz216 mz217 mz218 mz219 mz220 mz221 mz222 mz223 mz224 mz225 mz226 mz227 mz228 mz229 mz230 mz231 mz232 mz233 mz234 mz235 mz236 mz237 mz238 mz239 mz240 mz241 mz242 mz243 mz244 mz245 mz246 mz247 mz248 mz249 mz250 mz251 mz252 mz253 mz254 mz255 mz256 mz257 mz258 mz259 mz260 mz261 mz262 mz263 mz264 mz265 mz266 mz267 mz268 mz269 mz270 mz271 mz272 mz273 mz274 mz275 mz276 mz277 mz278 mz279 mz280 mz281 mz282 mz283 mz284 mz285 mz286 mz287 mz288 mz289 mz290 mz291 mz292 mz293 mz294 mz295 mz296 mz297 mz298 mz299 mz300");
			//write the .arff file headings
			if(!PRETTY)
				out.print(assembleAttributes("ecrelation", "ec"));

			//Import the filter data from a CSV file.
			String csvfilename = location + "/prediction/swissbap370.csv";
			importFilterData(csvfilename);
			System.out.println("Data imported");
			
			process(out);
			out.close();
			db.closeConnection();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Date end = new Date();
		System.out.println("time taken = " + (end.getTime() - start.getTime())
				+ " milliseconds.");
	}
	 
}
