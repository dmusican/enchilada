/*
 * Created on Dec 9, 2004
 *
 */
package edu.carleton.enchilada.experiments;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.swing.JOptionPane;

import edu.carleton.enchilada.database.InfoWarehouse;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.ATOFMS.*;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;

/**
 * @author ritza
 *
 */
public class ImportExperimentParticles {
	private static int amplitude;
	private ArrayList<ATOFMSParticle> particles;
	private InfoWarehouse db;
	
	public ImportExperimentParticles() {
		particles = new ArrayList<ATOFMSParticle>();
		ArrayList<Integer>  indices = new ArrayList<Integer>(2000);
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");


		try {
			Database.getDatabase("TestDB").rebuildDatabase("TestDB");
		} catch (ExceptionAdapter ea) {
			if (ea.originalException instanceof SQLException) {
				JOptionPane.showMessageDialog(null,
						"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
			} else {
				throw ea;
			}
		}

		//Open database connection:
		db = Database.getDatabase("TestDB");
		db.openConnection("TestDB");
		Connection con = db.getCon();




		ATOFMSParticle.currPeakParams = new PeakParams(30,30,0.01f,.50f);
		try {
			ATOFMSParticle.currCalInfo = 
				new CalInfo("Particles for Clustering\\040215a_33.cal", true);
		} catch(Exception exception) {
			System.out.println("error loading cal info.");
			exception.printStackTrace();
		}
		
		int[] id = new int[2];
		id = db.createEmptyCollectionAndDataset("ATOFMS", 0,"Amp: "+ 
				amplitude,"7 different particles",
				"'Particles for Clustering\\040215a_33.cal', '.noz file', " +
				ATOFMSParticle.currPeakParams.minHeight + ", " + 
				ATOFMSParticle.currPeakParams.minArea + ", " + 
				ATOFMSParticle.currPeakParams.minRelArea + ", 1"); 
		
		// Use the indices array to duplicate the number of each type of particle.
		// Choose a random number of duplications.  
		for (int p1 = 0; p1 < 100; p1++) 
			indices.add(new Integer(1));
		for (int p2 = 0; p2 < 100; p2++) 
			indices.add(new Integer(2));
		for (int p3 = 0; p3 < 100; p3++) 
			indices.add(new Integer(3));
		for (int p4 = 0; p4 < 100; p4++) 
			indices.add(new Integer(4));
		for (int p5 = 0; p5 < 100; p5++) 
			indices.add(new Integer(5));
		for (int p6 = 0; p6 < 100; p6++) 
			indices.add(new Integer(6));
		for (int p7 = 0; p7 < 100; p7++) 
			indices.add(new Integer(7));
		
		// randomize the particles in the array.
		// Use a random object with a seed to ensure
		// that they randomize the same way every time.
		indices.trimToSize();
		Random rnd = new Random(23713);
		Collections.shuffle(indices,rnd);		
		
		/**
		 * Indices to particles:
		 * 
		 * 1	a-020801071636-00055.amz	
		 * 2	a-040215084636-00033.amz
		 * 3	b-040215093256-00061.amz	
		 * 4	b-040215093918-00150.amz	
		 * 5	h-041120141836-00007.amz	
		 * 6	i-040808153000-00112.amz	
		 * 7	i-040808160921-00310.amz
		 */
		
		try {
			// insert the particles into the database, peaklisting them every time.
			int newAtomID = db.getNextID();
			int particleNumber;
			ReadExpSpec readSpec;
			String file;
			Date time;
			int marker = 100;
			for (int i = 0; i < indices.size(); i++) {
				if (i/marker == i/(double)marker)
					System.out.println("Inserting particle " + i);
				particleNumber = indices.get(i).intValue();
				switch (particleNumber) {
				case 1:
					file = "a-020801071636-00055.amz";
					time = df.parse("8/1/2002 07:16:36");
					break;
				case 2:
					file = "a-040215084636-00033.amz";
					time = df.parse("2/15/2004 08:46:36");
					break;
				case 3:
					file = "b-040215093256-00061.amz";
					time = df.parse("2/15/2004 09:32:56");
					break;
				case 4:
					file = "b-040215093918-00150.amz";
					time = df.parse("2/15/2004 09:39:18");
					break;
				case 5:
					file = "h-041120141836-00007.amz";
					time = df.parse("11/20/2004 14:18:36");
					break;
				case 6:
					file = "i-040808153000-00112.amz";
					time = df.parse("8/8/2004 15:30:00");
					break;
				default:
					file = "i-040808160921-00310.amz";
				    time = df.parse("8/8/2004 16:9:21");
				}
				readSpec = new ReadExpSpec("Particles for Clustering\\" + file, time); 
				db.insertParticle(readSpec.getParticle().particleInfoDenseString(db.getDateFormat()),
						readSpec.getParticle().getSparsePeakMap().values(), db.getCollection(id[0]),id[1],newAtomID++);
			}
			db.updateAncestors(db.getCollection(id[0]));
		}catch (Exception exception) {
			System.out.println("Caught exception");
			exception.printStackTrace();
		}
	}
	
	public static int getAmplitude() {
		return amplitude;
	}
	
	public static void main(String[] args) {
		//amplitude = Integer.parseInt(args[0]);
		amplitude = 40;
		ImportExperimentParticles imp = 
			new ImportExperimentParticles();
	}
}
