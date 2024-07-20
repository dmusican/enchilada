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
 * The Original Code is EDAM Enchilada's ReadSpec class.
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
 * Kate Nelson kate.nelson@alumni.carleton.edu
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


/*
 * Created on Jul 28, 2004
 *
 */
package edu.carleton.enchilada.ATOFMS;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.*;
import java.nio.*;

/**
 * @author ritza
 * TODO:  Needs a lot of work to reformat the data.  See the todo
 * below - the new data is larger than the old one.  Probably need 
 * to read the first 4 bytes, then pass it on to
 * the respective method.
 */
public class ReadSpec {
	private static final DateFormat dFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// yay, consistent date formatting.
	
	private  byte[] byteArray;
	private ByteBuffer byteBuff;
	private ATOFMSParticle particle;
	
	private  final int MAX_PEAKS = 16; 
	
	private int[] posdata;
	private int[] negdata;
	private int version;
	private short iontype;

	
	/**
	 * Constructor.
	 * Checks to see if the file is zipped(.amz) or unzipped(.ams) and
	 * reads accordingly.
	 * @param file - filename of particle.
	 */
	public ReadSpec(String file, Date d)  throws IOException, ZipException {
		int index = file.length() - 4;
		String fileType = file.substring(index);
		if (fileType.equals(".amz"))
			readZipped(file, d);
		else if (fileType.equals(".ams"))
			readUnzipped(file, d);
	}
	
	/**
	 * Takes a zipped file and reads it without having to unzip.
	 * @param filename - file name of particle.
	 * @return - the particle created from the file information. 
	 */
	public void readZipped(String filename, Date d) throws IOException, ZipException {		
		// Reads the file using ZipInputStream.
		FileInputStream in = new FileInputStream(filename);
		ZipInputStream zipInput = 
			new ZipInputStream(new BufferedInputStream(in));
		ZipEntry ze = zipInput.getNextEntry();
		
		int size = (int) ze.getSize();
//		Wraps the byte array in the byte buffer.
		byteArray = new byte[size];
		byteBuff = ByteBuffer.wrap(byteArray);
		byteBuff.order(ByteOrder.LITTLE_ENDIAN); // Reverses byte order.

		
		DataInputStream dataStream = new DataInputStream(zipInput);
		
		version = dataStream.readByte();
		if (version < 0)
			version = version + 256;
		int topHalf = dataStream.readByte();
		if (topHalf < 0)
			topHalf = topHalf + 256;
		version = version + topHalf * 256;
		//System.out.println("Version = " + version);


		//TODO:  Switch when dealing with later files.
		dataStream.readFully(byteArray,0,size-2);
		
		
		// Close all streams
		in.close();
		zipInput.close();
		dataStream.close();
		
		//read file:
		readFromFile(filename, d);
	}
	
	/**
	 * Reads an unzipped file.
	 * @param filename - filename of particle.
	 * @return - the particle created from the file information.
	 */
	public void readUnzipped(String filename, Date d) throws IOException {
		
		
		File f = new File(filename);
		int size = (int) f.length();
//		Wraps the byte array in the byte buffer.
		byteArray = new byte[size];
		byteBuff = ByteBuffer.wrap(byteArray);
		byteBuff.order(ByteOrder.LITTLE_ENDIAN); // Reverses byte order.
		
		
		FileInputStream in = new FileInputStream(filename);
		DataInputStream dataStream = new DataInputStream(in);
		version = dataStream.readByte();
		if (version < 0)
			version = version + 256;
		int topHalf = dataStream.readByte();
		if (topHalf < 0)
			topHalf = topHalf + 256;
		version = version + topHalf * 256;

//		 Using the readFully method gets an EOFException.
		dataStream.read(byteArray,0,size-2);
		
		
		//Close all streams.
		in.close();
		dataStream.close();
		
		//read file:
		readFromFile(filename, d);
	}
	
	public void readFromFile(String name, Date d) {
		byteBuff.clear(); // Clears the byte array.
		// Read information from the byte array.
		//version = byteBuff.getShort();
		switch(version) {
			case 201: case 202: {
				readFromOldFile(name, d);
				break;
			}
			case 770: case 768: {
				readFromNewFile(name, d);
				break;
			}
			default: readFromNewFile(name, d);
		}
	}
	/**
	 * Reads the information from the file.  Variables that are not needed
	 * are commented out; uncomment to use them.
	 *@return - the particle created from the file information.
	 */
	public void readFromOldFile(String name, Date d) {
		//Instantiate arrays that will contain the data.

		// Read information from the byte array.
		int numpoints = 
			byteBuff.getInt();
		assert (numpoints == 30000) : 
			"Version 201 and 202 ATOFMS files should have 30000 spectrum points";
		
		posdata = new int[numpoints];
		negdata = new int[numpoints];
		//int counter = 
			byteBuff.getInt();
		int scatdelay = 
			byteBuff.getInt();
		//float speed = 
			byteBuff.getFloat();
		float particlesize = byteBuff.getFloat();
		//String serialnum = 
			getByteString(byteBuff,20);
		//int notused = 
			byteBuff.getInt();
		iontype = 
			byteBuff.getShort();
		//long timestamp = 
			byteBuff.getInt();
		//String timetext = 
			getByteString(byteBuff,20);
		float laserpow = byteBuff.getFloat();
		//int posarea = 
		byteBuff.getInt();
		//int posbase = 
		byteBuff.getInt();
		//short calibrate = 
		byteBuff.getShort();
		//double posslope = 
		byteBuff.getDouble();
		//double posintercept = 
		byteBuff.getDouble();
		skipBytes(byteBuff, 8*2*MAX_PEAKS);
		//double negslope = 
		byteBuff.getDouble();
		//double negintercept = 
		byteBuff.getDouble();
		skipBytes(byteBuff, 8*2*MAX_PEAKS);	
		//short databits = 
		byteBuff.getShort();
		float digitrate = byteBuff.getFloat();
		//int negarea = 
		byteBuff.getInt();
		//int negbase = 
		byteBuff.getInt();
		//String reserved = 
		getByteString(byteBuff,248);
		// Reads the data into the int arrays.
		// Version 201 uses chars, version 202 uses unsigned shorts.
		short shrt = 0;
		if (version == 201) {
			for (int i=0; i < numpoints; i++)
			{
				int temp = byteBuff.get();
				if (temp < 0)
					posdata[i] = temp + 256;
				else	
					posdata[i] = temp;
			}
			for (int i=0; i < numpoints; i++) 
			{
				int temp = byteBuff.get();
				if (temp < 0)
					negdata[i] = temp + 256;
				else
					negdata[i] = temp;
			}
		}
		else { 
			for (int i=0; i < numpoints; i++) {
				posdata[i] = byteBuff.getChar();
			}
			for (int i=0; i < numpoints; i++) {
				negdata[i] = byteBuff.getChar();
			}
		}
		// Create the ATOFMS particle.

		particle = createParticle(name, d, laserpow, 
				digitrate, scatdelay, posdata, negdata);

	}
	
	/**
	 * Reads the information from the file.  Variables that are not needed
	 * are commented out; uncomment to use them.
	 *@return - the particle created from the file information.
	 */
	public void readFromNewFile(String name, Date d) {
		//Instantiate arrays that will contain the data.
		
		
		// Read information from the byte array.
		int numPoints = 
			byteBuff.getInt();
		posdata = new int[numPoints];
		negdata = new int[numPoints];
		//int counter = 
			byteBuff.getInt();
		int scatdelay = 
			byteBuff.getInt();
		//float speed = 
			byteBuff.getFloat();
		//float particlesize = 
			byteBuff.getFloat();
		//String serialnum = 
			getByteString(byteBuff,20);
		//TODO:  Make sure reading 
			// particles with only positive
			// or negative spectra works
		iontype = byteBuff.getShort();
		//long timestamp = 
			byteBuff.getInt();
		//This is the other part of timestamp.  Either they 
		//reversed the byte order but not the bit order for this 
		//value, or, what I think is more likely, they used a 32-bit
		// time value in the first 4 bytes and then didn't use the
		// next 4.
		float laserpow = 
			byteBuff.getFloat();
		//int posarea = 
			byteBuff.getInt();
		//int posbase = 
			byteBuff.getInt();
		//int negarea = 
			byteBuff.getInt();
		//int negbase = 
			byteBuff.getInt();
		//boolean calibrate = 
			byteBuff.getInt();
		//double posslope = 
			byteBuff.getDouble();
		//double posintercept = 
			byteBuff.getDouble();
		//PosCalibData below:
			skipBytes(byteBuff, 256);
		//double negslope = 
			byteBuff.getDouble();
		//double negintercept = 
			byteBuff.getDouble();
		//NegCalibData below
			skipBytes(byteBuff, 256);	
		//short mode = 
			byteBuff.getShort();
		float digitrate = 
			byteBuff.getFloat();
		//String reserved = 
			getByteString(byteBuff,64);
		//Short inletType = 
			byteBuff.getShort();
		//boolean bUseClassifier =
			byteBuff.getInt();
		//int DMA = 
			byteBuff.getInt();
		//int particleSize =
			byteBuff.getInt();
		//int sheathFlow = 
			byteBuff.getInt();
		//int sampleFlow = 
			byteBuff.getInt();
		//boolean bRunSettings =
			byteBuff.getInt();
		//int intervalLength =
			byteBuff.getInt();
		//int numIntervals = 
			byteBuff.getInt();
		//short incrementMode =
			byteBuff.getShort();
		//int endParticleSize =
			byteBuff.getInt();
		//unkown bytes below:
			byteBuff.getShort();
			//skipBytes(byteBuff, 6);
		// Reads the data into the int arrays.
		// EDR chars, non-EDR uses unsigned shorts.
		if (version == 768) {
			for (int i=0; i < numPoints; i++)
			{
				int temp = byteBuff.get();
				if (temp < 0)
					posdata[i] = temp + 256;
				else	
					posdata[i] = temp;
			}
			for (int i=0; i < numPoints; i++) 
			{
				int temp = byteBuff.get();
				if (temp < 0)
					negdata[i] = temp + 256;
				else
					negdata[i] = temp;
			}
		}
		else { 
			for (int i=0; i < numPoints; i++) {
				posdata[i] = byteBuff.getChar();
				//System.out.println("PosData "+i+": "+posdata[i]);
			}
			for (int i=0; i < numPoints; i++) {
				negdata[i] = byteBuff.getChar();
			}
		}

		particle = createParticle(name, d, laserpow, 
				digitrate, scatdelay, posdata, negdata);

	}
	
	public ATOFMSParticle createParticle(String name,
			Date time,
			float laserpow,
			float digitrate,
			int scatdelay,
			int[] posdata,
			int[] negdata) {

		ATOFMSParticle p = new ATOFMSParticle(name, time, laserpow, 
				digitrate, scatdelay, posdata, negdata);
		return p;
	}
	
	
	// Reads a String from the byte array.
	public String getByteString (ByteBuffer byteBuffer, int num) {
		byte[] b = new byte[num];
		byteBuffer.get(b,0,num);
		StringBuffer s = new StringBuffer(num);
		for(int i = 0; i < num; i++)
			s.append((char)b[i]);
		return s.toString();
	}
	
	// Skips num bytes - designed to skip the calibration data.
	public void skipBytes(ByteBuffer byteBuffer, int num) {
		byteBuffer.position(num + byteBuffer.position());
	}
	
	// Returns the particle.
	public ATOFMSParticle getParticle() {
		return particle;
	}
	
	public static void main(String[] args) throws IOException {
		ATOFMSParticle.currCalInfo = new CalInfo();
		ATOFMSParticle.currPeakParams = new PeakParams(30,30,0.01f,.50f);
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Date d;
		try { d = df.parse("3/13/2002 12:04:44"); } catch (Exception e) {d = null;}
		//ReadSpec rs = new ReadSpec
		//("C:\\Documents and Settings\\andersbe\\My Documents\\" +
		//		"atofms data\\07-27-2004\\a\\" +
		//		"a-040727113411-00002.amz");
		ReadSpec rs = new ReadSpec
		("C:\\Documents and Settings\\andersbe\\My Documents\\" +
				"atofms data\\Engine Data\\03-13-2002\\e\\" +
				"e-020313120444-00001.ams", d);
		//ReadSpec rs = new ReadSpec
		//("C:\\Documents and Settings\\andersbe\\My Documents\\" +
		//		"atofms data\\RSC-YS-TOF-1\\RSC-YS-TOF-1\\a\\" +
		//		"a-030902173038-00001.amz");
		
	}
}


