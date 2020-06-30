package edu.carleton.enchilada.chartlib.hist;

import java.awt.Color;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeak;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.Database.BPLOnlyCursor;


/**
 * Holds all the information needed to graph 
 * @author smitht
 *
 */

public class HistogramDataset {
	public ChainingHistogram[] hists;
	public int count;
	public Color color;
	
	private final static int maxMZ = 500; // ugly!  should be fixed sometime!
	private static float binWidth = 0.01f; // fix!
	
	public HistogramDataset(int count, ChainingHistogram[] hists, Color color) {
		this.count = count;
		this.hists = hists;
		this.color = color;
	}
	
	public static HistogramDataset[] analyseCollection(int collID, Color c) 
		throws SQLException
	{
		Database db = HistogramsPlot.getDB();
		Collection coll = db.getCollection(collID);
		if (! coll.getDatatype().equals("ATOFMS")) 
			throw new IllegalArgumentException("Spectrum Plots only work " +
					"on ATOFMS for now.");
		// I haven't tried them on AMS, they might work.  I don't understand AMS
		// enough to know.
		Database.BPLOnlyCursor particleCursor = db.getBPLOnlyCursor(coll);
		
		HistogramDataset[] ret = analyseBPLs(particleCursor, c);
		
		particleCursor.close();
		
		return ret;
	}
	
	public static HistogramDataset[] analyseBPLs(BPLOnlyCursor b, Color c)
	{
		BinnedPeakList peakList;
		int partnum = 0;
	
		ChainingHistogram[] histograms, posHists = new ChainingHistogram[maxMZ],
			negHists = new ChainingHistogram[maxMZ];
		
		while (b.next()) {
			ParticleInfo t = b.getCurrent();
			peakList = t.getBinnedList();
			peakList.normalize(DistanceMetric.EUCLIDEAN_SQUARED);
	
			++partnum;
			
			for (BinnedPeak p : peakList) {
				if (p.getKey() >= maxMZ)
					continue;
				else if (p.getKey() >= 0)
					histograms = posHists;
				else if (p.getKey() > - maxMZ) {
					histograms = negHists;
					p.setKey(- p.getKey());
				} else
					continue;
				if (histograms[p.getKey()] == null) {
					histograms[p.getKey()] = new ChainingHistogram(binWidth);
				}
				histograms[p.getKey()].addPeak(p.getValue(), t.getID());
			}
		}
		
		return new HistogramDataset[] {
				new HistogramDataset(partnum, posHists, c),
				new HistogramDataset(partnum, negHists, c)
		};
	}

	
	/**
	 * Warning!  As currently implemented, this will indeed take the intersection,
	 * but the count of particles in the resulting dataset will simply be the length
	 * of the list of AtomIDs, rather than the actual count of particles.  To
	 * fix this, change the HashSet to a HashMap<Integer, Boolean>, set an atom's
	 * entry to true if it is used, and return the count of true ones.  But that's
	 * more complicated.
	 * 
	 */
	public static HistogramDataset[] intersect(HistogramDataset[] spectra, 
			ArrayList<Integer> atomIDs)
	{
		HashSet<Integer> keep = new HashSet<Integer>(atomIDs);
		HistogramDataset[] intersected = new HistogramDataset[spectra.length];
		
		// probably once each for positive and negative spectra
		for (int ds = 0; ds < spectra.length; ds++) {
			intersected[ds] = new HistogramDataset(atomIDs.size(),
					new ChainingHistogram[maxMZ], spectra[ds].color);
			
			for (int mz = 0; mz < spectra[ds].hists.length; mz++) {
				if (spectra[ds].hists[mz] == null) continue;
				
				intersected[ds].hists[mz] = new ChainingHistogram(binWidth);
				ChainingHistogram src = spectra[ds].hists[mz], 
					dest = intersected[ds].hists[mz];
				
				ArrayList<Integer> srcList, destList;
				for (int index = 0; index < src.size(); index++) {
					srcList = src.get(src.getIndexMiddle(index));
					if (srcList == null) continue;
					
					destList = new ArrayList<Integer>();
					
					for (Integer id : srcList)
						if (keep.contains(id))
							destList.add(id);

					dest.setListAt(destList, src.getIndexMiddle(index));
				}
			}
		}
		return intersected;
	}
	
	public static HistogramDataset[] getSelection(HistogramDataset[] spectra,
			List<BrushSelection> selection) {
		ArrayList<Integer> atomIDs = new ArrayList<Integer>();
		
		for (BrushSelection sel : selection) {
			HistogramDataset ds = spectra[sel.spectrum];
			if (ds == null) continue;
			if (ds.hists == null) continue;
			ChainingHistogram hist = ds.hists[sel.mz];
			if (hist == null) continue;
			
			Iterator<ArrayList<Integer>> bins = hist.getIterator(sel.min, sel.max);
			
			while (bins.hasNext()) {
				ArrayList<Integer> bin = bins.next();
				if (bin == null) continue;
				atomIDs.addAll(bin);
			}
		}
		
		// It happens not to matter if there are duplicate atomIDs:  intersect()
		// constructs a hashset from the arraylist, and a set doesn't care about
		// duplicates.
		return intersect(spectra, atomIDs);
	}
	
	/**
	 * Equals is needed for the unit test.
	 */
	@Override
	public boolean equals(Object thatObject) {
		if (thatObject == null || !(thatObject instanceof HistogramDataset)) 
			return false;
		HistogramDataset that = (HistogramDataset) thatObject;
		
		if (this.count != that.count) return false;
		
		for (int i = 0; i < hists.length; i++) {
			if (hists[i] == null || hists[i].getHitCount() == 0)
				if (that.hists[i] == null || that.hists[i].getHitCount() == 0) {
					continue;
				} else {
					return false;
				}
			
			if (!(hists[i].equals(that.hists[i]))) return false;
		}
		
		return true;
	}
	
}
