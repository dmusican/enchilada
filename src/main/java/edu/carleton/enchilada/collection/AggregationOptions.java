package edu.carleton.enchilada.collection;

import java.util.*;

/**
 * Struct-like class to hold onto Collection-specific
 * aggregation options.
 * @author gregc
 *
 */
public class AggregationOptions {
	public static enum CombiningMethod { SUM, AVERAGE };
	
	// These are only relevant to ATOFMS:
	public double peakTolerance = .4;
	public boolean produceParticleCountTS = true;
	public boolean allMZValues = false;
	public CombiningMethod combMethod = CombiningMethod.SUM;
	public ArrayList<Integer> mzValues;
	public String mzString = "";
	public AggregationOptions(){
		this.setDefaultOptions();
	}

	/**
	 * Produces a String representing the SQL embodiment of the chosen combination method
	 * @return String representation of the combination method
	 */
	public String getGroupMethodStr() {
		if(combMethod == AggregationOptions.CombiningMethod.SUM) return new String("SUM"); 
		return new String("AVG");
	}
	
	/**
	 * Takes a String representing user input and transforms and stores it as the ArrayList representing the 
	 * m/z values to aggregate
	 * @param mzString String representation of values
	 */
	public void setMZValues(String mzString) {
		this.mzString = mzString;
		
		ArrayList<Integer> tempValues = new ArrayList<Integer>();
		

		allMZValues = false;
		if (mzString.trim().equals("")) {
			allMZValues = true;
			mzValues = null;
			return;
		}
		
		String[] ranges = mzString.split(",");
		for (int i = 0; i < ranges.length; i++) {
			String range = ranges[i].trim();
			String[] splitRange = range.split(" to ");
			int low = Integer.parseInt(splitRange[0]);
			int high = low;
			if(splitRange.length == 2) high = Integer.parseInt(splitRange[1]);
			
			// Swap if the user got them backwards...
			if (low > high) {
				int temp = low;
				high = temp;
				low = high;
			}
			
			if (low < -600 || low > 600 || high < -600 || high > 600)
				throw new NumberFormatException();
			
			while (low <= high)
				tempValues.add(low++);
		}
		
		Collections.sort(tempValues);
		mzValues = tempValues;
	}
	
	public void setDefaultOptions() {
		// These are only relevant to ATOFMS:
		peakTolerance = .4;
		produceParticleCountTS = true;
		combMethod = CombiningMethod.SUM;
		mzValues = new ArrayList<Integer>();
		
		// These two initializations go together: an empty mzString means that
		// all mzValues should be used.
		allMZValues = true;
		mzString = "";
	}
}
