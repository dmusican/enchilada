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
 * The Original Code is EDAM Enchilada's sparse BinnedPeakList.
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

package edu.carleton.enchilada.analysis;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author andersbe
 * @author smitht
 * @author jtbigwoo
 *
 * An implementation of a sparse array, this class is essentially
 * a peak list where every key is an integer value (rounded 
 * appropriately from a float).  Provides methods for adding peaks
 * from a regular peaklist, as well as methods for adding values
 * with no checks.
 */
public class BinnedPeakList implements Iterable<BinnedPeak> {
	
	protected SortedMap<Integer, Float> peaks;

	private Normalizable normalizable;
	public static long distTime = 0;
	public static long dist2Time = 0;
	private boolean isTransformed;

	/**
	 * A constructor for the peaklist, initializes the underlying
	 * ArrayLists to a size of 20.
	 */
	public BinnedPeakList(Normalizable norm)
	{
		peaks = new TreeMap<Integer, Float>();
		normalizable = norm;
	}

	/**
	 * A constructor for the peaklist, initializes the underlying
	 * ArrayLists to a size of 20.
	 */
	public BinnedPeakList(Normalizable norm, Map<Integer,Float> m)
	{
		peaks = new TreeMap<Integer, Float>(m);
		normalizable = norm;
	}

	/**
	 * Creates a BinnedPeakList with a new Normalizer
	 */
	public BinnedPeakList() {
		peaks = new TreeMap<Integer, Float>();
		normalizable = new Normalizer();
	}
	
	public Normalizable getNormalizable(){
		return normalizable;
	}
	
	/**
	 * Deep-copies the data in this BinnedPeakList from another BinnedPeakList
	 * @param original the BinnedPeakList to copy from
	 * @author dmusican
	 */
	public void copyBinnedPeakList(BinnedPeakList original) {
		// Note that the TreeMap copy-constructor is likely a shallow copy;
		// it does not copy the keys and values. But this contains Integer and
		// Float immutable objects, so this doesn't matter.
		peaks = new TreeMap<Integer,Float>(original.peaks);
		normalizable = original.getNormalizable();
	}
	
	// return a copy of this BPL, remember if it was xformed -- MM 2014
	public BinnedPeakList copyOf() {
		BinnedPeakList copy = new BinnedPeakList();
		copy.copyBinnedPeakList(this);
		copy.isTransformed = this.isTransformed;
		return copy;
	}
	
	/**
	 * Compares the BinnedPeak data of two BinnedPeakLists
	 * @param toCompare the other BinnedPeakList to use
	 * @return true if equal
	 */
	public boolean comparePeakLists(BinnedPeakList toCompare) {
		boolean equal = true;
		Iterator<Map.Entry<Integer, Float>> i = peaks.entrySet().iterator();
		Iterator<Map.Entry<Integer, Float>> i2 = toCompare.getPeaks().entrySet().iterator();
		Entry<Integer, Float> entry;
		Entry<Integer, Float> entry2;
		while(i.hasNext() && i2.hasNext()){
			entry = i.next();
			entry2 = i2.next();
			if(!entry.getKey().equals(entry2.getKey()) || !entry.getValue().equals(entry2.getValue())){
				equal = false;
				break;
			}
		}
		return equal;
	}
	
	/**
	 * Checks the BinnedPeakList for zero values
	 * @return true if this BinnedPeakList contains any zero values (peak areas)
	 */
	public boolean containsZeros() {
		
		Iterator<BinnedPeak> iter = iterator();
		BinnedPeak p;
		while (iter.hasNext()) {
			p = iter.next();
			if (p.getValue()==0)
				return true;
		}
		return false;
	}
	
	/**
	 * Create a new BinnedPeakList and copy in all non-zero valued 
	 * BinnedPeaks from this BinnedPeakList
	 * @return a BinnedPeakList with no zero-valued BinnedPeaks
	 */
	public BinnedPeakList getFilteredZerosList() {
		BinnedPeakList newSums = new BinnedPeakList(new Normalizer());
		Iterator<BinnedPeak> iter = iterator();
		BinnedPeak p;
		while (iter.hasNext()) {
			p = iter.next();
			if (p.getValue()!=0) {
				newSums.add(p);
			}
		}
		return newSums;
	}
	
	/**
	 * Find whether this BinnedPeakList is normalized with respect to the given DistanceMetric
	 * @param dMetric the DistanceMetric with which to test magnitude
	 * @return true if the magnitude of this BinnedPeakList is with 0.00001 of 1
	 */
	public boolean isNormalized(DistanceMetric dMetric) {
		float mag = getMagnitude(dMetric);
		if(mag <= 1.0002f && mag >=0.9998f)
			return true;
		else
			return false;
	}
	
	/**
	 * Return the magnitude of this peaklist, according to the supplied
	 * distance metric (it varies according to measurement).
	 * @param dMetric the DistanceMetric to use. Definitions of magnitude are:
	 * 			DistanceMetric.CITY_BLOCK: the sum of the list's values
	 * 			DistanceMetric.EUCLIDEAN_SQUARED, DistanceMetric.DOT_PRODUCT:
	 * 				the standard deviation of the list
	 */
	public float getMagnitude(DistanceMetric dMetric)
	{
		float magnitude = 0;
		
		Iterator<Map.Entry<Integer, Float>> i = peaks.entrySet().iterator();
		if (dMetric == DistanceMetric.CITY_BLOCK)
			while (i.hasNext())
			{
				magnitude += i.next().getValue();
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			while (i.hasNext())
			{
				currentArea = i.next().getValue();
				magnitude += currentArea*currentArea;
			}
			magnitude = (float) Math.sqrt(magnitude);
		}
	//	if(magnitude>1.0)
	//		System.out.println("BAD MAGNITUDE " + magnitude);
		return magnitude;
	}
	
	/**
	 * Returns the magnitude of a peaklist represented by a one-dimensional
	 * array.  The method is static, since it does not apply directly to the 
	 * BinnedPeakList data structure, and it needs to be accessed from outside.
	 * 
	 * @author benzaids
	 * 
	 * @param peakarray - a one-dimensional array representing a peaklist.
	 * @param dMetric - the distance metric to be used in the calculation.
	 */
	public static float getMagnitude4Array(float[] peakarray, DistanceMetric dMetric)
	{
		float magnitude = 0;

		if (dMetric == DistanceMetric.CITY_BLOCK)
			for (int i = 0; i < peakarray.length; i++)
			{
				magnitude += peakarray[i];
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			for (int i = 0; i < peakarray.length; i++)
			{
				currentArea = peakarray[i];
				magnitude += currentArea*currentArea;
			}
			magnitude = (float) Math.sqrt(magnitude);
		}
		return magnitude;
	}
	
	public Pair<Float, Float> getNegPosMagnitude(DistanceMetric dMetric)
	{
		float negMagnitude = 0;
		float posMagnitude = 0;
		Entry<Integer, Float> entry;
		
		Iterator<Map.Entry<Integer, Float>> i = peaks.entrySet().iterator();
		if (dMetric == DistanceMetric.CITY_BLOCK)
			while (i.hasNext())
			{
				entry = i.next();
				if(entry.getKey()<0)
					negMagnitude += entry.getValue();
				else
					posMagnitude += entry.getValue();
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			while (i.hasNext())
			{
				entry = i.next();
				if(entry.getKey()<0)
					negMagnitude += entry.getValue()*entry.getValue();
				else
					posMagnitude += entry.getValue()*entry.getValue();
			}
			negMagnitude = (float) Math.sqrt(negMagnitude);
			posMagnitude = (float) Math.sqrt(posMagnitude);
		}
	//	if(magnitude>1.0)
	//		System.out.println("BAD MAGNITUDE " + magnitude);
		return new Pair<Float, Float>(negMagnitude, posMagnitude);
	}
	/**
	 * @author steinbel
	 * Gets the magnitude of either the negative or non-negative peaks only.
	 * @param dMetric		distance metric to use
	 * @param negative		true if only negative peaks desired, false for non-neg
	 * @return	the magnitude
	 */
	public float getPartialMag(DistanceMetric dMetric, boolean negative){
		float magnitude = 0;
		Iterator<BinnedPeak> iter = posNegIterator(negative);
		if (dMetric == DistanceMetric.CITY_BLOCK){
			
			while (iter.hasNext())
				magnitude += iter.next().getValue();
			
		} else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
					dMetric == DistanceMetric.DOT_PRODUCT){
			
			float currentArea;
			while (iter.hasNext()){
				currentArea = iter.next().getValue();
				magnitude += currentArea*currentArea;
			}
			magnitude = (float) Math.sqrt(magnitude);
	
		}
		
		return magnitude;
	}

	/**
	 * Find the distance between this peaklist and another one.
	 * @param other The peaklist to compare to
	 * @param metric The distance metric to use
	 * @return the distance between the lists.
	 */
	public float getDistance(BinnedPeakList other, DistanceMetric metric) {
		long beginTime = System.currentTimeMillis();
		/*
		 * The following distance calculation algorithm is very similar to the
		 * merge part of merge sort, where you riffle through both lists looking
		 * for the lowest entry, and choosing that one.  The extra condition
		 * is when we have an entry for the same dimension in each list,
		 * in which case we don't choose one but calculate the distance between
		 * them. 
		 */
		
		Map.Entry<Integer, Float> i = null, j = null;
		Iterator<Map.Entry<Integer, Float>> thisIter = peaks.entrySet().iterator(),
			thatIter = other.peaks.entrySet().iterator();
			
		float distance = 0;
		
		// if one of the peak lists is empty, do something about it.
		if (thisIter.hasNext()) {
			i = thisIter.next();
		}
		if (thatIter.hasNext()) {
			j = thatIter.next();
		}
		// both lists have some particles, so 
		while (i != null && j != null) {
			if (i.getKey().equals(j.getKey()))
			{
				distance += DistanceMetric.getDistance(i.getValue(),
						j.getValue(),
						metric);
				
				
				if (thisIter.hasNext())
					i = thisIter.next();
				else i = null;
				
				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
			else if (i.getKey() < j.getKey())
			{
				distance += DistanceMetric.getDistance(0, i.getValue(), metric);
			
				if (thisIter.hasNext())
					i = thisIter.next();
				else i = null;
			}
			else
			{
				distance += DistanceMetric.getDistance(0, j.getValue(), metric);
				
				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
		}
	
		if (i != null) {
			assert(j == null);
			distance += DistanceMetric.getDistance(0, i.getValue(), metric);
			while (thisIter.hasNext()) {
				distance += DistanceMetric.getDistance(0, 
						thisIter.next().getValue(), metric);
			}
		} else if (j != null) {
			distance += DistanceMetric.getDistance(0, j.getValue(), metric);
			while (thatIter.hasNext()) {
				distance += DistanceMetric.getDistance(0,
						thatIter.next().getValue(), metric);
			}
		}
		
		if (metric == DistanceMetric.DOT_PRODUCT)
		    distance = 1-distance; 
		// dot product actually comes up with similarity, rather than distance,
		// so we take 1- it to find "distance".

		float rndDist = normalizable.roundDistance(this, other, metric, distance);
		distTime += (System.currentTimeMillis()-beginTime);
		return rndDist;
	}
	
	
	/**
	 * Retrieve the value of the peaklist at a given key
	 * @param key	The key of the value you wish to
	 * 					retrieve.
	 * @return			The value at the given key.
	 */
	public float getAreaAt(int location)
	{
		Float area = peaks.get(location);
		if (area == null) {
			return 0;
		} else {
			return area;
		}
	}

	
	
	
	/**
	 * Find the distance between this peaklist and another one. By including
	 * the magnitude of the other peak list, the method can be run faster.
	 * @param other The peaklist to compare to
	 * @param magnitude The magnitude of the other peak list
	 * @param metric The distance metric to use
	 * @return the distance between the lists.
	 */
	public float getDistance(BinnedPeakList other, float magnitude,
							 DistanceMetric metric) {
		
		/*
		 * The following distance calculation algorithm is very similar to the
		 * merge part of merge sort, where you riffle through both lists looking
		 * for the lowest entry, and choosing that one.  The extra condition
		 * is when we have an entry for the same dimension in each list,
		 * in which case we don't choose one but calculate the distance between
		 * them. 
		 */
		long temptime = System.currentTimeMillis();
		
		float distance = magnitude;
		
		// loop over this peak list, accumulating magnitude as you go,
		// but calculating distance if match with other (and subtracting off
		// that portion from magnitude with other)
		for (Map.Entry<Integer,Float> i : peaks.entrySet()) {
			int iKey = i.getKey();
			float iValue = i.getValue();
			if (other.peaks.containsKey(iKey)) {
				float otherValue = other.peaks.get(iKey);
				distance = distance +
					(DistanceMetric.getDistance(iValue,otherValue,metric) -
					DistanceMetric.getDistance(0,otherValue,metric));
			}
			else {
				distance += DistanceMetric.getDistance(0, iValue, metric);
			}
		}
		
		if (metric == DistanceMetric.DOT_PRODUCT)
		    distance = 1-distance; 
		// dot product actually comes up with similarity, rather than distance,
		// so we take 1- it to find "distance".
		
		dist2Time += System.currentTimeMillis() - temptime;
		
		return normalizable.roundDistance(this, other, metric, distance);
	}
	
	
	/**
	 * Find the distance between this peaklist and another one. By including
	 * the magnitude of the other peak list, the method can be run faster.
	 * @param other The peaklist (in array form) to compare to
	 * @param magnitude The magnitude of the other peak list
	 * @param metric The distance metric to use
	 * @return the distance between the lists.
	 */
	public float getDistance(float[] other, float magnitude,
							 DistanceMetric metric, int zeroOffset) {
		
		/*
		 * The following distance calculation algorithm is very similar to the
		 * merge part of merge sort, where you riffle through both lists looking
		 * for the lowest entry, and choosing that one.  The extra condition
		 * is when we have an entry for the same dimension in each list,
		 * in which case we don't choose one but calculate the distance between
		 * them. 
		 */
		long temptime = System.currentTimeMillis();
		
		float distance = magnitude;
		
		// loop over this peak list, accumulating magnitude as you go,
		// but calculating distance if match with other (and subtracting off
		// that portion from magnitude with other)
		for (Map.Entry<Integer,Float> i : peaks.entrySet()) {
			int iKey = i.getKey();
			if (iKey >= -zeroOffset && iKey <= zeroOffset) {
				float iValue = i.getValue();
				if (other[iKey + zeroOffset] != 0) {
					float otherValue = other[iKey + zeroOffset];
					distance = distance + 
						(DistanceMetric.getDistance(iValue,otherValue,metric) - 
								DistanceMetric.getDistance(0,otherValue,metric));
				}
				else {
					distance += DistanceMetric.getDistance(0,iValue,metric);
				}
			}
		}
		
		if (metric == DistanceMetric.DOT_PRODUCT)
		    distance = 1-distance; 
		// dot product actually comes up with similarity, rather than distance,
		// so we take 1- it to find "distance".
		
		dist2Time += System.currentTimeMillis() - temptime;
		
		return normalizable.roundDistance(this, other, metric, distance);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Add a regular peak to the peaklist.  This actually involves
	 * quite a bit of processing.  First, each float key is
	 * rounded to its nearest integer value.  Then, that key
	 * is checked in the current peak to see if it already exists.
	 * If it does, it adds the value of the new peak to the 
	 * preexisting value.  This is done so that when you have two
	 * peaks right next to eachother (ie 1.9999 and 2.0001) that
	 * probably should be both considered the same element, the
	 * signal is doubled.  
	 * 
	 * @param key
	 * @param value
	 */
	public void add(float location, float area)
	{
		int locationInt;
		
		// If the key is positive or zero, then add 0.5 to round.
		// Otherwise, subtract 0.5 to round.
		if (location >= 0.0f)
			locationInt = (int) ((float) location + 0.5);
		else
			locationInt = (int) ((float) location - 0.5);
		
		add(locationInt, area);
	}
	
	/**
	 * This is just like add(float, float) except that it is assumed that
	 * rounding the peaks to the right location has been done already.
	 * @param location
	 * @param area
	 */
	public void add(int location, float area) {
		assert !(peaks.containsKey(location)== true && peaks.get(location)== null) : "null peak is present in list";
		Float tempArea = peaks.get(location);
		if (tempArea != null)
		{
			peaks.put(location, tempArea + area);
		} else {
			peaks.put(location, area);
		}
	}
	
	/**
	 * Adds a BinnedPeak, with the same checks as add(float, float).
	 * Equivalent to add(bp.key, bp.value).
	 * @param bp the BinnedPeak to add.
	 */
	public void add(BinnedPeak bp) {
		add(bp.getKey(), bp.getValue());
	}
	private void add(Entry<Integer, Float> entry) {
		add(entry.getKey(), entry.getValue());
		
	}
	
	/**
	 * Returns the number of locations represented by this 
	 * Binned peaklist
	 * @return the number of locations in the list
	 */
	public int length()
	{
		return peaks.size();
	}
	
	/**
	 * This skips all the checks of add().  Do not use this unless
	 * you are copying from another list: if you add a peak where
	 * another one is already, the first one will be lost.
	 * @param key	The key of the peak
	 * @param value	The value of the peak at that key.
	 */
	public void addNoChecks(int location, float area)
	{
		peaks.put(location, area);
	}
	
	/**
	 * Divide the value at each peak by this amount.
	 * @param divisor
	 */
	public void divideAreasBy(int divisor) {
		Map.Entry<Integer,Float> e;
		Iterator<Map.Entry<Integer,Float>> i = peaks.entrySet().iterator();
		
		while (i.hasNext()) {
			e = i.next();
			e.setValue(e.getValue() / divisor);
		}
	}
	
	/**
	 * Print a representation of this peak list.
	 */
	public void printPeakList() {
		System.out.println("printing peak list");
		Iterator<BinnedPeak> i = iterator();
		BinnedPeak p;
		while (i.hasNext()) {
			p = i.next();
			System.out.println(p.getKey() + ", " + p.getValue());
		}
	}

	/**
	 * Find the largest value contained in the peaklist.  (not its index, the
	 * value itself).
	 */
	public float getLargestArea() {
		return Collections.max(peaks.values());
	}
	public SortedMap<Integer, Float> getPeaks() {
		return peaks;
	}
	/**
	 * Find the sum of two particles.
	 * @param other the particle to add to this one.
	 */
	public void addAnotherParticle(BinnedPeakList other) {
		Iterator<Map.Entry<Integer, Float>> i = other.peaks.entrySet().iterator();
		while (i.hasNext()) {
			add(i.next());
		}
	}
	
	/** 
	 * Adds a particle of a certain weight.
	 * @param  other the binnedPeakList that you are adding
	 * @param  factor the weight of the binnedPeakList you wish to add
	 */
	public void addWeightedParticle(BinnedPeakList other, int factor) {
		Iterator<Map.Entry<Integer, Float>> iter = other.peaks.entrySet().iterator();
		Map.Entry<Integer,Float> temp;
		while (iter.hasNext()) {
			temp = iter.next();
			assert !(peaks.containsKey(temp.getKey())== true && peaks.get(temp.getKey())== null) : "null peak is present in list";
			Float curArea = peaks.get(temp.getKey());
			if(curArea != null) 
				peaks.put(temp.getKey(), curArea + temp.getValue() * factor);
			else
				peaks.put(temp.getKey(), temp.getValue() * factor);
		}
	}	
	public HashMap<Integer, Float> addWeightedToHash(HashMap<Integer, Float> hash, float factor) {
		Iterator<Map.Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Map.Entry<Integer,Float> temp;
		while (iter.hasNext()) {
			temp = iter.next();
			Float value = hash.get(temp.getKey());
			long beginTime = System.currentTimeMillis();
			if(value!= null) {
				hash.put(temp.getKey(), value+temp.getValue()*factor);
			}
			else {
				hash.put(temp.getKey(), temp.getValue()*factor);
			}
		}	
		return hash;
	}
	public void addWeightedParticle2 (BinnedPeakList other, int factor) {	
		BinnedPeakList reduced = new BinnedPeakList(normalizable);

		Map.Entry<Integer, Float> i = null, j = null;
		Iterator<Map.Entry<Integer, Float>> thisIter = peaks.entrySet().iterator(),
			thatIter = other.peaks.entrySet().iterator();
		
		float distance = 0;
		
		// if one of the peak lists is empty, do something about it.
		if (thisIter.hasNext()) {
			i = thisIter.next();
		}
		if (thatIter.hasNext()) {
			j = thatIter.next();
		}
		// both lists have some particles, so 
		while (i != null && j != null) {
			//if both have peaks at the next key, add the values together
			if (i.getKey().equals(j.getKey()))
			{
				i.setValue(i.getValue() + j.getValue() * factor);
				if (thisIter.hasNext())
					i = thisIter.next();
				else i = null;
				
				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
			//if only i has a value at the next key, move on
			else if (i.getKey() < j.getKey())
			{
				if (thisIter.hasNext())
				{
					i = thisIter.next();
			//		System.out.println(i);
				}
				else i = null;
			}
			//if only j has a value at the next key, add it to i
			else
			{
				reduced.addNoChecks(j.getKey(), j.getValue() * factor);
				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
		}
		this.peaks.putAll(reduced.peaks);
		//if there are more j's
		while (j != null) {
			this.addNoChecks(j.getKey(),j.getValue() * factor);
			if(thatIter.hasNext())
				j = thatIter.next();
			else j = null;
		}
		
	}
	public class Node{
		private Integer key;
		private Float value;
		public Node(int k, Float v){
			key = k;
			value = v;
		}
		public Integer getKey(){
			return key;
		}
		public Float getValue() {
			return value;
		}
	}
	public void addWeightedParticle3 (BinnedPeakList other, int factor) {	
		
		SortedMap<Integer, Float> newPeaks = new TreeMap<Integer, Float>();
		
		Map.Entry<Integer, Float> i = null, j = null;
		Iterator<Map.Entry<Integer, Float>> thisIter = peaks.entrySet().iterator(),
			thatIter = other.peaks.entrySet().iterator();
		
		
		//build the two arrays
		Node[] thisArray = new Node[this.peaks.size()];
		Node[] thatArray = new Node[other.peaks.size()];
		int index = 0;
		while (thisIter.hasNext()) {
			i = thisIter.next();
			thisArray[index] = new Node(i.getKey(), i.getValue());
			index++;
		}
		index = 0;
		while (thatIter.hasNext()) {
			j = thatIter.next();
			thatArray[index] = new Node(j.getKey(), j.getValue());
			index++;
		}
		
		//go across the arrays and merge them
		int cur = 0;
		int k = 0;
		while (k < thisArray.length && cur<thatArray.length)
		{
			if(thisArray[k].getKey().equals(thatArray[cur].getKey())){
				newPeaks.put(thisArray[k].getKey(), thisArray[k].getValue() + thatArray[cur].getValue() * factor);
				k++;
				cur++;
			}
			else if(thisArray[k].getKey() < thatArray[cur].getKey()) {
				newPeaks.put(thisArray[k].getKey(), thisArray[k].getValue());
				k++;
			}
			else {
				newPeaks.put(thatArray[cur].getKey(), thatArray[cur].getValue() * factor);
				cur++;
			}
		}
		while(k<thisArray.length) {
			newPeaks.put(thisArray[k].getKey(), thisArray[k].getValue());
			k++;
		}
		while(cur < thatArray.length) {
			newPeaks.put(thatArray[cur].getKey(), thatArray[cur].getValue() *factor);
			cur++;
		}
		this.peaks = newPeaks;
	}
	
	// method to permit choice of normalization technique
	// Michael Murphy 2014
	public void normalize(DistanceMetric dMetric, boolean posNegNorm) {
		if (posNegNorm)
			posNegNormalize(dMetric);
		else
			normalize(dMetric);
	}
	
	/**
	 * A method to normalize this BinnedPeakList.  Depending 
	 * on which distance metric is
	 * used, this method will change the peaklist so that the distance 
	 * from <0,0,0,....,0> to the vector represented by the list is equal to 1.
	 * @param 	dMetric the distance metric to use to measure length
	 */
	public void normalize(DistanceMetric dMetric) {
		normalizable.normalize(this,dMetric);
	}
	
	/**
	 * @author steinbel
	 * A method to normalize this BinnedPeakList by normalizing the positive
	 * and negative spectra separately and then together.
	 * @param dMetric  the distance metric to use when normalizing
	 */
	public float posNegNormalize(DistanceMetric dMetric){
		return normalizable.posNegNormalize(this, dMetric);
	}
	
	/**
	 * @author steinbel
	 * A method to reduce the peak area by the power passed in (preprocessing
	 * to be used before clustering).
	 * @param power	The power to which to raise the area of the peaks.  (.5 is good.)
	 */
	public void preProcess(double power){
		normalizable.reducePeaks(this, power);
	}
	
	/**
	 * Change the Normalizer that will be used during calls to normalize() on
	 * this peaklist.
	 * @param norm the new normalizer.
	 */
	public void setNormalizer(Normalizer norm) {
		normalizable = norm;
	}
	
	public void setNormalizer(DummyNormalizer norm) {
		normalizable = norm;
	}
	
	// used for testing BIRCH
	public boolean testForMax(int max) {
		Iterator<BinnedPeak> iterator = iterator();
		BinnedPeak peak;
		while (iterator.hasNext()) {
			peak = iterator.next();
			if (peak.getValue() > max)
				return false;
		}
		return true;
	}
	
	/** 
	 * Multiply each value by a scalar factor.
	 * @param factor
	 */
	public void multiply(float factor) {
		Iterator<Map.Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Map.Entry<Integer,Float> temp;
		while (iter.hasNext()) {
			temp = iter.next();
			temp.setValue(temp.getValue() * factor);
		}
	}
	
	/** 
	 * Divide each value by a scalar factor.
	 * @param factor
	 */
	public void divideAreasBy(float factor) {
		Iterator<Map.Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Map.Entry<Integer,Float> temp;
		while (iter.hasNext()) {
			temp = iter.next();
			temp.setValue(temp.getValue() / factor);
		}
	}

	// peak area transforms -- Michael Murphy 2014
	public BinnedPeakList transformAreas(PeakTransform transform) {
		if (transform == PeakTransform.NONE || isTransformed) // prevent compound log transforms...
			return this;
		
		Iterator<Map.Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Map.Entry<Integer,Float> temp;
		float value;
		while (iter.hasNext()) {
			temp = iter.next();
			if (transform == PeakTransform.LOG)
				temp.setValue(new Float(Math.log10(temp.getValue())));
			else if (transform == PeakTransform.SQRT)
				temp.setValue(new Float(Math.sqrt(temp.getValue())));
		}
		
		isTransformed = true;
		return this;
	}
	
	public boolean isTransformed() {
		return isTransformed;
	}
	
	/**
	 * Return an iterator view of the binned peak list.  Note that modifying
	 * the elements accessed by the iterator will NOT modify the peaklist itself.
	 */
	public Iterator<BinnedPeak> iterator() {
		return new Iter(this);
	}
	
	/**
	 * @author steinbel
	 * Return a positive/negative iterator for the binned peak list.
	 * @param negative - true for negative peaks only, false for non-neg. only
	 * @return an iterator that only goes through either negative or non-neg. peaks
	 */
	public Iterator<BinnedPeak> posNegIterator(boolean negative){
		return new Iter(this, negative);
	}
	
	/**
	 * Warning!  This does not actually provide you with access to the
	 * underlying map structure, so any changes made to elements accessed by
	 * this iterator will NOT BE REFLECTED in the BPL itself.
	 * 
	 * @author smitht
	 *
	 */
	public class Iter implements Iterator<BinnedPeak> {
		private Iterator<Map.Entry<Integer,Float>> entries;
		
		/*
		 * Copy the set of peaks into a list, and get an iterator on the list.
		 * This happens to be sorted, yay.
		 */
		public Iter(BinnedPeakList bpl) {
			this.entries = bpl.peaks.entrySet().iterator();
		}
		
		/**
		 * @author steinbel
		 * overloaded constructor gives us an iterator for either negative or
		 * non-negative peaks only
		 * @param bpl	the list through which to iterate
		 * @param negative	true if only negative peaks desired, false for non-neg.
		 */
		public Iter(BinnedPeakList bpl, boolean negative){
			BinnedPeakList sub = new BinnedPeakList();
			for (BinnedPeak peak : bpl)
				if( (!negative && peak.getKey() >= 0) || (negative && peak.getKey() <0) )
					sub.add(peak);
			this.entries = sub.peaks.entrySet().iterator();
		}

		public boolean hasNext() {
			return entries.hasNext();
		}

		public BinnedPeak next() {
			Map.Entry<Integer,Float> e = entries.next();
			return new BinnedPeak(e.getKey(), e.getValue(), e);
		}

		public void remove() {
			throw new Error("Not implemented!");
		}
	}
	
}
