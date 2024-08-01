package edu.carleton.enchilada.chartlib.hist;

/**
 * BinAddressableArrayList.java - Slightly modified ArrayList for holding histograms.
 * 
 * There's no add() method, and the set method, expandAndSet(float, T), is
 * protected.  This is because the only child of this class, ChainingHistogram,
 * doesn't want that capability to be public:  rather than setting the entry
 * to something, it really wants to modify the entry.
 *
 * get(index) will not give you an IndexOutOfBoundsException if you add
 * past the end of the list: instead it just adds 0s between the current end
 * of the list and the new element.
 * 
 * @author Thomas Smith
 */


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/*
 * Basically, a BinAddressableArrayList is an ArrayList that can be accessed by peak height
 * rather than integral index.  You tell it how wide an index will be, and
 * it does the rest.
 */

public class BinAddressableArrayList<T> {
	/**
	 * I have no idea what a serialVersionUID is.
	 */
	private static final long serialVersionUID = 6682890280462067242L;
	private float binWidth;

	
	protected ArrayList<T> list;
	
	public BinAddressableArrayList(float binWidth) {
		super();
		list = new ArrayList<T>();
		this.binWidth = binWidth;
	}

	// revision that has 0's stored in the list: 1.11. HistListTest 1.3.
	/**
	 * Map float index to integral index
	 */
	protected int heightToIndex(float height) {
		return (int)(height / binWidth);
	}
	
	/**
	 * Map integral index to (minimum) float index
	 */
	protected float indexToMinHeight(int index) {
		return (index) * binWidth;
	}
	
	/**
	 * Get this index from the ArrayList
	 * 
	 * @note This used to be implemented using a try/catch block, which was
	 * bad coding style.  But it also made it at least 10 times slower.  There's
	 * another copy of this code floating around somewhere, which would benefit
	 * from this speedup if it ever gets used again.
	 */
	public T getByIndex(int index) {
		if (list.size() <= index) {
			return null;
		} else {
			return list.get(index);
		}
	}
	
	/**
	 * Get the contents of the bin that *height* would fall into.
	 */
	public T get(float height) {
		int index = heightToIndex(height);
		if (list.size() <= index) {
			return null;
		} else {
			return list.get(index);
		}
	}
	
	public float getIndexMin(int index) {
		return indexToMinHeight(index);
	}
	public float getIndexMax(int index) {
		return indexToMinHeight(index + 1);
	}
	public float getIndexMiddle(int index) {
		return indexToMinHeight(index) + (0.5f * binWidth);
	}
	
	public float getBinWidth() {
		return binWidth;
	}

	protected void setBinWidth(float binWidth) {
		this.binWidth = binWidth;
	}
	
	public void clear() {
		list.clear();
	}
	
	/**
	 * The maximum bin index + 1, not the number of particles represented 
	 * in the histogram.
	 */
	public int size() {
		return list.size();
	}
	
	protected void expandAndSet(float peakHeight, T value) {
		ensureBinExists(peakHeight);
		list.set(heightToIndex(peakHeight), value);
	}
	
	protected void ensureBinExists(float targetHeight) {
		int binsToAdd = 1 + 
			(int) ((targetHeight - indexToMinHeight(list.size())) / binWidth);
		while (binsToAdd >= 0) {
			list.add(null);
			binsToAdd--;
		}
	}
	
	public boolean equals(Object thatObject) {
		if (thatObject == null || !(thatObject.getClass().equals(this.getClass())))
			return false;
		BinAddressableArrayList that = (BinAddressableArrayList) thatObject;
		
		// need to use set equality.
		
		if (that.binWidth != this.binWidth) return false;
		
		
		for (int i = 0; i < list.size(); i++) {
			HashSet<T> thisSet, thatSet;
			
			if (list.get(i) == null) {
				thisSet = new HashSet<T>();
			} else {
				thisSet = new HashSet<T>((Collection) list.get(i));
			}
			
			if (i >= that.list.size() || that.list.get(i) == null) {
				thatSet = new HashSet<T>();
			} else {	
				thatSet = new HashSet<T>((Collection) that.list.get(i));
			}
			
			if (! thisSet.equals(thatSet)) {
				System.out.println("Set inequality on m/z " + i);
				return false;
			}
		}
		
		
		return true;
	}
	
	public Iterator<T> getIterator(float min, float max) {
		ensureBinExists(max);
		return list.subList(heightToIndex(min), heightToIndex(max) + 1).iterator();
	}
}
