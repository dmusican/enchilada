package edu.carleton.enchilada.gui;

import java.util.*;

import javax.swing.*;

import edu.carleton.enchilada.collection.AggregationOptions;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.*;

public class Aggregator {
	private JFrame parentFrame;
	private String timeBasisSQLstring;
	private boolean baseOnCollection;
	private Database db;
	
	private Calendar start, end, interval;
	private Collection basisCollection;
	
	private Aggregator(JFrame parentFrame, Database db, boolean baseOnCollection) {
		this.parentFrame = parentFrame;
		this.baseOnCollection = baseOnCollection;
		this.db = db;
	}
	
	public Aggregator(JFrame parentFrame, Database db, Collection basisCollection) {
		this(parentFrame, db, true);
		
		this.basisCollection = basisCollection;
	}
	
	public Aggregator(JFrame parentFrame, Database db, Calendar start, Calendar end, Calendar interval) {
		this(parentFrame, db, false);
		
		this.start = start;
		this.end = end;
		this.interval = interval;
		baseOnCollection = false;
	}

	/**
	 * @param collections the collections to be aggregated
	 * @return initial progressBar for createAggregateTimeSeries
	 * 
	 * This method should always be called before createAggregateTimeSeries;
	 * it generates and displays the initial modal progress bar.
	 * This is done outside of the actual createAggregateTimeSeries method
	 * because display of this method needs to happen from the EDT before any
	 * other events have the opportunity to get in between.
	 */
	
	public ProgressBarWrapper createAggregateTimeSeriesPrepare(Collection[] collections) {
		ProgressBarWrapper progressBar = 
			new ProgressBarWrapper(parentFrame, "Retrieving Valid M/Z Values", collections.length);
		progressBar.constructThis();
		return progressBar;
	}

	
	/**
	 * @param syncRootName name for time series
	 * @param collections the collections to be aggregated
	 * @param progressBar the initial progress bar, created via createAggregateTimeSeriesPrepare
	 * @param mainFrame main Enchilada frame containing tree to be updated
	 * @return collection id for the new collection
	 * 
	 * This method should always be called from outside the EDT, e.g. via
	 * SwingWorker.
	 * 
	 * Be aware that changing the way collectionIDs are allocated here will change how SyncAnalyzePanel and
	 *  Collection.compareTo works for aggregate collections.
	 * @return CollectionID of the new Collection or -1 if aggregation failed or was cancelled
	 */
	public int createAggregateTimeSeries(String syncRootName,
			Collection[] collections, final ProgressBarWrapper progressBar,
			MainFrame mainFrame) throws InterruptedException, AggregationException{
		final int[][] mzValues = new int[collections.length][];
		final int[] numSqlCalls = {1};
		
		//get the valid m/z values for each collection individually
		Date startTime,endTime; // start and end dates.
		for (int i = 0; i < collections.length; i++) {
			final String text = "Retrieving Valid M/Z Values for Collection # "+(i+1)+" out of "+collections.length;
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					progressBar.increment(text);
				}
			});
			Collection curColl = collections[i];
			AggregationOptions options = curColl.getAggregationOptions();
			if (options == null)
				curColl.setAggregationOptions(options = new AggregationOptions());				
			if (curColl.getDatatype().equals("ATOFMS") || 
					curColl.getDatatype().equals("AMS")) {
				if (baseOnCollection) {
					Calendar startDate = new GregorianCalendar();
					Calendar endDate = new GregorianCalendar();
					Collection[] array = {basisCollection};
					long begin = new Date().getTime();
					db.getMaxMinDateInCollections(array,startDate,endDate);
					long end = new Date().getTime();
					System.out.println("getMaxMinDateInCollections: "+(end-begin)/1000+" sec.");
					startTime = startDate.getTime();
					endTime = endDate.getTime();
				}
				else {
					startTime = start.getTime();
					endTime = end.getTime();
				}
				
				long begin = new Date().getTime();
				mzValues[i] = db.getValidSelectedMZValuesForCollection(curColl, startTime, endTime);
				long end = new Date().getTime();
				System.out.println("getValidMZValuesForCollection: "+(end-begin)/1000+" sec.");
				if (mzValues[i] != null)
					numSqlCalls[0] += mzValues[i].length;		
				if (options.produceParticleCountTS)
					numSqlCalls[0]++;
			} else if (curColl.getDatatype().equals("TimeSeries")) {
				numSqlCalls[0]++;
			}
		}
		if(progressBar.wasTerminated()){
			throw new InterruptedException();
		}
		progressBar.setTitle("Aggregating Time Series");
		progressBar.setMaximum(numSqlCalls[0]+1);
		
		// By constructing progressBar2 BEFORE disposing progressBar1, the
		// request for making progressBar2 is in the EDT queue waiting to go
		// when progressBar1 is finally disposed. If we dispose progressBar1
		// first (which is modal), we technically run the risk that another
		// event will get in between the two.
		
		//actually do the aggregation
		
		int rootCollectionID = db.createEmptyCollection("TimeSeries", 1, syncRootName, "", "");
		
		for (int i = 0; i < collections.length; i++) {
			final String name = collections[i].getName();
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					progressBar.increment("Constructing time basis for "+name);
				}
			});
			long begin = new Date().getTime();
			if (baseOnCollection)
				db.createTempAggregateBasis(collections[i],basisCollection);
			else {
				db.createTempAggregateBasis(collections[i],start,end,interval);
			}
			if(progressBar.wasTerminated()){
				throw new InterruptedException();
			}
			long end = new Date().getTime();
			System.out.println("createTempAggBasis: "+(end-begin)/1000+" sec.");
			begin = new Date().getTime();
			
			db.createAggregateTimeSeries(progressBar, rootCollectionID, collections[i], mzValues[i]);
			end = new Date().getTime();
			System.out.println("createAggregateTimeSeries: "+(end-begin)/1000+" sec.");
			db.deleteTempAggregateBasis();
		}
		
		if (mainFrame != null)
			mainFrame.updateSynchronizedTree(rootCollectionID);


		return rootCollectionID;
	}
}
