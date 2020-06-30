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
 * The Original Code is EDAM Enchilada's PeaksChart class.
 *
 * The Initial Developer of the Original Code is
 * The EDAM Project at Carleton College.
 * Portions created by the Initial Developer are Copyright (C) 2005
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * Greg Cipriano gregc@cs.wisc.edu
 * Jonathan Sulman sulmanj@carleton.edu
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
 * Created on Mar 7, 2005
 */
package edu.carleton.enchilada.gui;

import edu.carleton.enchilada.database.Database;
import gnu.trove.iterator.TFloatIntIterator;
import gnu.trove.map.hash.TFloatIntHashMap;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.SwingWorker;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.DistanceMetric;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.chartlib.DataPoint;
import edu.carleton.enchilada.chartlib.Dataset;
import edu.carleton.enchilada.chartlib.DistancePlot;
import edu.carleton.enchilada.chartlib.ZoomableChart;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.NonZeroCursor;

// Adaptation of ParticleAnalyzeWindow, visualizes distances between particles in a cluster and their mean
// Michael Murphy 2014

// Refactoring of this class is probably warranted, but it works

public class ClusterDistanceWindow extends JFrame 
implements ChangeListener, MouseMotionListener, MouseListener, ActionListener, KeyListener {
	
	private JFrame parent;
	private Database db;
	
	//GUI elements
	private DistancePlot chart;
	private ZoomableChart zchart;
	private JButton nextButton, zoomDefaultButton, prevButton, origPButton, writeToFile;
	private JLabel metricLabel;
	private JComboBox<String> metricBox;
	private JCheckBox normBox;
	private JPanel centerPanel;
	private JPanel optionPanel;
	private JSlider binSlider;
	private JDialog processingBox;
	private MainFrame mf;
	
	//Data elements
	private int collectionPos;
	private Collection collection;
	private String datatype;
	
	private int atomId;
	private Date timet;
	private int clusterId;
	private int parentId;
	private ArrayList<Integer> childIds;
	private ArrayList<Integer> collectionIds;
	private ArrayList<TFloatIntHashMap> rawDistances;
	private ArrayList<TFloatIntHashMap> binnedDistances;
	private ArrayList<double[]> clusterStats;
	private float[][] centroidDistances;
	private TFloatIntHashMap currentData;
	
	private boolean isParent = false;
	private DistanceMetric dMetric = DistanceMetric.EUCLIDEAN_SQUARED;
	private boolean posNegNorm = true;
	
	// Make sure that queued threads don't waste work 
	// (since only last-queued thread in each window will matter)
	private JButton zoomOutButton;
	
	private static final int DEFAULT_XMIN = 0;
	private static final int DEFAULT_XMAX = 2;
	
	final static String CITY_BLOCK = "City Block";
	final static String EUCLIDEAN_SQUARED = "Euclidean Squared";
	final static String DOT_PRODUCT = "Dot Product";

	private static JDialog updateBox;
	private static SwingWorker<Void,Void> worker;
	
	private static void showUpdateBox() {
		updateBox = new JDialog(new JFrame(), "Processing...",true);
		JLabel updateLabel = new JLabel("Computing histograms...       ");
		updateBox.setBounds(0,0,100,150);
		updateBox.add(updateLabel);
		updateBox.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		updateBox.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosed(WindowEvent e) {
				worker.cancel(true);
			}
		});
		updateBox.pack();
		updateBox.validate();
		updateBox.setVisible(true);
	}
	
	private static void closeUpdateBox() {
		updateBox.dispose();
	}
	
	
	/**
	 * Makes a new panel containing a zoomable chart and a table of values.
	 * Both begin empty.
	 * @param chart
	 * @throws SQLException 
	 */
	public ClusterDistanceWindow(MainFrame parentFrame, CollectionTree cTree, Database db) throws SQLException {
		super();
		
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		
		setLocation(10, 10);

		this.db = db;
		collection = cTree.getSelectedCollection();
		collectionIds = new ArrayList<Integer>(); // zeroth element will be parent always
		childIds = new ArrayList<Integer>();
		
		// determine if selected collection is a clustering parent, an individual cluster, or neither
		if (collection.isCluster()) {
			isParent = false;
			parentId = collection.getParentCollection().getCollectionID();
			childIds.addAll(collection.getParentCollection().getSubCollectionIDs());
		} else if (collection.isClusterParent()) {
			isParent = true;
			parentId = collection.getCollectionID();
			childIds.addAll(collection.getSubCollectionIDs());
		} else {
			JOptionPane.showMessageDialog(this, "Please select a cluster or cluster parent collection.", 
					"Improper collection selected.", JOptionPane.WARNING_MESSAGE);
			dispose();
			return;
		}
		
		collectionIds.add(parentId);
		collectionIds.addAll(childIds);
		collectionPos = collectionIds.indexOf(collection.getCollectionID());
		
		ArrayList<String> collectionNames = new ArrayList<String>();
		for (int cid : collectionIds)
			collectionNames.add(db.getCollectionName(cid));

		// compute distance distributions for parent and all clusters
		System.out.print("Computing distances... ");
		calculateRawDistances(childIds);
		System.out.println("done");
		
		// sets up chart
		chart = new DistancePlot();
		
		zchart = new ZoomableChart(chart);
		chart.addMouseMotionListener(this);
		zchart.addMouseListener(this);
		zchart.setFocusable(true);
		//zchart.setCScrollMin(DEFAULT_XMIN);
		//zchart.setCScrollMax(DEFAULT_XMAX);
		zchart.setForceY(true);
		
		setupCenterPanel();
		setupOptionPanel();

		JPanel mainPanel = new JPanel(new BorderLayout());
		mainPanel.add(centerPanel, BorderLayout.CENTER);
		mainPanel.add(optionPanel, BorderLayout.SOUTH);
		add(mainPanel);

		pack();
		
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
			}
		});

		setSize(720,480);

		setVisible(true);
		
		showGraph();
	}
	
	private float[][] calculateCentroidDistances(ArrayList<Integer> childIds) {
		int numClusters = childIds.size();
		float[][] distMtx = new float[numClusters][numClusters];
		ArrayList<BinnedPeakList> centroids = new ArrayList<BinnedPeakList>(numClusters);
		for (int i = 0; i < numClusters; i++) {
			ParticleInfo p = getClusterCentroid(db.getCollection(childIds.get(i)));
			BinnedPeakList pBinnedList = p.getBinnedList();
			pBinnedList.normalize(dMetric,posNegNorm);
			centroids.add(pBinnedList);
		}
		for (int i = 0; i < numClusters; i++) {
			for (int j = i+1; j < numClusters; j++) {
				float distance = centroids.get(i).getDistance(centroids.get(j), dMetric);
				distMtx[i][j] = distMtx[j][i] = distance;
			}
		}
		return distMtx;
	}
	
	private void calculateRawDistances(ArrayList<Integer> childIds) {
		final ArrayList<Integer> cids = childIds;
		
		worker = new SwingWorker<Void,Void>() {
			ArrayList<TFloatIntHashMap> dists = new ArrayList<TFloatIntHashMap>();
			ArrayList<TFloatIntHashMap> binned = new ArrayList<TFloatIntHashMap>();
			ArrayList<double[]> stats = new ArrayList<double[]>();
			float[][] cdists;
			
			@Override
			public Void doInBackground() {
				// might as well do these in a swingworker too
				cdists = calculateCentroidDistances(cids);
				
				TFloatIntHashMap netDistances = new TFloatIntHashMap();
				
				// raw counts
				for (int cid : cids) {
					TFloatIntHashMap d = getDistancesFromCentroid(db.getCollection(cid), dMetric, posNegNorm);
					dists.add(d);
					stats.add(getDistanceStats(d));
					binned.add(calculateBinnedDistances(d));
					// generate whole-collection distribution
					for (TFloatIntIterator it = d.iterator(); it.hasNext();) {
						it.advance();
						float distance = it.key();
						if (netDistances.containsKey(distance))
							netDistances.adjustValue(distance, it.value());
						else
							netDistances.put(distance, it.value());
					}
				}
				dists.add(0, netDistances);
				stats.add(0, getDistanceStats(netDistances));
				binned.add(0, calculateBinnedDistances(netDistances));
				return null;
			}
			@Override
			public void done() {
				setData(dists, binned, stats, cdists);
				closeUpdateBox();
			}
		};
		worker.execute();
		showUpdateBox();
	}
	
	public void setData(ArrayList<TFloatIntHashMap> dists, ArrayList<TFloatIntHashMap> binned, ArrayList<double[]> stats, float[][] cdists) {
		this.clusterStats = stats;
		this.binnedDistances = binned;
		this.rawDistances = dists;
		this.centroidDistances = cdists;
	}

	/**
	 * 
	 */
	private void setupCenterPanel() {
		centerPanel = new JPanel(new BorderLayout());
		JPanel nextPrevPanel = new JPanel(new FlowLayout());
		nextPrevPanel.add(prevButton = new JButton("Previous"));
		prevButton.addActionListener(this);
		nextPrevPanel.add(zoomDefaultButton = new JButton("Zoom ->Default"));
		zoomDefaultButton.addActionListener(this);
		nextPrevPanel.add(zoomOutButton = new JButton("Zoom Out"));
		zoomOutButton.addActionListener(this);
		nextPrevPanel.add(nextButton = new JButton("Next"));
		nextButton.addActionListener(this);
		nextPrevPanel.add(writeToFile = new JButton("Export..."));
		writeToFile.addActionListener(this);

		centerPanel.add(zchart, BorderLayout.CENTER);
		centerPanel.add(nextPrevPanel, BorderLayout.SOUTH);
	}
	
	private void setupOptionPanel() {
		optionPanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 5));
		metricLabel = new JLabel("Distance metric:");
		optionPanel.add(metricLabel);
		String[] metricNames = {EUCLIDEAN_SQUARED, CITY_BLOCK, DOT_PRODUCT};
		metricBox = new JComboBox<String>(metricNames);
		metricBox.addActionListener(this);
		optionPanel.add(metricBox);
		normBox = new JCheckBox("Normalize +/- peaks separately", true);
		normBox.addActionListener(this);
		optionPanel.add(normBox);
		JLabel binLabel = new JLabel("Number of bins:"); // log scale would be pretty cool here
		binSlider = new JSlider(1,10000,100);
		
		//binSlider.addChangeListener(this);
		
		//optionPanel.add(binLabel);
		//optionPanel.add(binSlider);
	}
	
	public TFloatIntHashMap calculateBinnedDistances(TFloatIntHashMap dists) {
		TFloatIntHashMap binned = new TFloatIntHashMap();
		// first bin them - easier to do with a hash map
		float max = 2.0f;
		int numBins = 100;
		for (TFloatIntIterator it = dists.iterator(); it.hasNext();) {
			it.advance();
			float distance = (float)Math.floor(it.key()/max*numBins)/numBins*max; // kind of lame, hard-coded to 100 bins
			int count = it.value();
			if (binned.containsKey(distance))
				binned.adjustValue(distance, count);
			else
				binned.put(distance, count);
		}
		return binned;
	}

	public void setPeaks(TFloatIntHashMap binned)
	{
		currentData = binned;

		//chart.setBarWidth(numBins);
		display();
		
		chart.packData(false, true, true); //updates the Y axis scale.

		//double xMin = chart.getXRange()[0];
		//double xMax = chart.getXRange()[1];
		//zchart.setCScrollMin(DEFAULT_XMIN < xMin ? DEFAULT_XMIN : xMin);
		//zchart.setCScrollMax(DEFAULT_XMAX > xMax ? DEFAULT_XMAX : xMax);
		unZoom();
	}
	
	public ParticleInfo getClusterCentroid(Collection collection) {
		CollectionCursor curs = null;
		
		try {
			curs = db.getCentroidCursor(collection);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		curs.next();
		ParticleInfo centroid = curs.getCurrent(); // this cursor should only return one result anyway
		curs.close();
		
		return centroid;
	}
	
	/**
	 * Sets the owner of this window.
	 * @param owner	The parent window	
	 */
	public void setOwner(MainFrame owner){
		mf = owner;
	}
	
	/**
	 * Returns the chart to its original zoom.
	 *
	 */
	private void unZoom()
	{
		// Need to ensure that the zoom values are actually changed
		// if previous zoom values were default values, you need to change to something else
		// before you default zoom
		zchart.zoomOut();
	}
	
	private void zoomOut()
	{
		zchart.zoomOutHalf();
	}

	private void showPreviousCluster() {
		if (collectionPos > 0)
			collectionPos--;
		collection = db.getCollection(collectionIds.get(collectionPos));
		showGraph();
		unZoom();
	}
	
	private void showNextCluster() {
		if (collectionPos < (collectionIds.size()-1));
			collectionPos++;
		collection = db.getCollection(collectionIds.get(collectionPos));
		showGraph();
		unZoom();
	}
	
	/**
	 * @author steinbel
	 * Grabs necessary info from table-copy and database and sets up window to 
	 * show the selected particle.
	 */
	private void showGraph(){
		
		//enable and disable buttons according to data available
		if(collectionPos<=0){
			prevButton.setEnabled(false);
		}else{
			prevButton.setEnabled(true);
		}
		if(collectionPos>= (collectionIds.size()-1)){
			nextButton.setEnabled(false);
		}else{
			nextButton.setEnabled(true);
		}
		
		setTitle("Analyze Cluster: "+collection.getName()+
				(collection.getCollectionID()!=parentId?" ("+db.getCollectionName(parentId)+")":""));
		
		setPeaks(binnedDistances.get(collectionPos));
		//System.out.println("Mean of distances: "+clusterStats.get(collectionPos)[0]);
		//System.out.println("Standard deviation: "+clusterStats.get(collectionPos)[1]);
		
		//System.out.println("Distance bins:");
		//System.out.println(rawDistances.get(collectionPos));
	}
	
	/**
	 * When an arrow key is pressed, moves to
	 * the next particle.
	 */
	public void keyPressed(KeyEvent e)
	{
		int key = e.getKeyCode();

		if (key == KeyEvent.VK_RIGHT || key == KeyEvent.VK_DOWN)
			showNextCluster();
		else if(key == KeyEvent.VK_LEFT || key == KeyEvent.VK_UP)
			showPreviousCluster();
//		//Z unzooms the chart.
//		else if(key == KeyEvent.VK_Z)
//			unZoom();
	}
	
	public void keyReleased(KeyEvent e){}
	public void keyTyped(KeyEvent e){}
	public void mouseMoved(MouseEvent e){}	
	public void mouseDragged(MouseEvent e){}
	public void mouseClicked(MouseEvent e) {}
	public void mousePressed(MouseEvent e){}
	public void mouseReleased(MouseEvent e) {}	
	public void mouseExited(MouseEvent e) {}	
	public void mouseEntered(MouseEvent e) {}	
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == prevButton || source == nextButton) {
			if (source == prevButton)
				showPreviousCluster();
			else
				showNextCluster();
		}
		else if (source == metricBox) {
			int idx = metricBox.getSelectedIndex();
			// don't know why, but switch-case wasn't working
			if (idx == 0)
				this.dMetric = DistanceMetric.EUCLIDEAN_SQUARED;
			else if (idx == 1)
				this.dMetric = DistanceMetric.CITY_BLOCK;
			else if (idx == 2)
				this.dMetric = DistanceMetric.DOT_PRODUCT; 
			calculateRawDistances(childIds);
			showGraph();
		}
		else if (source == normBox) {
			if (normBox.isSelected())
				posNegNorm = true;
			else
				posNegNorm = false;
			calculateRawDistances(childIds);
			showGraph();
		}
		else if (source == zoomDefaultButton)
			unZoom();
		else if (source == zoomOutButton)
			zoomOut();
		else if (source == origPButton){
			assert(clusterId != -1):"There is no cluster to show.";
			showParent(clusterId);
		}
		else if (source == writeToFile){
			//exportData();
			new ClusterDistanceExportDialog(this);
		}
	}
	
	/**
	 * Shows collection in the main window of Enchilada.  (Used to show clusters.)
	 * @param cID - the collection to display
	 */
	private void showParent(int cID){
		//get hold of the main window and pretend to click on the proper collection
		//in the collection tree
		assert(mf != null): "Owner of this window wasn't set.";
		mf.updateCurrentCollection(cID);
		//display the main window on top
		mf.toFront();
	}
	
	/**
	 * Sets the chart to display peaks.
	 */
	public void display()
	{
		chart.display(getDistDS());
		// probably could format this nicer
		chart.setTitle((collection.getCollectionID()==parentId?"All Clusters":"Cluster "+collection.getName())+
				"; Mean Distance: "+(float)clusterStats.get(collectionPos)[0]+
				"; SD: "+(float)clusterStats.get(collectionPos)[1]);
		unZoom();
	}
	
	// normalize distances here; kind of a hack here to deal w/ dot product too
	private Dataset getDistDS() {
		Dataset dataset = new Dataset();
		for (TFloatIntIterator it = currentData.iterator(); it.hasNext();) {
			it.advance();
			float distance = it.key();
			float count = it.value();
			dataset.add(new DataPoint(distance, count));
		}
		return dataset;
	}
	
	// generate a non-binned list of distances between particles and centroid
	public TFloatIntHashMap getDistancesFromCentroid(Collection collection, DistanceMetric dMetric, boolean posNegNorm) {
		TFloatIntHashMap distances = new TFloatIntHashMap();
		NonZeroCursor curs = null;
		
		try {
			curs = new NonZeroCursor(db.getBPLOnlyCursor(collection));
		} catch (SQLException e) {}
		
		ParticleInfo centroid = getClusterCentroid(collection);
		BinnedPeakList centroidPeaks = centroid.getBinnedList().copyOf();
		centroidPeaks.normalize(dMetric); // k-means doesn't pos-neg the centroids?
		float magnitude = centroidPeaks.getMagnitude(dMetric);
		
		while (curs.next()) {
			ParticleInfo p = curs.getCurrent();
			BinnedPeakList thisBinnedList = p.getBinnedList();
			thisBinnedList.normalize(dMetric,posNegNorm);
			float distance = thisBinnedList.getDistance(centroidPeaks, magnitude, dMetric);
			// hacks for dot product
			if (dMetric == DistanceMetric.DOT_PRODUCT)
				distance += 1;
			// clean up rounding
			distance = distance < 0 ? 0 : distance;
			if (distances.containsKey(distance))
				distances.adjustValue(distance, 1);
			else
				distances.put(distance, 1);
		}
		curs.close();
		
		return distances;
	}
	
	// get mean and SD of distances, could have done this while generating histogram...
	public double[] getDistanceStats(TFloatIntHashMap dists) {
		double sum = 0;
		double sqsum = 0;
		double n = 0;
		for (TFloatIntIterator it = dists.iterator(); it.hasNext();) {
			it.advance();
			sum += it.key()*it.value();
			sqsum += Math.pow(it.key()*it.value(),2);
			n += it.value();
		}
		// avoid round-off ugliness
		if (n==1)
			return new double[]{0d,0d};
		double mean = sum / n;
		double sd = Math.sqrt(sqsum/n-(sum/n)*(sum/n));
		return new double[]{mean,sd};
	}

	public void stateChanged(ChangeEvent e) {
		Object source = e.getSource();
		
		if (source == binSlider) {
			showGraph(); // repaint the graph, I don't need to requery the DB here
		}
	}
	
	public ArrayList<TFloatIntHashMap> getRawDistances() {
		return this.rawDistances;
	}
	
	public ArrayList<TFloatIntHashMap> getBinnedDistances() {
		return this.binnedDistances;
	}
	
	public float[][] getCentroidDistances() {
		return this.centroidDistances;
	}
}