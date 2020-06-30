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
import gnu.trove.list.array.TFloatArrayList;
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
import javax.swing.JPanel;
import javax.swing.SwingWorker;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.chartlib.DataPoint;
import edu.carleton.enchilada.chartlib.Dataset;
import edu.carleton.enchilada.chartlib.SizePlot;
import edu.carleton.enchilada.chartlib.ZoomableChart;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.CollectionCursor;

// Log-normal size binning of particles in collection
// Michael Murphy 2014

// currently confined with prev/next to parent and children
// should display stats as well

public class SizeHistogramWindow extends JFrame 
implements MouseMotionListener, MouseListener, ActionListener, KeyListener {
	
	private JFrame parent;
	private Database db;
	
	//GUI elements
	private SizePlot chart;
	private ZoomableChart zchart;
	private JButton nextButton, zoomDefaultButton, prevButton, origPButton, writeToFile;
	private JLabel metricLabel;
	private JComboBox<String> metricBox;
	private JCheckBox normBox;
	private JPanel centerPanel;
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
	protected ArrayList<Integer> collectionIds;
	protected ArrayList<TFloatIntHashMap> sizeDists;
	protected ArrayList<TFloatArrayList> sizeData;
	protected ArrayList<double[]> sizeStats;
	
	private boolean isParent = false;
	
	private JButton zoomOutButton;
	
	private static final int DEFAULT_XMIN = 0;
	private static final int DEFAULT_XMAX = 2;
	
	private static final int SIZE_THRESH = 1000;

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
	public SizeHistogramWindow(MainFrame parentFrame, CollectionTree cTree, Database db) throws SQLException {
		super();
		
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		
		setLocation(10, 10);

		this.db = db;
		collection = cTree.getSelectedCollection();
		collectionIds = new ArrayList<Integer>(); // zeroth element will be parent always
		childIds = new ArrayList<Integer>();
		sizeDists = new ArrayList<TFloatIntHashMap>();
		sizeStats = new ArrayList<double[]>();
		sizeData =  new ArrayList<TFloatArrayList>();
		
		parentId = collection.getCollectionID();
		childIds.addAll(collection.getSubCollectionIDs());
		
		collectionIds.add(parentId);
		collectionIds.addAll(childIds);
		collectionPos = collectionIds.indexOf(collection.getCollectionID());
		
		ArrayList<String> collectionNames = new ArrayList<String>();
		for (int cid : collectionIds)
			collectionNames.add(db.getCollectionName(cid));
		
		// compute size distributions
		System.out.print("Computing size distributions... ");
		computeSizeDistributions();
		System.out.println("done");
		
		// sets up chart
		chart = new SizePlot();
		
		zchart = new ZoomableChart(chart);
		chart.addMouseMotionListener(this);
		zchart.addMouseListener(this);
		zchart.setFocusable(true);
		//zchart.setCScrollMin(DEFAULT_XMIN);
		//zchart.setCScrollMax(DEFAULT_XMAX);
		zchart.setForceY(true);
		
		setupCenterPanel();

		JPanel mainPanel = new JPanel(new BorderLayout());
		mainPanel.add(centerPanel, BorderLayout.CENTER);
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
	
	private void computeSizeDistributions() {
		
		worker = new SwingWorker<Void,Void>() {
			ArrayList<TFloatIntHashMap> dists = new ArrayList<TFloatIntHashMap>();
			ArrayList<TFloatIntHashMap> binned = new ArrayList<TFloatIntHashMap>();
			ArrayList<double[]> stats = new ArrayList<double[]>();
			TFloatArrayList rawData;
			TFloatIntHashMap binnedData;
			
			@Override
			public Void doInBackground() {
				// compute distribution
				for (int cid : collectionIds) {
					rawData = getParticleSizes(db.getCollection(cid));
					binnedData = binParticleSizes(rawData);
					sizeDists.add(binnedData);
					sizeData.add(rawData);
					sizeStats.add(getSizeStats(rawData));
				}
				return null;
			}
			@Override
			public void done() {
				closeUpdateBox();
			}
		};
		worker.execute();
		showUpdateBox();
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

	public void setPeaks()
	{
		//chart.setBarWidth(numBins);
		display();
		
		chart.packData(false, true, true); //updates the Y axis scale.

		double xMin = chart.getXRange()[0];
		double xMax = chart.getXRange()[1];
		zchart.setCScrollMin(DEFAULT_XMIN < xMin ? DEFAULT_XMIN : xMin);
		zchart.setCScrollMax(DEFAULT_XMAX > xMax ? DEFAULT_XMAX : xMax);
		unZoom();
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
		
		setTitle("Analyze Collection: "+collection.getName()+
				(collection.getCollectionID()!=parentId?" ("+db.getCollectionName(parentId)+")":""));

		setPeaks();
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
		else if (source == zoomDefaultButton)
			unZoom();
		else if (source == zoomOutButton)
			zoomOut();
		else if (source == origPButton){
			assert(clusterId != -1):"There is no cluster to show.";
			showParent(clusterId);
		}
		else if (source == writeToFile){
			new SizeExportDialog(this);
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
		chart.display(getSizeDS());
		// probably could format this nicer
		chart.setTitle("Collection: "+collection.getName()+
				"; Mean Size: "+(float)sizeStats.get(collectionPos)[0]+
				"; SD: "+(float)sizeStats.get(collectionPos)[1]);
		unZoom();
	}
	
	// normalize distances here; kind of a hack here to deal w/ dot product too
	private Dataset getSizeDS() {
		Dataset dataset = new Dataset();
		for (TFloatIntIterator it = sizeDists.get(collectionPos).iterator(); it.hasNext();) {
			it.advance();
			float distance = it.key();
			float count = it.value();
			dataset.add(new DataPoint(distance, count));
		}
		return dataset;
	}
	
	public TFloatIntHashMap binParticleSizes(TFloatArrayList raw) {
		// compute max value for normalization
		float max = -1;
		for (int i = 0; i < raw.size(); i++) {
			if (raw.get(i) > max)
				max = raw.get(i);
		}
		//int numBins = (int)Math.sqrt(raw.size());
		int numBins = 1000;
		TFloatIntHashMap binned = new TFloatIntHashMap();
		for (int i = 0; i < raw.size(); i++) {
			float distance = (float)Math.floor(raw.get(i)/max*numBins)*max/numBins; // kind of lame, hard-coded to 1000 bins
			binned.adjustOrPutValue(distance, 1, 1);
		}
		return binned;
	}
	
	// generate a non-binned list of distances between particles and centroid
	public TFloatArrayList getParticleSizes(Collection collection) {
		TFloatArrayList sizes = new TFloatArrayList();
		
		CollectionCursor curs = db.getAtomInfoOnlyCursor(collection);
		
		while (curs.next()) {
			ParticleInfo p = curs.getCurrent();
			float size = p.getATOFMSParticleInfo().getSize();
			if (size < SIZE_THRESH) // catch invalid particles
				sizes.add(size);
		}
		curs.close();
		
		return sizes;
	}
	
	// get mean and SD of distances, could have done this while generating histogram...
	public double[] getSizeStats(TFloatArrayList sizes) {
		double sum = 0;
		double sqsum = 0;
		double n = 0;
		for (int i = 0; i < sizes.size(); i++) {
			sum += sizes.get(i);
			sqsum += Math.pow(sizes.get(i),2);
			n++;
		}
		double mean = sum / n;
		double sd = Math.sqrt(sqsum/n-(sum/n)*(sum/n));
		return new double[]{mean,sd};
	}
	
	public TFloatIntHashMap getBinnedSizes() {
		return this.sizeDists.get(collectionPos);
	}
	
	public TFloatArrayList getRawSizes() {
		return this.sizeData.get(collectionPos);
	}
}