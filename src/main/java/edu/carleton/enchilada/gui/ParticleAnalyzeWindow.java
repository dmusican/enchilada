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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;
import java.util.Date;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import edu.carleton.enchilada.ATOFMS.AMSPeak;
import edu.carleton.enchilada.ATOFMS.ATOFMSParticle;
import edu.carleton.enchilada.ATOFMS.ATOFMSPeak;
import edu.carleton.enchilada.ATOFMS.CalInfo;
import edu.carleton.enchilada.ATOFMS.Peak;
import edu.carleton.enchilada.ATOFMS.ReadSpec;
import edu.carleton.enchilada.chartlib.DataPoint;
import edu.carleton.enchilada.chartlib.Dataset;
import edu.carleton.enchilada.chartlib.SpectrumPlot;
import edu.carleton.enchilada.chartlib.ZoomableChart;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;


/**
 * @author gregc, sulmanj
 *
 * A chart specialized for this SpASMS.
 * Deals directly with Peak data.
 * Contains a JTable that displays the text of the peak data displayed in the
 * chart.
 */
public class ParticleAnalyzeWindow extends JFrame 
implements MouseMotionListener, MouseListener, ActionListener, KeyListener {
	
	private final int CACHE_NUM = 1000;//see createCache() below
	
	//GUI elements
	private SpectrumPlot chart;
	private ZoomableChart zchart;
	private JTable peaksTable; 
	private JRadioButton peakButton, specButton;
	private JButton nextButton, zoomDefaultButton, prevButton, origPButton, writeToFile;
	private JTextPane labelText;
	private JScrollPane labelScrollPane;
	private JCheckBox labelPeaks;
	private MainFrame mf;
	private JPanel centerPanel;
	private JPanel peaksPanel;
	private JPanel rightPanel;
	private JPanel labelPanel;
	private JPanel sigPanel;
	private JPanel bottomPanel;
	
	//Data elements
	private Database db;
	private Vector<Vector<Object>> particlesData;
	private int curRow;
	private Collection coll;
	private String datatype;
	
	private LabelLoader labelLoader;
	private AbstractTableModel peaksDataModel;
	private ArrayList<Peak> peaks;
	private ArrayList<Peak> posPeaks;
	private ArrayList<Peak> negPeaks;
	private ArrayList<LabelingIon> posLabels;
	private ArrayList<LabelingIon> negLabels;
	private Dataset posSpecDS, negSpecDS;
	private int atomID;
	private Date timet;
	private int clusterID;
	private String atomFile;
	private double selectedMZ = 0;
	
	private boolean spectrumLoaded = false;

	private ArrayList<LabelingIon> negIons;
	private ArrayList<LabelingIon> posIons;
	
	// Make sure that queued threads don't waste work 
	// (since only last-queued thread in each window will matter)
	private volatile int numRunningThreads = 0;
	private JButton zoomOutButton;
	
	private static final int SPECTRUM_RESOLUTION = 1;
	private static final int DEFAULT_XMIN = 0;
	private static final int DEFAULT_XMAX = 300;
	
	private static double labelingThreshold = .5;
	public static Path labelingDir = MainFrame.userDataLocation.resolve("labeling");

	// Object to hold sync-lock to make sure that any calls to Lei's external labeling
	// code are synchronized (since only one spectrum can be labeled at a time)
	private static Object labelLock = new Object();
	
	private static ArrayList<LabelingIon> cachedNegIons;
	private static ArrayList<LabelingIon> cachedPosIons;
	private static int numIonRows;
	
	static {
		File f = new File("config.ini");
		try {
			Scanner scan = new Scanner(f);
			while (scan.hasNext()) {
				String tag = scan.next();
				String val = scan.next();
				if (scan.hasNext())
					scan.nextLine();
				
				if (tag.equalsIgnoreCase("labeling_threshold:")) {
					try {
						labelingThreshold = Double.parseDouble(val);
					} catch (NumberFormatException e) {
						System.out.println("Error! Value: " + val + " isn't a number. Using '.5' as a label threshold.");
					}
				}
				else if (tag.equalsIgnoreCase("labeling_dir:")) { labelingDir = Paths.get(val); }
			}
			scan.close();

			copyLabelingFilesOutOfResources();
		} catch (FileNotFoundException e) { 
			// Don't worry if the file doesn't exist... 
			// just go on with the default values 
		}
	}

	// Copy files as needed into user labeling directory
	public static void copyLabelingFilesOutOfResources() {
		// Verify that path for database exists
		if (!labelingDir.toFile().exists()) {
			boolean success = labelingDir.toFile().mkdirs();
			if (!success)
				throw new RuntimeException("Failed to make directory to store labeling info.");
		}

		try {
			String[] filesToCopy = {"pion-sigs.txt", "nion-sigs.txt", "run.bat", "spectrum.exe"};
			for (String fileToCopy : filesToCopy) {
				Files.copy(ParticleAnalyzeWindow.class.getResourceAsStream("/labeling/" + fileToCopy),
						labelingDir.resolve(fileToCopy), StandardCopyOption.REPLACE_EXISTING);
			}
		} catch (IOException e) {
			throw new ExceptionAdapter(e);
		}

	}


	/**
	 * Makes a new panel containing a zoomable chart and a table of values.
	 * Both begin empty.
	 */
	public ParticleAnalyzeWindow(final Database db, JTable dt, int curRow,
                                 Collection collection) {
		super();

		setDefaultCloseOperation(DISPOSE_ON_CLOSE);

		// Do one-time loading of ion signature information from file, db
		if (cachedNegIons == null) {
			cachedPosIons = new ArrayList<LabelingIon>();
			cachedNegIons = new ArrayList<LabelingIon>();
			try {
				buildBothLabelSigs(cachedPosIons, cachedNegIons);
			} catch (FileNotFoundException e) {
				ErrorLogger.writeExceptionToLog("Signature file",e.getMessage());
				System.err.println("Signature file not found!");
			}

			numIonRows = Math.max(cachedPosIons.size(), cachedNegIons.size());

			try {
				db.syncWithIonsInDB(cachedPosIons, cachedNegIons);
			} catch (SQLException e){
				ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL exception retrieving Ion data.");
				System.err.println("Error retrieving Ion data.");
				e.printStackTrace();
			}
		}

		// Fill in this window's array of ions. Note: most data is cached
		// for efficiency, but ions still need to be per-window, since they hold
		// onto their checkbox states per-window
		posIons = new ArrayList<LabelingIon>(numIonRows);
		for (LabelingIon ion : cachedPosIons)
			posIons.add(new LabelingIon(ion));
		
		negIons = new ArrayList<LabelingIon>(numIonRows);
		for (LabelingIon ion : cachedNegIons)
			negIons.add(new LabelingIon(ion));
		
		setLocation(10, 10);
		
	    this.db = db;
	    this.curRow = curRow;
	    this.particlesData = createCache(dt, curRow);
	    this.coll = collection;
	    this.datatype = collection.getDatatype();
	    labelLoader = new LabelLoader(this);
		
		peaks = new ArrayList<Peak>();
		atomFile = null;
		
		// sets up chart
		chart = new SpectrumPlot();
		
		zchart = new ZoomableChart(chart);
		chart.addMouseMotionListener(this);
		zchart.addMouseListener(this);
		zchart.setFocusable(true);
		zchart.setCScrollMin(DEFAULT_XMIN);
		zchart.setCScrollMax(DEFAULT_XMAX);
		zchart.setForceY(true);
		
		setupCenterPanel();
		
		setupPeaksPanel();
		
		setupLabelPanel();
		
		setupSignaturePanel();
		
		final JSplitPane labelingControlPane
			= new JSplitPane(JSplitPane.VERTICAL_SPLIT, labelPanel, sigPanel);

		final JSplitPane mainControlPane
			= new JSplitPane(JSplitPane.VERTICAL_SPLIT, peaksPanel, labelingControlPane);
		
		setupBottomPanel(labelingControlPane, mainControlPane);
		
		rightPanel = new JPanel(new BorderLayout());
		rightPanel.add(mainControlPane, BorderLayout.CENTER);
		rightPanel.add(bottomPanel, BorderLayout.SOUTH);

		JPanel mainPanel = new JPanel(new BorderLayout());
		mainPanel.add(centerPanel, BorderLayout.CENTER);
		mainPanel.add(rightPanel, BorderLayout.EAST);
		add(mainPanel);

		showGraph();
		pack();
		
		// If we never found data files, then disable labeling permanently...
		if (numIonRows == 0) {
			labelPeaks.setSelected(false);
			labelPeaks.setEnabled(false);
			labelPeaks.setToolTipText("Can't label -- label code/data not found in subfolder: '" + labelingDir + "'");
		}
		
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				db.saveAtomRemovedIons(atomID, posIons, negIons);
			}
		});
	}

	/**
	 * @author steinbel
	 * Creates a deep copy of CACHE_NUM rows before the selected row and
	 * after it.
	 * @param orig - the original table from which the copy will be made
	 * @param selected - the row selected in the original table
	 * @return a copy of the data from the original table within range of
	 * 			the selected row
	 */
	private Vector<Vector<Object>> createCache(JTable orig, int selected){
		Vector<Vector<Object>> cache = new Vector<Vector<Object>>();
		TableModel data = orig.getModel();
		
		//determine the range for data to cache
		int cols = data.getColumnCount();
		int rows = data.getRowCount();
		int start = selected - CACHE_NUM;
		if (start < 0)
			start = 0;
		int end = selected + CACHE_NUM;
		if (end > rows)
			end = rows;
		
		//copy data
		for (int i=start; i<end; i++){
			cache.add(i, new Vector<Object>());
			for (int j=0; j<cols; j++){
				cache.get(i).add(j, data.getValueAt(i, j));
			}
		}
		
		return cache;
	}

	/**
	 * @param db
	 * @param curRow
	 * @param labelingControlPane
	 * @param mainControlPane
	 * @return
	 */
	private void setupBottomPanel(final JSplitPane labelingControlPane, final JSplitPane mainControlPane) {
		JPanel buttonPanel = new JPanel(new GridLayout(2,1));
		ButtonGroup bg = new ButtonGroup();

		if (datatype.equals("ATOFMS")) {
			specButton = new JRadioButton("Spectrum");
			specButton.setActionCommand("spectrum");
			specButton.addActionListener(this);
			bg.add(specButton);
			buttonPanel.add(specButton);
		}
		
		peakButton = new JRadioButton("Peaks");
		peakButton.setActionCommand("peaks");
		peakButton.addActionListener(this);
		bg.add(peakButton);
		buttonPanel.add(peakButton);
		
		peakButton.setSelected(true);
		
		JPanel peakButtonPanel = new JPanel(new GridLayout(2,1));
		
		int aID = ((Integer)
				particlesData.get(curRow).get(0)).intValue();
		
		clusterID = db.getRepresentedCluster(aID);
		
		//If the atom is a cluster center, create link to see its represented
		//cluster
		if (clusterID != -1){
			JPanel origParticles = new JPanel(new FlowLayout());
			origPButton = new JButton("See original particles.");
			origPButton.setActionCommand("orig");
			origPButton.addActionListener(this);
			origParticles.add(origPButton);
			bottomPanel = new JPanel(new GridLayout(3,1));//change this line after bug fix
			bottomPanel.add(peakButtonPanel);
			bottomPanel.add(origParticles);
		} else{
			bottomPanel = new JPanel (new GridLayout(1, 2));
			bottomPanel.add(buttonPanel);
			bottomPanel.add(peakButtonPanel);
		}
		
		peakButtonPanel.add(labelPeaks = new JCheckBox("Label Peaks", true));
		labelPeaks.addItemListener(new ItemListener() {
			int lastDividerLocation = 0;
			
			public void itemStateChanged(ItemEvent evt) {
				boolean isSelected = evt.getStateChange() == ItemEvent.SELECTED;
				labelingControlPane.setVisible(isSelected);
				
				if (isSelected) {
					doLabeling(false);
					mainControlPane.setDividerLocation(lastDividerLocation);
				} else {
					lastDividerLocation = mainControlPane.getDividerLocation();
					mainControlPane.setDividerLocation(1.0);
				}

				mainControlPane.validate();
			}
		});
	}

	/**
	 * 
	 */
	private void setupSignaturePanel() {
		sigPanel = new JPanel(new BorderLayout());
		JPanel signatures = new JPanel(new GridLayout(numIonRows + 1, 2));
		signatures.add(new JLabel("Negative Ions:"));
		signatures.add(new JLabel("Positive Ions:"));
		for (int i = 0; i < numIonRows; i++) {
			if (i < negIons.size())
				signatures.add(negIons.get(i).getCheckboxPanelForIon(this));
			else
				signatures.add(new JPanel());
			
			if (i < posIons.size())
				signatures.add(posIons.get(i).getCheckboxPanelForIon(this));
			else
				signatures.add(new JPanel());
		}
		
		sigPanel.add(new JLabel("Signature", SwingConstants.CENTER), BorderLayout.NORTH);
		sigPanel.add(new JScrollPane(signatures), BorderLayout.CENTER);
		sigPanel.setPreferredSize(new Dimension(100, 150));
	}

	/**
	 * 
	 */
	private void setupLabelPanel() {
		labelText = new JTextPane();
		labelText.setEditable(false);
		labelText.setContentType("text/html");			
		labelText.setText("Processing labels...");
		
		labelScrollPane = new JScrollPane(labelText);
		
		labelPanel = new JPanel(new BorderLayout());
		labelPanel.add(new JLabel("Selected peak labels:", SwingConstants.CENTER), BorderLayout.NORTH);
		labelPanel.add(labelScrollPane, BorderLayout.CENTER);
		labelPanel.setPreferredSize(new Dimension(300, 200));
	}

	/**
	 * 
	 */
	private void setupPeaksPanel() {
		//sets up table
		peaksDataModel = new PeaksTableModel();
		peaksTable = new JTable(peaksDataModel);
		peaksTable.setFocusable(false);
		peaksTable.getColumnModel().getColumn(1).setPreferredWidth(50);
		peaksTable.getColumnModel().getColumn(2).setPreferredWidth(50);
		peaksTable.setRowSelectionAllowed(true);
		peaksTable.setColumnSelectionAllowed(false);
		peaksTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		ListSelectionModel rowSM = peaksTable.getSelectionModel();
		rowSM.addListSelectionListener(new ListSelectionListener() {
		    public void valueChanged(ListSelectionEvent e) {
		        //Ignore extra messages.
		        if (e.getValueIsAdjusting()) return;

		        ListSelectionModel lsm =
		            (ListSelectionModel)e.getSource();
		        if (!lsm.isSelectionEmpty()) {
		            int selectedRow = lsm.getMinSelectionIndex();
		            selectedMZ = 
		            	((Number) peaksDataModel.getValueAt(selectedRow, 0))
		            			.doubleValue();
					setLabels();
		        } else 
		        	selectedMZ = 0;
		    }
		});
		
		peaksPanel = new JPanel(new BorderLayout());
		peaksPanel.add(new JLabel("Peaks", SwingConstants.CENTER), BorderLayout.NORTH);
		JScrollPane scrollPane = new JScrollPane(peaksTable);

		// We manually set the size of the scroll pane around the table.
		// We used to this instead:
		// peaksTable.setPreferredScrollableViewportSize(new Dimension(300, 200));
		// The problem here is that as the scoll bar appears and disappears
		// from the table (depending on how many peaks there are), Java doesn't
		// estimate the size of the table quite right, and it keeps changing
		// size. It's cleaner to just set the size of the table outright.
		scrollPane.setPreferredSize(new Dimension(300,200));
		peaksPanel.add(scrollPane, BorderLayout.CENTER);
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
		centerPanel.add(zchart, BorderLayout.CENTER);
		centerPanel.add(nextPrevPanel, BorderLayout.SOUTH);
	}
	
	/**
	 * Sets the chart to display a new set of peaks.
	 * @param newPeaks The new peaks to display.
	 * @param title atomID The id number of the atom the peaks are from.
	 * @param filename The path of the file in which the data resides.  Null if no file
	 * is found or desired.  This file is used for importing the full spectrum.
	 */
	public void setPeaks(ArrayList<Peak> newPeaks, int atomID, String filename, Date date)
	{
		this.atomID = atomID;
		this.atomFile = filename;
		spectrumLoaded = false;
		peaks = newPeaks;
		this.timet = date;
		posPeaks = new ArrayList<Peak>();
		negPeaks = new ArrayList<Peak>();
		
		//loads peaks
		Map<Integer, Peak> map = new LinkedHashMap<Integer, Peak>();
		
		for (Peak p : peaks)
		{
			if(p.massToCharge > 0){
				posPeaks.add(p);
			}
			else{
				negPeaks.add(p);
			}
		}
		
		
		
		doLabeling(true);
		
		//sets up chart to detect mouse hits on peaks
		/*double[] xCoords = new double[posPeaks.size()];
		for(int i = 0; i<xCoords.length; i++)
		{
			xCoords[i] = posPeaks.get(i).massToCharge;
		}
		chart.setHitDetectCoords(0,xCoords);
		
		xCoords = new double[negPeaks.size()];
		for(int i = 0; i<xCoords.length; i++)
		{
			xCoords[i] = -negPeaks.get(i).massToCharge;
		}
		chart.setHitDetectCoords(1,xCoords);
		*/
		
		if(peakButton.isSelected())
			displayPeaks();
		else if(specButton.isSelected())
			displaySpectrum();
		
		chart.packData(false, true, true); //updates the Y axis scale.
		
		if (datatype.equals("ATOFMS"))
			chart.setTitle("Particle from " + filename);
		else
			chart.setTitle(datatype + " item " + atomID);
		
		double xMax = chart.getXRange()[1];
		zchart.setCScrollMax(DEFAULT_XMAX > xMax ? DEFAULT_XMAX : xMax);
		unZoom();
		// argh, just realized that getXRange is a horribly inefficient way
		// of doing this.  could just keep track of it above, where we're
		// iterating through the peaks anyway.  But it's written, and ... bleh.
		peaksDataModel.fireTableDataChanged();
	}
	
	public void doLabeling(boolean forceLabeling) {
		// Force Labeling means we want to make sure to always  
		// label, even if valid labels are already loaded 
		if (forceLabeling)
			labelLoader.invalidate();
		
		if (!labelLoader.hasValidLabels() && labelPeaks.isSelected()) {
			// Fire up labeling process...
			labelLoader.loadLabels();
		}
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

	
	private void showPreviousParticle() {
		if (curRow > 0)
			curRow--;
		showGraph();
		unZoom();
	}
	
	private void showNextParticle() {
		if (curRow < (particlesData.size()-1));
			curRow++;
		showGraph();
		unZoom();
	}
	
	
	/**
	 * @author steinbel
	 * Grabs necessary info from table-copy and database and sets up window to 
	 * show the selected particle.
	 */
	private void showGraph(){
		
		int atomID = ((Integer)
				particlesData.get(curRow).get(0)).intValue();
		
		//enable and disable buttons according to data available
		if(curRow<=0){
			prevButton.setEnabled(false);
		}else{
			prevButton.setEnabled(true);
		}
		if(curRow>= (particlesData.size()-1)){
			nextButton.setEnabled(false);
		}else{
			nextButton.setEnabled(true);
		}
		
		setTitle("Analyze Particle - AtomID: " + atomID);
		
		//grab this from the table COPY
		String filename = null;
		if (datatype.equals("ATOFMS"))
			filename = (String)particlesData.get(curRow).get(5);
		
		String dateTime = (String)particlesData.get(curRow).get(1);
		Date time = null;
		// dateTime can be null if the particle is artificial, such as a
		// cluster center
		if (dateTime != null)
		{
			int length = dateTime.length();
			String newDate = dateTime.substring(0, length-2);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				time = df.parse(newDate);
			}
			catch (ParseException pe)
			{
				ErrorLogger.writeExceptionToLog("Particle Analysis", pe.getMessage());
			}
		}

		
		String peakString = "Peaks:\n";
		
		System.out.println("AtomID = " + atomID);
		ArrayList<Peak> peaks = db.getPeaks(db.getAtomDatatype(atomID), atomID);
		
		for (Peak p : peaks)
		{
			peakString += 
				"\t" + p.toString() + "\n";
		}
		
		System.out.println(peakString);
		setPeaks(peaks, atomID, filename, time);		
		
		db.buildAtomRemovedIons(atomID, posIons, negIons);
	}
	
	/**
	 * When an arrow key is pressed, moves to
	 * the next particle.
	 */
	public void keyPressed(KeyEvent e)
	{
		int key = e.getKeyCode();

		if (key == KeyEvent.VK_RIGHT || key == KeyEvent.VK_DOWN)
			showNextParticle();
		else if(key == KeyEvent.VK_LEFT || key == KeyEvent.VK_UP)
			showPreviousParticle();
//		//Z unzooms the chart.
//		else if(key == KeyEvent.VK_Z)
//			unZoom();
	}
	
	public void keyReleased(KeyEvent e){}
	public void keyTyped(KeyEvent e){}
	
	
	/**
	 * Checks if the mouse is near a peak.  If so, highlights it
	 * in the table.
	 */
	public void mouseMoved(MouseEvent e)
	{
		if(peaks.isEmpty()) return;

		Point mousePoint = e.getPoint();  //the mouse location
		Peak peak;
		int chartIndex = chart.getChartIndexAt(mousePoint); //the chart the mouse is point at
		if(chartIndex == -1) return; //mouse not pointing at a chart -> don't do anything
		
		//java.awt.geom.Point2D.Double dataPoint = chart.getDataValueForPoint(chartIndex, mousePoint);
		try{
			Double dp = chart.getBarForPoint(chartIndex, mousePoint);
		int multiplier, adder = 0;
		
		if(dp != null)
		{
			
			//System.out.println("Point detecting");
			ArrayList<Peak> peaks;
			if(chartIndex == 0)  // in positive peaks chart
			{
				peaks = posPeaks;
				multiplier = 1;
				adder = negPeaks.size(); //positive peaks are after neg peaks in table
			}
			else //in negative peaks chart
			{
				peaks = negPeaks;
				multiplier = -1; //peaks have negative values, so mult by -1
			}
			
			//checks each peak for a match
			for(int count = 0; count < peaks.size(); count++)
			{
				peak = peaks.get(count);
				
				if(peak.massToCharge == multiplier * dp) 
				{
					int peakIndex = count + adder;
					peaksTable.clearSelection();
					peaksTable.addRowSelectionInterval(peakIndex, peakIndex);
					
					selectedMZ = peak.massToCharge;
					setLabels();
				}
			}
		}
		}catch(ClassCastException c){
			;
		}
	}
	
	public void mouseDragged(MouseEvent e){}
	public void mouseClicked(MouseEvent e) {}
	public void mousePressed(MouseEvent e){}
	public void mouseReleased(MouseEvent e) {}	
	public void mouseExited(MouseEvent e) {}	
	public void mouseEntered(MouseEvent e) {}	
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == peakButton)
			displayPeaks();
		else if (source == specButton)
			displaySpectrum();
		else if (source == prevButton || source == nextButton) {
			db.saveAtomRemovedIons(atomID, posIons, negIons);
			if (source == prevButton)
				showPreviousParticle();
			else
				showNextParticle();
		}
		else if (source == zoomDefaultButton)
			unZoom();
		else if (source == zoomOutButton)
			zoomOut();
		else if (source == origPButton){
			assert(clusterID != -1):"There is no cluster to show.";
			showParent(clusterID);
		}
	/*	else if(source == writeToFile){
			FileDialogPicker dialog = new FileDialogPicker("Save as", ".txt", this, false);
			if(dialog.getFileName()!=null){
				System.out.println(dialog.getFileName());
				writeToFile(dialog.getFileName());
			}
			
		}*/

	}
	/*private void writeToFile(String filename){
		try {
			PrintWriter pw = new PrintWriter(filename);
			for(int i = 0; i<negPeaks.size();i++){
				pw.println(negPeaks.get(i).massToCharge + "," + negPeaks.get(i).value);
			}
			for(int i = 0; i<posPeaks.size();i++){
				pw.println(posPeaks.get(i).massToCharge + "," + posPeaks.get(i).value);
			}
			pw.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/
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
	public void displayPeaks()
	{
		chart.displayPeaks(getPosDS(), getNegDS());
		unZoom();
	}
	
	private Dataset getPosDS() {
		Dataset posDS = new Dataset();
		for(Peak p : posPeaks)
		{
			posDS.add(new DataPoint(p.massToCharge, p.value));
		}
		return posDS;
	}
	
	private Dataset getNegDS() {
		Dataset negDS = new Dataset();
		for(Peak p : negPeaks)
		{
			negDS.add(new DataPoint(-p.massToCharge, p.value));
		}
		return negDS;
	}
	
	public void displaySpectrum()
	{
		if(!spectrumLoaded) {
			try{
				getSpectrum();
				chart.displaySpectra(posSpecDS, negSpecDS, getPosDS(), getNegDS());
			}
			catch (Exception e)
			{
				System.err.println("Error loading spectrum");
				e.printStackTrace();
				posSpecDS = new Dataset();
				negSpecDS = new Dataset();
				peakButton.setSelected(true);
			}
		}
		System.out.println("preparing to unzoom");
		unZoom();
	}
	
	/**
	 * Fetches the spectrum from the data file
	 */
	private void getSpectrum() throws SQLException, IOException, Exception
	{
		ResultSet rs;
		int origDataSetID=0;
		String massCalFile;
		boolean autocal;
		ATOFMSParticle particle;
		
		/*
		 * Procedure:
		 * (1) Use atomID to get OrigDataSet.
		 * (2) Use OrigDataSet to get Calibration data
		 * (3) Create CalInfo object
		 * (4) create ATOFMS particle
		 */
		
		//Get OrigDataSet from database
		
		Database db = MainFrame.db;
		Connection con = db.getCon();
		
		try {
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT OrigDataSetID\n" +
										"FROM DataSetMembers\n" +
										"WHERE AtomID = " +
										atomID);
			rs.next();
			origDataSetID = rs.getInt("OrigDataSetID");
			} catch (SQLException e) {
				//System.err.println("Exception getting OrigDataSetID");
				JOptionPane.showMessageDialog(null,
						"An error occured retrieving the dataset ID from the database."+
						"  Try reimporting the collection.  " +
						"  Please check the ErrorLog.",
						"Spectrum Display Error", JOptionPane.ERROR_MESSAGE);
				ErrorLogger.writeExceptionToLog("ATOFMS Spetrum Viewing","Exception getting calibration data: "+e.toString());
				//peakButton.setSelected(true);
				posSpecDS = new Dataset();
				negSpecDS = new Dataset();
				
				throw e;
			}	
			
			
		//Get Calibration data
		try {
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT *\n" +
					"FROM ATOFMSDataSetInfo\n" +
					"WHERE DataSetID = " +
					origDataSetID);
			rs.next();
			massCalFile = rs.getString("MassCalFile");
			autocal = rs.getBoolean(6);
			} catch (SQLException e) {
				//System.err.println("Exception getting calibration data");
				JOptionPane.showMessageDialog(null,
						"An error occured while retrieving the calibration data from the database." +
						"  Try reimporting the collection.  " +
						"Please check the ErrorLog.",
						"Spectrum Display Error", JOptionPane.ERROR_MESSAGE);
				ErrorLogger.writeExceptionToLog("ATOFMS Spetrum Viewing","Exception getting calibration data: "+e.toString());
				//peakButton.setSelected(true);
				posSpecDS = new Dataset();
				negSpecDS = new Dataset();
				
				throw e;
			}
		
			//create CalInfo object
		try{
			ATOFMSParticle.currCalInfo = new CalInfo(massCalFile, autocal);
		} catch (IOException e)
		{
			//System.err.println("Exception opening calibration file");
			JOptionPane.showMessageDialog(null,
					"An error occurred while opening the calibration file.  " +
					"The calibration file may be corrupt or missing.  "+
					"Please check the ErrorLog.",
					"Spectrum Display Error", JOptionPane.ERROR_MESSAGE);
			ErrorLogger.writeExceptionToLog("ATOFMS Spetrum Viewing","Exception opening calibration file: "+e.toString());
			//peakButton.setSelected(true);
			posSpecDS = new Dataset();
			negSpecDS = new Dataset();
			
			throw e;
		}
		
		//read spectrum
		try {
			particle = new ReadSpec(atomFile, timet).getParticle();
		} catch (Exception e)
		{
			//System.err.println("Exception opening atom file");
			JOptionPane.showMessageDialog(null,
					"An error occurred while opening the atom file.  " +
					"The file may be corrupt or missing.  " +
					"Please check the ErrorLog.",
					"Spectrum Display Error", JOptionPane.ERROR_MESSAGE);
			ErrorLogger.writeExceptionToLog("ATOFMS Spetrum Viewing","Exception opening atom file: "+e.toString());
			//peakButton.setSelected(true);
			posSpecDS = new Dataset();
			negSpecDS = new Dataset();
			
			throw e;
		}
		
		//	create dataset
		DataPoint[] posDP = particle.getPosSpectrum();
		DataPoint[] negDP = particle.getNegSpectrum();
		
		posSpecDS = new Dataset();
		negSpecDS = new Dataset();
		//get rid of some points, for efficiency
		for(int i=0; i < posDP.length; i+=SPECTRUM_RESOLUTION)
		{
			posSpecDS.add(posDP[i]);
		}
		for(int i=0; i < negDP.length; i+=SPECTRUM_RESOLUTION)
		{
			negSpecDS.add(negDP[i]);
		}
	}
	
	private void setLabels() {
		if (selectedMZ == 0) {
			labelText.setText("Finished processing labels. <br>Select a peak to see its label.");
			return;
		}
		
		StringBuilder sb = new StringBuilder();
		ArrayList<LabelingIon> ionList             = selectedMZ < 0 ? negIons : posIons;
		Hashtable<LabelingIon, Double> labeledIons = selectedMZ < 0 ? labelLoader.foundNegIons : labelLoader.foundPosIons;
		
		if (labeledIons == null)
			return;
		
		int selectedMZRounded = (int) Math.round(Math.abs(selectedMZ));
		
		for (int labelType = 0; labelType < 3; labelType++) {
			for (int i = 0; i < ionList.size(); i++) {
				LabelingIon ion = ionList.get(i);
				int[] mzVals = ion.mzVals;
				
				for (int j = 0; j < mzVals.length; j++) {
					if (mzVals[j] == selectedMZRounded && ion.ratios[j] > labelingThreshold) {
						String color = null;
						
						if (labelType == 0 && labeledIons.containsKey(ion))
							color = "red";
						else if (labelType == 1)
							color = "black";
						else if (labelType == 2 && !ion.isChecked())
							color = "#98AFC7";
						
						if (color != null)
							sb.append("<b><font color='" + color + "'>" + ion.name + "</font><br>");
					}
				}
			}
		}
		
		if (sb.length() > 0)
			labelText.setText("<html>" + sb.toString() + "</html>");
		else
			labelText.setText("No labels found for this peak!");
		
		// Total hack... make sure label textarea scrolls to top after setting text...
		SwingUtilities.invokeLater(new Runnable()
		{
			public void run()
			{
				labelScrollPane.getViewport().setViewPosition(new Point(0, 0));
			}
		});		
	}
	
	private class LabelLoader implements Runnable {
		private boolean hasValidLabels = false;
		
		public Hashtable<LabelingIon, Double> foundPosIons;
		public Hashtable<LabelingIon, Double> foundNegIons;

		private ParticleAnalyzeWindow callbackWindow;
		
		public LabelLoader(ParticleAnalyzeWindow callbackWindow) {
			this.callbackWindow = callbackWindow;
		}
		
		public void loadLabels() {
			// Only run if signature data was found...
			if (numIonRows > 0) {
				numRunningThreads++;
				new Thread(this).start();
			}
		}
		
		public void invalidate() { hasValidLabels = false; }
		public boolean hasValidLabels() { return hasValidLabels; }
		
		public void run() {
			// Only one thread at a time should run...
			synchronized (labelLock) {
				labelText.setText("Processing labels...");
				if (numRunningThreads == 1) {
					ArrayList<LabelingIon> negWrittenIons = new ArrayList<LabelingIon>();
					ArrayList<LabelingIon> posWrittenIons = new ArrayList<LabelingIon>();
					try {
						writeBothSpectra(posPeaks, negPeaks);
						writeSignature(labelingDir + "/temp_pion-sigs.txt", posIons, posWrittenIons);
						writeSignature(labelingDir + "/temp_nion-sigs.txt", negIons, negWrittenIons);
						
						// Run labeling program:
						
						ProcessBuilder pb = new ProcessBuilder(System.getProperty("user.dir") + "/" + labelingDir + "/run.bat");
						pb.directory(labelingDir.toFile());
						Process p = pb.start();
		
					    BufferedReader br =
					    	new BufferedReader(new InputStreamReader(p.getInputStream()));
		
					    while (br.readLine() != null) {}
					    
					    // And read in its output:
					    
					    foundPosIons = new Hashtable<LabelingIon, Double>();
					    foundNegIons = new Hashtable<LabelingIon, Double>();
						
					    Scanner s = new Scanner(new File(labelingDir + "/label_positive.txt"));
					    if (s.hasNext()) {
						    s.nextLine();
							String[] tokens = s.nextLine().split(" ");
		
							for (int i = 0; i <  tokens.length / 2; i++) { 
								foundPosIons.put(posWrittenIons.get(Integer.parseInt(tokens[2 * i])), 
										Double.parseDouble(tokens[2 * i + 1]));
		
							}
					    }
						s.close();
						
					    s = new Scanner(new File(labelingDir + "/label_negative.txt"));
						if (s.hasNext()) {
							s.nextLine();
							String[] tokens = s.nextLine().split(" ");
		
							for (int i = 0; i <  tokens.length / 2; i++) { 
								foundNegIons.put(negWrittenIons.get(Integer.parseInt(tokens[2 * i])), 
										Double.parseDouble(tokens[2 * i + 1]));
		
							}
						}
						s.close();
		
						hasValidLabels = true;

						// Other threads could have already started...
						// Only update label window if this is the only (last) one.
						if (numRunningThreads == 1)
							callbackWindow.setLabels();
						
						new File(labelingDir + "/spc_positive.txt").delete();
						new File(labelingDir + "/spc_negative.txt").delete();
						new File(labelingDir + "/temp_pion-sigs.txt").delete();
						new File(labelingDir + "/temp_nion-sigs.txt").delete();
						new File(labelingDir + "/label_negative.txt").delete();
						new File(labelingDir + "/label_positive.txt").delete();
					} 
					catch (IOException e) { e.printStackTrace(); }
				} else {
					System.out.println("Thread never ran... num queued: " + numRunningThreads);
				}
			}
			
			numRunningThreads--;
		}

		private void writeSignature(String sigFileName, ArrayList<LabelingIon> masterIonList, ArrayList<LabelingIon> writtenIons)
		throws FileNotFoundException {
			PrintStream writer = new PrintStream(new File(sigFileName));
			for (LabelingIon ion : masterIonList) {
				if (ion.isChecked()) {
					writer.println(ion);
					writtenIons.add(ion);
				}
			}
			writer.close();	
		}
	}
	
	/**
	 * Handles the data for the table.
	 * @author sulmanj
	 */
	private class PeaksTableModel extends AbstractTableModel
	{
		/**
		 * Comment for <code>serialVersionUID</code>
		 */
		private static final long serialVersionUID = 3618133463824348212L;

		public int getColumnCount() {
			
			return 4;
		}

		public int getRowCount() {
			if(peaks == null) return 0;
			else return peaks.size();
		}

		public Number getValueAt(int row, int column) {
			Peak peak;
			if(row < negPeaks.size())
				peak = negPeaks.get(row);
			else
				peak = posPeaks.get(row - negPeaks.size());
			if(coll.getDatatype().equals("ATOFMS")){
			switch(column)
			{
			// Putting these in wrapper classes, hope this
			// helps 
			// -Ben
			case 0: return new Integer((int)((ATOFMSPeak)peak).massToCharge);
			case 1: return new Integer(((ATOFMSPeak)peak).height);
			case 2: return new Integer(((ATOFMSPeak)peak).area);
			case 3: return new Float(((ATOFMSPeak)peak).relArea);
			default: return null;
			}
			}else if(coll.getDatatype().equals("AMS")){
				switch(column)
				{
				case 0: return new Integer((int)((AMSPeak)peak).massToCharge);
				case 1: return new Double(((AMSPeak)peak).height);
				}
				
			}
			
			return null;
		}
		
		public String getColumnName(int column)
		{
			if(coll.getDatatype().equals("ATOFMS")){
				
			switch(column)
			{
			case 0: return "Location";
			case 1: return "Height";
			case 2: return "Area";
			case 3: return "Relative Area";
			default: return "";
			}
			}else{
				switch(column)
				{
				case 0: return "Location";
				case 1: return "Height";
				default: return "";
				}
			}
		}
		
	}

	public static void buildBothLabelSigs(ArrayList<LabelingIon> cachedPosIons, ArrayList<LabelingIon> cachedNegIons)
			throws FileNotFoundException {
		buildLabelSigs(ParticleAnalyzeWindow.class.getResourceAsStream("/labeling/pion-sigs.txt"), cachedPosIons);
		buildLabelSigs(ParticleAnalyzeWindow.class.getResourceAsStream("/labeling/nion-sigs.txt"), cachedNegIons);
	}

	private static void buildLabelSigs(InputStream inputStream, ArrayList<LabelingIon> ionListToBuild)
			throws FileNotFoundException {

		Scanner s = new Scanner(inputStream);
		while (s.hasNext()) {
			LabelingIon ion = new LabelingIon(s.nextLine());
			if (ion.isValid())
				ionListToBuild.add(ion);
		}
		s.close();
	}

	public static void writeBothSpectra(ArrayList<Peak> posPeaks, ArrayList<Peak> negPeaks) throws FileNotFoundException {
		writeSpectrum(labelingDir + "/spc_positive.txt", posPeaks);
		writeSpectrum(labelingDir + "/spc_negative.txt", negPeaks);
	}

	private static void writeSpectrum(String spectrumFileName, ArrayList<Peak> peaks) throws FileNotFoundException {
		PrintStream writer = new PrintStream(new File(spectrumFileName));
		writer.println("1");
		for (Peak p : peaks)
			writer.printf("%d %d ", (int) Math.round(Math.abs(p.massToCharge)), (int) p.value);
		writer.print("-1");
		writer.close();
	}


}