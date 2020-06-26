package edu.carleton.enchilada.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;

import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.clustering.Art2A;
import edu.carleton.enchilada.analysis.clustering.Cluster;
import edu.carleton.enchilada.analysis.clustering.ClusterHierarchical;
import edu.carleton.enchilada.analysis.clustering.ClusterInformation;
import edu.carleton.enchilada.analysis.clustering.ClusterK;
import edu.carleton.enchilada.analysis.clustering.KMeans;
import edu.carleton.enchilada.analysis.clustering.KMedians;

import edu.carleton.enchilada.database.DynamicTable;
import edu.carleton.enchilada.database.InfoWarehouse;

public abstract class AbstractClusterDialog extends JDialog implements ItemListener, ActionListener {

	/* Declared variables */
	protected CollectionTree cTree;
	protected InfoWarehouse db;
	
	protected JFrame parent;
	protected JPanel algorithmCards, specificationCards, clusteringInfo; 
	protected JButton okButton, cancelButton, advancedButton;
	protected JTextField commentField, passesText, vigText, learnText, kClusterText, otherText;
	protected JCheckBox normalizer;
	protected JComboBox clusterDropDown, initialCentroids;
	protected JComboBox metricDropDown, averageClusterDropDown, infoTypeDropdown, hierClusterDropDown;
	private JLabel denseKeyLabel,sparseKeyLabel;
	protected ArrayList<JRadioButton> sparseButtons;
	protected ArrayList<JCheckBox> denseButtons; 
	protected JCheckBox preClusterCheckBox;
	protected JButton preClusterSettings;
	
	// dropdown options
	final static String ART2A = "Art2a";
	final static String KCLUSTER = "K-Cluster";
	final static String KMEANS = "K-Means / Euclidean Squared";
	final static String KMEDIANS = "K-Medians / City Block";
	final static String SKMEANS = "K-Means / Dot Product";
	final static String HIERARCHICAL = "Hierarchical";
	final static String HIER_WARDS = "Ward's Method";
	final static String OTHER = "Other";
	final static String CITY_BLOCK = "City Block";
	final static String EUCLIDEAN_SQUARED = "Euclidean Squared";
	final static String DOT_PRODUCT = "Dot Product";
	final static String init = " ";
	final static String dense = "Dense Particle Information";
	final static String sparse = "Sparse Particle Information";
	final static String denseKey = " Key = Automatic (1, 2, 3, etc) ";
	final static String FARTHEST = "Farthest Point (default)";
	final static String RANDOM = "Random Points (fastest)";
	final static String REFINED = "Refined Centroids (slow)";
	final static String KMEANSPP = "KMeans++ (fast)";
	final static String USERDEF = "User-Defined Centroids...";
	
	protected String[] clusterNames;
	protected ArrayList<String> filenames;
	
	protected static ArrayList<String> sparseKey;
	
	protected boolean randomSample = false;
	protected double sampleFraction = 1;
	
	protected String initialCentroidMethod = FARTHEST;
	protected ClusterK.CentroidsApproach initialCentroidsInt = ClusterK.CentroidsApproach.FARTHEST_DIST_CENTROIDS;
	protected String dMetric = CITY_BLOCK;
	protected String currentShowing = ART2A;
	private String[] nameArray;

	public AbstractClusterDialog(JFrame parent, String title, boolean modal) {
		super(parent, title, modal);
	}
	
	protected Border getSectionBorder(String title) {
		TitledBorder border = BorderFactory.createTitledBorder(title);
		// this likes to throw a null-pointer exception, don't know why
		try {
			Font font = border.getTitleFont();
			border.setTitleFont(new Font(font.getName(), font.getStyle(), 16));
		} catch (NullPointerException e) {}
		Border superBorder = BorderFactory.createCompoundBorder(
				BorderFactory.createEmptyBorder(10, 10, 10, 10), border);
		return superBorder;
	}
	
	/**
	 * creates panel that displays clustering algorithms.
	 * @return JPanel
	 */
	public JPanel setClusteringAlgorithms(String[] clusterNames, boolean showRangeInstructions) {
		JLabel header = new JLabel("Cluster using: ");
		
		//Create the drop down menu and the dividing line.
		JPanel dropDown = new JPanel();
		clusterDropDown = new JComboBox(clusterNames);
		clusterDropDown.setEditable(false);
		clusterDropDown.addItemListener(this);
		dropDown.add(clusterDropDown);
		normalizer = new JCheckBox("Normalize data");
		normalizer.setSelected(true);

		JPanel headerAndDropDown = new JPanel();
		headerAndDropDown.add(header);
		headerAndDropDown.add(dropDown);
		headerAndDropDown.add(normalizer);
		
		JSeparator divider = new JSeparator(JSeparator.HORIZONTAL);
		divider.setBorder(BorderFactory.createRaisedBevelBorder());
		
		//Create the art2a panel that will show when "Art2a" is selected.
		JPanel parameters = new JPanel();
		parameters.setLayout(new FlowLayout());
		JLabel vigLabel = new JLabel("Vigilance:");
		JLabel learnLabel = new JLabel("Learning Rate:");
		JLabel passesLabel = new JLabel("Max # of Passes: ");
		passesText = new JTextField(5);
		vigText = new JTextField(5);
		learnText = new JTextField(5);
		parameters.add(vigLabel);
		parameters.add(vigText);
		parameters.add(learnLabel);
		parameters.add(learnText);
		parameters.add(passesLabel);
		parameters.add(passesText);
		
		JLabel distMetricLabel = new JLabel("Choose Distance Metric: ");
		JPanel art2aDropDown = new JPanel();
		String[] metricNames = {CITY_BLOCK, EUCLIDEAN_SQUARED, DOT_PRODUCT};
		metricDropDown = new JComboBox(metricNames);
		metricDropDown.setEditable(false);
		metricDropDown.addItemListener(this);
		art2aDropDown.add(distMetricLabel);
		art2aDropDown.add(metricDropDown);
		
		JPanel art2aCard = new JPanel();
		art2aCard.add(art2aDropDown);
		art2aCard.add(parameters);
		art2aCard.setLayout(
				new BoxLayout(art2aCard, BoxLayout.PAGE_AXIS));
		
		//Create the kCluster panel that will show when "K-Cluster" is selected.
		parameters = new JPanel();
		JLabel kLabel = new JLabel("Number of Clusters:");
		kClusterText = new JTextField(5);
		JLabel initialLabel = new JLabel("Initial Centroids:");
		//String[] initialNames = {FARTHEST, RANDOM, REFINED, KMEANSPP, USERDEF};
		String[] initialNames = {FARTHEST, RANDOM, REFINED, KMEANSPP};
		initialCentroids = new JComboBox(initialNames);
		initialCentroids.setEditable(false);
		initialCentroids.addItemListener(this);
		initialCentroids.addActionListener(this);
		parameters.add(kLabel);
		parameters.add(kClusterText);
		parameters.add(initialLabel);
		parameters.add(initialCentroids);
		
		JLabel kClusterLabel = new JLabel("Choose algorithm: ");
		JPanel kClusterDropDown = new JPanel();
		String[] averagingNames = {KMEANS,KMEDIANS,SKMEANS};
		averageClusterDropDown = new JComboBox(averagingNames);
		averageClusterDropDown.setEditable(false);
		averageClusterDropDown.addItemListener(this);
		kClusterDropDown.add(kClusterLabel);
		kClusterDropDown.add(averageClusterDropDown);

		JPanel kClusterCard = new JPanel();
		kClusterCard.add(kClusterDropDown);
		kClusterCard.add(parameters);
		if (showRangeInstructions) {
			JLabel kInstructions = new JLabel("Enter number of clusters and or ranges separated by commas (e.g. 10, 12-14)");
			kClusterCard.add(kInstructions);
		}
		kClusterCard.setLayout(
				new BoxLayout(kClusterCard, BoxLayout.PAGE_AXIS));
		
		//Create the other panel that will show when "Other" is selected.
		JPanel otherCard = new JPanel();
		JLabel otherLabel = new JLabel("Parameters:");
		otherText = new JTextField(10); 
		otherCard.add(otherLabel);
		otherCard.add(otherText);

		//Create the hCluster panel that will show when "Hierarchical-Cluster" is selected.
		JLabel preClusterLabel = new JLabel("Pre-cluster to reduce processing? ");
		JPanel preClusterPanel = new JPanel();
		preClusterCheckBox = new JCheckBox();
		preClusterSettings = new JButton("Settings...");
		preClusterSettings.setEnabled(false);
		preClusterPanel.add(preClusterLabel);
		preClusterPanel.add(preClusterCheckBox);
		preClusterPanel.add(preClusterSettings);

		JPanel hClusterCard = new JPanel();
		hClusterCard.add(preClusterPanel);
		hClusterCard.setLayout(
				new BoxLayout(hClusterCard, BoxLayout.PAGE_AXIS));
		preClusterCheckBox.addActionListener(this);
		preClusterSettings.addActionListener(this);
		
		// Add the previous three panels to the algorithmCards JPanel using CardLayout.
		algorithmCards = new JPanel (new CardLayout());
		algorithmCards.add(art2aCard, ART2A);
		algorithmCards.add(kClusterCard,KCLUSTER);
		algorithmCards.add(hClusterCard, HIERARCHICAL);
		algorithmCards.add(otherCard, OTHER);
				
	
		
		// Add all of the components to the main panel.
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.PAGE_AXIS));
		mainPanel.add(headerAndDropDown);
		mainPanel.add(Box.createVerticalStrut(5));
		mainPanel.add(divider);
		mainPanel.add(Box.createVerticalStrut(5));
		mainPanel.add(algorithmCards);
		return mainPanel;
	}
	
	/**
	 * creates the panel that displays the clustering specifications
	 * @return JPanel
	 */
	public JPanel setClusteringSpecifications() {
		// Set button arraylists
		denseButtons = getDenseColumnNames(cTree.getSelectedCollection().getDatatype());
		sparseButtons = getSparseColumnNames(cTree.getSelectedCollection().getDatatype());

		// Set dropdown for dense and sparse information
		String[] infoNames = {init, dense, sparse};
		infoTypeDropdown = new JComboBox(infoNames);
		infoTypeDropdown.addItemListener(this);
		
		// Set dense panel
		JPanel densePanel = new JPanel();
		densePanel.setLayout(new BoxLayout(densePanel, BoxLayout.PAGE_AXIS));	
		denseKeyLabel = new JLabel(denseKey);
		densePanel.add(denseKeyLabel);
		JLabel denseChoose = new JLabel("Choose one or more values below:");
		densePanel.add(denseChoose);
		JScrollPane denseButtonPane = getDenseButtonPane(denseButtons);
		densePanel.setBorder(BorderFactory.createEmptyBorder(0, 30, 0, 30));
		densePanel.add(denseButtonPane);

		// set sparse panel
		JPanel sparsePanel = new JPanel();
		sparsePanel.setLayout(new BoxLayout(sparsePanel, BoxLayout.PAGE_AXIS));
		sparseKey = db.getPrimaryKey(cTree.getSelectedCollection().getDatatype(),DynamicTable.AtomInfoSparse);
		assert (sparseKey.size() == 1) : "More than one sparse key!";
		sparseKeyLabel = new JLabel("key = " + sparseKey.get(0));
		sparsePanel.add(sparseKeyLabel);
		JLabel sparseChoose = new JLabel("Choose one value below:");
		sparsePanel.add(sparseChoose);
		JScrollPane sparseButtonPane = getSparseButtonPane(sparseButtons);
		sparsePanel.setBorder(BorderFactory.createEmptyBorder(0, 30, 0, 30));
		sparsePanel.add(sparseButtonPane);
		
		// Add dense and sparse panels to cards
		specificationCards = new JPanel(new CardLayout());
		specificationCards.add(new JPanel(), init);
		specificationCards.add(densePanel, dense);
		specificationCards.add(sparsePanel, sparse);
		
		// add cards to final panel
		JPanel panel = new JPanel(new BorderLayout());
		JPanel topPanel = new JPanel();
//***We now only allow clustering on PeakArea - benzaids
//***To revert to original, remove all comments with ***
/***
//		topPanel.setLayout(new BoxLayout(topPanel, BoxLayout.LINE_AXIS));

		
//////////////////// HACK until bug is fixed - steinbel - then cut below and
		//uncomment line above setting layout to BoxLayout
		topPanel.setLayout(new BorderLayout());
		topPanel.add(new JLabel("NOTE: Clustering on anything other than peak area \n"
				+" renders the cluster centers meaningless.  This is a known bug we are working to fix."),
				BorderLayout.NORTH);
//////////////////// end HACK
***/
		
		//***topPanel.add(new JLabel("Choose Type of Particle Information to Cluster on: "), BorderLayout.WEST);
		//***topPanel.add(infoTypeDropdown, BorderLayout.CENTER);
		//***panel.add(specificationCards, BorderLayout.CENTER);
		
		topPanel.add(new JLabel("Clustering will be done on Peak Area."));
		panel.add(topPanel, BorderLayout.NORTH);
		
		return panel;
		
	}
	
	/**
	 * gets the list of dense check boxes
	 * 
	 * @param buttons - arraylist of buttons
	 * @return JScrollPane
	 */
	public JScrollPane getDenseButtonPane(ArrayList<JCheckBox> buttons) {
		JPanel pane = new JPanel();
		pane.setLayout(new BoxLayout(pane, BoxLayout.PAGE_AXIS));
		for (int i = 0; i < buttons.size(); i++) 
			pane.add(buttons.get(i));
		JScrollPane scrollPane = new JScrollPane(pane);
		return scrollPane;	
	}
	
	/**
	 * gets the list of grouped sparse radio buttons in a scrollable pane.
	 * 
	 * @param buttons - arraylist of buttons
	 * @return JScrollPane
	 */
	public JScrollPane getSparseButtonPane(ArrayList<JRadioButton> buttons) {
		JPanel pane = new JPanel();
		pane.setLayout(new BoxLayout(pane, BoxLayout.PAGE_AXIS));
		ButtonGroup group = new ButtonGroup(); 
		for (int i = 0; i < buttons.size(); i++) {
			group.add(buttons.get(i));
			pane.add(buttons.get(i));
		}
		JScrollPane scrollPane = new JScrollPane(pane);
		return scrollPane;	
	}
	
	public ArrayList<JCheckBox> getDenseColumnNames(String datatype) {
		ArrayList<JCheckBox> buttonsToCheck = new ArrayList<JCheckBox>();
		ArrayList<ArrayList<String>> namesAndTypes = 
			MainFrame.db.getColNamesAndTypes(datatype, DynamicTable.AtomInfoDense);
		for (int i = 0; i < namesAndTypes.size(); i++) {
			if ((namesAndTypes.get(i).get(1).equals("INT") || 
					namesAndTypes.get(i).get(1).equals("REAL")) && 
					!namesAndTypes.get(i).get(0).equals("AtomID")) 
			buttonsToCheck.add(new JCheckBox(namesAndTypes.get(i).get(0) + 
					" : " + namesAndTypes.get(i).get(1)));
		}
		return buttonsToCheck;
	}
	
	public ArrayList<JRadioButton> getSparseColumnNames(String datatype) {
		ArrayList<JRadioButton> buttonsToCheck = new ArrayList<JRadioButton>();
		ArrayList<ArrayList<String>> namesAndTypes = 
			MainFrame.db.getColNamesAndTypes(datatype, DynamicTable.AtomInfoSparse);
		for (int i = 0; i < namesAndTypes.size(); i++) {
			if ((namesAndTypes.get(i).get(1).equals("INT") || 
					namesAndTypes.get(i).get(1).equals("REAL")) && 
					!namesAndTypes.get(i).get(0).equals("AtomID")) 
			buttonsToCheck.add(new JRadioButton(namesAndTypes.get(i).get(0) + 
					" : " + namesAndTypes.get(i).get(1)));
		}
		return buttonsToCheck;
	}

	
	/**
	 * setCommonInfo() lays out the information that the two tabbed panels share;
	 * the name field, the OK button, the Advanced button, and the CANCEL button.  
	 * This method cuts back on redundant programming and makes the two panels look similar.
	 * @return - a JPanel with the text field and the bottons.
	 */
	public JPanel setCommonInfo(){
		JPanel commonInfo = new JPanel();
		//Create Name text field;
		JPanel comment = new JPanel();
		JLabel commentLabel = new JLabel("Comment: ");
		commentField = new JTextField(30);
		comment.add(commentLabel);
		comment.add(commentField);
		
		// Create the OK, Advanced and CANCEL buttons
		JPanel buttons = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		advancedButton = new JButton("Advanced...");
		advancedButton.addActionListener(this);
		buttons.add(okButton);
		buttons.add(cancelButton);
		buttons.add(advancedButton);
		
		//Add info to panel and lay out.
		commonInfo.add(comment);
		commonInfo.add(buttons);
		commonInfo.setLayout(new BoxLayout(commonInfo, 
				BoxLayout.Y_AXIS));
		
		return commonInfo;
	}	

	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		DistanceMetric dMetInt = DistanceMetric.CITY_BLOCK;
		if (dMetric.equals(CITY_BLOCK) || 
				dMetric.equals(KMEDIANS))
		{
			dMetInt = DistanceMetric.CITY_BLOCK;
		}
		else if (dMetric.equals(EUCLIDEAN_SQUARED) || 
				dMetric.equals(KMEANS) || dMetric.equals(HIER_WARDS))
		{
			dMetInt = DistanceMetric.EUCLIDEAN_SQUARED;
		}
		else if (dMetric.equals(DOT_PRODUCT) || 
				dMetric.equals(SKMEANS))
		{
			dMetInt = DistanceMetric.DOT_PRODUCT;
		}

		if (initialCentroidMethod.equals(FARTHEST)) {
			initialCentroidsInt = ClusterK.CentroidsApproach.FARTHEST_DIST_CENTROIDS;
		}
		else if (initialCentroidMethod.equals(RANDOM)) {
			initialCentroidsInt = ClusterK.CentroidsApproach.RANDOM_CENTROIDS;
		}
		else if (initialCentroidMethod.equals(REFINED)) {
			initialCentroidsInt = ClusterK.CentroidsApproach.REFINED_CENTROIDS;
		}
		else if (initialCentroidMethod.equals(KMEANSPP)) {
			initialCentroidsInt = ClusterK.CentroidsApproach.KMEANS_PLUS_PLUS_CENTROIDS;
		}
		else if (initialCentroidMethod.equals(USERDEF)) {
			initialCentroidsInt = ClusterK.CentroidsApproach.USER_DEFINED_CENTROIDS;
		}
		// hack to make the popup appear
		if (source == initialCentroids) {
			if (initialCentroidMethod.equals(USERDEF)) {
				new CustomCentroidsDialog(this);
				kClusterText.setEnabled(false);
			} else {
				kClusterText.setEnabled(true);
			}
		}
		if (source == okButton) {
			doOKButtonAction(dMetInt, initialCentroidsInt);
		}
		if (source == preClusterCheckBox) {
			preClusterSettings.setEnabled(preClusterCheckBox.isSelected());
		}
		else if (source == preClusterSettings) {
			new PreClusterDialog(parent, this, cTree, db);
			dispose();
		}
		else if (source == advancedButton) {
			new AdvancedClusterDialog((JDialog)this);
		}
		else if (source == cancelButton) {
			dispose();
		}
//		else  
//			dispose();
		
		db.clearCache();
	}

	public abstract void doOKButtonAction(DistanceMetric dMetInt, ClusterK.CentroidsApproach initialCentroidsInt);

	/**
	 * itemStateChanged(ItemEvent evt) needs to be defined, as the 
	 * ClusterDialog implements ItemListener.
	 */
	public void itemStateChanged(ItemEvent evt) {
		if (evt.getSource() == clusterDropDown)
		{
			CardLayout cl = (CardLayout)(algorithmCards.getLayout());
			String newEvent = (String)evt.getItem();
			cl.show(algorithmCards, newEvent);
			if (newEvent.equals(KCLUSTER))
				dMetric = KMEANS;
			if (newEvent.equals(ART2A))
				dMetric = CITY_BLOCK;
			if (newEvent.equals(HIERARCHICAL))
				dMetric = HIER_WARDS;
			currentShowing = newEvent;
		}
		else if (evt.getSource() == metricDropDown)
		{
			dMetric = (String)evt.getItem();
		}
		else if (evt.getSource() == averageClusterDropDown)
		{
			dMetric = (String)evt.getItem();
		}
		else if (evt.getSource() == initialCentroids)
		{
			initialCentroidMethod = (String)evt.getItem();
		}
		else if (evt.getSource() == infoTypeDropdown) {
			CardLayout cl = (CardLayout)(specificationCards.getLayout());
			String newEvent = (String)evt.getItem();
			if (newEvent.equals(init) || newEvent.equals(dense)) {
				for (int i = 0; i < sparseButtons.size(); i++)
					sparseButtons.get(i).setSelected(false);
			}
			else if (newEvent.equals(init) || newEvent.equals(sparse)) {
				for (int i = 0; i < denseButtons.size(); i++)
					denseButtons.get(i).setSelected(false);
			}
			cl.show(specificationCards, newEvent);
		}
	}

	// method to receive filenames from CustomCentroidsDialog -- MM 2014
	public void setCentroidFilenames(ArrayList<String> filenames) {
		this.filenames = filenames;
		this.kClusterText.setText(""+filenames.size());
	}
	
	public void setSample(boolean rs, double frac) {
		this.randomSample = rs;
		this.sampleFraction = frac;
	}
	
}
