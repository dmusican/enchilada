package edu.carleton.enchilada.gui;

import edu.carleton.enchilada.chartlib.*;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.*;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.text.*;
import java.util.*;

import javax.swing.*;
import javax.swing.event.*;

public class SyncAnalyzePanel extends JPanel {
	private MainFrame parentFrame;
	private CollectionTree tree;
	private Collection collectionToBaseOn;
	private Database db;
	private JScrollPane bottomPane;
	private JComboBox firstSeq, secondSeq;
	private SyncCollectionModel firstCollectionModel, secondCollectionModel;
	
	private JComboBox[] conditionSeq, conditionComp, conditionType,	conditionSeq2;

	private SyncCollectionModel[] conditionModel, conditionModel2;
	private JTextField[] conditionValue;
	private JComboBox[] booleanOps;
	private ArrayList<Hashtable<Date,Double>> data;
	private Dataset[] datasets;
	private Dataset[] scatterplotData;
	private Collection[] collectionsToExport;

	private int numConditions = 2;
	ArrayList<String> conditionStrs;
	ArrayList<Collection> condCollections;
	
	private JPanel topPanel;
	private ZoomableChart zchart;
	private JButton zoomOutButton;
	private double xMin, xMax;

	private static String[] comparators = { "", " <", " >", " <=", " >=", " =", " <>" };
	private static String[] comptypes = { " VAL: ", " SEQ: " };
	private static String[] booleans = { " AND", " OR" };

	public SyncAnalyzePanel(MainFrame parentFrame, Database db,
                            CollectionTree tree, Collection collectionToBaseOn) {
		
		super(new BorderLayout());

		this.parentFrame = parentFrame;
		this.db = db;
		this.tree = tree;
		this.collectionToBaseOn = collectionToBaseOn;

		/** TODO  Change this stuff so that export button loads up a new window with collections*/
		firstCollectionModel = new SyncCollectionModel(collectionToBaseOn);
		secondCollectionModel = new SyncCollectionModel(firstCollectionModel);

		topPanel = new JPanel(new BorderLayout());
		JPanel seqAndZoom = new JPanel(new FlowLayout(FlowLayout.LEFT));
		seqAndZoom.add(new JLabel("1st Sequence: "));
		seqAndZoom.add(firstSeq = new JComboBox(firstCollectionModel));
		seqAndZoom.add(new JLabel("              2nd Sequence: "));
		seqAndZoom.add(secondSeq = new JComboBox(secondCollectionModel));
		
		zoomOutButton = new JButton("Zoom Out");
		zoomOutButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				zchart.zoomOut();
			}
		});
		
		seqAndZoom.add(zoomOutButton);

		JButton exportToCSV, refresh;
		Box buttonPanel = new Box(BoxLayout.Y_AXIS);
		JPanel refreshPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		JPanel exportToCSVPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		refreshPanel.add(refresh = new JButton("Refresh"));
		exportToCSVPanel.add(exportToCSV = new JButton("Export to CSV"));

		buttonPanel.add(refreshPanel);
		buttonPanel.add(exportToCSVPanel);

		refresh.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent evt) {
				setupBottomPane();
			}
		});

		exportToCSV.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent evt) {
				int[] preSelected = new int[2];
				preSelected[0] = firstSeq.getSelectedIndex();
				preSelected[1] = secondSeq.getSelectedIndex();
				new CollectionChooser(preSelected);
			}
		});

		JPanel bottomHalf = new JPanel(new BorderLayout());
		bottomHalf.add(buildConditionPanels(firstCollectionModel), BorderLayout.WEST);
		bottomHalf.add(buttonPanel, BorderLayout.CENTER);

		topPanel.add(seqAndZoom, BorderLayout.NORTH);
		topPanel.add(bottomHalf, BorderLayout.CENTER);

		bottomPane = new JScrollPane();
		setupBottomPane();
		bottomPane.getVerticalScrollBar().setUnitIncrement(10);

		add(topPanel, BorderLayout.NORTH);
		add(bottomPane, BorderLayout.CENTER);

		validate();
	}

	private JPanel buildConditionPanels(SyncCollectionModel basisModel) {
		conditionSeq = new JComboBox[numConditions];
		conditionComp = new JComboBox[numConditions];
		conditionType = new JComboBox[numConditions];
		conditionSeq2 = new JComboBox[numConditions];
		conditionModel = new SyncCollectionModel[numConditions];
		conditionModel2 = new SyncCollectionModel[numConditions];
		conditionValue = new JTextField[numConditions];
		booleanOps = new JComboBox[numConditions - 1];

		final JPanel conditionsPanel = new JPanel(new GridLayout(numConditions, 1, 5, 5));

		for (int i = 0; i < numConditions; i++) {
			conditionModel[i] = new SyncCollectionModel(basisModel);
			conditionModel2[i] = new SyncCollectionModel(basisModel);

			final JPanel thisCondPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
			final JComboBox condType = new JComboBox(comptypes);
			final JComboBox condSeq2 = new JComboBox(conditionModel2[i]);
			final JTextField condVal = new JTextField(10);

			conditionSeq2[i] = condSeq2;

			if (i > 0) {
				thisCondPanel.add(booleanOps[i - 1] = new JComboBox(booleans));
			} else {
				JPanel p = new JPanel();
				p.setPreferredSize(new JComboBox(booleans).getPreferredSize());
				thisCondPanel.add(p);
			}

			thisCondPanel.add(new JLabel(" Condition " + (i + 1) + ": "));
			thisCondPanel
					.add(conditionSeq[i] = new JComboBox(conditionModel[i]));
			thisCondPanel.add(conditionComp[i] = new JComboBox(comparators));
			thisCondPanel.add(conditionType[i] = condType);
			thisCondPanel.add(conditionValue[i] = condVal);

			condType.addItemListener(new ItemListener() {
				public void itemStateChanged(ItemEvent e) {
					thisCondPanel.remove(5);
					if (condType.getSelectedIndex() == 0)
						thisCondPanel.add(condVal);
					else
						thisCondPanel.add(condSeq2);

					topPanel.validate();
					topPanel.repaint();
				}
			});

			conditionSeq[i].setPreferredSize(new Dimension((int) conditionSeq[i].getPreferredSize().getWidth(), 20));
			conditionComp[i].setPreferredSize(new Dimension((int) conditionComp[i].getPreferredSize().getWidth(), 20));
			conditionsPanel.add(thisCondPanel);
		}

		return conditionsPanel;
	}

	public void setupBottomPane() {
		JPanel panePanel = new JPanel(new BorderLayout());
		bottomPane.setViewportView(panePanel);

		Collection seq1 = (Collection) firstSeq.getSelectedItem();
		Collection seq2 = (Collection) secondSeq.getSelectedItem();

		conditionStrs = new ArrayList<String>();
		condCollections = new ArrayList<Collection>();

		if (seq1 != null) {
			for (int i = 0; i < numConditions; i++) {
				Collection condColl = (Collection) conditionSeq[i].getSelectedItem();
				
				boolean compareAgainstValue = (conditionType[i].getSelectedIndex() == 0);
				
				String condVal = conditionValue[i].getText().trim();
				Collection compareColl = (Collection) conditionSeq2[i].getSelectedItem();
				int curIndex = condCollections.size();

				if (condColl != null && conditionComp[i].getSelectedIndex() > 0
						&& (compareAgainstValue && !condVal.equals("") || !compareAgainstValue && compareColl != null)) {
					String condStr = "";

					if (i > 0 && curIndex > 0)
						condStr = booleanOps[i - 1].getSelectedItem() + " ";

					condStr += "C" + curIndex + ".Value "
							+ conditionComp[i].getSelectedItem() + " ";

					condCollections.add(condColl);
					
					if (compareAgainstValue)
						condStr += condVal;
					else {
						condStr += "C" + (++curIndex) + ".Value ";
						condCollections.add(compareColl);
					}
					
					conditionStrs.add(condStr);
				}
			}
			
			data = new ArrayList<Hashtable<Date,Double>>();
			
			data.add(db.getConditionalTSCollectionData(seq1, condCollections, conditionStrs));
			if(seq2!=null)
				data.add(db.getConditionalTSCollectionData(seq2, condCollections, conditionStrs));
		}

		if (data!= null && data.size() > 0 && data.get(0).size() > 0) {
			int numSequences = (seq2 != null ? 2 : 1);
			
			datasets = new Dataset[numSequences];
			for (int i = 0; i < numSequences; i++)
				datasets[i] = new Dataset();
			scatterplotData = new Dataset[numSequences];
			for(int i=0;i<numSequences;i++){
				scatterplotData[i] = new Dataset();
			}
			
			ArrayList<ArrayList<Date>> dateSet = new ArrayList<ArrayList<Date>>();
			for(int i=0;i<data.size();i++){
				dateSet.add(new ArrayList<Date>(data.get(i).keySet()));
				Collections.sort(dateSet.get(i));
			}

			// This casting longs to doubles could come back to bite
			// me in the ass... but it's the only way to shove both
			// dates and regular data into the x-axis of a datapoint...
			double lastTimePoint = 0;
			double maxValue[] = new double[numSequences];
			double startTime = (double) dateSet.get(0).get(0).getTime();
			for(int i=0;i<numSequences;i++){
				if(startTime > (double) dateSet.get(i).get(0).getTime())
					startTime = (double) dateSet.get(i).get(0).getTime();
				for (Date d : dateSet.get(i)) {
					lastTimePoint = (double) d.getTime();
					double value = data.get(i).get(d);
					
					if (value > maxValue[i])
						maxValue[i] = value;
					
					datasets[i].add(new DataPoint(lastTimePoint, value));
					
				}
			}
			for (Date d : dateSet.get(0)) {
				boolean plottable = true;
				for (int i = 1; i < numSequences && plottable; i++) {
					plottable = (data.get(i).get(d) != null);
				}
				if (plottable) {
					for (int i = 0; i < numSequences; i++) {
						lastTimePoint = (double) d.getTime();
						double value = data.get(i).get(d);
						scatterplotData[i].add(new DataPoint(lastTimePoint, value));
					}
				}
			}
			xMin = startTime - 1000;
			xMax = lastTimePoint + 1000;
			
			for (int i = 0; i < numSequences; i++) {
				if (maxValue[i] <= 0)
					maxValue[i] = 10;
			}

			// sets up chart
			TimeSeriesPlot chart = new TimeSeriesPlot(datasets);
			chart.setTitle("<html><b>Time Series Comparison</b></html>");
			
			chart.repaint();

			zchart = new ZoomableChart(chart);
			zchart.setFocusable(true);
			//zchart.setCScrollMin(xMin);
			//zchart.setCScrollMax(xMax);
			// Set up comparison charts
			JPanel bottomPanel = addComponent(zchart, panePanel);

			int dataSetIndex = numSequences;
			
			if (numSequences > 1) {
				Chart scatterChart = new ScatterPlot(scatterplotData[0], scatterplotData[1]);
				scatterChart.setTitle("<html><b>Time Series Scatter Plot -- R^2: %10.5f</b></html>");
				scatterChart.setPreferredSize(new Dimension(400,400));
				scatterChart.repaint();
				
				bottomPanel = addComponent(scatterChart, bottomPanel);
			}
		} else {
			JPanel textPanel = new JPanel(new FlowLayout());
			textPanel.add(new JLabel("No data matches query"));
			panePanel.add(textPanel, BorderLayout.CENTER);
		}
	}

	
	/**Method dovetails with CollectionChooser class through collectionsToExport
	 * Significantly changed to allow for multiple collection exports
	 * @author rzeszotj
	 */
	private void exportDataToCSV() {
		//One or two collections the user has already selected

		// Rebuild data, and show new graph before exporting...
		setupBottomPane();
		
		try {
			JFileChooser fc = new JFileChooser("Choose Output File");
			int result = fc.showSaveDialog(null);
			if (result == JFileChooser.APPROVE_OPTION) {
				//Read in the dates for any given collections
				ArrayList<Date> dateSet;
				dateSet = db.getCollectionDates(collectionsToExport);
				Collections.sort(dateSet);
				
				SimpleDateFormat dformat = new SimpleDateFormat(
						"M/d/yyyy hh:mm:ss a");
				File f = fc.getSelectedFile();
				PrintWriter fWriter = new PrintWriter(f);
					
				//Print r^2 value iff 2 datasets were selected
				int numCollections = collectionsToExport.length;
				if (numCollections == 2 && datasets.length >= 2){
					fWriter.println("R^2: "	+ datasets[0].getCorrelationStats(datasets[1]).r2);
				}
				
				//Load in conditions
				String condString = "";
				for (int i = 0; i < numConditions; i++) {
					Collection condColl = (Collection) conditionSeq[i].getSelectedItem();
						boolean compareAgainstValue = (conditionType[i].getSelectedIndex() == 0);
					
					String condVal = conditionValue[i].getText().trim();
					Collection compareColl = (Collection) conditionSeq2[i].getSelectedItem();
						if (condColl != null && conditionComp[i].getSelectedIndex() > 0
							&& (compareAgainstValue && !condVal.equals("") || 
							    !compareAgainstValue && compareColl != null)) {
					
						if (condString.length() > 0)
							condString += booleanOps[i - 1].getSelectedItem()
									+ " ";
							condString += condColl + " "
								+ conditionComp[i].getSelectedItem() + " ";
						
						if (compareAgainstValue)
							condString += condVal;
						else
							condString += compareColl;
					}
				}
				
				if (condString.length() == 0)
					condString = "None";
				else
					condString = "\"" + condString + "\"";
					fWriter.println("Condition applied: " + condString);
					String line1 = "";
				String line2 = "Date";
				
				//Sequence and collection info
				for(int i = 0; i < collectionsToExport.length; i++)
				{
					line1 += ",Sequence " + (i+1);
					line2 += ",\"" + 
					    collectionsToExport[i].getName().replace("\"", "\"\"") + "\"";
				}
				
				//More conditions
				int index = 1;
				for (int i = 0; i < numConditions; i++) {
					Collection condColl = (Collection) conditionSeq[i].getSelectedItem();
					if (condColl != null) {
						line1 += ",Condition " + index;
						line2 += ",\"" + condColl.getName().replace("\"", "\"\"") + "\"";
						
						Collection compareColl = (Collection) conditionSeq2[i].getSelectedItem();
						if (compareColl != null) {
							line1 += ",Condition " + index + " Comparison";
							line2 += ",\"" + compareColl.getName().replace("\"", "\"\"") + "\"";
						}
						
						index++;
					}
				}
					
				fWriter.println(line1);
				fWriter.println(line2);
				//Parse through collectionsToExport - similar to "data" arrayList in setupBottom()
				ArrayList<Hashtable<Date,Double>> colData = new ArrayList<Hashtable<Date,Double>>();
				for (Collection c : collectionsToExport)
					colData.add(db.getConditionalTSCollectionData(c, condCollections, conditionStrs));
									
				//Print out the values, finally
				for (Date d : dateSet) {
					fWriter.print(dformat.format(d));
					
					for(Hashtable<Date,Double> table : colData){
						Double value = table.get(d);
						String stringVal = "";
						if(value != null) stringVal = ""+value;
						fWriter.print("," + stringVal);
					}
					fWriter.println();
				}
				fWriter.close();
			}
		} catch (FileNotFoundException e) {
			System.err.println("Error! File not found!");
			e.printStackTrace();
		}
	}


	private JPanel addComponent(JComponent newComponent, JPanel parent) {
		JPanel bottomHalf = new JPanel();
		parent.setLayout(new BorderLayout());
		parent.add(newComponent, BorderLayout.NORTH);
		parent.add(bottomHalf, BorderLayout.CENTER);
		return bottomHalf;
	}

	public void updateModels(Collection collection) {
		firstCollectionModel.setupModelFromCollection(collection);
		secondCollectionModel.setupModelFromOtherModel(firstCollectionModel);

		for (int i = 0; i < numConditions; i++)
			conditionModel[i].setupModelFromOtherModel(firstCollectionModel);
	}

	public boolean containsCollection(Collection c) {
		return firstCollectionModel.getMatchingItem(c) != null;
	}

	public void selectCollection(Collection c) {
		firstSeq.setSelectedItem(firstCollectionModel.getMatchingItem(c));
		secondSeq.setSelectedIndex(0);

		for (int i = 0; i < numConditions; i++) {
			conditionSeq[i].setSelectedIndex(0);
			conditionComp[i].setSelectedIndex(0);
			conditionValue[i].setText("");
		}

		repaint();

		setupBottomPane();
	}
	
	private class SyncCollectionModel implements ComboBoxModel {
		private Collection[] collections;

		private Collection selectedItem = null;

		public SyncCollectionModel(Collection collectionToBaseOn) {
			setupModelFromCollection(collectionToBaseOn);
		}

		public SyncCollectionModel(SyncCollectionModel otherModel) {
			setupModelFromOtherModel(otherModel);
		}

		public void setupModelFromCollection(Collection collectionToBaseOn) {
			ArrayList<Collection> allCollectionsInTree = tree
					.getCollectionsInTreeOrderFromRoot(1, collectionToBaseOn);
			ArrayList<Integer> collectionIDs = new ArrayList<Integer>();

			for (int i = 0; i < allCollectionsInTree.size(); i++)
				collectionIDs
						.add(allCollectionsInTree.get(i).getCollectionID());

			collectionIDs = db.getCollectionIDsWithAtoms(collectionIDs);

			collections = new Collection[collectionIDs.size()];

			int index = 0;
			// Make sure that if all else fails, the first item is selected...
			boolean selectNext = true;

			for (int i = 0; i < allCollectionsInTree.size(); i++) {
				Collection curCollection = allCollectionsInTree.get(i);

				if (collectionToBaseOn.equals(curCollection))
					selectNext = true;

				if (collectionIDs.contains(curCollection.getCollectionID())) {
					if (selectNext) {
						selectedItem = curCollection;
						selectNext = false;
					}

					collections[index++] = curCollection;
				}
			}
		}

		public void setupModelFromOtherModel(SyncCollectionModel otherModel) {
			collections = new Collection[otherModel.collections.length + 1];

			// Make blank entry at i = 0
			for (int i = 0; i < otherModel.collections.length; i++)
				collections[i + 1] = otherModel.collections[i];
		}

		// This helps to ensure that only the originally
		// constructed items are thrown into this collection
		public Collection getMatchingItem(Collection c) {
			for (int i = 0; i < collections.length; i++)
				if (c.equals(collections[i]))
					return collections[i];

			return null;
		}

		public Object getSelectedItem() {
			return selectedItem;
		}

		public void setSelectedItem(Object item) {
			selectedItem = (Collection) item;
		}

		public int getSize() {
			return collections.length;
		}

		public Object getElementAt(int index) {
			return collections[index];
		}

		public void addListDataListener(ListDataListener l) {
		}

		public void removeListDataListener(ListDataListener l) {
		}
	}
	
	/**CollectionChooser
	 *  UI window that allows user to select multiple collections from a list
	 * Chains with exportDataToCSV() from Export button
	 * Updates on "Okay" press, sends collections to collectionsToExport global var
	 * 
	 * @author rzeszotj
	 */
	public class CollectionChooser extends JDialog implements ActionListener, ListSelectionListener{
		private JButton okay,arrowR,arrowL;
		private JList colList,selList;
		
		public CollectionChooser(int[] selected) {
			super();
			setSize(450,450);
			setTitle("Select Collections to Export");
			setBorder(BorderFactory.createEmptyBorder(2,2,2,2));
			setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);

			okay = new JButton("OK");
			okay.addActionListener(this);
			arrowR = new JButton("->");
			arrowR.addActionListener(this);
			arrowL = new JButton("<-");
			arrowL.addActionListener(this);
			
			Collection[] c = getCollections();
			Arrays.sort(c);
			colList = new JList(c);
			colList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
			colList.setLayoutOrientation(JList.VERTICAL);
			colList.setVisibleRowCount(20);
	        colList.addListSelectionListener(this);
	        
	        selList = new JList();
			selList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
			selList.setLayoutOrientation(JList.VERTICAL);
			selList.setVisibleRowCount(20);
	        selList.addListSelectionListener(this);
	        
	        //Trick it into selecting and moving items from previous window
			if (selected[1] == 0 || selected[1] == -1)
				colList.setSelectedIndex(selected[0]);
			else
			{
				//Have to compensate for the null location
				selected[1]--;
				colList.setSelectedIndices(selected);
			}
			moveRight();
			okay.setEnabled(true);//
			
			//Now entering terrible UI section
			JScrollPane colListScroll = new JScrollPane(colList);
			JScrollPane selListScroll = new JScrollPane(selList);
			colListScroll.setPreferredSize(new Dimension(190,360));
			selListScroll.setPreferredSize(new Dimension(190,360));
			colListScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
			selListScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
			
			JLabel l = new JLabel("Hold CTRL and click to select multiple collections.");
			JLabel h = new JLabel("To get R^2 values, select two collections " +
					                          "in the main window and press okay here.");
			Dimension a = new Dimension(10,25);
			//Splitting it up into subpanes seemed the only choice to get the correct look
			JPanel pane = new JPanel();
			JPanel textPane = new JPanel();
			JPanel buttonPane = new JPanel();
			JPanel listPane = new JPanel();
			
			textPane.add(l);
			textPane.add(h);
			buttonPane.add(arrowR);
			buttonPane.add(new Box.Filler(a,a,a));
			buttonPane.add(arrowL);
			buttonPane.add(new Box.Filler(a,a,a));
			buttonPane.add(new Box.Filler(a,a,a));
			buttonPane.add(okay);
			listPane.add(colListScroll);
			listPane.add(buttonPane);
			listPane.add(selListScroll);
	
			textPane.setLayout(new BoxLayout(textPane,BoxLayout.Y_AXIS));
			buttonPane.setLayout(new BoxLayout(buttonPane,BoxLayout.Y_AXIS));
			listPane.setLayout(new BoxLayout(listPane,BoxLayout.X_AXIS));
			
			
			pane.add(textPane);
			pane.add(listPane);
			add(pane);
			
			setVisible(true);
			getRootPane().setDefaultButton(okay);
		}
		//Grabs all collections in the aggregate tree
		private Collection[] getCollections() {
			ArrayList<Collection> allCollectionsInTree = tree
					.getCollectionsInTreeOrderFromRoot(1, collectionToBaseOn);
			ArrayList<Integer> collectionIDs = new ArrayList<Integer>();

			for (int i = 0; i < allCollectionsInTree.size(); i++)
				collectionIDs.add(allCollectionsInTree.get(i).getCollectionID());

			collectionIDs = db.getCollectionIDsWithAtoms(collectionIDs);

			Collection[] collections = new Collection[collectionIDs.size()];

			//Populate collections with the correct items
			int index = 0;
			for (int i = 0; i < allCollectionsInTree.size(); i++) {
				Collection curCollection = allCollectionsInTree.get(i);
				if (collectionIDs.contains(curCollection.getCollectionID())) {
					collections[index++] = curCollection;
				}
			}
			return collections;
		}
		//Gets elements in a given list
		public Collection[] getElements(JList list)
		{
			ListModel mod = list.getModel();
			Collection[] elements = new Collection[mod.getSize()];
			for (int i = 0; i < elements.length; i++) {
				elements[i] = (Collection)mod.getElementAt(i);
			}
			return elements;
		}
		//Copies selected items from colList to selList
		public void moveRight() {
			ListModel toModel = selList.getModel();
				
			//Get everything in the selected list
			ArrayList<Collection> toAll = new ArrayList<Collection>();
			for (int i = 0; i < toModel.getSize(); i++)
				toAll.add((Collection)toModel.getElementAt(i));
			
			//Add what was selected
			Object[] fromObj = colList.getSelectedValues();
			ArrayList<Collection> fromCol = new ArrayList<Collection>();
			for (int i = 0; i < fromObj.length; i++) {
				Collection c = (Collection)fromObj[i];
				if (!toAll.contains(c))
					toAll.add((Collection)fromObj[i]);
			}
			
			//Convert back to arrays
			Object[] toObj = toAll.toArray();
			Collection[] toCol = new Collection[toObj.length];
			for (int i = 0; i < toCol.length; i++)
				toCol[i] = (Collection)toObj[i];
			
			//Reassign new data set
			Arrays.sort(toCol);
			selList.setListData(toCol);
		}
		//Deletes selected items from selList
		public void moveLeft() {
			ListModel toModel = selList.getModel();
			
			//Get everything in the selected list
			ArrayList<Collection> toAll = new ArrayList<Collection>();
			for (int i = 0; i < toModel.getSize(); i++)
				toAll.add((Collection)toModel.getElementAt(i));
			
			//Get everything that was selected
			Object[] selObj = selList.getSelectedValues();
			ArrayList<Collection> selCol = new ArrayList<Collection>();
			for (int i = 0; i < selObj.length; i++)
				selCol.add((Collection)selObj[i]);
			
			//Remove the selection
			for (Collection c : selCol)
				toAll.remove(c);
			
			//Convert back to arrays
			Object[] toObj = toAll.toArray();
			Collection[] toCol = new Collection[toObj.length];
			for (int i = 0; i < toCol.length; i++)
				toCol[i] = (Collection)toObj[i];
			
			//Reassign new data set
			selList.setListData(toCol);
		}		
		//Listener disables buttons if conditions not met
	    public void valueChanged(ListSelectionEvent evt) {
	        if (evt.getValueIsAdjusting() == false) {
	        	arrowR.setEnabled(true);
	        	arrowL.setEnabled(true);
	        	//Cannot move stuff to the right
	            if (colList.getSelectedIndex() < 0) {
	                arrowR.setEnabled(false);
	            } 
	            //Cannot move stuff to the left
	            if (selList.getSelectedIndex() < 0) {
	            	arrowL.setEnabled(false);
	            }
	            
	            //Nothing is in selected side
	            if (selList.getModel().getSize() < 1) {
	            	
	                okay.setEnabled(false);
	            }
	            else {
	            //Something is selected, enable OK
	                okay.setEnabled(true);
	            }
	        }
	    }
		//Update collectionsToExport and exportDataToCSV()
	    //Move selections
		public void actionPerformed(ActionEvent evt) {
			Object source = evt.getSource();
			if (source == okay) {
				collectionsToExport = getElements(selList);
				dispose();
				if (collectionsToExport.length > 0)
					exportDataToCSV();
			}
			else if (source == arrowL)
				moveLeft();
			else if (source == arrowR) {
				moveRight();
				okay.setEnabled(true);
			}
		}
	}
	
}
