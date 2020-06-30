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
 * The Original Code is EDAM Enchilada's ClusterDialog class.
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

package edu.carleton.enchilada.gui;

import edu.carleton.enchilada.collection.AggregationOptions;
import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.*;
import edu.carleton.enchilada.externalswing.SwingWorker;

import java.awt.*;
import java.awt.event.*;
import java.util.*;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.*;

/**
 * This class is a gui object for aggregating data. It automatically adjusts options depending on
 * data type and automatically parses dates of varying format. 
 * 
 */
public class AggregateWindow extends JFrame implements ActionListener, ListSelectionListener {
	private MainFrame parentFrame;
	private JButton createSeries, cancel, calculateTimes;
	private JTextField descriptionField;
	private TimePanel startTime, endTime, intervalPeriod;
	private JList collectionsList;
	private CollectionListModel collectionListModel;
	private JRadioButton selSeqRadio, timesRadio; 
	private JRadioButton naDateRadio,eurDateRadio;
	private JComboBox matchingCombo;
	
	private Database db;
	private Hashtable<Collection, JPanel> cachedCollectionPanels;
	private Collection[] collections;
	
	private JPanel centerPanel,timesPanel, dateRadioPanel;

	public AggregateWindow(MainFrame parentFrame, Database db, Collection[] collections) {
		super("Aggregate Collections");
		
		this.parentFrame = parentFrame;
		this.db = db;
		this.collections = collections;
		
		setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				cancel();
				
			}
		});
		
		cachedCollectionPanels = new Hashtable<Collection, JPanel>();
		collectionsList = new JList(collectionListModel = new CollectionListModel(collections));
		
		setPreferredSize(new Dimension(500, 625));
		setSize(500,625);
		setResizable(true);

		JPanel mainPanel = new JPanel(new BorderLayout());

		mainPanel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
		
		JPanel topPanel = setupTopPanel(collections);
		JPanel timePanel = setupTimePanel();
		
		JPanel buttonPanel = new JPanel(new FlowLayout());
		buttonPanel.add(createSeries = new JButton("Create Series"));
		buttonPanel.add(cancel = new JButton("Cancel"));
		
		JPanel bottomPanel = new JPanel(new BorderLayout());
		bottomPanel.add(timePanel, BorderLayout.CENTER);
		bottomPanel.add(buttonPanel, BorderLayout.SOUTH);
		
		mainPanel.add(topPanel, BorderLayout.CENTER);
		mainPanel.add(bottomPanel, BorderLayout.SOUTH);
		
		add(mainPanel);
		
		createSeries.addActionListener(this);
		cancel.addActionListener(this);
		collectionsList.addListSelectionListener(this);
		
		collectionsList.setSelectedIndex(0);
		
	}
	

	private JPanel setupTopPanel(Collection[] collections) {
		JPanel topPanel = new JPanel(new BorderLayout());
		centerPanel = new JPanel(new GridLayout(1, 2, 5, 0));
		
		centerPanel.add(setupLeftPanel(), 0);
		centerPanel.add(getPanelForCollection(collections[0]), 1);
		
		JPanel descriptionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
		descriptionPanel.add(new JLabel("Description:  "));
		descriptionPanel.add(descriptionField = new JTextField(30));
		
		topPanel.add(descriptionPanel, BorderLayout.NORTH);
		topPanel.add(centerPanel, BorderLayout.CENTER);
		
		return topPanel;
	}
	
	private JPanel setupLeftPanel() {
		JPanel leftPanel = new JPanel(new BorderLayout());
		JScrollPane collections = new JScrollPane(collectionsList);
		collectionsList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		collections.setPreferredSize(new Dimension(230, 190));
		
		leftPanel.add(new JLabel("Collections:"), BorderLayout.NORTH);
		leftPanel.add(collections);
		
		return leftPanel;
	}
	
	private JPanel setupTimePanel() {
		JPanel bottomPanel = new JPanel(new BorderLayout());
		bottomPanel.setBorder(new EmptyBorder(10, 5, 5, 0));

		JLabel timeBasis = new JLabel("Time Basis:");
	    ButtonGroup group_1 = new ButtonGroup();
	    group_1.add(selSeqRadio = new JRadioButton("Match to:"));
	    group_1.add(timesRadio = new JRadioButton("Times"));
	    timesRadio.setSelected(true);
	    
	    ButtonGroup group_2 = new ButtonGroup();
	    group_2.add(naDateRadio = new JRadioButton("MM/DD/YYYY or MM-DD-YYYY"));
	    group_2.add(eurDateRadio = new JRadioButton("DD/MM/YYYY or DD.MM.YYYY"));
	    naDateRadio.addActionListener(this);
	    eurDateRadio.addActionListener(this);
	    naDateRadio.setSelected(true);
	    
	    JPanel matchingPanel = new JPanel();
	    matchingCombo = new JComboBox(collections);
		matchingCombo .setEditable(false);
		//matchingPanel.add(selSeqRadio);
		//matchingPanel.add(matchingCombo);
	    
	    Calendar startDate = new GregorianCalendar(), endDate = new GregorianCalendar();
	    Calendar interval = new GregorianCalendar(0, 0, 0, 0, 0, 0);
	    // add things with indices so that you can remove them later!
	    timesPanel = new JPanel(new GridLayout(4, 1, 0, 5));
	    timesPanel.add(calculateTimes=new JButton("Calculate Time Interval"),0);
	    calculateTimes.addActionListener(this);
	    timesPanel.add(startTime = new TimePanel("Start Time:", startDate, false,naDateRadio,eurDateRadio),1);
	    timesPanel.add(endTime = new TimePanel("End Time:", endDate, false,naDateRadio,eurDateRadio),2);
	    timesPanel.add(intervalPeriod = new TimePanel("Interval:", interval, true,naDateRadio,eurDateRadio),3);
	    timesPanel.setBorder(new EmptyBorder(0, 25, 0, 0));
	    
	    dateRadioPanel = new JPanel(new GridLayout(1,2,3,3));
	    dateRadioPanel.add(naDateRadio,0);
	    dateRadioPanel.add(eurDateRadio,1);
	    dateRadioPanel.setBorder(new EmptyBorder(0, 25, 0, 0));
	    
		JPanel bottomHalf = addComponent(timeBasis, bottomPanel);
		//bottomHalf = addComponent(matchingPanel, bottomHalf);
		bottomHalf = addComponent(selSeqRadio, bottomHalf);
		bottomHalf = addComponent(matchingCombo, bottomHalf);
		
		bottomHalf = addComponent(timesRadio, bottomHalf);
		bottomHalf = addComponent(timesPanel, bottomHalf);
		bottomHalf = addComponent(dateRadioPanel, bottomHalf);
		
	    return bottomPanel;
	}
	private JPanel getPanelForCollection(Collection collection) {
		if (cachedCollectionPanels.containsKey(collection))
			return cachedCollectionPanels.get(collection);
		
		JPanel ret;
		
		AggregationOptions options = collection.getAggregationOptions();
		if (options == null)
			collection.setAggregationOptions(options = new AggregationOptions());
		
		if (collection.getDatatype().equals("ATOFMS") || 
				collection.getDatatype().equals("AMS")) {
			ret = getATOFMSPanel(collection);
		} else if (collection.getDatatype().equals("TimeSeries")) {
			ret = getTimeSeriesPanel(collection);
		} else {
			ret = new JPanel(); // Blank if unknown collection type...
		}
		
		cachedCollectionPanels.put(collection, ret);
		
		return ret;
	}
	
	private JPanel getATOFMSPanel(Collection collection) {
		final AggregationOptions options = collection.getAggregationOptions();
		
	    JCheckBox partCount = new JCheckBox();
	    partCount.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent evt) {
				options.produceParticleCountTS = (evt.getStateChange() == ItemEvent.SELECTED);
			}
		});
	    partCount.setSelected(options.produceParticleCountTS);
		
	    // right now it's going to do all or just those you select.  it might
	    // also be clever to do those you select or the complement of those you
	    // select.  But I'm not going to do that right now.  --Thomas
	    
	    final JTextField mzValues = new JTextField(100);
	    try {
			options.setMZValues(options.mzString);
		} catch (NumberFormatException e) {
			JOptionPane.showMessageDialog(
					mzValues, 
					"Error: Invalid input (" + options.mzString + ")\n" +
							"Make sure your input is of the form: \"-10 to -4, 5 to 6, 100 to 400\"\n" +
							"and that all values are within the range of -600 to 600", 
					"Invalid Input", 
					JOptionPane.ERROR_MESSAGE); 
			mzValues.setText(options.mzString);
		}
		mzValues.setText(options.mzString);
	    mzValues.addFocusListener(new FocusListener() {
	    	private String savedText;
	    	
	    	public void focusGained(FocusEvent evt) {
	    		savedText = ((JTextField) evt.getSource()).getText();
	    	}
	    	
	    	public void focusLost(FocusEvent evt) {
	    		JTextField mzValues = ((JTextField) evt.getSource());
	    		String newText = mzValues.getText();
	    		try {
	    			options.setMZValues(newText);
	    		} catch (NumberFormatException e) {
	    			JOptionPane.showMessageDialog(
	    					mzValues, 
	    					"Error: Invalid input (" + newText + ")\n" +
	    							"Make sure your input is of the form: \"-10 to -4, 5 to 6, 100 to 400\"\n" +
	    							"and that all values are within the range of -600 to 600", 
	    					"Invalid Input", 
	    					JOptionPane.ERROR_MESSAGE); 
	    			mzValues.setText(savedText);
	    		}
	    	}
	    });
	    
	    ButtonGroup bg = getValueCombiningButtons(options);
	    Enumeration<AbstractButton> combiningButtons = bg.getElements();
	    
		JPanel mainPanel = new JPanel();
		
		JPanel partCountPanel = new JPanel(new BorderLayout(10, 0));
		partCountPanel.add(partCount, BorderLayout.WEST);
		partCountPanel.add(new JLabel("<html>Produce particle-count <br>time series</html>"), BorderLayout.CENTER);
		
		final JCheckBox noMZValues = new JCheckBox("Don't aggregate M/Z values", false);
		noMZValues.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				options.setMZValues(noMZValues.isSelected() ? "" : mzValues.getText());
				if (noMZValues.isSelected())
					options.allMZValues = false;
				mzValues.setEnabled(! noMZValues.isSelected());
			}
		});
		
		JPanel bottomHalf = addComponent(new JPanel(), mainPanel);
		bottomHalf = addComponent(new JLabel("Collection Options for " + collection.getName() + ":"), bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(new JLabel("Combining Method:"), bottomHalf);
		
		while (combiningButtons.hasMoreElements())
			bottomHalf = addComponent(combiningButtons.nextElement(), bottomHalf);
		
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(partCountPanel, bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(noMZValues, bottomHalf);
		bottomHalf = addComponent(new JLabel("Produce time series"), bottomHalf);
		bottomHalf = addComponent(new JLabel("for m/z values (leave blank for all):"), bottomHalf);
		bottomHalf = addComponent(mzValues, bottomHalf);
		
		return mainPanel;
	}
	
	private JPanel getTimeSeriesPanel(Collection collection) {
		final AggregationOptions options = collection.getAggregationOptions();
		
	    JCheckBox isContinuousData = new JCheckBox();
	    isContinuousData.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent evt) {
				options.treatDataAsContinuous = (evt.getStateChange() == ItemEvent.SELECTED);
			}
		});
	    isContinuousData.setSelected(options.treatDataAsContinuous);
	    
	    ButtonGroup bg = getValueCombiningButtons(options);
	    Enumeration<AbstractButton> combiningButtons = bg.getElements();
	    
		JPanel continuousDataPanel = new JPanel(new BorderLayout(10, 0));
		continuousDataPanel.add(isContinuousData, BorderLayout.WEST);
		continuousDataPanel.add(new JLabel("<html>Treat Data as Continuous</html>"), BorderLayout.CENTER);
		
		JPanel mainPanel = new JPanel();
		JPanel bottomHalf = addComponent(new JPanel(), mainPanel);
		bottomHalf = addComponent(new JLabel("Collection Options for " + collection.getName() + ":"), bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(continuousDataPanel, bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(new JPanel(), bottomHalf);
		bottomHalf = addComponent(new JLabel("Combining Method:"), bottomHalf);
		
		while (combiningButtons.hasMoreElements())
			bottomHalf = addComponent(combiningButtons.nextElement(), bottomHalf);
		
		return mainPanel;
	}

	private ButtonGroup getValueCombiningButtons(final AggregationOptions options) {
		JRadioButton combWithSum = new JRadioButton("Sum");
		JRadioButton combWithAverage = new JRadioButton("Average");
		
		combWithSum.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (((JRadioButton) e.getSource()).isSelected())
					options.combMethod = AggregationOptions.CombiningMethod.SUM;
			}
		});
		
		combWithAverage.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (((JRadioButton) e.getSource()).isSelected())
					options.combMethod = AggregationOptions.CombiningMethod.AVERAGE;
			}
		});
		
	    ButtonGroup group = new ButtonGroup();
	    group.add(combWithSum);
	    group.add(combWithAverage);
	    combWithSum.setSelected(options.combMethod == AggregationOptions.CombiningMethod.SUM);
	    combWithAverage.setSelected(options.combMethod == AggregationOptions.CombiningMethod.AVERAGE);

	    return group;
	}
	
	private JPanel addComponent(JComponent newComponent, JPanel parent) {
		JPanel bottomHalf = new JPanel();
		parent.setLayout(new BorderLayout());
		parent.add(newComponent, BorderLayout.NORTH);
		parent.add(bottomHalf, BorderLayout.CENTER);
		return bottomHalf;
	}

	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == calculateTimes){
			final Calendar startDate = new GregorianCalendar(), endDate = new GregorianCalendar();
		    final ProgressBarWrapper progressBar = 
				new ProgressBarWrapper(this, "Calculating Time Interval", collections.length);
			progressBar.constructThis();
			progressBar.setIndeterminate(true);
			final AggregateWindow thisRef = this;
			final SwingWorker aggWorker = new SwingWorker() {
				private int collectionID;
				public Object construct() {
					db.getMaxMinDateInCollections(collections, startDate, endDate);
					return new Integer(0);
				}
				public void finished() {
					timesPanel.remove(1);
				    timesPanel.add(startTime = new TimePanel("Start Time:", startDate,
				    		                                 false,naDateRadio,eurDateRadio),1);
				    timesPanel.remove(2);
				    timesPanel.add(endTime = new TimePanel("End Time:", endDate,
				    		                               false,naDateRadio,eurDateRadio),2);
				    thisRef.pack();
				    thisRef.repaint();
					progressBar.disposeThis();
				}
			};
			aggWorker.start();
		    
		    
		}else if (source == createSeries) {
			final long timingStart = new Date().getTime();
			final String newSeriesName = descriptionField.getText().trim().replace("'", "''");			
			if (newSeriesName.equals("")) {
    			JOptionPane.showMessageDialog(
    					this, 
    					"Please fill out description field before aggregating.", 
    					"No Description", 
    					JOptionPane.ERROR_MESSAGE);
    			
    			return;
			}
			
			String timeBasisSQLStr = null;
			boolean baseSequenceOnCollection = selSeqRadio.isSelected(); 
			
			final Aggregator aggregator;
			if (baseSequenceOnCollection) {
				Collection selectedCollection = collections[matchingCombo.getSelectedIndex()];
				aggregator = new Aggregator(this, db, selectedCollection);
				System.out.println("selected Collection: "+selectedCollection.getName()+
						"\tID: "+ selectedCollection.getCollectionID());
			} else {
				//Check the time scales for correct formatting
				if(startTime.isBad())
				{
					JOptionPane.showMessageDialog(
		    				this, 
		    				"Please enter a valid time for \"Start Time\".\n"+
		    				"Invalid times will appear in red.", 
		    				"Invalid Time String", 
		    				JOptionPane.ERROR_MESSAGE);
		    		return;
				}
				else if(endTime.isBad())
				{
					JOptionPane.showMessageDialog(
		    				this, 
		    				"Please enter a valid time for \"End Time\".\n"+
		    				"Invalid times will appear in red.", 
		    				"Invalid Time String", 
		    				JOptionPane.ERROR_MESSAGE);
		    		return;
				}
				else if(intervalPeriod.isBad())
				{
					JOptionPane.showMessageDialog(
		    				this, 
		    				"Please enter a valid time for \"Interval\".\n"+
		    				"Invalid times will appear in red.", 
		    				"Invalid Time String", 
		    				JOptionPane.ERROR_MESSAGE);
		    		return;
				}
				Calendar start = startTime.getDate();
				Calendar end = endTime.getDate();
				Calendar interval = intervalPeriod.getDate();
				if (end.before(start)) {
					JOptionPane.showMessageDialog(
	    					this, 
	    					"Start time must come before end time...", 
	    					"Invalid times used for aggregation basis", 
	    					JOptionPane.ERROR_MESSAGE);
	    			
	    			return;
				} else if (interval.get(Calendar.HOUR_OF_DAY) == 0 &&
							interval.get(Calendar.MINUTE) == 0 &&
							interval.get(Calendar.SECOND) == 0) {
					JOptionPane.showMessageDialog(
	    					this, 
	    					"Time interval cannot be zero...", 
	    					"Invalid times used for aggregation basis", 
	    					JOptionPane.ERROR_MESSAGE);
	    			
	    			return;
				}
				aggregator = new Aggregator(this, db, start, end, interval);
				System.out.println("start: "+start.getTimeInMillis()+"\nend: "+end.getTimeInMillis()+
						"\ninterval: "+interval.getTimeInMillis());
			}
			final ProgressBarWrapper initProgressBar =
				aggregator.createAggregateTimeSeriesPrepare(collections);
			System.out.print("collections: ");
			for(Collection collection : collections){
				System.out.print(collection.getCollectionID()+", ");
			}
			System.out.println();
			
			final SwingWorker aggWorker = new SwingWorker() {
				private int collectionID;
				public Object construct() {
					db.beginTransaction();
					try{
					collectionID = aggregator.createAggregateTimeSeries(
							newSeriesName,collections,initProgressBar,
							parentFrame);
					db.commitTransaction();
					} catch(InterruptedException e){
						db.rollbackTransaction();
						return null;
					} catch(AggregationException f){
						final String name = f.collection.getName();
						SwingUtilities.invokeLater(new Runnable(){
							public void run() {
								JOptionPane.showMessageDialog(null,
										"The start and stop dates which you selected resulted" +
										" in the collection: "+name+" having " +
										"no data points to aggregate.  Either remove the collection or try" +
										"different dates.");
							}
							
						});
						db.rollbackTransaction();
						return null;
					}
					return new Integer(collectionID);
				}
				public void finished() {
					initProgressBar.disposeThis();
					setVisible(false);
					long timingEnd = new Date().getTime();
					System.out.println("Aggregation Time: " + (timingEnd-timingStart));
					// for all collections, set default Aggregation Options.
					for (int i = 0; i < collections.length; i++) {
						AggregationOptions ao = new AggregationOptions();
						ao.setDefaultOptions();
						collections[i].setAggregationOptions(ao);
						//collections[i].getAggregationOptions().setDefaultOptions();
					}
					
					dispose();		
				}
			};
			aggWorker.start();

		} else if (source == cancel) {
			cancel();
		}
		//When the radio buttons are clicked, adjust the text in each box to conform to the new format
		//It's faster just to delete them and make new ones using the correct standard
		else if (source == naDateRadio)
		{
			Calendar start = startTime.getDate();
			Calendar end = endTime.getDate();
			final AggregateWindow thisRef = this;
			timesPanel.remove(1);
		    timesPanel.add(startTime = new TimePanel("Start Time:", start, false,naDateRadio,eurDateRadio),1);
		    timesPanel.remove(2);
		    timesPanel.add(endTime = new TimePanel("End Time:", end, false,naDateRadio,eurDateRadio),2);
		    thisRef.pack();
		    thisRef.repaint();
		}
		else if (source == eurDateRadio)
		{
			Calendar start = startTime.getDate();
			Calendar end = endTime.getDate();
			final AggregateWindow thisRef = this;
			timesPanel.remove(1);
		    timesPanel.add(startTime = new TimePanel("Start Time:", start, false,naDateRadio,eurDateRadio),1);
		    timesPanel.remove(2);
		    timesPanel.add(endTime = new TimePanel("End Time:", end, false,naDateRadio,eurDateRadio),2);
		    thisRef.pack();
		    thisRef.repaint();
		}
	}
	
	public void cancel(){
//		 for all collections, set default Aggregation Options.
		for (int i = 0; i < collections.length; i++) {
			if(collections[i]==null)
				System.out.println("collections["+i+"] is null");
			if(collections[i].getAggregationOptions()==null)
				System.out.println("collections["+i+"].getAggregationOptions() is null");
			if(collections[i].getAggregationOptions()!=null)
				collections[i].getAggregationOptions().setDefaultOptions();
		}
		setVisible(false);
		dispose();
	}
	public void valueChanged(ListSelectionEvent e) {
		int index = collectionsList.getSelectedIndex();
		
		if (!e.getValueIsAdjusting() && index > -1) {
			Collection collection = collectionListModel.getCollectionAt(index);

			centerPanel.remove(1);
			centerPanel.add(getPanelForCollection(collection), 1);
			centerPanel.validate();
			centerPanel.repaint();
		}
	}
	
	public class CollectionListModel extends AbstractListModel {
		Collection[] collections;
		
		public CollectionListModel(Collection[] collections) {
			this.collections = collections;
		}
		
		public int getSize() { return collections.length; }
		public Object getElementAt(int index) {
			return " " + getCollectionAt(index).getName(); 
		}
		
		public Collection getCollectionAt(int index) {
			return collections[index];
		}
	};
}