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
 * The Original Code is EDAM Enchilada's EmptyCollectionDialog class.
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
 * Tom Bigwood tom.bigwood@nevelex.com
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
 * Created on March 8, 2009
 */
package edu.carleton.enchilada.gui;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.dataExporters.CSVDataSetExporter;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.externalswing.SwingWorker;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

/**
 * @author jtbigwoo
 */
public class ExportHistogramCSVDialog extends JDialog implements ActionListener
{
	public static String EXPORT_FILE_EXTENSION = "csv";

	private final JButton okButton;
	private final JButton cancelButton;
	private final JComboBox<String> queryList;
	private String selectedQueryType = null;
	private final JTextField csvFileField;
	private final JButton csvDotDotDot;
	private final JTextField startTimeField;
	private final JTextField endTimeField;
	private final JTextField timeResField;
	private final Database db;
	private JFrame parent = null;
	private Collection[] collection = null;

	/**
	 * Called when you want to export a particular particle or whole collection of particles
	 * @param parent
	 * @param db
	 * @param c
	 */
	public ExportHistogramCSVDialog(JFrame parent, Database db, Collection[] c) {
		super (parent,"Export Histogram to CSV file", true);
		this.db = db;
		this.parent = parent;
		this.collection = c;
		setSize(450,300);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel csvFileLabel = new JLabel("." + EXPORT_FILE_EXTENSION + " File: ");
		csvFileField = new JTextField(15);
		csvDotDotDot = new JButton("...");
		csvDotDotDot.addActionListener(this);
		
		JLabel queryTypePrompt = new JLabel("Query type: ");
		String[] queryTypes = {"height sum", "rel. area sum", "area sum", "peak count", "size count"};
		queryList = new JComboBox<String>(queryTypes);
		queryList.addActionListener(this);

		JLabel timeLabel = new JLabel("For times, enter YYYY-MM-DD hh:mm:ss, blank for all");
		JLabel startTimeLabel = new JLabel("Start time: ");
		startTimeField = new JTextField(15);

		JLabel endTimeLabel = new JLabel("End time: ");
		endTimeField = new JTextField(15);

		JLabel timeResLabel = new JLabel("Time resolution (secs)");
		timeResField = new JTextField(15);

		JPanel buttonPanel = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		buttonPanel.add(okButton);
		buttonPanel.add(cancelButton);

		JPanel mainPanel = new JPanel();
		SpringLayout layout = new SpringLayout();
	    mainPanel.setLayout(layout);	
		
	    mainPanel.add(csvFileLabel);
	    mainPanel.add(csvFileField);
	    mainPanel.add(csvDotDotDot);
	    mainPanel.add(queryTypePrompt);
	    mainPanel.add(queryList);
	    mainPanel.add(timeLabel);
		mainPanel.add(startTimeLabel);
		mainPanel.add(startTimeField);
		mainPanel.add(endTimeLabel);
		mainPanel.add(endTimeField);
		mainPanel.add(timeResLabel);
		mainPanel.add(timeResField);
	    mainPanel.add(buttonPanel);
	    
		layout.putConstraint(SpringLayout.WEST, csvFileLabel,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, csvFileLabel,
                15, SpringLayout.NORTH, mainPanel);

		layout.putConstraint(SpringLayout.WEST, csvFileField,
                170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, csvFileField,
                10, SpringLayout.NORTH, mainPanel);

		layout.putConstraint(SpringLayout.WEST, csvDotDotDot,
                375, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, csvDotDotDot,
                10, SpringLayout.NORTH, mainPanel);

		layout.putConstraint(SpringLayout.WEST, queryTypePrompt,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, queryTypePrompt,
                15, SpringLayout.SOUTH, csvFileField);

		layout.putConstraint(SpringLayout.WEST, queryList,
                170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, queryList,
                10, SpringLayout.SOUTH, csvFileField);

		layout.putConstraint(SpringLayout.WEST, timeLabel,
							 10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, timeLabel,
							 10, SpringLayout.SOUTH, queryTypePrompt);

		layout.putConstraint(SpringLayout.WEST, startTimeLabel,
							 10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, startTimeLabel,
							 10, SpringLayout.SOUTH, timeLabel);

		layout.putConstraint(SpringLayout.WEST, startTimeField,
							 170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, startTimeField,
							 10, SpringLayout.SOUTH, timeLabel);

		layout.putConstraint(SpringLayout.WEST, endTimeLabel,
							 10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, endTimeLabel,
							 10, SpringLayout.SOUTH, startTimeLabel);

		layout.putConstraint(SpringLayout.WEST, endTimeField,
							 170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, endTimeField,
							 10, SpringLayout.SOUTH, startTimeLabel);

		layout.putConstraint(SpringLayout.WEST, timeResLabel,
							 10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, timeResLabel,
							 10, SpringLayout.SOUTH, endTimeLabel);

		layout.putConstraint(SpringLayout.WEST, timeResField,
							 170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, timeResField,
							 10, SpringLayout.SOUTH, endTimeLabel);

		layout.putConstraint(SpringLayout.WEST, buttonPanel,
							 160, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, buttonPanel,
							 10, SpringLayout.SOUTH, timeResLabel);

		add(mainPanel);
		
		setVisible(true);	
	}
	
	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		if (source == csvDotDotDot) {
			String fileName = "*." + EXPORT_FILE_EXTENSION;
			if (!csvFileField.getText().equals("")) {
				fileName = csvFileField.getText();
			}
			csvFileField.setText((new FileDialogPicker("Choose ." + EXPORT_FILE_EXTENSION + " file destination",
					 fileName, this, false)).getFileName());
		} else if (source == queryList) {
			selectedQueryType = (String)((JComboBox<String>)source).getSelectedItem();

		} else if (source == okButton) {
			if(!csvFileField.getText().equals("") && !csvFileField.getText().equals("*." + EXPORT_FILE_EXTENSION)) {

				final String choice;
				final int numbins;
				ArrayList<Double> bins = new ArrayList<>();
				int lpeak = -100;
				int upeak = -100;
				if (selectedQueryType.equals("size count")) {
					choice = JOptionPane.showInputDialog(
							"Enter 16, 32, 64, or 128 for standard bins per decade or press enter to create your own bins");

					assert choice != null;
					if (choice.equals("")) {
						numbins = Integer.parseInt(JOptionPane.showInputDialog("Enter total number of size bins:"));
						bins.add(0.);
						for (int x = 0; x < numbins; x++) {
							bins.add(Double.parseDouble(
									JOptionPane.showInputDialog("Enter upper bound for size bin " + (x + 1))));
						}
					} else {
						numbins = -1;
					}
				} else {
					choice = "";
					numbins = -1;
					lpeak = Integer.parseInt(JOptionPane.showInputDialog("Enter lower bound for peak range:"));
					upeak = Integer.parseInt(JOptionPane.showInputDialog("Enter upper bound for peak range:"));
					for (int x = lpeak; x <= upeak; x++) {
						bins.add((double)x);
					}
				}

				final ProgressBarWrapper progressBar =
						new ProgressBarWrapper(parent, CSVDataSetExporter.TITLE, 100);
				final CSVDataSetExporter cse =
						new CSVDataSetExporter(
								this, db,progressBar);

				progressBar.constructThis();
				final String csvFileName = csvFileField.getText().equals("") ? null : csvFileField.getText();

				final SwingWorker worker = new SwingWorker(){
					public Object construct() {
						try {
							cse.exportHistogramToCSV(collection, csvFileName, selectedQueryType,
													 startTimeField.getText(), endTimeField.getText(),
													 Integer.parseInt(timeResField.getText()), choice, numbins, bins);
						} catch (Exception e1) {
							ErrorLogger.displayException(progressBar,e1.toString());
						}
						return null;
					}
					public void finished() {
						progressBar.disposeThis();
						ErrorLogger.flushLog(parent);
						parent.validate();
					}
				};
				worker.start();
				dispose();
			}
			//If they didn't enter a name, force them to enter one
			else
				JOptionPane.showMessageDialog(this, "Please enter an export file name.");
		}
		else if (source == cancelButton) {
			dispose();
		}
	}
}
