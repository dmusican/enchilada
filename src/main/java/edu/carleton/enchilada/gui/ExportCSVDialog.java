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

import javax.swing.*;
import javax.swing.table.TableModel;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.dataExporters.CSVDataSetExporter;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.externalswing.SwingWorker;

import java.awt.event.*;
import java.util.ArrayList;

/**
 * @author jtbigwoo
 */
public class ExportCSVDialog extends JDialog implements ActionListener 
{
	public static String EXPORT_FILE_EXTENSION = "csv";
	
	private JButton okButton;
	private JButton cancelButton;
	private JTextField maxMZField;
	private JTextField csvFileField;
	private JCheckBox onePerFileBox;
	private JButton csvDotDotDot;
	private Database db;
	private JFrame parent = null;
	private Collection collection = null;
	private boolean exportAverages = false;
	private boolean onePerFile = false;
	
	/**
	 * Called when you want to export a particular particle or whole collection of particles
	 * @param parent
	 * @param db
	 * @param c
	 * @param exportAverages - if this is true, we call exportHierarchyToCSV which exports
	 * the averages of all subcollections of the selected collection.
	 */
	public ExportCSVDialog(JFrame parent, Database db, Collection c, boolean exportAverages) {
		super (parent,"Export " + (exportAverages ? "Hierarchy " : "") + "to CSV file", true);
		this.db = db;
		this.parent = parent;
		this.collection = c;
		this.exportAverages = exportAverages;
		setSize(450,150);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel csvFileLabel = new JLabel("." + EXPORT_FILE_EXTENSION + " File: ");
		csvFileField = new JTextField(25);
		csvDotDotDot = new JButton("...");
		csvDotDotDot.addActionListener(this);
		
		JLabel maxMZLabel = new JLabel("Highest m/z value +/- to export: ");
		maxMZField = new JTextField(25);
		
		onePerFileBox = new JCheckBox("Sparse particle format");
		onePerFileBox.addActionListener(this);
		
		JPanel buttonPanel = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		buttonPanel.add(okButton);
		buttonPanel.add(cancelButton);
	    buttonPanel.add(onePerFileBox);
		
		JPanel mainPanel = new JPanel();
		SpringLayout layout = new SpringLayout();
	    mainPanel.setLayout(layout);	
		
	    mainPanel.add(csvFileLabel);
	    mainPanel.add(csvFileField);
	    mainPanel.add(csvDotDotDot);
	    mainPanel.add(maxMZLabel);
	    mainPanel.add(maxMZField);
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
		layout.putConstraint(SpringLayout.WEST, maxMZLabel,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, maxMZLabel,
                15, SpringLayout.SOUTH, csvFileField);
		layout.putConstraint(SpringLayout.WEST, maxMZField,
                170, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, maxMZField,
                10, SpringLayout.SOUTH, csvFileField);
		layout.putConstraint(SpringLayout.WEST, buttonPanel,
                160, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, buttonPanel,
                10, SpringLayout.SOUTH, maxMZLabel);
		
		add(mainPanel);
		
		setVisible(true);	
	}
	
	public void actionPerformed(ActionEvent e) {
		int maxMZValue;
		Object source = e.getSource();
		if (source == csvDotDotDot) {
			String fileName = "*." + EXPORT_FILE_EXTENSION;
			if (!csvFileField.getText().equals("")) {
				fileName = csvFileField.getText();
			}
			csvFileField.setText((new FileDialogPicker("Choose ." + EXPORT_FILE_EXTENSION + " file destination",
					 fileName, this, false)).getFileName());
		}
		if (source == onePerFileBox) {
			onePerFile = onePerFileBox.isSelected();
			if (onePerFile)
				maxMZField.setEnabled(false);
			else
				maxMZField.setEnabled(true);
		}
		else if (source == okButton) {
			try {
				maxMZValue = Integer.parseInt(maxMZField.getText());
			}
			catch (NumberFormatException nfe) {
				maxMZValue = -1;
			}
			if(!csvFileField.getText().equals("") && !csvFileField.getText().equals("*." + EXPORT_FILE_EXTENSION)) {
				if (maxMZValue > 0 || onePerFile) {
					final Database dbRef = (Database)db;
					
					final ProgressBarWrapper progressBar = 
						new ProgressBarWrapper(parent, CSVDataSetExporter.TITLE, 100);
					final CSVDataSetExporter cse = 
							new CSVDataSetExporter(
									this, dbRef,progressBar);
					cse.setOnePerFile(onePerFile);
					
					progressBar.constructThis();
					final String csvFileName = csvFileField.getText().equals("") ? null : csvFileField.getText();
					final int mzValue = maxMZValue;
					
					final SwingWorker worker = new SwingWorker(){
						public Object construct() {
							try {
								if (exportAverages) {
									cse.exportHierarchyToCSV(collection, csvFileName, mzValue);
								}
								else {
									cse.exportToCSV(collection, csvFileName, mzValue);
								}
							}catch (DisplayException e1) {
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
				else
					JOptionPane.showMessageDialog(this, "Highest m/z value to export must be a number greater than zero.");
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
