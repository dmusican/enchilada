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

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import gnu.trove.iterator.TFloatIntIterator;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.hash.TFloatIntHashMap;

import java.awt.event.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.TreeSet;

public class SizeExportDialog extends JDialog implements ActionListener 
{
	public static String EXPORT_FILE_EXTENSION = "csv";
	
	private JButton okButton;
	private JButton cancelButton;
	private JTextField csvFileField;
	private JRadioButton rawButton;
	private JRadioButton binnedButton;
	private JButton csvDotDotDot;
	private Database db;
	private SizeHistogramWindow parent = null;
	
	private boolean raw = false;
	
	public SizeExportDialog(SizeHistogramWindow sizeHistogramWindow) {
		super (sizeHistogramWindow,"Export to CSV file", true);
		this.parent = sizeHistogramWindow;
		setSize(600,150);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel csvFileLabel = new JLabel("." + EXPORT_FILE_EXTENSION + " File: ");
		csvFileField = new JTextField(25);
		csvDotDotDot = new JButton("...");
		csvDotDotDot.addActionListener(this);
		
		binnedButton = new JRadioButton("Binned counts",true);
		binnedButton.addActionListener(this);
		rawButton = new JRadioButton("Raw data",false);
		rawButton.addActionListener(this);
		
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
	    mainPanel.add(binnedButton);
	    mainPanel.add(rawButton);
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
                500, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, csvDotDotDot,
                10, SpringLayout.NORTH, mainPanel);
		layout.putConstraint(SpringLayout.WEST, binnedButton,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, binnedButton,
                10, SpringLayout.SOUTH, csvFileField);
		layout.putConstraint(SpringLayout.WEST, rawButton,
                140, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, rawButton,
                10, SpringLayout.SOUTH, csvFileField);
		layout.putConstraint(SpringLayout.WEST, buttonPanel,
                160, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, buttonPanel,
                10, SpringLayout.SOUTH, binnedButton);
		
		add(mainPanel);
		
		setVisible(true);	
	}
	
	public void actionPerformed(ActionEvent e) {
		int maxMZValue;
		Object source = e.getSource();
		if (source == rawButton) {
			binnedButton.setSelected(!rawButton.isSelected());
			raw = rawButton.isSelected();
		}
		else if (source == binnedButton) {
			rawButton.setSelected(!binnedButton.isSelected());
			raw = !binnedButton.isSelected();
		}
		else if (source == csvDotDotDot) {
			String fileName = "*." + EXPORT_FILE_EXTENSION;
			if (!csvFileField.getText().equals("")) {
				fileName = csvFileField.getText();
			}
			csvFileField.setText((new FileDialogPicker("Choose ." + EXPORT_FILE_EXTENSION + " file destination",
					 fileName, this, false)).getFileName());
		}
		else if (source == okButton) {
			if(!csvFileField.getText().equals("") && !csvFileField.getText().equals("*." + EXPORT_FILE_EXTENSION)) {
				final String csvFileName = csvFileField.getText().equals("") ? null : csvFileField.getText();
				try {
					exportToCSV(csvFileName, raw);
				} catch (DisplayException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				dispose();
			}
			else
				JOptionPane.showMessageDialog(this, "Please enter an export file name.");
		}
		else if (source == cancelButton) {
			dispose();
		}
//		else  
//			dispose();
	}
	
	public boolean exportToCSV(String fileName, boolean raw) throws DisplayException {		
			if (fileName == null) {
				return false;
			} else if (! fileName.endsWith(ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION)) {
				fileName = fileName + "." + ExportHierarchyCSVDialog.EXPORT_FILE_EXTENSION;
			}
	
			fileName = fileName.replaceAll("'", "");
	
			try {
				if (raw)
					writeOutDistancesToFile(parent.getRawSizes(), fileName);
				else
					writeOutDistancesToFile(parent.getBinnedSizes(), fileName);
			} catch (IOException e) {
				ErrorLogger.writeExceptionToLogAndPrompt("CSV Data Exporter","Error writing file. Please ensure the application can write to the specified file.");
				System.err.println("Problem writing file: ");
				e.printStackTrace();
				return false;
		}
		return true;
	}
	
	private void writeOutDistancesToFile(TFloatArrayList data, String fileName) throws IOException {
		PrintWriter out = null;
		File csvFile;
		TFloatIntHashMap currentData;
		DecimalFormat formatter = new DecimalFormat("0.00");

		csvFile = new File(fileName);

		out = new PrintWriter(new FileOutputStream(csvFile, false));
		StringBuffer sbHeader = new StringBuffer();
		sbHeader.append("Size");
		out.println(sbHeader.toString());

		StringBuffer sbValues = new StringBuffer();
		
		for (int i = 0; i < data.size(); i++) {
			sbValues.append(data.get(i));
			out.println(sbValues.toString());
			sbValues.setLength(0);
		}
		out.close();
	}
		
	private void writeOutDistancesToFile(TFloatIntHashMap data, String fileName) throws IOException {
		PrintWriter out = null;
		File csvFile;
		TFloatIntHashMap currentData;
		DecimalFormat formatter = new DecimalFormat("0.00");

		csvFile = new File(fileName);

		out = new PrintWriter(new FileOutputStream(csvFile, false));
		StringBuffer sbHeader = new StringBuffer();
		sbHeader.append("SizeBin,Count");
		out.println(sbHeader.toString());
		
		StringBuffer sbValues = new StringBuffer();
		
		// maps aren't sorted, but histograms are
		TreeSet<float[]> sortedBins = new TreeSet<float[]>(new Comparator<float[]>() {
			public int compare(float[] a, float[] b) {
				if (a[0] > b[0]) return 1;
				else return -1;
			}
		}); 
		for (TFloatIntIterator it = data.iterator(); it.hasNext();) {
			it.advance();
			sortedBins.add(new float[]{it.key(),it.value()});
		}
		for (float[] v : sortedBins) {
			sbValues.append(v[0]+","+(int)v[1]);
			out.println(sbValues.toString());
			sbValues.setLength(0);
		}
		out.close();
	}
}
