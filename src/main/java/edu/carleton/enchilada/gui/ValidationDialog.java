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
 * The Original Code is EDAM Enchilada's QueryDialog class.
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


/*
 * Created on Jul 19, 2004
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.clustering.ClusterValidation;
import edu.carleton.enchilada.analysis.clustering.ClusterValidation.SampleType;
import edu.carleton.enchilada.analysis.clustering.ClusterValidation.ValidityTest;

import edu.carleton.enchilada.database.Database;

import java.awt.*;
import java.awt.event.*;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 
 * ValidationDialog opens a window to perform cluster validation tests upon existing clustering solutions.
 * Michael Murphy 2014, University of Toronto
 * 
 */

// Add option to choose indices to calculate

public class ValidationDialog extends JDialog implements ActionListener
{
	private JFrame parent;
	private Database db;
	
	private JButton okButton; //Default button
	private JButton cancelButton;
	
	private DistanceMetric dMetric = DistanceMetric.EUCLIDEAN_SQUARED;
	
	private boolean posNegNorm = true;
	private boolean isNormalized = true;
	private boolean randSample = false;
	private ValidityTest vTest = ValidityTest.BOTH;
	private SampleType sampleType = SampleType.FULL;
	
	private CollectionTree cTree;
	private Collection collection;
	
	private JComboBox metricDropDown;
	private JCheckBox pnNormBox;
	private JCheckBox dunnBox;
	private JCheckBox silBox;
	private JCheckBox statBox;
	private JCheckBox randBox;
	private JCheckBox haloBox;
	private JTextField excludeField;
	private JTextField fracBox;
	private JTextField iterBox;
	
	public JDialog progressDialog;
	
	public ValidationDialog(JFrame frame, CollectionTree cTree, Database db) {
		super(frame,"Cluster Validation", true);
		parent = frame;
		setSize(280,360);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);

		this.db = db;
		collection = cTree.getSelectedCollection();
		
		// determine if selected collection is a clustering parent, an individual cluster, or neither
		try {
			if (!collection.isClusterParent()) {
				JOptionPane.showMessageDialog(this, "Please select a cluster parent collection.", 
						"Improper collection selected.", JOptionPane.WARNING_MESSAGE);
				dispose();
				return;
			}
		} catch (HeadlessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// add in distance metric selection
		dMetric = DistanceMetric.EUCLIDEAN_SQUARED;
		
		JPanel header = new JPanel();
		JLabel headerLabel = new JLabel();
		headerLabel.setText("<html><body><div align='center'>Calculate Dunn and silhouette indices for<br>selected clustering solution</div></body></html>");
		header.add(headerLabel);
		
		JPanel p = new JPanel();
		JLabel distMetricLabel = new JLabel("Choose distance metric: ");
		String[] metricNames = {"Euclidean Squared", "City Block"};
		metricDropDown = new JComboBox(metricNames);
		metricDropDown.setEditable(false);
		metricDropDown.addActionListener(this);
		p.add(distMetricLabel);
		p.add(metricDropDown);
		
		JPanel p1 = new JPanel();
		JLabel excludeLabel = new JLabel("Exclude clusters (commas): ");
		excludeField = new JTextField(10);
		p1.add(excludeLabel);
		p1.add(excludeField);
		
		JPanel p2 = new JPanel();
		pnNormBox = new JCheckBox("Normalize +/- peaks separately", true);
		pnNormBox.addActionListener(this);
		p2.add(pnNormBox);
		
		JPanel p3 = new JPanel();
		dunnBox = new JCheckBox("Dunn index", true);
		silBox = new JCheckBox("Silhouettes", true);
		
		p3.add(dunnBox);
		p3.add(silBox);
		
		JPanel p4 = new JPanel();
		JPanel p5 = new JPanel();
		JLabel sampleLabel = new JLabel("Subsampling: ");
		randBox = new JCheckBox("Random",false);
		randBox.addActionListener(this);
		haloBox = new JCheckBox("Halo",false);
		haloBox.addActionListener(this);
		statBox = new JCheckBox("Means");
		statBox.addActionListener(this);
		JLabel fracLabel = new JLabel("Sample fraction: ");
		fracBox = new JTextField(3);
		fracBox.setEnabled(false);
		JLabel iterLabel = new JLabel("Iterations: ");
		iterBox = new JTextField(3);
		iterBox.setEnabled(false);
		p4.add(sampleLabel);
		p4.add(randBox);
		p4.add(haloBox);
		p4.add(statBox);
		p5.add(fracLabel);
		p5.add(fracBox);
		p5.add(iterLabel);
		p5.add(iterBox);
		
		JPanel buttons = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		buttons.add(okButton);
		buttons.add(cancelButton);
		
		add(header);
		add(p3);
		add(p);
		add(p4);
		add(p5);
		add(p1);
		add(p2);
		add(buttons);
		setLayout(new FlowLayout());
		
		getRootPane().setDefaultButton(okButton);
		setVisible(true);
	}

	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		if (source == metricDropDown) {
			// get distance metric
			int dMetricInt = metricDropDown.getSelectedIndex();
			switch (dMetricInt) {
				case 0: dMetric = DistanceMetric.EUCLIDEAN_SQUARED;
				case 1: dMetric = DistanceMetric.CITY_BLOCK;
			}
		}
		if (source == randBox) {
			silBox.setEnabled(true);
			if (randBox.isSelected()) {
				haloBox.setSelected(false);
				statBox.setSelected(false);
				fracBox.setEnabled(true);
				iterBox.setEnabled(true);
				sampleType = SampleType.RAND;
			}
			if (!haloBox.isSelected() && !randBox.isSelected()) {
				fracBox.setEnabled(false);
				iterBox.setEnabled(false);
				sampleType = SampleType.FULL;
			}
		}
		if (source == haloBox) {
			silBox.setEnabled(true);
			if (haloBox.isSelected()) {
				randBox.setSelected(false);
				statBox.setSelected(false);
				fracBox.setEnabled(true);
				iterBox.setEnabled(false);
				sampleType = SampleType.HALO;
			}
			if (!haloBox.isSelected() && !randBox.isSelected()) {
				fracBox.setEnabled(false);
				iterBox.setEnabled(false);
				sampleType = SampleType.FULL;
			}
		}
		if (source == statBox) {
			randBox.setSelected(false);
			haloBox.setSelected(false);
			if (statBox.isSelected()) {
				fracBox.setEnabled(false);
				iterBox.setEnabled(false);
				silBox.setEnabled(false);
				silBox.setSelected(false);
			} else {
				fracBox.setEnabled(true);
				iterBox.setEnabled(true);
				silBox.setEnabled(true);
			}
			sampleType = SampleType.MEAN;
		}
		if (source == pnNormBox) {
			posNegNorm = pnNormBox.isSelected();
		}
		if (source == randBox) {
			posNegNorm = pnNormBox.isSelected();
		}
		if (source == okButton) {
			String excludeString = excludeField.getText();
			ArrayList<Integer> excludeClusters = new ArrayList<Integer>();
			
			float frac = 1;
			int iter = 1;
			
			try {
				if (fracBox.getText().length() > 0 && (sampleType == SampleType.RAND || sampleType == sampleType.HALO)) {
					frac = Float.parseFloat(fracBox.getText());
					if (frac <= 0 || frac > 1) {
						JOptionPane.showMessageDialog(new JFrame(), "Required: 0.0 < sample fraction <= 1", "Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
				}
				if (iterBox.getText().length() > 0 && sampleType == SampleType.RAND) {
					iter = Integer.parseInt(iterBox.getText());
					if (iter <= 0) {
						JOptionPane.showMessageDialog(new JFrame(), "Required: number of iterations > 0", "Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
				}
			} catch (NumberFormatException ex) {
				JOptionPane.showMessageDialog(new JFrame(), "Invalid number format.", "Error", JOptionPane.ERROR_MESSAGE);
				return;
			}
			
			if (dunnBox.isSelected() & silBox.isSelected())
				vTest = ValidityTest.BOTH;
			if (dunnBox.isSelected() & !silBox.isSelected())
				vTest = ValidityTest.DUNN;
			if (!dunnBox.isSelected() & silBox.isSelected())
				vTest = ValidityTest.SIL;
			if (!dunnBox.isSelected() & !silBox.isSelected()) {
				JOptionPane.showMessageDialog(new JFrame(), "No validity test selected.", "Error", JOptionPane.ERROR_MESSAGE);
				return;
			}
			
			// run algorithm
			if (excludeString.length() > 0) {
				for (String s : excludeString.split("[, ]+")) {
					try {
						excludeClusters.add(Integer.parseInt(s));
					} catch (NumberFormatException ex) {
						JOptionPane.showMessageDialog(new JFrame(), "Invalid name. Cluster names must be comma-separated integers.", "Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
				}
			}
			
			ClusterValidation.run(db, collection, dMetric, posNegNorm, excludeClusters, vTest, sampleType, frac, iter);
			dispose();
		}
		if (source == cancelButton) {
			dispose();
		}
	}
}
