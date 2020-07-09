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
 * Created on Feb 8, 2009
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.dataExporters.MSAnalyzeDataSetExporter;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.externalswing.SwingWorker;

import java.awt.*;
import java.awt.event.*;

/**
 * @author jtbigwoo
 */
public class ExportMSAnalyzeDialog extends JDialog implements ActionListener 
{
	private JButton okButton;
	private JButton cancelButton;
	private JTextField parFileField;
	private JButton parDotDotDot;
	private JTextField accessFileField;
	private JButton accessDotDotDot;
	private Database db;
	private JFrame parent = null;
	private Collection collection = null;
	
	public ExportMSAnalyzeDialog(JFrame parent, Database db, Collection c) {
		super (parent,"Export to MS-Analyze", true);
		this.db = db;
		this.parent = parent;
		this.collection = c;
		setSize(450,150);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel parFileLabel = new JLabel(".par File: ");
		parFileField = new JTextField(25);
		parDotDotDot = new JButton("...");
		parDotDotDot.addActionListener(this);
		
		JLabel accessFileLabel = new JLabel("MS Analyze DB File: ");
		accessFileField = new JTextField(25);
		accessDotDotDot = new JButton("...");
		accessDotDotDot.addActionListener(this);
		
		JPanel buttonPanel = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		buttonPanel.add(okButton);
		buttonPanel.add(cancelButton);
		
		JPanel mainPanel = new JPanel();
		LayoutManager layout = new GridBagLayout();
	    mainPanel.setLayout(layout);

		GridBagConstraints gbc = new GridBagConstraints();
		gbc.gridx = 0;
		gbc.gridy = 0;
	    mainPanel.add(parFileLabel, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 1;
		gbc.gridy = 0;
	    mainPanel.add(parFileField, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 2;
		gbc.gridy = 0;
	    mainPanel.add(parDotDotDot, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 0;
		gbc.gridy = 1;
		mainPanel.add(accessFileLabel, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 1;
		gbc.gridy = 1;
	    mainPanel.add(accessFileField, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 2;
		gbc.gridy = 1;
	    mainPanel.add(accessDotDotDot, gbc);

		gbc = new GridBagConstraints();
		gbc.gridx = 0;
		gbc.gridy = 2;
		mainPanel.add(buttonPanel, gbc);


//		layout.putConstraint(SpringLayout.WEST, parFileLabel,
//                10, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, parFileLabel,
//                15, SpringLayout.NORTH, mainPanel);
//		layout.putConstraint(SpringLayout.WEST, parFileField,
//                170, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, parFileField,
//                10, SpringLayout.NORTH, mainPanel);
//		layout.putConstraint(SpringLayout.WEST, parDotDotDot,
//                375, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, parDotDotDot,
//                10, SpringLayout.NORTH, mainPanel);
//		layout.putConstraint(SpringLayout.WEST, accessFileLabel,
//                10, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, accessFileLabel,
//                15, SpringLayout.SOUTH, parFileField);
//		layout.putConstraint(SpringLayout.WEST, accessFileField,
//                170, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, accessFileField,
//                10, SpringLayout.SOUTH, parFileField);
//		layout.putConstraint(SpringLayout.WEST, accessDotDotDot,
//				375, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, accessDotDotDot,
//                10, SpringLayout.SOUTH, parFileField);
//		layout.putConstraint(SpringLayout.WEST, buttonPanel,
//                160, SpringLayout.WEST, mainPanel);
//		layout.putConstraint(SpringLayout.NORTH, buttonPanel,
//                10, SpringLayout.SOUTH, accessFileLabel);
		
		add(mainPanel);
		
		setVisible(true);	
	}
	
	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		if (source == parDotDotDot) {
			parFileField.setText((new FileDialogPicker("Choose .par file destination",
					 "par", this)).getFileName());
		}
		else if (source == accessDotDotDot) {
			accessFileField.setText((new FileDialogPicker("Choose .mdb file",
					 "mdb", this)).getFileName());
		}
		else if (source == okButton) {
			if(!parFileField.getText().equals("") && !parFileField.getText().equals("*.par")) {
				if (accessFileField.getText().equals("") || accessFileField.getText().endsWith(".mdb")) {
					final JDialog parentRef = this;
					final Database dbRef = (Database)db;
					
					final ProgressBarWrapper progressBar = 
						new ProgressBarWrapper(parent, MSAnalyzeDataSetExporter.TITLE, 100);
					final MSAnalyzeDataSetExporter dse = 
							new MSAnalyzeDataSetExporter(
									this, dbRef,progressBar);
					
					progressBar.constructThis();
					final String accessFileName = accessFileField.getText().equals("") ? null : accessFileField.getText();
					
					final SwingWorker worker = new SwingWorker(){
						public Object construct(){
								try{
									dse.exportToPar(collection, parFileField.getText(), accessFileName);
								}catch (DisplayException e1) {
									ErrorLogger.displayException(progressBar,e1.toString());
								} 
							return null;
						}
						public void finished(){
							progressBar.disposeThis();
							ErrorLogger.flushLog(parent);
							parent.validate();
						}
					};
					worker.start();
					dispose();
				}
				//If they didn't enter a valid name for our access database, force them to enter one
				else
					JOptionPane.showMessageDialog(this, "Please enter a valid access database.");
			}
			//If they didn't enter a name, force them to enter one
			else
				JOptionPane.showMessageDialog(this, "Please enter a par file name.");
		}			
		else  
			dispose();
	}

}
