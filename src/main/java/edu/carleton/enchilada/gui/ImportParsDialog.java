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
 * The Original Code is EDAM Enchilada's ImportParsDialog class.
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


import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.HeadlessException;
import java.awt.event.*;
import java.awt.*;

import javax.swing.*;
import javax.swing.table.TableColumn;


import edu.carleton.enchilada.dataImporters.ATOFMSDataSetImporter;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.*;
import edu.carleton.enchilada.externalswing.SwingWorker;


/**
 * @author andersbe, Jamie Olson
 *
 */

public class ImportParsDialog extends JDialog implements ActionListener {
	private JButton okButton, cancelButton, advancedOptionsButton;
	private JCheckBox parentButton;
	private JLabel parentLabel;
	private JPanel listPane;
	private ParTableModel pTableModel,advancedTableModel,basicTableModel;
	private JProgressBar progressBar;
	private int dataSetCount;
	private JFrame parent = null;
	private boolean importedTogether = false, showAdvancedOptions = false;
	private Database db;
	private int parentID = 0; //default parent collection is root
	
	/**
	 * Extends JDialog to form a modal dialogue box for importing 
	 * par files.  
	 * @param owner The parent frame of this dialog box, should be the 
	 * main frame.  This frame will become inactive while ImportPars is
	 * active.  
	 * @throws java.awt.HeadlessException From the constructor of 
	 * JDialog.  
	 */
	public ImportParsDialog(JFrame owner) throws HeadlessException {
		// calls the constructor of the superclass (JDialog), sets the title and makes the
		// dialog modal.  
		super(owner, "Import ATOFMS *.pars as Collections", true);
		this.db = MainFrame.db;
		parent = owner;
		setSize(890,500);
		
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel label = new JLabel("Choose Datasets to Convert");
		
		
		okButton = new JButton("OK");
		okButton.setMnemonic(KeyEvent.VK_O);
		okButton.addActionListener(this);
		
		cancelButton = new JButton("Cancel");
		cancelButton.setMnemonic(KeyEvent.VK_C);
		cancelButton.addActionListener(this);
		
		advancedOptionsButton = new JButton("Advanced>>>");
		advancedOptionsButton.addActionListener(this);
		
		//an option to import all the datasets into one parent collection
		parentButton = new JCheckBox(
				"Create a parent collection for all incoming datasets.",
				false);
		parentButton.setMnemonic(KeyEvent.VK_P);
		parentButton.addActionListener(this);
		
		parentLabel = new JLabel();
		
		
		listPane = getListPane();
		
		JPanel panel = new JPanel(new BorderLayout());
		label.setHorizontalAlignment(JLabel.CENTER);
		label.setHorizontalTextPosition(JLabel.CENTER);
		panel.add(label, BorderLayout.NORTH);
		panel.add(listPane, BorderLayout.CENTER);
		panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
		
		JPanel buttonPane = new JPanel();
		buttonPane.setLayout(new BoxLayout(buttonPane, BoxLayout.X_AXIS));
		buttonPane.setBorder(BorderFactory.createEmptyBorder(10,10,10,10));
		
		buttonPane.add(parentButton);
		buttonPane.add(parentLabel);
		buttonPane.add(Box.createHorizontalGlue());
		buttonPane.add(okButton);
		buttonPane.add(Box.createRigidArea(new Dimension(5,0)));
		buttonPane.add(advancedOptionsButton);
		buttonPane.add(Box.createRigidArea(new Dimension(5,0)));
		buttonPane.add(cancelButton);
		
		add(panel, BorderLayout.CENTER);
		add(buttonPane, BorderLayout.SOUTH);
		
		setVisible(true);	
	}
	
	public ImportParsDialog(JFrame owner, Database db) throws HeadlessException {
		this(owner);
		this.db = db;
	}
	
	public ImportParsDialog() {
		// TODO Auto-generated constructor stub
	}
	
	private JTable getParTable()
	{
		return getParTable(showAdvancedOptions);
	}
	
	private JTable getParTable(boolean advanced)
	{
		pTableModel = new ParTableModel(advanced);
		JTable pTable = new JTable(pTableModel);	
        pTable.setDefaultEditor(Float.class, new FloatEditor());
        pTable.setDefaultEditor(Integer.class, new IntegerEditor());
        pTable.setDefaultRenderer(Float.class, new FloatRenderer());
        pTable.setDefaultRenderer(Integer.class, new IntegerRenderer());
        int numColumns = 7;
        if(showAdvancedOptions)numColumns = 8;
		TableColumn[] tableColumns = new TableColumn[numColumns];
		for (int i = 0; i < numColumns; i++)
			tableColumns[i] = pTable.getColumnModel().getColumn(i+1);
		tableColumns[0].setCellEditor(
				new FileDialogPickerEditor("par","Import",this));
		tableColumns[0].setPreferredWidth(200);
		tableColumns[1].setCellEditor(
				new FileDialogPickerEditor("cal","Mass Cal file",this));
		tableColumns[1].setPreferredWidth(200);
		tableColumns[2].setCellEditor(
				new FileDialogPickerEditor("noz","Size Cal file",this));
		tableColumns[2].setPreferredWidth(200);

		TableColumn numColumn = pTable.getColumnModel().getColumn(0);
		numColumn.setPreferredWidth(10);
		
		for (int i = 3; i < numColumns; ++i)
			tableColumns[i].setPreferredWidth(60);
		
		pTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		// if you don't put this next line in, the current change will be lost
		// when you click on a button outside the table
		pTable.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);
		
		return pTable;
	}
	
	private JPanel getListPane(){
		JPanel simple = new JPanel(new BorderLayout());
		JTable parTable;
		parTable = getParTable(false);
		basicTableModel = (ParTableModel)parTable.getModel();
		JScrollPane scrollPane = new JScrollPane(parTable);
		//scrollPane.setPreferredSize(new Dimension(795,200));
		//simple.add(Box.createRigidArea(new Dimension(0, 4)));
		simple.add(scrollPane, BorderLayout.CENTER);

		
		JPanel complex = new JPanel(new BorderLayout());
		parTable = getParTable(true);
		advancedTableModel = (ParTableModel)parTable.getModel();
		scrollPane = new JScrollPane(parTable);
		//scrollPane.setPreferredSize(new Dimension(850,200));
		//complex.add(Box.createRigidArea(new Dimension(0, 4)));
		complex.add(scrollPane, BorderLayout.CENTER);
		

		JPanel panel = new JPanel(new CardLayout());
		panel.add(simple, "Simple");
		panel.add(complex, "Advanced");
		
		
		return panel;
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == okButton) {
				final JDialog thisRef = this;
				final CardLayout card = (CardLayout)(listPane.getLayout());
				final Database dbRef = db;
				//construct everything
				final ProgressBarWrapper progressBar = 
					new ProgressBarWrapper(parent, ATOFMSDataSetImporter.title, 100);
				final ATOFMSDataSetImporter dsi;
				if(showAdvancedOptions){
					advancedTableModel.fireTableDataChanged();
					dsi = 
						new ATOFMSDataSetImporter(
								advancedTableModel, parent, progressBar);
				}else{
					basicTableModel.fireTableDataChanged();
					dsi = 
						new ATOFMSDataSetImporter(
								basicTableModel, parent, progressBar);
				}
				if (importedTogether)
					dsi.setParentID(parentID);
				else dsi.setParentID(0);
				// If a .par file or a .cal file is missing, don't start the process.
				try {
					dsi.checkNullRows();
				} catch (DisplayException e1) {
//					 Exceptions here mostly have to do with mis-entered data.
					// Those that don't should probably be handled differently,
					// but I'm just reworking this so that it uses exceptions
					// in a way that's less silly, so I'm not worrying about that
					// for now.  -Thomas
					ErrorLogger.displayException(progressBar,e1.toString());
					return;
				} 
				progressBar.constructThis();
				
				
				final SwingWorker worker = new SwingWorker(){
					public Object construct(){
						dsi.collectTableInfo();
						for(int i=0;i<dsi.getNumCollections();i++){
							dbRef.beginTransaction();
							try{
								dsi.collectRowInfo();
								if(progressBar.wasTerminated())
									throw new InterruptedException();
								dbRef.commitTransaction();
							} catch (InterruptedException e2){
								dbRef.rollbackTransaction();
							}catch (DisplayException e1) {
								ErrorLogger.displayException(progressBar,e1.toString());
								dbRef.rollbackTransaction();
							} 
							
						}
						return null;
					}
					public void finished(){
						progressBar.disposeThis();
						dispose();
					}
				};
				worker.start();
				
		}
		else if (source == parentButton){
			//pop up a "create new collections" dialog box & keep number of new collection
			if (parentButton.isSelected()) {
				EmptyCollectionDialog ecd = 
					new EmptyCollectionDialog((JFrame)parent, "ATOFMS", false, db);
				parentID = ecd.getCollectionID();
				
				if (parentID == -1) {
					parentButton.setSelected(false);
				} else {
					parentLabel.setText("Importing into collection " + ecd.getCollectionName());
					importedTogether = true;
				}
			}
			else {
				if(!EmptyCollectionDialog.removeEmptyCollection(db, parentID))
					System.err.println("Error deleting temporary collection");
				parentLabel.setText("");
				importedTogether = false;
			}
		}
		else if (source == advancedOptionsButton){
			showAdvancedOptions = !showAdvancedOptions;
			CardLayout card = (CardLayout)(listPane.getLayout());
			//Copy values from the current TableModel into the new TableModel
			// and display the new TableModel
			if(showAdvancedOptions){
				basicTableModel.fireTableDataChanged();
				advancedOptionsButton.setText("<<<Simple");
				for(int i=0;i<basicTableModel.getRowCount();i++){
					for(int j=0;j<basicTableModel.getColumnCount()-1; j++){
						Object value = basicTableModel.getValueAt(i, j);
						advancedTableModel.setValueAt(value, i, j);
					}
					//advancedTableModel.setValueAt(new Float(.50), i,basicTableModel.getColumnCount()-1);
					advancedTableModel.setValueAt(basicTableModel.getValueAt(i, basicTableModel.getColumnCount()-1),
							i,advancedTableModel.getColumnCount()-1);
				}
				card.show(listPane, "Advanced");
			}else{
				advancedTableModel.fireTableDataChanged();
				advancedOptionsButton.setText("Advanced>>>");
				for(int i=0;i<advancedTableModel.getRowCount();i++){
					for(int j=0;j<basicTableModel.getColumnCount()-1; j++){
						Object value = advancedTableModel.getValueAt(i, j);
						basicTableModel.setValueAt(value, i, j);
					}
					basicTableModel.setValueAt(advancedTableModel.getValueAt(i, advancedTableModel.getColumnCount()-1),
							i,basicTableModel.getColumnCount()-1);
				}
				card.show(listPane, "Simple");
			}
		}
		else if (source == cancelButton) {
			System.out.println("min relative area: "+((Float)basicTableModel.getValueAt(0,6)).floatValue());
			if (importedTogether) {
				if(!EmptyCollectionDialog.removeEmptyCollection(db, parentID))
					System.err.println("Error deleting temporary collection");
				parentLabel.setText("");
				importedTogether = false;
			}
			dispose();
		}
	}

	/**
	 * Method to determine whether the datasets are being imported together into
	 * a single parent collection.
	 * 
	 * @return True if datasets are being imported together.
	 */
	public boolean parentExists(){
		
		return importedTogether;
		
	}
	
	/**
	 * Accessor method to obtain the parent collection's ID for the datasets being
	 * imported.
	 * 
	 * @return	parentID
	 */
	public int getParentID(){
		
		return parentID;
		
	}
	

}