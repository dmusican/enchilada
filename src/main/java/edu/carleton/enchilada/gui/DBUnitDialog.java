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
 * The Original Code is EDAM Enchilada's SQLServerDatabase unit test class.
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
 * Created on Jul 29, 2004
 *
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package edu.carleton.enchilada.gui;


import java.awt.BorderLayout;
import java.awt.FileDialog;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.FileNotFoundException;
import java.sql.SQLException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;


import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.SQLServerDatabase;
import edu.carleton.enchilada.database.InfoWarehouse;

import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.externalswing.SwingWorker;



/**
 * This class is a simple interface for saving and restoring a database from the xml file
 * that dbunit uses.  This is most useful in creating snapshots of the database to compare with
 * in dbunit unit tests.  You can take a snapshot of the results of aggregation with a working version of
 * Enchilada and use that snapshot to create dbunit tests.  This Dialog will not interfere with Enchilada's rebuild
 * or SQL Server Enterprise Manager's restore or backup capabilities because it does not keep a database connection
 * open.
 * 
 * @author olsonja
 *
 */
public class DBUnitDialog extends JFrame implements ActionListener{
	private JButton saveButton;
	private JButton loadButton;
	private JButton exitButton;
	private static final String databaseName = MainFrame.dbname;

	public DBUnitDialog(){
		super("DB Unit Save/Restore Interface");
		
//		 Verify that database exists, and give user opportunity to create
		// if it does not.
		if (Database.getDatabase(databaseName).isPresent()) {
			if (JOptionPane.showConfirmDialog(this,
					"Database: '"+databaseName+"' not found. Would you like to create one?\n" +
					"Make sure to select yes only if there is no database already present,\n"
					+ "since this will remove any pre-existing Enchilada database.") ==
						JOptionPane.YES_OPTION) {
				
				try {
					Database.getDatabase(databaseName).rebuildDatabase(databaseName);
				} catch (ExceptionAdapter ea) {
					if (ea.originalException instanceof SQLException) {
						JOptionPane.showMessageDialog(this,
								"Could not rebuild the database." +
										"  Close any other programs that may be accessing the database and try again.");
					} else {
						throw ea;
					}
				}
			} else {
				return; // no database?  we shouldn't do anything at all.
			}
		}
		
		init();
	}
	
	public void init(){
		JLabel label = new JLabel("What do you want to do?");
		
		JPanel buttonPanel = new JPanel();
		saveButton = new JButton("Save Database");
		saveButton.addActionListener(this);
		
		loadButton = new JButton("Load Database");
		loadButton.addActionListener(this);
		
		exitButton = new JButton("Exit");
		exitButton.addActionListener(this);
		
		buttonPanel.add(saveButton);
		buttonPanel.add(loadButton);
		buttonPanel.add(exitButton);
		
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		mainPanel.add(label,BorderLayout.NORTH);
		mainPanel.add(buttonPanel,BorderLayout.CENTER);
		
		setSize(400,200);
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		add(mainPanel);
		setVisible(true);
	}
	public void actionPerformed(ActionEvent e)
	{
		final InfoWarehouse db = Database.getDatabase(databaseName);
		db.openConnection();
		
		//Open database connection:
		
		Object source = e.getSource();
		if (source == saveButton) {
			
			int temp = -1;
			String fileFilter = "";
			fileFilter = "*.xml";
			temp = 1;
			final int fileType = temp;
			FileDialog fileChooser = new FileDialog(this, 
                    "Create a file to store the database:",
                     FileDialog.LOAD);
			fileChooser.setFile(fileFilter);
			fileChooser.setVisible(true);
			final String filename = fileChooser.getDirectory()+fileChooser.getFile();
			System.out.println("File: "+filename);
			
			final ProgressBarWrapper progressBar = 
				new ProgressBarWrapper(null, "Exporting Database",100);
			progressBar.setIndeterminate(true);
			progressBar.constructThis();
			SwingWorker worker = new SwingWorker(){
				public Object construct(){
					try {
						System.out.println("exporting database");
						((SQLServerDatabase)db).exportDatabase(filename,fileType);
						System.out.println("exported database");
					} catch (FileNotFoundException e1) {
						SwingUtilities.invokeLater(new Runnable(){
							public void run(){
								JOptionPane.showMessageDialog(null,"The file: "+ filename+" could not be created.");
							}
						});
						
					}
					return null;
				}
				public void finished() {
					progressBar.disposeThis();
					JOptionPane.showMessageDialog(null,
					"Database successfully saved.");
					db.closeConnection();
					//setVisible(false);
					//dispose();		
				}
			};
			worker.start();
			
		}
		
		else if (source == loadButton) {
			final int fileType;
			String fileFilter = "";
			fileFilter = "*.xml";
			fileType = 1;
			FileDialog fileChooser = new FileDialog(this, 
                    "Choose a database file to restore:",
                     FileDialog.LOAD);
			fileChooser.setFile(fileFilter);
			fileChooser.setVisible(true);
			final String filename = fileChooser.getDirectory()+fileChooser.getFile();
			System.out.println("File: "+filename);
			if (fileChooser.getFile() != null && JOptionPane.showConfirmDialog(this,
					"Are you sure? " +
					"This will destroy all data in your database and restore it from the file:\n"+filename) ==
						JOptionPane.YES_OPTION) {
				final ProgressBarWrapper progressBar = 
					new ProgressBarWrapper(null, "Exporting Database",100);
				progressBar.setIndeterminate(true);
				progressBar.constructThis();
				SwingWorker worker = new SwingWorker(){
					public Object construct(){
						try {
							System.out.println("importing database");
							((SQLServerDatabase)db).importDatabase(filename,fileType);
							System.out.println("imported database");
						} catch (FileNotFoundException e1) {
							SwingUtilities.invokeLater(new Runnable(){
								public void run(){
									JOptionPane.showMessageDialog(null,"The file: "+ filename+" could not be created.");
								}
							});

						}
						return null;
					}
					public void finished() {
						progressBar.disposeThis();
						JOptionPane.showMessageDialog(null,
								"Database successfully restored.");
						db.closeConnection();
						//setVisible(false);
						//dispose();		
					}
				};
				worker.start();
				
			}			
		} else if (source == exitButton){
			db.closeConnection();
			System.exit(0);
		}
		
		
	}
	
	public static void main(String[] args){
		new DBUnitDialog();
	}

}
