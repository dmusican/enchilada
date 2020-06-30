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
 * Created Jan 8, 2007
 */
package edu.carleton.enchilada.gui;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;

import edu.carleton.enchilada.database.Database;

import java.awt.*;
import java.awt.event.*;

import javax.swing.*;
import javax.swing.event.*;
import edu.carleton.enchilada.externalswing.SwingWorker;

/**
 * A GUI that allows the user to use the fast database backup/restore commands
 * to save/restore the entire contents of the database to a file (or, potentially,
 * other backup devices like tape drives or network locations)
 * 
 * @author shaferia
 */
public class BackupDialog extends JDialog {
	//The list of backup locations
	JList list;
	DefaultListModel model;
	
	//buttons enabled by selecting a backup location
	JButton removeButton;
	JButton backupButton;
	JButton restoreButton;
	
	JFrame owner;
	Database db;
	
	/**
	 * Attempt to only accept .dat files for backup
	 */
	FilenameFilter filter = new FilenameFilter() {
		public boolean accept(File dir, String name) {
			File f = new File(name);
			return f.isDirectory() || name.endsWith(".dat");
		}
	};
	
	/**
	 * Refresh the DefaultListModel that drives the backup locations list, 
	 * which should update the JList accordingly.
	 */
	public void refreshLocations() {
		model.removeAllElements();
		ArrayList<HashMap<String,String>> locations = ((Database)db).getBackupLocations();
		for (HashMap<String,String> loc : locations) {
			model.addElement(loc);
		}
		if (removeButton != null)
			setSelectedButtonsEnabled(false);
	}
	
	private void setSelectedButtonsEnabled(boolean enabled) {
		removeButton.setEnabled(enabled);
		backupButton.setEnabled(enabled);
		restoreButton.setEnabled(enabled);
	}
	
	/**
	 * Create a backup dialog with the given parent and database on which to
	 * perform queries and backups
	 * @param owner the parent frame, 
	 *		will have its data refreshed when a backup occurs if a MainFrame
	 * @param db the database to backup or restore
	 */
	public BackupDialog(MainFrame owner, Database db) {
		super(owner, "Backup Database", true);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		this.owner = owner;
		this.db = db;
		
		Container cont = getContentPane();
		cont.setLayout(new BorderLayout());
		
		//make central panel
		JPanel centerPane = new JPanel();
		centerPane.setLayout(new BorderLayout());
		centerPane.setBorder(BorderFactory.createTitledBorder("Select a Backup Location"));
		
		model = new DefaultListModel();
		refreshLocations();
		
		//create the list of backup locations
		list = new JList(model);
		list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		list.setLayoutOrientation(JList.VERTICAL);
		list.setCellRenderer(new DeviceCellRenderer());
		list.addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				setSelectedButtonsEnabled(true);
			}
		});
		JScrollPane listScroll = new JScrollPane(
				list, 
				JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, 
				JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
		listScroll.setPreferredSize(new Dimension(300,150));
		centerPane.add(listScroll, BorderLayout.CENTER);
		
		JPanel actions = new JPanel();
		
		//buttons below listview
		JButton addButton = new JButton("Add...");
		addButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addBackupLocation();
			}			
		});
		removeButton = new JButton("Delete");
		removeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				HashMap<String, String> cval = (HashMap<String, String>) list.getSelectedValue();
				removeBackupLocation(cval.get("name"), cval.get("size"));
			}			
		});
		removeButton.setEnabled(false);	
		backupButton = new JButton("Backup to Selected");
		backupButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				HashMap<String, String> cval = (HashMap<String, String>) list.getSelectedValue();
				execBackup(cval.get("name"), cval.get("size"));
			}			
		});
		backupButton.setEnabled(false);
		restoreButton = new JButton("Restore from Selected");
		restoreButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				HashMap<String, String> cval = (HashMap<String, String>) list.getSelectedValue();
				execRestore(cval.get("name"), cval.get("size"));
			}			
		});
		restoreButton.setEnabled(false);

		actions.add(addButton);
		actions.add(removeButton);
		actions.add(backupButton);
		actions.add(restoreButton);
		centerPane.add(actions, BorderLayout.SOUTH);

		cont.add(centerPane, BorderLayout.CENTER);
		
		//make bottom panel
		JPanel bottomPane = new JPanel();
		bottomPane.setBorder(BorderFactory.createEmptyBorder(10,10,10,10));
		bottomPane.setLayout(new BoxLayout(bottomPane, BoxLayout.X_AXIS));
		bottomPane.add(Box.createHorizontalGlue());
		JButton okButton = new JButton("OK");
		okButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				dispose();
			}
		});
		okButton.setMnemonic(KeyEvent.VK_O);
		bottomPane.add(okButton);
		cont.add(bottomPane, BorderLayout.SOUTH);
		
		pack();
		setMinimumSize(getSize());
		setVisible(true);
	}
	
	/**
	 * Determines whether the given name is acceptable for a backup location name
	 * @param name the name to test
	 * @return true if acceptable
	 */
	private boolean nameOK(String name) {
		boolean ok = true;
		String notok = " .,<>?/\\-_+=!@#$%^&*()\'\"\t\n";
		StringBuffer message = new StringBuffer(
				"Please choose a different name - " + 
				"the specified name contains the following illegal characters: ");
		
		for (int i = 0; i < notok.length(); ++i) {
			if (name.indexOf(notok.charAt(i)) > -1) {
				ok = false;
				message.append(notok.charAt(i) + " ");
			}
		}
		
		if (!ok)
			JOptionPane.showMessageDialog(
					this, 
					message.toString(), 
					"Invalid name", 
					JOptionPane.WARNING_MESSAGE);
		
		return ok;
	}
	
	/**
	 * Give the user a prompt for file location of a backup and the associated backup name
	 * Show an error dialog if the backup location was not created properly.
	 */
	private void addBackupLocation() {
		FileDialog dialog = new FileDialog(this, "Select a location...", FileDialog.LOAD);
		dialog.setFilenameFilter(filter);
		dialog.setVisible(true);
		
		if (dialog.getFile() == null)
			return;
		
		String dispfile = dialog.getFile();
		String path = dialog.getDirectory() + dialog.getFile();
		String name = null;
		
		//don't offer to name a location with a .
		if (dispfile.indexOf(".") > -1)
			dispfile = dispfile.substring(0, dispfile.indexOf("."));
		
		do {
			name = (String) JOptionPane.showInputDialog(
				this,
				"Enter a name for the new backup:",
				"Backup name",
				JOptionPane.QUESTION_MESSAGE,
				null,
				null,
				dispfile);
			
			if (name == null)
				return;
		} 
		while (!nameOK(name));
		
		
		boolean success = ((Database)db).addBackupFile(name, path);
		
		if (success) {
			refreshLocations();
		}
		else {
			JOptionPane.showMessageDialog(
					this,
					"Error adding backup location", 
					"Error", 
					JOptionPane.ERROR_MESSAGE);
		}		
	}

	/**
	 * Removes the selected backup location (as given by name), prompts for confirmation if size is nonzero 
	 * Show an error dialog if the backup location was not removed properly.
	 * @param name the name of the backup location
	 * @param size the size of the location, or "Empty" if not yet written to
	 */
	private void removeBackupLocation(String name, String size) {
		boolean delfile = false;
		
		if (!size.equals("Empty")) {
			String[] options = {"Delete Location", "Delete Location and File", "Cancel"};
			int sel = JOptionPane.showOptionDialog(
					this, 
					"The backup location " + name + " is not empty - would you like to delete the file?", 
					"Confirm deletion", 
					JOptionPane.DEFAULT_OPTION, 
					JOptionPane.QUESTION_MESSAGE, 
					null, 
					options, 
					2);
			
			switch (sel) {
				case 0:
					//simply proceed
					delfile = false;
					break;
				case 1:
					//remove the file
					delfile = true;
					break;
				case 2:
					return;
			}
		}
		
		boolean success = ((Database)db).removeBackupFile(name, delfile);
		
		if (success) {
			refreshLocations();
		}
		else {
			JOptionPane.showMessageDialog(
					this,
					"Error removing backup location", 
					"Error", 
					JOptionPane.ERROR_MESSAGE);
		}			
	}
	
	/**
	 * Backup the database currently in db to the backup location specified by name
	 * Prompt if an overwrite would occur
	 * @param name the name of the backup location
	 * @param size the size of the backup file - Empty if none
	 */
	private void execBackup(String name, String size) {
		if (!size.equals("Empty")) {
			int sel = JOptionPane.showConfirmDialog(
					this, 
					"Backing up to " + name + " will overwrite its previous contents - continue?", 
					"Confirm overwrite", 
					JOptionPane.YES_NO_OPTION);
			
			if (sel == JOptionPane.NO_OPTION)
				return;
		}
		
		String message = null;
		final BRProgress pbar = new BRProgress(this, "Backup", "Executing backup...");
		final String target = name;
		
		SwingWorker worker = new SwingWorker() {
			public Object construct() {
				return ((Database)db).backupDatabase(target);
			}
			public void finished() {
				String val = (String) getValue();
				if (val.indexOf("successfully") > -1) {
					pbar.setDone("Backup Successful", val);
					refreshLocations();
				}
				else
					pbar.setDone("Backup Failed", val);
			}
		};
		
		worker.start();
	}
	
	/**
	 * Restore to db the database from the backup location specified by name
	 * Prompt to confirm overwrite of current database.
	 * Prompt with error if the selected backup has no size (is empty)
	 * @param name the name of the backup location
	 * @param size the size of the backup file - Empty if none
	 */
	private void execRestore(String name, String size) {
		if (size.equals("Empty")) {
			JOptionPane.showMessageDialog(
					this, 
					"Cannot restore from an empty backup file", 
					"Backup empty", 
					JOptionPane.WARNING_MESSAGE);
			return;
		}
		
		int sel = JOptionPane.showConfirmDialog(
				this, 
				"Restoring from disk will delete all data currently in the database - continue?", 
				"Confirm restore", 
				JOptionPane.YES_NO_OPTION);
		
		if (sel == JOptionPane.NO_OPTION)
			return;
		
		String message = null;
		final BRProgress pbar = new BRProgress(this, "Restore", "Executing restore...");
		final String target = name;
		
		SwingWorker worker = new SwingWorker() {
			public Object construct() {
				return ((Database)db).restoreDatabase(target);
			}
			public void finished() {
				String val = (String) getValue();
				if (val.indexOf("successfully") > -1) {
					//refresh the main window
					if (owner instanceof MainFrame)
						((MainFrame)owner).refreshData();
					
					pbar.setDone("Restore Successful", val);
					refreshLocations();
				}
				else
					pbar.setDone("Restore Failed", val);
			}
		};
		
		worker.start();		
	}
	
	/**
	 * ProgressBarWrapper doesn't work for JDialogs, sigh.
	 * @author shaferia
	 */
	class BRProgress extends JDialog {
		JLabel majorDoneText;
		JTextArea doneText;
		CardLayout layout = new CardLayout();
		
		public BRProgress(Dialog parent, String title, String action) {
			super(parent, title, true);
			setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
			setLayout(layout);
			Container cont = getContentPane();
			
			//panel displayed when backup/restore is in process
			JPanel working = new JPanel(new BorderLayout());
			working.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			JProgressBar prog = new JProgressBar(JProgressBar.HORIZONTAL);
			prog.setPreferredSize(new Dimension(450, 40));
			prog.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			prog.setIndeterminate(true);
			working.add(prog, BorderLayout.NORTH);
			JLabel workinglabel = new JLabel(action);
			working.add(workinglabel, BorderLayout.CENTER);
			cont.add(working, "working");
			
			//panel displayed when operation is complete
			JPanel done = new JPanel(new BorderLayout());
			done.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			majorDoneText = new JLabel();
			majorDoneText.setFont(majorDoneText.getFont().deriveFont(Font.BOLD, 
					majorDoneText.getFont().getSize2D()*1.5f));
			doneText = new JTextArea();
			doneText.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			doneText.setEditable(false);
			doneText.setBackground(this.getBackground());
			done.add(majorDoneText, BorderLayout.NORTH);
			done.add(doneText, BorderLayout.CENTER);
			JButton okay = new JButton("OK");
			okay.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent arg0) {
					dispose();
				}
			});
			done.add(okay, BorderLayout.SOUTH);
			cont.add(done, "done");
			
			//this needs to be done for non-blocking display
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					pack();
					setVisible(true);
				}	
			});
		}
		
		/**
		 * Switch to the done panel of the progress bar display
		 * @param majortext	the header to display
		 * @param text the more detailed text to output
		 */
		public void setDone(String majortext, String text) {
			setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			majorDoneText.setText(majortext);
			doneText.setText(text);
			layout.show(getContentPane(), "done");
			pack();
		}
	}
	
	/**
	 * Draws the cells of the JList of backup devices
	 * @author shaferia
	 */
	class DeviceCellRenderer extends JPanel implements ListCellRenderer {
		JLabel label;
		JLabel pathLabel;
		JLabel iconLabel;
		JPanel central;
		
		Color background = UIManager.getDefaults().getColor("List.background");
		Color foreground = UIManager.getDefaults().getColor("List.foreground");
		Color selectionBackground = UIManager.getDefaults().getColor("List.selectionBackground");
		Color selectionForeground = UIManager.getDefaults().getColor("List.selectionForeground");
		Icon fileIcon = UIManager.getDefaults().getIcon("FileChooser.detailsViewIcon"); //FileView.hardDriveIcon
		
		public DeviceCellRenderer() {
			setLayout(new BorderLayout());
			
			central = new JPanel(new BorderLayout());
			central.setOpaque(false);
			
			label = new JLabel("");
			label.setFont(label.getFont().deriveFont(Font.BOLD, label.getFont().getSize2D()*1.3f));
			label.setOpaque(false);
			central.add(label, BorderLayout.CENTER);

			pathLabel = new JLabel("");
			pathLabel.setOpaque(false);
			central.add(pathLabel, BorderLayout.SOUTH);
			add(central, BorderLayout.CENTER);
			
			iconLabel = new JLabel(fileIcon);
			iconLabel.setOpaque(false);
			iconLabel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			add(iconLabel, BorderLayout.WEST);
			
			setOpaque(true);
		}

		public Component getListCellRendererComponent(JList list, Object val, int index, boolean selected, boolean focus) {
			HashMap<String, String> cval = (HashMap<String, String>) val;
			label.setText(cval.get("name"));
			pathLabel.setText(cval.get("path") + " - " + cval.get("size"));
			
			if (selected) {
				label.setForeground(selectionForeground);
				pathLabel.setForeground(selectionForeground);
				setBackground(selectionBackground);
			} else {
				label.setForeground(foreground);
				pathLabel.setForeground(foreground);
				setBackground(background);
			}
			
			return this;
		}
	}
}
