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
 * Created on Aug 1, 2004
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;

import edu.carleton.enchilada.database.Database;

import java.awt.event.*;
import java.util.ArrayList;

/**
 * @author ritza
 */
public class EmptyCollectionDialog extends JDialog implements ActionListener 
{
	private JButton okButton;
	private JButton cancelButton;
	private JTextField nameField;
	private JTextField commentField;
	private JComboBox datatypeBox;
	private int collectionID = -1;
	private String collectionName = "";
	private Database db;
	
	public EmptyCollectionDialog(JFrame parent, Database db) {
		this(parent, "", true, db);
	}
	
	public EmptyCollectionDialog (JFrame parent, String datatype,
                                  boolean datatypeEditable, Database db)
	{
		super (parent,"Empty Collection", true);
		this.db = db;
		setSize(400,200);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel nameLabel = new JLabel("Name: ");
		nameField = new JTextField(25);
		
		JLabel commentLabel = new JLabel("Comment: ");
		commentField = new JTextField(25);
		
		JLabel datatypeLabel = new JLabel("Datatype: ");
		
		if (datatypeEditable) {
			ArrayList<String> datatypeNames = MainFrame.db.getKnownDatatypes();
			String[] nameArray = new String[datatypeNames.size() + 1];
			nameArray[0] = "Choose One:  ";
			for (int i = 1; i <= datatypeNames.size(); i++)
				nameArray[i] = datatypeNames.get(i-1);
			datatypeBox = new JComboBox(nameArray);
			datatypeBox.setSelectedIndex(0);
		} else {
			datatypeBox = new JComboBox(new String[] {datatype});
			datatypeBox.setSelectedIndex(0);
			datatypeBox.setEditable(false);
		}
		
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
		
	    mainPanel.add(nameLabel);
	    mainPanel.add(nameField);
	    mainPanel.add(commentLabel);
	    mainPanel.add(commentField);
	    mainPanel.add(datatypeLabel);
	    mainPanel.add(datatypeBox);
	    mainPanel.add(buttonPanel);

	    
		layout.putConstraint(SpringLayout.WEST, nameLabel,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, nameLabel,
                15, SpringLayout.NORTH, mainPanel);
		layout.putConstraint(SpringLayout.WEST, nameField,
                80, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, nameField,
                10, SpringLayout.NORTH, mainPanel);
		layout.putConstraint(SpringLayout.WEST, commentLabel,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, commentLabel,
                15, SpringLayout.SOUTH, nameField);
		layout.putConstraint(SpringLayout.WEST, commentField,
                80, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, commentField,
                10, SpringLayout.SOUTH, nameField);
		layout.putConstraint(SpringLayout.WEST, datatypeLabel,
                10, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, datatypeLabel,
               20, SpringLayout.SOUTH, commentField);
		layout.putConstraint(SpringLayout.WEST, datatypeBox,
                80, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, datatypeBox,
                15, SpringLayout.SOUTH, commentField);
		layout.putConstraint(SpringLayout.WEST, buttonPanel,
                120, SpringLayout.WEST, mainPanel);
		layout.putConstraint(SpringLayout.NORTH, buttonPanel,
                20, SpringLayout.SOUTH, datatypeLabel);
		
		add(mainPanel);
		
		setVisible(true);	
	}
	
	/**
	 * Accessor method.
	 * 
	 * @return	collectionID for new empty collection
	 */
	public int getCollectionID() {
		return collectionID;
	}
	
	/**
	 * @return the name of the collection inputted into the dialog
	 */
	public String getCollectionName() {
		return collectionName;
	}
	
	/**
	 * Removes an empty collection
	 * @param id the id of the empty collection to remove
	 * @return true on success
	 * @author shaferia
	 */
	public static boolean removeEmptyCollection(Database db, int id) {
		boolean success = db.removeEmptyCollection(db.getCollection(id));
		return success;
	}
	
	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		if (source == okButton) {
			if(!nameField.getText().equals("")) {
				if(!((String)datatypeBox.getSelectedItem()).equals("Choose One:  ")) {
					collectionID = db.createEmptyCollection((String)datatypeBox.getSelectedItem(), 0,nameField.getText(),commentField.getText(),"");
					collectionName = nameField.getText();
					System.out.println("Empty Collection ID: " + collectionID);
					dispose();
				}
				//If they didn't enter a data type, force them to choose one, this prevents
				//errors later on when pasting collections into the created collection
				else
					JOptionPane.showMessageDialog(this, "Please select a data type.");
			}
			//If they didn't enter a name, force them to type one, this prevents errors
			//later on because you can't select collections that don't have names
			else
				JOptionPane.showMessageDialog(this, "Please enter a name for the collection.");
		}			
		else  
			dispose();
	}

}
