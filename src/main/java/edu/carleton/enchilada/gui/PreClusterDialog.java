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


/*
 * Created on Jul 19, 2004
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.DynamicTable;

import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.clustering.*;

import java.awt.*;
import java.util.ArrayList;

/**
 * 
 * The PreClusterDialog opens a JDialog Object where the user can choose
 * a clustering algorithm to perform on the selected collection before the
 * system clusters the collection hierarchically.
 * 
 * @version 1.0 August 23, 2009 Kobe Bryant's 31st birthday
 * @author jtbigwoo
 */
public class PreClusterDialog extends AbstractClusterDialog 
{

	protected String[] clusterNames = {ART2A, KCLUSTER};

	/**
	 * Constructor.  Creates and shows the dialogue box.
	 * 
	 * @param frame - the parent JDialog of the JDialog object.
	 */
	public PreClusterDialog(JFrame frame, JDialog parent, CollectionTree cTree,
                            Database db) {
		super(frame,"Cluster",true);
		this.parent = frame;
		this.cTree = cTree;
		this.db = db;
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JPanel rootPanel = new JPanel();
		
		JPanel clusterAlgorithms = setClusteringAlgorithms(clusterNames, false);
		JPanel clusterSpecs = setClusteringSpecifications();
		
		//Create common info panel:
		JPanel commonInfo = setCommonInfo();
		getRootPane().setDefaultButton(okButton);
		
		//Changed to "Information" - benzaids
		clusterSpecs.setBorder(getSectionBorder("Information"));
		rootPanel.add(clusterSpecs);
		
		//Changed from step 2 to step 1 - benzaids
		clusterAlgorithms.setBorder(getSectionBorder("1. Choose Appropriate Clustering Algorithm"));
		rootPanel.add(clusterAlgorithms);

		//Changed from step 3 to step 2 - benzaids
		commonInfo.setBorder(getSectionBorder("2. Begin Clustering"));
		rootPanel.add(commonInfo);
		rootPanel.setLayout(new BoxLayout(rootPanel, BoxLayout.PAGE_AXIS));
		
		add(rootPanel, BorderLayout.CENTER);
		
		setLocation(20,20);
		//rootPanel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
		pack();
		
		//Display the dialogue box.
		setVisible(true);
		
	}

	public void doOKButtonAction(DistanceMetric dMetInt, ClusterK.CentroidsApproach initialCentroidsInt) {
		// TODO: error check here to make sure something is selected.
		// TODO: make this more graceful.
		// Get clustering specifications and create ClusterInformation object.
		
		//We now only use sparse - benzaids
		//String infoType = (String)infoTypeDropdown.getSelectedItem();
		String infoType = sparse;
		int dividedParticleCollectionId = 0;
		
		ArrayList<String> list = new ArrayList<String>();
		String key = null, weight = null;
		boolean auto = false;
		boolean norm = normalizer.isSelected();
		String denseTableName = db.getDynamicTableName(DynamicTable.AtomInfoDense, 
				cTree.getSelectedCollection().getDatatype());
		String sparseTableName = db.getDynamicTableName(DynamicTable.AtomInfoSparse, 
				cTree.getSelectedCollection().getDatatype());
		
		list.add(sparseTableName + ".PeakArea");//We only use PeakArea now (i = 1 in loop above) - benzaids
		key = sparseKey.get(0);//added this line outside of if statement - benzaids
		ClusterInformation cInfo = new ClusterInformation(list, key, weight, auto, norm);
		
		// Call the appropriate algorithm.
		if (currentShowing == ART2A)
		{
			try {
				System.out.println("Collection ID: " +
						cTree.getSelectedCollection().
						getCollectionID());
				
				// Get information from the dialogue box:
				float vig = Float.parseFloat(vigText.getText());
				float learn = Float.parseFloat(learnText.getText());
				int passes = Integer.parseInt(passesText.getText());
				
				// Check to make sure that these are valid params:
				if (vig < 0 || vig > 2 || learn < 0 || 
						learn > 1	|| passes <= 0) {
					JOptionPane.showMessageDialog(parent,
							"Error with parameters.\n" +
							"Appropriate values are:\n" +
							"0 <= vigilance <= 2\n" +
							"0 <= learning rate <= 1\n" +
							"number of passes > 0",
							"Number Format Exception",
							JOptionPane.ERROR_MESSAGE);
				}
				else {
					Art2A art2a = new Art2A(
							cTree.getSelectedCollection().
							getCollectionID(),db, 
							vig, learn, passes, dMetInt, 
							commentField.getText(), cInfo);
					
					art2a.setDistanceMetric(dMetInt);
					//TODO:  When should we use disk based and memory based 
					// cursors?
					if (db.getCollectionSize(
							cTree.getSelectedCollection().
							getCollectionID()) < 10000)
					{
						art2a.setCursorType(Cluster.STORE_ON_FIRST_PASS);
					}
					else
					{
						art2a.setCursorType(Cluster.DISK_BASED);
					}
					art2a.setCreateCentroids(false);
					dividedParticleCollectionId = art2a.divide();
				}
				
			} catch (NullPointerException exception) {
				exception.printStackTrace();
				JOptionPane.showMessageDialog(parent,
						"Make sure you have selected a collection.",
						"Null Pointer Exception",
						JOptionPane.ERROR_MESSAGE);
			}
			catch (NumberFormatException exception) {
				exception.printStackTrace();
				JOptionPane.showMessageDialog(parent,
						"Make sure you have entered parameters.",
						"Number Format Exception",
						JOptionPane.ERROR_MESSAGE);
			}
		}
		if (currentShowing == KCLUSTER)
		{
			
			try {
				System.out.println("Collection ID: " +
						cTree.getSelectedCollection().
						getCollectionID());
				
				// Get information from dialogue box:
				int k = Integer.parseInt(kClusterText.getText());
				
				// Check to make sure it's valid:
				if (k <= 0) {
					JOptionPane.showMessageDialog(parent,
							"Error with parameters.\n" +
							"Appropriate values are: k > 0",
							"Number Format Exception",
							JOptionPane.ERROR_MESSAGE);
				}
				else {
					if (dMetInt == DistanceMetric.CITY_BLOCK) {
						KMedians kMedians = new KMedians(
								cTree.getSelectedCollection().
								getCollectionID(),db, k, "", 
								commentField.getText(), 
								initialCentroidsInt, 
								cInfo);
						kMedians.addInfo(cInfo);
						kMedians.setDistanceMetric(dMetInt);
						if (db.getCollectionSize(
								cTree.getSelectedCollection().
								getCollectionID()) < 0) // WAS 10000
						{
							kMedians.setCursorType(Cluster.STORE_ON_FIRST_PASS);
						}
						else
						{
							kMedians.setCursorType(Cluster.DISK_BASED);
						}
						kMedians.setCreateCentroids(false);
						dividedParticleCollectionId = kMedians.divide();
					}
					else if (dMetInt == DistanceMetric.EUCLIDEAN_SQUARED ||
					         dMetInt == DistanceMetric.DOT_PRODUCT) {
						KMeans kMeans = new KMeans(
								cTree.getSelectedCollection().
								getCollectionID(),db, 
								Integer.parseInt(kClusterText.getText()), 
								"", commentField.getText(), 
								initialCentroidsInt, cInfo);
						kMeans.addInfo(cInfo);
						kMeans.setDistanceMetric(dMetInt);
						//TODO:  When should we use disk based and memory based 
						// cursors?
						if (db.getCollectionSize(
								cTree.getSelectedCollection().
								getCollectionID()) < 10000) // Was 10000
						{
							kMeans.setCursorType(Cluster.STORE_ON_FIRST_PASS);
						}
						else
						{
							kMeans.setCursorType(Cluster.DISK_BASED);
						}
						kMeans.setCreateCentroids(false);

						dividedParticleCollectionId = kMeans.divide();
					}
				}
			} catch (NullPointerException exception) {
				exception.printStackTrace();
				JOptionPane.showMessageDialog(parent,
						"Make sure you have selected a collection.",
						"Null Pointer Exception",
						JOptionPane.ERROR_MESSAGE);
			}
			catch (NumberFormatException exception) {
				exception.printStackTrace();
				JOptionPane.showMessageDialog(parent,
						"Make sure you have entered parameters.",
						"Number Format Exception",
						JOptionPane.ERROR_MESSAGE);
			}
		
		}
		
		if (dividedParticleCollectionId != 0) {
			ClusterHierarchical hCluster = new ClusterHierarchical(
					dividedParticleCollectionId,db, 
					"", commentField.getText(), cInfo, parent);
			hCluster.addInfo(cInfo);
			hCluster.setDistanceMetric(dMetInt);
			//TODO:  When should we use disk based and memory based 
			// cursors?
			hCluster.setPreClustered();
			if (db.getCollectionSize(
					cTree.getSelectedCollection().
					getCollectionID()) < 10000) // Was 10000
			{
				hCluster.setCursorType(Cluster.STORE_ON_FIRST_PASS);
			}
			else
			{
				hCluster.setCursorType(Cluster.DISK_BASED);
			}
			hCluster.divide();
		}
		else {
			JOptionPane.showMessageDialog(parent,
					"Pre-Clustering Failed.",
					"Exception",
					JOptionPane.ERROR_MESSAGE);
		}
		dispose();
		cTree.updateTree();
	}

}
