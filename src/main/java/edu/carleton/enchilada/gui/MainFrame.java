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
 * The Original Code is EDAM Enchilada's MainFrame class.
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
 * Jonathan Sulman sulmanj@carleton.edu
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
 * Created on Jul 16, 2004
 *
 */
package edu.carleton.enchilada.gui;

import javax.swing.*;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;

import edu.carleton.enchilada.ATOFMS.Peak;

import edu.carleton.enchilada.collection.*;

import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;
import java.util.Vector;

import edu.carleton.enchilada.chartlib.hist.HistogramsWindow;
import edu.carleton.enchilada.chartlib.tree.TreeViewWindow;
import edu.carleton.enchilada.dataImporters.ATOFMSDataSetImporter;
import edu.carleton.enchilada.dataImporters.FlatFileATOFMSDataSetImporter;
import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.database.VersionChecker;
import edu.carleton.enchilada.database.DynamicTable;
import edu.carleton.enchilada.errorframework.DisplayException;
import edu.carleton.enchilada.errorframework.ErrorLogger;
import edu.carleton.enchilada.errorframework.ExceptionAdapter;
import edu.carleton.enchilada.externalswing.SwingWorker;

/**
 * @author ritza, jtbigwoo
 * TODO:  In all files, check to make sure that when exceptions are
 * thrown/caught etc, that the application makes it back to a 
 * workable state.
 */
public class MainFrame extends JFrame implements ActionListener
{
	public static final int DESCRIPTION = 3;
	private JToolBar buttonPanel;
	private JSplitPane mainSplitPane;
	
//	private JButton importEnchiladaDataButton;
	private JButton importAMSDataButton;
	private JButton importParsButton;
	private JButton importFlatButton;
	private JButton exportParsButton;
	private JButton emptyCollButton;
	private JButton analyzeParticleButton;
	private JButton aggregateButton;
	private JButton mapValuesButton;
//	private JButton clusterDialogButton;
	private JMenu analysisMenu;
//	private JMenuItem loadEnchiladaDataItem;
	private JMenuItem loadAMSDataItem;
	private JMenuItem loadATOFMSItem;
	private JMenuItem loadSPASSItem;
	private JMenuItem loadSPLATItem;
	private JMenuItem loadPKLItem;
	private JMenuItem loadPALMSItem;
	private JMenuItem txtLoadATOFMSItem;
	private JMenuItem batchLoadATOFMSItem;
	private JMenuItem MSAexportItem;
	private JMenuItem CSVexportItem;
	private JMenuItem HierarchyCSVexportItem;
	/*
	 * These capabilities work, but only with trivially small databases
	private JMenuItem importXmlDatabaseItem;
	private JMenuItem importXlsDatabaseItem;
	private JMenuItem importCsvDatabaseItem;
	private JMenuItem exportXmlDatabaseItem;
	private JMenuItem exportXlsDatabaseItem;
	private JMenuItem exportCsvDatabaseItem;
	*/
	private JMenuItem emptyCollection;
	private JMenuItem saveParticle;
	private JMenuItem queryItem;
	private JMenuItem clusterQueryItem;
	private JMenuItem clusterItem;
	private JMenuItem detectPlumesItem;
	private JMenuItem rebuildItem;
	private JMenuItem exitItem;
	private JMenuItem compactDBItem;
	private JMenuItem backupItem;
	private JMenuItem cutItem;
	private JMenuItem copyItem;
	private JMenuItem pasteItem;
	private JMenuItem deleteAdoptItem;
	private JMenuItem dataFormatItem;
	private JMenuItem recursiveDeleteItem;
	private JMenuItem renameItem;
	private CollectionTree collectionPane;
	private CollectionTree synchronizedPane;
	private JTextArea descriptionTA;
	private JComboBox chooseParticleSet;
	
	private CollectionTree selectedCollectionTree = null;
	
	private ArrayList<Integer> copyIDs = null;
	private ArrayList<Set <Integer>> childrenIDs;
	private ArrayList<String> copyCollectionNames;
	private boolean cutBool = false;
	
	private JTable particlesTable = null;
	private Vector<Vector<Object>> data = null;
	
	public static Database db;
	private JComponent infoPanel;
	
	private JTabbedPane collectionViewPanel;
	private JPanel particlePanel;
	private JScrollPane particleTablePane;
	private JScrollPane collInfoPane;
	private JMenuItem visualizeItem;
	private JMenuItem visualizeHierarchyItem;
	private JMenuItem outputItem;
	private JMenuItem aboutItem;
	private JMenuItem validationItem;
	private JMenuItem distogramItem;
	private JMenuItem sizeogramItem;
	
	private OutputWindow outputFrame;
	private JTextField searchFileBox;
	private JButton forwardButton;
	private JButton backwardButton;
	
	private int currCollectionSize;
	private int currHigh;
	private int currLow;
	private JLabel currentlyShowing;
	private JButton searchButton;
	private int currCollection;

	public static String dbname = "SpASMSdb";
	public static Path userDataLocation = Paths.get(System.getProperty("user.home"), "enchilada");

	/**
	 * Constructor.  Creates and shows the GUI.	 
	 */
	public MainFrame()
	{
		super("Enchilada");

		/* "If you are going to set the look and feel, you should do it as the 
		 * very first step in your application. Otherwise you run the risk of 
		 * initializing the Java look and feel regardless of what look and feel 
		 * you've requested. This can happen inadvertently when a static field 
		 * references a Swing class, which causes the look and feel to be 
		 * loaded. If no look and feel has yet been specified, the default Java 
		 * look and feel is loaded."
		 * From http://java.sun.com/docs/books/tutorial/uiswing/misc/plaf.html
		 */
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			//UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//HACK!!
		//Responsibility lies with:
		//@author shaferia
		//@see http://java.sun.com/j2se/1.4.2/docs/api/javax/swing/UIManager.html
		Font f = new Font("Dialog", Font.PLAIN, 11);
		fixFonts(f);
		
		setIconImage(Toolkit.getDefaultToolkit().getImage("new_icon.gif"));
		
		setSize(800, 600);
		
		setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		
		/* @author steinbel
		 * Set ErrorLogger testing boolean to "false" so error dialogs will show.
		 */
		ErrorLogger.testing = false;
		
		/**
		 * Create and add a menu bar to the frame.
		 */
		setupMenuBar();
		/**
		 * Create and add a button bar using JToolBar.
		 */
		setupButtonBar();
		/**
		 * Create the main panel consisting of a splitpane between the
		 * collections tree and the browsing tabs.
		 */	
		setupSplitPane();
		/**
		 * Use a SpringLayout to layout the components that have been added
		 */
		performLayout();
		
		//Display the window.
		setVisible(true);
		
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				exit();
				
			}
		});
		//Various hacks, the fault of:
		//@author shaferia
		// - fix Swing focus bug that causes problems with fast alt-tabbing
		// - apply Enchilada icon to all frames
		addWindowFocusListener(new WindowFocusListener() {
			public void windowLostFocus(WindowEvent event) {
				Window w = event.getOppositeWindow();
				if (w != null) {
					/* Below code commented out for nov2006 release to prevent
					 * output window from appearing on top of main window at 
					 * startup. TEMPORARY hack because this is what fixes the
					 * fast alt-tab bug.  - steinbel 11.8.06
					 */
					//if (event.getWindow() instanceof MainFrame)
						//w.requestFocus();
					/* end commenting by steinbel 11.8.06 */
					if (w instanceof Frame) {
						boolean found = false;
						for (WindowFocusListener listen : w.getWindowFocusListeners()) {
							if (this == listen) {
								found = true;
								break;
							}
						}
						if (!found) {
							if (event.getWindow() != null && event.getWindow() instanceof Frame)
								((Frame) w).setIconImage(((Frame)event.getWindow()).getIconImage());
							w.addWindowFocusListener(this);
						}
					}
				}
			}
			public void windowGainedFocus(WindowEvent e) {
			}
		});
	}
	
	private static void checkMemory() {
		// memory status message - Michael Murphy 2014
		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		long heapSize = memoryBean.getHeapMemoryUsage().getMax();
		int vmBits = Integer.parseInt(System.getProperty("sun.arch.data.model"));
		
		System.out.println("Max memory available to Enchilada is "+(heapSize/(1024*1024))+"MB");
		System.out.println("Java VM is running in "+vmBits+"-bit mode");
		if (vmBits == 32)
			System.out.println("It is recommended to use Enchilada with a 64-bit Java installation");
		System.out.println();
	}
	
	/**
	 * Call when a complete change is performed to the database contents
	 * (upon database rebuild or restore)
	 * @author shaferia
	 */
	public void refreshData() {
		remove(mainSplitPane);
		
		setupSplitPane();
		add(mainSplitPane);
		
		performLayout();
		
		getContentPane().validate();
	}
	
	/**
	 * Layout the components on the frame with a SpringLayout
	 */
	private void performLayout() {
		/**
		 * The Spring Layout is a flexible layout that places every panel in the
		 * frame in relation to that panels surrounding it.
		 */
		SpringLayout layout = new SpringLayout();
		setLayout(layout);
		
		Container contentPane = getContentPane();
		layout.putConstraint(SpringLayout.NORTH, buttonPanel, 5, 
				SpringLayout.NORTH, contentPane);
		layout.putConstraint(SpringLayout.WEST, buttonPanel, 5,
				SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, mainSplitPane, 5,
				SpringLayout.SOUTH, buttonPanel);
		layout.putConstraint(SpringLayout.WEST, mainSplitPane, 5,
				SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.EAST, contentPane, 5,
				SpringLayout.EAST, buttonPanel);
		layout.putConstraint(SpringLayout.EAST, contentPane, 5,
				SpringLayout.EAST, mainSplitPane);
		layout.putConstraint(SpringLayout.SOUTH, contentPane, 5,
				SpringLayout.SOUTH, mainSplitPane);		
	}
	
	private void fixFonts(Font f) {
		UIDefaults defaults = UIManager.getDefaults();
		Object key = null;
        for (java.util.Enumeration<Object> keys = UIManager.getDefaults().keys(); 
        	keys.hasMoreElements();
        	key = keys.nextElement())
        {
        	if (key != null && key.toString().endsWith(".font")) {
           		defaults.put(key, f);      		
        	}
        }
	}
	
	/**
	 * Provides additional UI-specific builtin functionality to SwingWorker.
	 * The only additional necessary thing to do with this class is explicity call super.finished()
	 * 	when using finished()
	 * @author shaferia
	 */
	private abstract class UIWorker extends SwingWorker {
		protected Component[] disable;
		
		@Override
		public void start() {
			start(new Component[0]);
		}
		
		/**
		 * Starts the UIWorker, disabling the given components.
		 * The components will be enabled again when the UIWorker finishes.
		 * @param disable
		 */
		public void start(Component[] disable) {
			this.disable = disable;
			setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			for (Component c : disable)
				c.setEnabled(false);
			
			super.start();
		}
		
		@Override
		public void finished() {
			super.finished();
			for (Component c : disable)
				c.setEnabled(true);
			setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		
		if (source == aboutItem) {
			JOptionPane.showMessageDialog(this, "EDAM Enchilada\n" +
					"is supported by NSF ITR Grant IIS-0326328.\n" +
					"For support, please contact dmusican@carleton.edu.\n" +
					"Software Version jul-2014\n" +
					"Additions by Michael Murphy, University of Toronto"
//					+"Carleton Contributors:\n" +
//					"Anna Ritz, Ben Anderson, Leah Steinberg,\n" +
//					"Thomas Smith, Deborah Gross, Jamie Olson,\n" +
//					"Janara Christensen, David Musicant, Jon Sulman\n" +
//					"Sami Benzaid, Emma Turetsky, Jeff Rzeszotarski,\n +"
//					"Rob Atlas, Tom Bigwood\n" +
//					"Madison Contributors:\n"
					);
		}
		
		else if (source == outputItem) {
			if (outputFrame == null) {
				outputFrame = new OutputWindow(this);
				outputFrame.setSize(getSize().width / 2, getSize().height / 2);
				outputFrame.setVisible(true);		
			}
			else
				outputFrame.setVisible(true);
		}
		
//		if (source == importEnchiladaDataButton ||
//				source == loadEnchiladaDataItem) {
//			new ImportEnchiladaDataDialog(this, db);
//			collectionPane.updateTree();
//			validate();
//		}
//		
		if (source == batchLoadATOFMSItem) {
			ATOFMSBatchImportGUI abig = new ATOFMSBatchImportGUI(this);
			if (abig.init()) abig.go(collectionPane);
			// tree update and validate is being done in method go,
			// when 'finished' method runs --- DRM
		}
		if (source == txtLoadATOFMSItem){
			FileDialog fileChooser = new FileDialog(this, 
					"Locate a SPASS File:",
					FileDialog.LOAD);
			fileChooser.setFile("*.*");
			fileChooser.setVisible(true);
			final String filename = fileChooser.getDirectory()+fileChooser.getFile();
			if (fileChooser.getFile() == null) {
				return;
				// the user selected cancel, cancel the operation
			}
			
			
			final JFrame thisRef = this;
			final Database dbRef = (Database)db;
			//construct everything
			final ProgressBarWrapper progressBar = 
				new ProgressBarWrapper(this, ATOFMSDataSetImporter.title, 100);
			final FlatFileATOFMSDataSetImporter dsi = 
					new FlatFileATOFMSDataSetImporter(
							this, dbRef,progressBar);
			
			progressBar.constructThis();
			
			
			final SwingWorker worker = new SwingWorker(){
				public Object construct(){
						dbRef.beginTransaction();
						try{
							dsi.processFile(filename);
							dbRef.commitTransaction();
						} catch (InterruptedException e2){
							dbRef.rollbackTransaction();
						}catch (DisplayException e1) {
							ErrorLogger.displayException(progressBar,e1.toString());
							dbRef.rollbackTransaction();
						} 
						
					
					return null;
				}
				public void finished(){
					progressBar.disposeThis();
					collectionPane.updateTree();
					thisRef.validate();
					//progressBar.dispose();
				}
			};
			worker.start();
		}
		
		if (source == importParsButton || source == loadATOFMSItem) 
		{
			new ImportParsDialog(this);
			
			collectionPane.updateTree();
			validate();
		}
		
		if (source == importAMSDataButton || source == loadAMSDataItem) {
				new ImportAMSDataDialog(this, db);
				collectionPane.updateTree();
				validate();
		}
		
		if (source == loadSPASSItem) {
			new ImportSPASSDataDialog(this, db);
			collectionPane.updateTree();
			validate();
		}
		
		if (source == loadSPLATItem) {
			new ImportSPLATDataDialog(this, db);
			collectionPane.updateTree();
			validate();
		}
		
		if (source == loadPKLItem) {
			new ImportPKLDataDialog(this, db); // change to PKL
			collectionPane.updateTree();
			validate();
		}
		
		if (source == loadPALMSItem) {
			new ImportPALMSDataDialog(this, db);
			collectionPane.updateTree();
			validate();
		}
		
		else if (source == importFlatButton) {
			new FlatImportGUI(this, db);
			
			collectionPane.updateTree();
			validate();
		}
		
		else if (source == emptyCollButton || source == emptyCollection) {
			new EmptyCollectionDialog(this, db);
			collectionPane.updateTree();
			validate();
		}
		else if(source == saveParticle){
			int[] selectedRows = particlesTable.getSelectedRows();
			int curRow = selectedRows[0];
			if(selectedRows.length>0){
				int atomID= ((Integer)
						data.get(curRow).get(0)).intValue();
				
				FileDialogPicker dialog = new FileDialogPicker("Save as", ".txt", this, false);
				if(dialog.getFileName()!=null){
					System.out.println(dialog.getFileName());
					writeToFile(dialog.getFileName(), atomID);
				}
				
			}
		}
		else if (source == exportParsButton || source == MSAexportItem)
		{
			final Collection c = getSelectedCollection();
			if (c == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to export.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				return;
			}
			new ExportMSAnalyzeDialog(this, db, c);
		}
		else if (source == CSVexportItem)
		{
			final Collection c = getSelectedCollection();
			if (c == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to export.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				return;
			}
			new ExportCSVDialog(this, db, c, false);
		}
		else if (source == HierarchyCSVexportItem)
		{
			final Collection c = getSelectedCollection();
			if (c == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to export.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				return;
			}
			new ExportCSVDialog(this, db, c, true);
		}
		else if (source == deleteAdoptItem)
		{
			final Collection[] c = getSelectedCollections();
			if(c == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to delete.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				return;
			}
			
			UIWorker worker = new UIWorker() {
				public Object construct() {
					boolean updateRequired = false;
					for (int i = 0; i < c.length; ++i)
						updateRequired |= db.orphanAndAdopt(c[i]);

					return new Boolean(updateRequired);					
				}
				public void finished() {
					super.finished();
					if (get() != null && (Boolean)get()) {
						selectedCollectionTree.updateTree();
						clearTable();
						validate();
					}
				}
			};
			worker.start(new Component[]{selectedCollectionTree});
			
		}
		
		else if (source == recursiveDeleteItem)
		{
			final Collection[] c = getSelectedCollections();
			final CollectionTree collTree = selectedCollectionTree;
			if(c == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to delete.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				return;
			}
			
			UIWorker worker = new UIWorker() {
				public Object construct() {
					boolean updateRequired = false;
					for (int i = 0; i < c.length; ++i)
						updateRequired |= db.recursiveDelete(c[i]);

					return new Boolean(updateRequired);					
				}
				public void finished() {
					super.finished();
					if (get() != null && (Boolean)get()) {
						selectedCollectionTree.updateTree();
						clearTable();
						validate();
					}
				}
			};
			worker.start(new Component[]{selectedCollectionTree});
		}
		
		else if (source == renameItem)
		{
			final Collection[] c = getSelectedCollections();
			if(c==null)
			{
				JOptionPane.showMessageDialog(this, "Please select one collection to rename.",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
			}
			else if(c.length>1)
			{
				JOptionPane.showMessageDialog(this, "Please select one collection to rename.",
						"Too many collections selected", JOptionPane.WARNING_MESSAGE);
			}
			else
			{
				String newName=JOptionPane.showInputDialog(this,"Choose a new name for collection.","Rename Collection",JOptionPane.QUESTION_MESSAGE);
				db.renameCollection(c[0], newName);
				c[0].setName(newName);
				collectionPane.updateTree(); //Mostly unnecessary, but without it, there is a problem when the name of a collection is changes length.
			}
		}
		else if (source == copyItem || source == cutItem)
		{
			// @author jtbigwoo changed this to copy/cut multiple collections at once
			Collection[] copyCollections = getSelectedCollections();
			String dataType = null;
			copyIDs = new ArrayList<Integer>();
			childrenIDs = new ArrayList<Set <Integer>>();
			copyCollectionNames = new ArrayList<String>();
			if (copyCollections == null) {
				JOptionPane.showMessageDialog(this, "Please select a collection to " + (source == cutItem ? "cut" : "copy") + ".",
						"No collection selected", JOptionPane.WARNING_MESSAGE);
				copyIDs = null;
				return;
			}
			for (int i = 0; i < copyCollections.length; i++) {
				int copyID = copyCollections[i].getCollectionID();
				if (copyID == 0) { //don't allow copying/pasting of root
					JOptionPane.showMessageDialog(this, "Please select a collection to " + (source == cutItem ? "cut" : "copy") + ".",
							"No collection selected", JOptionPane.WARNING_MESSAGE);
					copyIDs = null;
					return;
				}
				else if (dataType != null && !dataType.equals(copyCollections[i].getDatatype())) {
					JOptionPane.showMessageDialog(this, "Cannot " + (source == cutItem ? "cut" : "copy") + " collections of different data types.",
							"Invalid collection", JOptionPane.WARNING_MESSAGE);
					copyIDs = null;
					return;
				}
				else {
					copyIDs.add(i, copyID);
					// be sure to get sub collections all the way down the tree bug 2078722 - jtbigwoo
					childrenIDs.add(i, copyCollections[i].getCollectionIDSubTree());
					copyCollectionNames.add(i, copyCollections[i].getName());
					cutBool = source == cutItem;
					dataType = copyCollections[i].getDatatype();
					pasteItem.setEnabled(true);
				}
			}
		}
		else if (source == pasteItem)
		{
			// @author jtbigwoo changed this to paste multiple collections at once
			// first, check that we have a copied collection(s)
			if (copyIDs == null || copyIDs.size() == 0) {
				JOptionPane.showMessageDialog(this, "Collections within the same folder must" +
						" be of the same data type", "Invalid collection", JOptionPane.WARNING_MESSAGE);
				return;
			}

			// second check that a collection is not being pasted into itself or its children
			Collection targetCollection = getSelectedCollection();
			if (copyIDs.contains(targetCollection.getCollectionID())) {
				JOptionPane.showMessageDialog(this, "Cannot copy/paste to the same " +
						"destination as the source.", "Invalid collection", JOptionPane.WARNING_MESSAGE);
				return;
			}
			for (int i = 0; i < childrenIDs.size(); i++) {
				Set<Integer> subCollectionIDs = childrenIDs.get(i);
				if (subCollectionIDs.contains(targetCollection.getCollectionID())) {
					JOptionPane.showMessageDialog(this, "Cannot paste "  +  copyCollectionNames.get(i) + ": " + " the destination is a subcollection of " + copyCollectionNames.get(i) + "." );
					return;
				}
			}
			//if no collection selected, paste into root - @author steinbel

			//check if the datatypes are the same
			if(getSelectedCollection().getCollectionID() == 0
					|| db.getCollection(copyIDs.get(0)).getDatatype().equals
										(getSelectedCollection().getDatatype())){
				UIWorker worker = new UIWorker() {
					public Object construct() {
						if (cutBool == false)
						{
							for (int copyID : copyIDs) {
								db.copyCollection(db.getCollection(copyID), 
									getSelectedCollection());
							}
						}
						else
						{
							for (int copyID : copyIDs) {
								db.moveCollection(db.getCollection(copyID), 
									getSelectedCollection());
							}
						}
						return null;
					}
					public void finished() {
						super.finished();
						collectionPane.updateTree();
						validate();
					}
				};
				worker.start(new Component[]{collectionPane});
			}
			else
				JOptionPane.showMessageDialog(this, "Collections within the same folder must" +
						" be of the same data type", "Invalid collection", JOptionPane.WARNING_MESSAGE);
		}
		else if (source == queryItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select a collection to query.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				new QueryDialog(this, collectionPane, db, getSelectedCollection());
		}
		
		else if (source == clusterQueryItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select a collection to cluster query.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else{
				new ClusterQueryDialog(this, collectionPane, db);
			
				collectionPane.updateTree();
				validate();
			}
		}
		
		else if (source == clusterItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select a collection to cluster.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				new ClusterDialog(this, collectionPane, db);
		}
		else if (source == validationItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select a cluster collection to validate.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				new ValidationDialog(this, collectionPane, db);
		}
		else if (source == distogramItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select one or more collections to analyze.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				try {
					new ClusterDistanceWindow(this, collectionPane, db);
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		}
		else if (source == sizeogramItem) {
			if (collectionPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select one or more collections to analyze.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				try {
					new SizeHistogramWindow(this, collectionPane, db);
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		}
		else if (source == visualizeItem) {
			if (getSelectedCollection().getCollectionID() == 0)
				JOptionPane.showMessageDialog(this, "Please select a collection to visualize.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
			try {
				(new HistogramsWindow(
					getSelectedCollection().getCollectionID())).setVisible(true);
			} catch (IllegalArgumentException exce) {
				JOptionPane.showMessageDialog(this, "Spectrum Histograms only" +
						" work on ATOFMS collections for now.");
			}
		}
		else if (source == visualizeHierarchyItem) {
			if (getSelectedCollection().getCollectionID() == 0)
				JOptionPane.showMessageDialog(this, "Please select a collection to visualize.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
			try {
				(new TreeViewWindow(db,
					getSelectedCollection().getCollectionID())).setVisible(true);
			} catch (IllegalArgumentException exce) {
				JOptionPane.showMessageDialog(this, "Tree View of Hierarchy only" +
						" work on ATOFMS collections for now.");
			}
		}
		else if (source == detectPlumesItem){
			if (synchronizedPane.getSelectedCollection() == null)
				JOptionPane.showMessageDialog(this, "Please select a collection which to detect plumes.", 
						"No collection selected.", JOptionPane.WARNING_MESSAGE);
			else
				new DetectPlumesDialog(this,synchronizedPane, db);
		}
		
		else if (source == rebuildItem) {
			if (JOptionPane.showConfirmDialog(this,
			"Are you sure? This will destroy all data in your database.") ==
				JOptionPane.YES_OPTION) {
				//db.rebuildDatabase();
				final JFrame thisref = this;
				final ProgressBarWrapper pbar = 
					new ProgressBarWrapper(thisref, "Rebuilding Database", 100);
				pbar.setIndeterminate(true);
				pbar.setText("Rebuilding Database...");
				
				UIWorker worker = new UIWorker() {
					public Object construct() {
						db.closeConnection();
						Database.getDatabase(dbname).rebuildDatabase(dbname);
						return true;
					}
					public void finished() {
						super.finished();
						if ((Boolean) get()) {
							//changed by shaferia, 1/8/06
							//no restart on database rebuild
							/*
							JOptionPane.showMessageDialog(thisref,
								"The program will now shut down to reset itself. " +
								"Start it up again to continue.");
							dispose();
							*/
							db.openConnection();
							refreshData();
							pbar.disposeThis();
						}
						else {
							pbar.disposeThis();
							JOptionPane.showMessageDialog(thisref,
								"Could not rebuild the database." +
								"  Close any other programs that may be accessing the database and try again.");
						}
					}
				};
				
				pbar.constructThis();
				worker.start();
			}			
		}
		else if (source == compactDBItem){
			final ProgressBarWrapper progressBar = 
				new ProgressBarWrapper(this, "Compacting Database",100);
			progressBar.constructThis();
			progressBar.setIndeterminate(true);
			progressBar.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
			UIWorker worker = new UIWorker() {
				public Object construct() {
					((Database)db).compactDatabase(progressBar);
					return null;
				}
				public void finished() {
					super.finished();
					progressBar.disposeThis();
				}
			};
			worker.start();
		}
		else if (source == backupItem) {
			new BackupDialog(this, db);
		}
		else if (source == exitItem) {
			exit();
		}
		else if(source == analyzeParticleButton) 
		{
			showAnalyzeParticleWindow();
		}
		else if (source == aggregateButton)
		{

			Collection[] selectedCollections = collectionPane.getSelectedCollections();
			if (selectedCollections != null && selectedCollections.length > 0) {
				AggregateWindow aw = new AggregateWindow(this, db, selectedCollections);
				aw.setVisible(true);
			}
		}
		else if (source == mapValuesButton) 
		{
			Collection selectedCollection = synchronizedPane.getSelectedCollection();
			if (selectedCollection != null) { 
				MapValuesWindow bw = new MapValuesWindow(this, db, selectedCollection);
				bw.setVisible(true);
			}
		}
		else if (source == dataFormatItem) {
			new DataFormatViewer(this);
		}
		/*
		 * @author steinbel
		 */
		else if (source == forwardButton) {
			currLow = currHigh + 1;
			if ( (currHigh + 1000) >= currCollectionSize ){
				currHigh = currCollectionSize;
				forwardButton.setEnabled(false);
			} else
				currHigh += 1000;
			backwardButton.setEnabled(true);
			setTable();
		}
		/*
		 * @author steinbel
		 */
		else if (source == backwardButton) {
			currHigh = currLow - 1;
			if ( (currLow - 1000) <= 1 ){
				currLow = 1;
				backwardButton.setEnabled(false);
			} else
				currLow -= 1000;

			forwardButton.setEnabled(true);
			setTable();	
		}
		/*
		 * @author steinbel
		 */
		else if (source == searchFileBox) {
			//see if filename is valid and set table accordingly
			String searchMe = searchFileBox.getText();
			if (!searchMe.equals(" Enter a filename to search for a particle."))
				searchOn(searchMe);
			else 
				ErrorLogger.displayException(this, "Please enter a filename.");
		}
		/*
		 * @author steinbel
		 * //commented out for fall 06 release - steinbel
		 */
/*		else if (source == searchButton) {
			//see if filename is valid and set table accordingly
			String searchMe = searchFileBox.getText();
			if (!searchMe.equals(" Enter a filename to search for a particle."))
				searchOn(searchMe);
			else 
				ErrorLogger.displayException(this, "Please enter a filename.");
		}
*/		
		ErrorLogger.flushLog(this);
	}
	
	/**
	 * @author steinbel
	 * Searches the db for the filename and sets the particle pane to show that
	 * filename and its surrounding 1000 particles if the particle is in the
	 * database.  (If atom is in a different collection, pops it open.)
	 * @param searchString - the file name desired (must include entire path)
	 */
	private void searchOn(String searchString) {
		
		//find out the atomid for future reference
		int atomID = db.getATOFMSAtomID(searchString);
		//if the atom actually exists
		if (atomID >= 0){
			//if already showing the correct section of the collection
			if ((currLow <= atomID) && (atomID <= currHigh)){
				//highlight particle
				int particleRow;
				if (atomID <= 1001)
					particleRow = atomID - 1;
				else
					particleRow = atomID - 1001;
				particlesTable.changeSelection(particleRow, 0, false, false);
				
			} else if (db.collectionContainsAtom(currCollection, atomID)){
				//switch to correct high and low to show particle
				
			} else { //switch to correct collection
				
				
			}
				
		} //else do nothing - error message is in the InfoWarehouse-level method
		
	}

	
//	private void showClusterDistanceWindow() {
//		int[] selectedRows = particlesTable.getSelectedRows();
//		
//		for (int row : selectedRows) {
//			Collection coll = collectionPane.getSelectedCollection();
//			ClusterDistanceWindow cw = 
//				new ClusterDistanceWindow(db, particlesTable, row, coll);
//			//set this as the owner
//			cw.setOwner(this);
//			cw.setVisible(true);
//		}
//	}

	public void exit(){
		if (db.isDirty() && JOptionPane.showConfirmDialog(null,
				"One or more Collections has been deleted. It is recommended to clean up the database" +
				" in order to remove broken particle records. Would you like to do this now?","Compact Database?",
				JOptionPane.YES_NO_OPTION) ==
					JOptionPane.YES_OPTION) {
			final ProgressBarWrapper progressBar = 
				new ProgressBarWrapper(this, "Compacting Database",100);
			progressBar.constructThis();
			progressBar.setIndeterminate(true);
			UIWorker worker = new UIWorker() {
				public Object construct() {
					((Database)db).compactDatabase(progressBar);
					db.clearCache();
					db.closeConnection();
					return null;
				}
				public void finished() {
					super.finished();
					progressBar.disposeThis();
					dispose();
					
					System.exit(0);
				}
			};
			worker.start();
			
		}else{
			
			db.clearCache();
			db.closeConnection();
			dispose();
			
			System.exit(0);
		}
	}
	
	public Collection getSelectedCollection() {
		Collection c = collectionPane.getSelectedCollection();
		if (c != null)
			return c;
		else{
			c = synchronizedPane.getSelectedCollection();
			if (c == null)	//if no collection is selected, return the root
				c = db.getCollection(0);
		}
		return c;
			
	}
	
	private Collection[] getSelectedCollections() {
		Collection[] c = collectionPane.getSelectedCollections();
		if (c != null)
			return c;
		else
			return synchronizedPane.getSelectedCollections();
	}
	
	private void showAnalyzeParticleWindow() {
		int[] selectedRows = particlesTable.getSelectedRows();
		
		for (int row : selectedRows) {
			Collection coll = collectionPane.getSelectedCollection();
			ParticleAnalyzeWindow pw = 
				new ParticleAnalyzeWindow(db, particlesTable, row, coll);
			//set this as the owner
			pw.setOwner(this);
			pw.setVisible(true);
		}
	}
	
	/**
	 * setupMenuBar() sets up  File, Edit, Analysis, Collection, 
	 * and Help menus.  All menu options have keyboard shortcuts 
	 * except for the "Delete Selected and All Children" item
	 * in the Collection menu.
	 * 
	 */
	private void setupMenuBar()
	{
		JMenuBar menuBar = new JMenuBar();
		
		// Add a file menu to the menu bar
		JMenu fileMenu = new JMenu("File");
		fileMenu.setMnemonic(KeyEvent.VK_F);
		menuBar.add(fileMenu);
		
		emptyCollection = new JMenuItem(
				"New empty collection", 
				KeyEvent.VK_N);
		emptyCollection.addActionListener(this);
		saveParticle = new JMenuItem("Save particle as");
		saveParticle.addActionListener(this);
		JMenu importCollectionMenu = new JMenu("Import Collection. . . ");
		loadATOFMSItem = new JMenuItem("from ATOFMS data. . .");
		loadATOFMSItem.addActionListener(this);
//		loadEnchiladaDataItem = new JMenuItem("from Enchilada data. . .");
//		loadEnchiladaDataItem.addActionListener(this);
		loadAMSDataItem = new JMenuItem("from AMS data. . .");
		loadAMSDataItem.addActionListener(this); 
		loadSPASSItem = new JMenuItem("from SPASS data. . .");
		loadSPASSItem.addActionListener(this); 
		loadSPLATItem = new JMenuItem("from SPLAT data. . .");
		loadSPLATItem.addActionListener(this); 
		loadPKLItem = new JMenuItem("from PKL data. . .");
		loadPKLItem.addActionListener(this); 
		loadPALMSItem = new JMenuItem("from PALMS data. . .");
		loadPALMSItem.addActionListener(this);
		batchLoadATOFMSItem = new JMenuItem("from ATOFMS data (with bulk file). . .");
		batchLoadATOFMSItem.addActionListener(this);
		txtLoadATOFMSItem = new JMenuItem("from txt data file . . .");
		txtLoadATOFMSItem.addActionListener(this);
		importCollectionMenu.setMnemonic(KeyEvent.VK_I);
		importCollectionMenu.add(loadATOFMSItem);
		importCollectionMenu.add(batchLoadATOFMSItem);
		importCollectionMenu.add(txtLoadATOFMSItem);
//		importCollectionMenu.add(loadEnchiladaDataItem);
		importCollectionMenu.add(loadAMSDataItem);
		importCollectionMenu.add(loadSPASSItem);
		importCollectionMenu.add(loadSPLATItem);
		importCollectionMenu.add(loadPKLItem);
		importCollectionMenu.add(loadPALMSItem);
		
		JMenu exportCollectionMenu = new JMenu("Export Collection. . .");
		MSAexportItem = new JMenuItem("to MS-Analyze. . .");
		MSAexportItem.addActionListener(this);
		CSVexportItem = new JMenuItem("to CSV File. . .");
		CSVexportItem.addActionListener(this);
		HierarchyCSVexportItem = new JMenuItem("to CSV File as Hierarchy of Average Particles. . .");
		HierarchyCSVexportItem.addActionListener(this);
		exportCollectionMenu.setMnemonic(KeyEvent.VK_E);
		exportCollectionMenu.add(MSAexportItem);
		exportCollectionMenu.add(CSVexportItem);
		exportCollectionMenu.add(HierarchyCSVexportItem);
		
		/*
		 * These capabilities work, but only with trivially small databases
		JMenu importDatabaseMenu = new JMenu("Restore Database. . . ");
		importXmlDatabaseItem = new JMenuItem("from XML. . .");
		importXmlDatabaseItem.addActionListener(this);
		importXlsDatabaseItem = new JMenuItem("from Xls. . .");
		importXlsDatabaseItem.addActionListener(this);
		importCsvDatabaseItem = new JMenuItem("from Csv. . .");
		importCsvDatabaseItem.addActionListener(this);
		importDatabaseMenu.add(importXmlDatabaseItem);
		importDatabaseMenu.add(importXlsDatabaseItem);
		importDatabaseMenu.add(importCsvDatabaseItem);
		
		JMenu exportDatabaseMenu = new JMenu("Export Database. . . ");
		exportXmlDatabaseItem = new JMenuItem("to XML. . .");
		exportXmlDatabaseItem.addActionListener(this);
		exportXlsDatabaseItem = new JMenuItem("to Xls. . .");
		exportXlsDatabaseItem.addActionListener(this);
		exportCsvDatabaseItem = new JMenuItem("to Csv. . .");
		exportCsvDatabaseItem.addActionListener(this);
		exportDatabaseMenu.add(exportXmlDatabaseItem);
		exportDatabaseMenu.add(exportXlsDatabaseItem);
		exportDatabaseMenu.add(exportCsvDatabaseItem);
		*/
		compactDBItem = new JMenuItem("Compact Database", KeyEvent.VK_C);
		compactDBItem.addActionListener(this);
		
		rebuildItem = new JMenuItem("Rebuild Database", KeyEvent.VK_R);
		rebuildItem.addActionListener(this);
		
		backupItem = new JMenuItem("Backup/Restore...", KeyEvent.VK_B);
		backupItem.addActionListener(this);
		
		exitItem = new JMenuItem("Exit", KeyEvent.VK_X);
		exitItem.addActionListener(this);
		
		fileMenu.add(saveParticle);
		fileMenu.addSeparator();
		fileMenu.add(emptyCollection);
		fileMenu.addSeparator();
		fileMenu.add(importCollectionMenu);
		fileMenu.add(exportCollectionMenu);
		fileMenu.addSeparator();
		/*
		 * These capabilities work, but only with trivially small databases
		fileMenu.add(importDatabaseMenu);
		fileMenu.add(exportDatabaseMenu);
		*/
		fileMenu.add(compactDBItem);
		fileMenu.add(rebuildItem);
		fileMenu.add(backupItem);
		fileMenu.addSeparator();
		fileMenu.add(exitItem);
		
		// Add an edit menu to the menu bar.
		JMenu editMenu = new JMenu("Edit");
		editMenu.setMnemonic(KeyEvent.VK_E);
		cutItem = new JMenuItem("Cut",KeyEvent.VK_T);
		cutItem.addActionListener(this);
		copyItem = new JMenuItem("Copy",KeyEvent.VK_C);
		copyItem.addActionListener(this);
		pasteItem = new JMenuItem("Paste",KeyEvent.VK_P);
		pasteItem.addActionListener(this);
		pasteItem.setEnabled(false);
		JMenuItem selectAllItem = new JMenuItem("Select All", KeyEvent.VK_A);
		
		menuBar.add(editMenu);
		editMenu.add(cutItem);
		editMenu.add(copyItem);
		editMenu.add(pasteItem);
		editMenu.addSeparator();
		editMenu.add(selectAllItem);
		
		// Add an analysis menu to the menu bar.
		analysisMenu = new JMenu("Analysis");
		analysisMenu.setMnemonic(KeyEvent.VK_A);
		menuBar.add(analysisMenu);
		
		clusterItem = new JMenuItem("Cluster. . .", KeyEvent.VK_C);
		clusterItem.addActionListener(this);
		clusterQueryItem = new JMenuItem("Cluster query. . .", KeyEvent.VK_C);
		clusterQueryItem.addActionListener(this);
//		JMenuItem labelItem = new JMenuItem("Label. . .", 
//				KeyEvent.VK_L);
//		JMenuItem classifyItem = new JMenuItem("Classify. . . ", 
//				KeyEvent.VK_F);
		queryItem = new JMenuItem("Query. . . ", KeyEvent.VK_Q);
		queryItem.addActionListener(this);
		visualizeItem = new JMenuItem("Visualize. . .", KeyEvent.VK_V);
		visualizeItem.addActionListener(this);
		visualizeHierarchyItem = new JMenuItem("Visualize Hierarchy. . .", KeyEvent.VK_H);
		visualizeHierarchyItem.addActionListener(this);
		detectPlumesItem = new JMenuItem("Detect Plumes. . .", KeyEvent.VK_W);
		detectPlumesItem.addActionListener(this);
		validationItem = new JMenuItem("Clustering indices. . . ");
		validationItem.addActionListener(this);
		distogramItem = new JMenuItem("Similarity histogram. . . ");
		distogramItem.addActionListener(this);
		sizeogramItem = new JMenuItem("Size histogram. . . ");
		sizeogramItem.addActionListener(this);
		
		analysisMenu.add(clusterItem);
		analysisMenu.add(clusterQueryItem);
//		analysisMenu.add(labelItem);
//		analysisMenu.add(classifyItem);
		analysisMenu.add(queryItem);
		analysisMenu.add(visualizeItem);
		analysisMenu.add(visualizeHierarchyItem);
		//analysisMenu.add(detectPlumesItem);
		analysisMenu.add(validationItem);
		analysisMenu.add(distogramItem);
		analysisMenu.add(sizeogramItem);
		
		//Add a collection menu to the menu bar.
		JMenu collectionMenu = new JMenu("Collection");
		collectionMenu.setMnemonic(KeyEvent.VK_C);
		menuBar.add(collectionMenu);
		
		deleteAdoptItem = 
			new JMenuItem("Delete Selected and Adopt Children", 
					KeyEvent.VK_D);
		deleteAdoptItem.addActionListener(this);
		recursiveDeleteItem = 
			new JMenuItem("Delete Selected and All Children");
		recursiveDeleteItem.addActionListener(this);
		renameItem = new JMenuItem("Rename Collection");
		renameItem.addActionListener(this);
		
		collectionMenu.add(deleteAdoptItem);
		collectionMenu.add(recursiveDeleteItem);
		collectionMenu.add(renameItem);
		
		// add a datatype menu to the menu bar.
		JMenu datatypeMenu = new JMenu("Datatype");
		datatypeMenu.setMnemonic(KeyEvent.VK_D);
		menuBar.add(datatypeMenu);
		dataFormatItem = new JMenuItem("Data Format Viewer", KeyEvent.VK_D);
		dataFormatItem.addActionListener(this);
		
		datatypeMenu.add(dataFormatItem);
		
		//Add a help menu to the menu bar.
		JMenu helpMenu = new JMenu("Help");
		helpMenu.setMnemonic(KeyEvent.VK_H);
		menuBar.add(helpMenu);
		//helpMenu.add(helpItem);
		outputItem = new JMenuItem("Show Output Window", KeyEvent.VK_S);
		outputItem.addActionListener(this);
		helpMenu.add(outputItem);
		helpMenu.addSeparator();
		aboutItem = new JMenuItem("About Enchilada", 
						KeyEvent.VK_A);
		aboutItem.addActionListener(this);
		helpMenu.add(aboutItem);
		
		setJMenuBar(menuBar);  //Add menu bar to the frame
	}
	
	/**
	 * Creates a button bar - use right now for importing, exporting,
	 * and creating empty collections.
	 *
	 */
	private void setupButtonBar()
	{
		buttonPanel = new JToolBar();
		buttonPanel.setBorder(new EtchedBorder());
		
		importParsButton = new JButton("Import ATOFMS Data");
		importParsButton.setBorder(new EtchedBorder());
		importParsButton.addActionListener(this);
		
//		importEnchiladaDataButton = new JButton("Import Enchilada Data Sets");
//		importEnchiladaDataButton.setBorder(new EtchedBorder());
//		importEnchiladaDataButton.addActionListener(this);
		
		importAMSDataButton = new JButton("Import AMS Data");
		importAMSDataButton.setBorder(new EtchedBorder());
		importAMSDataButton.addActionListener(this);
		
		importFlatButton = new JButton("Import Time Series");
		importFlatButton.setBorder(new EtchedBorder());
		importFlatButton.addActionListener(this);
		
		emptyCollButton = new JButton("New Empty Collection");
		emptyCollButton.setBorder(new EtchedBorder());
		emptyCollButton.addActionListener(this);
		
		exportParsButton = new JButton("Export to MS-Analyze");
		exportParsButton.setBorder(new EtchedBorder());
		exportParsButton.addActionListener(this);
		
		buttonPanel.add(emptyCollButton);
		buttonPanel.add(importParsButton);
		buttonPanel.add(importFlatButton);
//		buttonPanel.add(importEnchiladaDataButton);
		buttonPanel.add(importAMSDataButton);
		buttonPanel.add(exportParsButton);
		add(buttonPanel);
	}
	
	/**
	 * @author steinbel
	 * setupSplitPane() creates and adds a split pane to the frame. The 
	 * left side of the split pane contains a collection and synchronization
	 * trees the right side of the split pane contains a tabbed pane.  
	 * Everything except the spectrum viewer is scrollable.
	 */  
	private void setupSplitPane()
	{
		// Add a JTabbedPane to the split pane.
		collectionViewPanel = new JTabbedPane();
		
		Vector<String> columns = new Vector<String>(1);
		columns.add("");
		
		data = new Vector<Vector<Object>>(1000);
		Vector<Object> row = new Vector<Object>(1);
		row.add("");
		data.add(row);
		

		currentlyShowing = new JLabel("Currently showing 0 particles");
		
		forwardButton = new JButton("Next");
		forwardButton.setEnabled(false);
		forwardButton.addActionListener(this);
		
		backwardButton = new JButton("Previous");
		backwardButton.setEnabled(false);
		backwardButton.addActionListener(this);
		
		String initSearch = " Enter a filename to search for a particle.";
		searchFileBox = new JTextField(initSearch);
		searchFileBox.setEnabled(false);
		searchFileBox.addActionListener(this);	
		
		searchButton = new JButton("Search");
		searchButton.setEnabled(false);
		searchButton.addActionListener(this);
		
		particlesTable = new JTable(data, columns);
		
		analyzeParticleButton = new JButton("Analyze Particle");
		analyzeParticleButton.setEnabled(false);
		analyzeParticleButton.addActionListener(this);
		
		JPanel comboPane = new JPanel(new BorderLayout());
		comboPane.add(currentlyShowing, BorderLayout.NORTH);
		comboPane.add(backwardButton, BorderLayout.WEST);
		comboPane.add(forwardButton, BorderLayout.EAST);
		
		JPanel searchPane = new JPanel(new BorderLayout());
		//put next two buttons on another line
//		searchPane.add(searchFileBox, BorderLayout.NORTH);//commented out for fall 06 release - steinbel
//		searchPane.add(searchButton, BorderLayout.SOUTH);
		
		JPanel buttonsPane = new JPanel(new BorderLayout());
		buttonsPane.add(comboPane, BorderLayout.EAST);
		buttonsPane.add(searchPane, BorderLayout.WEST);
		
		particlePanel = new JPanel(new BorderLayout());
		particleTablePane = new JScrollPane(particlesTable);
		
		JPanel partOpsPane = new JPanel(new FlowLayout());
		partOpsPane.add(analyzeParticleButton, BorderLayout.CENTER);
		
		particlePanel.add(buttonsPane, BorderLayout.NORTH);
		particlePanel.add(particleTablePane, BorderLayout.CENTER);
		particlePanel.add(partOpsPane, BorderLayout.SOUTH);
		
		collectionViewPanel.addTab("Particle List", null, particlePanel,
				null);
		
		//rightPane.addTab("Spectrum Viewer Text", null, panel2, null);
		descriptionTA = new JTextArea("Description here");
		infoPanel = makeTextPanel(descriptionTA);
		collInfoPane = new JScrollPane(infoPanel);
		collectionViewPanel.addTab("Collection Information", 
				null, collInfoPane, null);
		// Create and add the split panes.
		
		// Add a JTree to the split pane.
		JPanel topLeftPanel = new JPanel(new BorderLayout());
		JPanel topLeftButtonPanel = new JPanel(new FlowLayout());
		JPanel bottomLeftPanel = new JPanel(new BorderLayout());
		JPanel bottomLeftButtonPanel = new JPanel(new FlowLayout());
		collectionPane = new CollectionTree(db, this, false);
		synchronizedPane = new CollectionTree(db, this, true);
		topLeftButtonPanel.add(aggregateButton = new JButton("Aggregate Selected"));
		bottomLeftButtonPanel.add(mapValuesButton = new JButton("Map Values"));
		
		//TODO: Remove when Map Values becomes usable
		mapValuesButton.setVisible(false);
		
		topLeftPanel.add(collectionPane, BorderLayout.CENTER);
		topLeftPanel.add(topLeftButtonPanel, BorderLayout.SOUTH);
		bottomLeftPanel.add(synchronizedPane, BorderLayout.CENTER);
		bottomLeftPanel.add(bottomLeftButtonPanel, BorderLayout.SOUTH);
		
		JSplitPane leftPane 
		= new JSplitPane(JSplitPane.VERTICAL_SPLIT, topLeftPanel, bottomLeftPanel);
		leftPane.setMinimumSize(new Dimension(170,64));
		leftPane.setDividerLocation(200);
		
		mainSplitPane = 
			new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPane, collectionViewPanel);
		add(mainSplitPane);
		
		aggregateButton.addActionListener(this);
		mapValuesButton.addActionListener(this);
		aggregateButton.setEnabled(false);
		mapValuesButton.setEnabled(false);
	}
	
	public void collectionSelected(CollectionTree colTree, Collection collection) {
		if (selectedCollectionTree != null && colTree != selectedCollectionTree)
			selectedCollectionTree.clearSelection();
		
		selectedCollectionTree = colTree;
		
		setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
		
		int dividerLocation = mainSplitPane.getDividerLocation();
		boolean panelChanged = false;
		
		if (colTree == collectionPane) {
			panelChanged = setupRightWindowForCollection(collection);
			aggregateButton.setEnabled(collection.containsData());
			analysisMenu.remove(detectPlumesItem);
			mapValuesButton.setEnabled(false);
		} else if (colTree == synchronizedPane) {
			panelChanged = setupRightWindowForSynchronization(collection);
			mapValuesButton.setEnabled(collection.containsData());
			analysisMenu.add(detectPlumesItem);
			aggregateButton.setEnabled(false);
		}
		
		if (panelChanged) {
			// Bah. Java can't just remember this... need to remind it.
			mainSplitPane.setDividerLocation(dividerLocation);
			
			validate();
		}
		
		editText(MainFrame.DESCRIPTION, collection.getDescription());
		setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
	}
	
	/**
	 * Display collection information
	 * @param collection
	 * @return
	 */
	private boolean setupRightWindowForCollection(Collection collection) {
		mainSplitPane.setBottomComponent(collectionViewPanel);
		
		String dataType = collection.getDatatype();
		ArrayList<String> colnames = db.getColNames(dataType, DynamicTable.AtomInfoDense);
		
		/*
		 * @author steinbel - changed to work with next/prev buttons
		 */ 
		currCollectionSize = db.getCollectionSize(collection.getCollectionID());
		currCollection = collection.getCollectionID();
		
		currLow = 1;
		backwardButton.setEnabled(false);
		
		if (currCollectionSize > 1000) {
			forwardButton.setEnabled(true);
			currHigh = 1000;
		}
		else{
			currHigh = currCollectionSize;
			forwardButton.setEnabled(false);
		}
		
		//allow searching in this collection
//		searchButton.setEnabled(true);	//commented out for fall 06 release - steinbel
//		searchFileBox.setEnabled(true);
		
		Vector<Object> columns = new Vector<Object>(colnames.size());
		for (int i = 0; i < colnames.size(); i++) {
			String temp = colnames.get(i);
			temp = temp.substring(1,temp.length()-1);
			columns.add(temp);
		}
		
		data = new Vector<Vector<Object>>(1000);
		Vector<Object> row = new Vector<Object>(colnames.size());
		for (int i = 0; i < colnames.size(); i++) 
			row.add("");
		
		data.add(row);
		
		particlesTable = new JTable(data, columns);
		particlesTable.setDefaultEditor(Object.class, null);
		
		particleTablePane.setViewportView(particlesTable);
		
		particlesTable.setEnabled(true);
		ListSelectionModel lModel = 
			particlesTable.getSelectionModel();
		
		lModel.setSelectionMode(
				ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		lModel.addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent arg0) {		
				/*	// If collection isn't ATOFMS, don't display anything.
				 if (!db.getAtomDatatype(atomID).equals("ATOFMS"))
				 return;
				 */
				int row = particlesTable.getSelectedRow();
				
				analyzeParticleButton.setEnabled(row != -1);
			}
		});		
		
		particlesTable.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				if (e.getClickCount() > 1)
					showAnalyzeParticleWindow();
			}
		});
		
		collectionViewPanel.setComponentAt(0, particlePanel);
		collectionViewPanel.repaint();
		
		//call setTable, which populates the table.
		setTable();
		//old code
		//data.clear();
		//data = db.updateParticleTable(collection, data, low, high);

		return true;
	}
	
	
	/**
	 * Display time-series information
	 * @param collection
	 * @return
	 */
	private boolean setupRightWindowForSynchronization(Collection collection) {
		Component rightWindow = mainSplitPane.getBottomComponent();
		
		if (rightWindow instanceof SyncAnalyzePanel) {
			SyncAnalyzePanel sap = (SyncAnalyzePanel) rightWindow;
			
			if (sap.containsCollection(collection)) { 
				sap.selectCollection(collection);
				return false;
			}
		}
		
		mainSplitPane.setBottomComponent(new SyncAnalyzePanel(this, db, synchronizedPane, collection));
		return true;
	}
	
	/**
	 * The makeTextpanel method makes a text panel that can be added to any 
	 * other panel - used for the template.  Once other methods are implemented 
	 * this will be removed.
	 * 
	 * @param text
	 * @return a JComponent object that contains the desired tex
	 * 
	 */
	protected static JComponent makeTextPanel(JTextArea filler) {
		JPanel panel = new JPanel(false);
		filler.setEditable(false);
		filler.getDocument().getLength();
		//filler.setHorizontalAlignment(JLabel.CENTER);
		panel.setLayout(new GridLayout(1, 1));
		panel.add(filler);
		return panel;
	}
	
	public void editText(int panelID, String text)
	{
		if (panelID == DESCRIPTION)
		{
			descriptionTA.setText(text);
			descriptionTA.setCaretPosition(0);
			
			JScrollBar vert = collInfoPane.getVerticalScrollBar();
			int page = vert.getVisibleAmount();
			vert.setUnitIncrement(page / 15);
			vert.setBlockIncrement(page);
			
			/*
			 int docLength = descriptionTA.getDocument().getLength();
			 
			 descriptionTA.replaceRange(text, 0,docLength);*/
		}
	}
	private void writeToFile(String filename, int atomID){
		PrintWriter pw;
		try {
			pw = new PrintWriter(filename);
			pw.println(db.getATOFMSFileName(atomID));
			ArrayList<Peak> peaks = db.getPeaks(db.getAtomDatatype(atomID), atomID);		

			for(int i = 0; i<peaks.size();i++){
				pw.println(peaks.get(i).massToCharge + "," + peaks.get(i).value);
			}
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public void updateSynchronizedTree(int collectionID) {
		synchronizedPane.updateTree(collectionID);
	}
	
	public void updateAnalyzePanel(Collection c) {
		Component rightWindow = mainSplitPane.getBottomComponent();
		
		if (rightWindow instanceof SyncAnalyzePanel) {
			SyncAnalyzePanel sap = (SyncAnalyzePanel) rightWindow;
			sap.updateModels(c);
		}
	}
	
	/**
	 * Offers functionality for connecting different databases while maintaining
	 * the connection to dbname in main method, refactored by @author xzhang9
	 */
	public static void main(String[] args) {
		/* "If you are going to set the look and feel, you should do it as the 
		 * very first step in your application. Otherwise you run the risk of 
		 * initializing the Java look and feel regardless of what look and feel 
		 * you've requested. This can happen inadvertently when a static field 
		 * references a Swing class, which causes the look and feel to be 
		 * loaded. If no look and feel has yet been specified, the default Java 
		 * look and feel is loaded."
		 * From http://java.sun.com/docs/books/tutorial/uiswing/misc/plaf.html
		 */
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			//UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// @author xzhang9 
		// Verify that production database exists, and give user opportunity to create if it does not.
		if(!connectDB(dbname)) return;
		db.openConnection();
		

		//@author steinbel
		VersionChecker vc = new VersionChecker(db);
		try {
			if (! vc.isDatabaseCurrent()) {
				//new error window, but it should have button options, so the
				//existing error framework isn't any good.
				//soooo want new JOptionPane

				Object[] options = {"Rebuild database", "Quit"};
				int action = JOptionPane.showOptionDialog(null, 
						"Your database is an old version and " +
						"incompatible with this version of Enchilada.\n Please" +
						" either rebuild the database (permanently deletes " +
						"your current database\n and all data within it) or " +
						"quit and use an older version of Enchilada.",
						"Warning: Incompatible database",
						JOptionPane.DEFAULT_OPTION, JOptionPane.WARNING_MESSAGE,
						null, options, options[1]);
				if (action == 0){
					try {
						db.closeConnection();
						Database.getDatabase(dbname).rebuildDatabase(dbname);
						db.openConnection();
					} catch(ExceptionAdapter ea) {
						if (ea.originalException instanceof  SQLException) {
							ea.printStackTrace();
							JOptionPane.showMessageDialog(null,
									"Your database is an old version",
									"Error: Could not connect",
									JOptionPane.PLAIN_MESSAGE);
							System.exit(0);
						} else {
							throw ea;
						}
					}
				} else
					System.exit(0);
				
			}
		} catch (Exception e) {
			System.out.println("SQL Exception retrieving version!");
			e.printStackTrace();
		}
		
		//Schedule a job for the event-dispatching thread:
		//creating and showing this application's GUI.
		javax.swing.SwingUtilities.invokeLater(new MainFrameRun(args));
	}

	/**
	 * Offers functionality for connecting different databases while maintaining
	 * the connection to dbname in main method
	 * @author xzhang9
	 */
	private static boolean connectDB(String dbName) {
		// Verify that database exists, and give user opportunity to create
		// if it does not.
		if (!Database.getDatabase(dbName).isPresent()) {
			if (JOptionPane.showConfirmDialog(null,
					"No database found. Would you like to create one?\n" +
					"Make sure to select yes only if there is no database already present,\n"
					+ "since this will remove any pre-existing Enchilada database.") ==
						JOptionPane.YES_OPTION) {
				try{
					Database.getDatabase(dbName).rebuildDatabase(dbName);
				} catch (ExceptionAdapter ea) {
					if (ea.originalException instanceof SQLException) {
						JOptionPane.showMessageDialog(null,
								"Could not rebuild the database." +
										"  Close any other programs that may be accessing the database and try again.");
						return false;
					} else {
						throw ea;
					}
				}
			} else {
				return false; // no database?  we shouldn't do anything at all.
			}
		}
		
		//Open database connection:
		db = Database.getDatabase(dbName);
		return true;
	}
	
	/**
	 * Offers simple functionality for parsing command-line arguments while maintaining
	 * earlier deferred construction of MainFrame
	 * @author shaferia
	 */
	private static class MainFrameRun implements Runnable {
		String[] args;
		private MainFrame mf;
		
		/**
		 * Creates MainFrameRun with command-line arguments
		 * @param args
		 * 	-redirectOutput: take output from standard output and redirect to a window.
		 *		If started with this option, will continue to redirect to window even when it is closed.
		 */
		public MainFrameRun(String[] args) {
			this.args = args;
		}
	
		public void run() {
			mf = new MainFrame();
			
			//Check command-line arguments
			for (String s : args) {
				if (s.startsWith("-"))
					s = s.substring(1);
				else
					continue;
				
				try {
					java.lang.reflect.Method m = getClass().getMethod(s, (Class[]) null);
					m.invoke(this, new Object[0]);
				}
				catch (Exception ex) {
					System.out.print("Invalid argument: " + s + " - ");
					System.out.println(ex.getMessage());
				}
			}
		}

		/**
		 * Redirect standard output to a separate JFrame
		 */
		public void redirectOutput() {
			try {
				//This option will be invoked by those not running from inside an IDE -
				//	output will continue to be redirected to a window throughout the session.
				OutputWindow.setReturnOutputOnClose(true);
				
				OutputWindow w = mf.outputFrame = new OutputWindow(mf);
				w.setSize(mf.getSize().width / 2, mf.getSize().height / 2);
			
				//the window will flash in front briefly, but reversing the order of these calls
				//	doesn't properly send the window to the back.
				w.setVisible(true);
				w.toBack();
				
				checkMemory();
			}
			catch (Exception ex) {
				System.out.println("Couldn't reassign program output to window");
			}
		}
	}
		
	/**
	 * @return Returns the data.
	 */
	public Vector<Vector<Object>> getData() 
	{
		return data;
	}
	
	/**
	 * @return Returns the particlesTable.
	 */
	public JTable getParticlesTable() {
		return particlesTable;
	}
	
	public JComponent getInfoPanel() {
		return infoPanel;
	}
	
	public void clearOtherTreeSelections(CollectionTree colTree) {
		if (colTree == collectionPane)
			synchronizedPane.clearSelection();
		else if (colTree == synchronizedPane)
			collectionPane.clearSelection();
	}
		
	/* (non-Javadoc)
	 * @see javax.swing.event.ListSelectionListener#valueChanged(javax.swing.event.ListSelectionEvent)
	 */
	/**
	 * Updates with peaklist information for the selected 
	 * atom
	 */
	public void valueChanged(ListSelectionEvent arg0) {
		int row = particlesTable.getSelectedRow();
		
		analyzeParticleButton.setEnabled(row != -1);
	}	
	
	/** 
	 * This clears the particle table when a collection is deleted.  
	 */
	public void clearTable() {
		//data.clear();
		data = new Vector<Vector<Object>>(1000);
		Vector<Object> row = new Vector<Object>(6);
		for (int x = 0; x < 6; ++x)
			row.add("");
		data.add(row);
		Vector<Object> columns = new Vector<Object>(1);
		columns.add("");
		
		particlesTable = new JTable(data, columns);
		particlesTable.doLayout();
		particleTablePane.setViewportView(particlesTable);
		collectionViewPanel.setComponentAt(0, particlePanel);
		collectionViewPanel.repaint();
		currentlyShowing.setText("Currently showing 0 particles");
		
//		searchFileBox.setEnabled(false);	//commented out for fall 06 release - steinbel
//		searchButton.setEnabled(false);
		
		analyzeParticleButton.setEnabled(false);
		
		particlesTable.validate();
		analyzeParticleButton.validate();
	}
	
	/**
	 * @author steinbel
	 * This sets the particle table to show 1000 (or fewer) particles at a time.
	 * 
	 * It is called by the setupRightWindow method and by the actionlistener
	 * when the next or previous buttons are clicked.
	 *
	 */
	public void setTable() {
			
			//System.out.println("low " + currLow + " high " + currHigh + " "
			//		+ ((currHigh - currLow)+1));//TESTING
			//clear data in table and repopulate it with appropriate 
			// data.
			currentlyShowing.setText("Currently showing particles " + currLow +
					"-" + currHigh + " of " + currCollectionSize + ".");
			data.clear();
			db.updateParticleTable(getSelectedCollection(),data,currLow,currHigh);
			particlesTable.tableChanged(new TableModelEvent(particlesTable.getModel()));
			particlesTable.doLayout();
	
	}
	
	/**
	 * Updates the collection tree to show the requested collection.
	 * @param cID	The collection to highlight.
	 */
	public void updateCurrentCollection(int cID){
		
		collectionPane.switchCollections(cID);
	}
}