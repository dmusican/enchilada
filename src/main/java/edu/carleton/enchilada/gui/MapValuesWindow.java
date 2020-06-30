package edu.carleton.enchilada.gui;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.Database;

import java.awt.*;
import java.awt.event.*;
import java.util.*;

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.table.*;

public class MapValuesWindow extends JFrame implements ListSelectionListener, ActionListener {
	private MainFrame parentFrame;
	private JButton applyValueMap, cancel, saveCurrentMap, reset, addSplitPoint;
	private JTextField splitPoint;
	private JTable mapRangesTable;
	private JList mapList;
	private JPopupMenu deletePopupMenu;
	private JMenuItem deleteSplitPoint;
	
	private MapRangesDataModel mapRangesDataModel;
	private MapListModel savedMapData;
	private Collection collection;
	private Database db;
	
	private int selectedRow, selectedColumn;
	
	public MapValuesWindow(MainFrame parentFrame, Database db, Collection collection) {
		super("Set up Value Maps for: " + collection.getName());
		
		// Only Time Series should be mapped... (for now)
		assert(collection.getDatatype().equals("TimeSeries"));
		
		this.parentFrame = parentFrame;
		this.db = db;
		this.collection = collection;
		
		setSize(500, 510);
		setResizable(false);
		
		JPanel mainPanel = new JPanel(new BorderLayout());
		JPanel topPanel = new JPanel(new GridLayout(1, 2, 5, 0));
		
		JPanel savedMapPanel = setupSavedMapsPanel(topPanel);
		JPanel mapOptionsPanel = setupMapRangesPanel(topPanel);
		
		topPanel.add(savedMapPanel);
		topPanel.add(mapOptionsPanel);
		
		JPanel buttonPanel = new JPanel(new FlowLayout());
		buttonPanel.add(applyValueMap = new JButton("Apply Current Mapping"));
		buttonPanel.add(cancel = new JButton("Cancel"));
		
		mainPanel.add(topPanel, BorderLayout.CENTER);
		mainPanel.add(buttonPanel, BorderLayout.SOUTH);
		add(mainPanel);
		
		mapList.addListSelectionListener(this);
		applyValueMap.addActionListener(this);
		cancel.addActionListener(this);
		saveCurrentMap.addActionListener(this);
		reset.addActionListener(this);
		addSplitPoint.addActionListener(this);
		deleteSplitPoint.addActionListener(this);
		
		if (savedMapData.getSize() > 0)
			mapList.setSelectedIndex(0);
	}
	
	private JPanel setupSavedMapsPanel(JPanel topPanel) {
		savedMapData = new MapListModel(db.getValueMaps()); 
		
		SpringLayout savedMapLayout = new SpringLayout();
		JPanel savedMapPanel = new JPanel(savedMapLayout);
		JLabel savedMaps = new JLabel("Saved Value Mappings:");
		JScrollPane maps = new JScrollPane(mapList = new JList(savedMapData));
		mapList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		maps.setPreferredSize(new Dimension(230, 400));
		
		savedMapPanel.add(savedMaps);
		savedMapPanel.add(maps);
		
		savedMapLayout.putConstraint(SpringLayout.WEST, savedMaps, 5, 
				SpringLayout.WEST, topPanel);
		savedMapLayout.putConstraint(SpringLayout.NORTH, savedMaps, 5, 
				SpringLayout.NORTH, topPanel);
		savedMapLayout.putConstraint(SpringLayout.WEST, maps, 5, 
				SpringLayout.WEST, topPanel);
		savedMapLayout.putConstraint(SpringLayout.NORTH, maps, 5, 
				SpringLayout.SOUTH, savedMaps);
		
		return savedMapPanel;
	}
	
	private JPanel setupMapRangesPanel(JPanel topPanel) {
		mapRangesDataModel = new MapRangesDataModel(db.getValueMapRanges());
		
		SpringLayout mapOptionsLayout = new SpringLayout();
		JPanel mapOptionsPanel = new JPanel(mapOptionsLayout);
		JLabel map = new JLabel("Current Mapping:");
		JScrollPane mapRanges = new JScrollPane(mapRangesTable = new JTable(mapRangesDataModel));
		mapRanges.setPreferredSize(new Dimension(230, 300));
		JPanel mapButtonPanel = new JPanel(new FlowLayout());
		
		deletePopupMenu = new JPopupMenu();
		deleteSplitPoint = deletePopupMenu.add("Delete this value");
		
		mapRangesTable.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				if (e.getButton() == MouseEvent.BUTTON3) {
					JTable table = (JTable) e.getComponent();
					
					selectedRow = table.rowAtPoint(e.getPoint());
					selectedColumn = table.columnAtPoint(e.getPoint());
					Object value = table.getValueAt(selectedRow, selectedColumn);
					
					if (selectedColumn > 0 && !value.toString().contains("Inf."))
						deletePopupMenu.show(table, e.getX(), e.getY());
				} else {
					deletePopupMenu.setVisible(false);
				}
			}
		});
		
		mapButtonPanel.add(reset = new JButton("Reset"));
		mapButtonPanel.add(saveCurrentMap = new JButton("Save Current Map"));
		
		JPanel splitPointPanel = new JPanel(new FlowLayout());
		splitPointPanel.add(new JLabel("Split Point:"));
		splitPointPanel.add(splitPoint = new JTextField(8));
		splitPointPanel.add(addSplitPoint = new JButton("Add"));
		
		mapOptionsPanel.add(map);
		mapOptionsPanel.add(mapRanges);
		mapOptionsPanel.add(splitPointPanel);
		mapOptionsPanel.add(mapButtonPanel);
		mapOptionsLayout.putConstraint(SpringLayout.NORTH, map, 5, 
				SpringLayout.NORTH, topPanel);
		mapOptionsLayout.putConstraint(SpringLayout.NORTH, mapRanges, 5, 
				SpringLayout.SOUTH, map);
		mapOptionsLayout.putConstraint(SpringLayout.NORTH, splitPointPanel, 5, 
				SpringLayout.SOUTH, mapRanges);
		mapOptionsLayout.putConstraint(SpringLayout.NORTH, mapButtonPanel, 5, 
				SpringLayout.SOUTH, splitPointPanel);
		mapOptionsLayout.putConstraint(SpringLayout.WEST, mapButtonPanel, 10, 
				SpringLayout.WEST, splitPointPanel);
		
		return mapOptionsPanel;
	}
	
	public void valueChanged(ListSelectionEvent e) {
		int index = mapList.getSelectedIndex();
		
		if (!e.getValueIsAdjusting() && index > -1) {
			int newMapID = savedMapData.getIDAt(index);
			mapRangesDataModel.setCurrentMapID(newMapID);
			mapRangesTable.revalidate();
		}
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		
		if (source == applyValueMap) {
			String selectedName = (String) savedMapData.getElementAt(mapList.getSelectedIndex());
			
			if (selectedName == null)
				selectedName = "Unnamed map";
			else
				selectedName = selectedName.substring(1, selectedName.length());
			
			db.applyMap("Map: \"" + selectedName + "\"", mapRangesDataModel.getCurrentMap(), collection);
			parentFrame.updateSynchronizedTree(collection.getCollectionID());
			parentFrame.updateAnalyzePanel(collection);
			setVisible(false);
			dispose();
		} else if (source == cancel) {
			setVisible(false);
			dispose();
		} else if (source == saveCurrentMap) {
			String mapName = 
				(String)JOptionPane.showInputDialog(
                    this,
                    "Enter a name for this map: ",
                    "Name the map...",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");

			int mapID = mapRangesDataModel.saveSet(mapName);
			savedMapData.addMap(mapID, mapName);
			mapList.revalidate();
			mapList.setSelectedIndex(savedMapData.getSize() - 1);
		} else if (source == reset) {
			mapRangesDataModel.reset();
			mapRangesTable.revalidate();
		} else if (source == addSplitPoint) {
			String splitPointVal = splitPoint.getText();
			try {
				int splitPointNum = Integer.parseInt(splitPointVal);
				mapRangesDataModel.addSplitPoint(splitPointNum);
				mapRangesTable.revalidate();
			} catch (NumberFormatException nfe) {
				System.err.println("Invalid value: " + splitPointVal);
			}
			
			splitPoint.setText("");
		} else if (source == deleteSplitPoint) {
			mapRangesDataModel.deleteSplitPoint(selectedRow, selectedColumn);
			mapRangesTable.revalidate();
		}
	}
	
	public class MapListModel extends AbstractListModel {
		Vector<String> names;
		Vector<Integer> ids;
		
		public MapListModel(Hashtable<Integer, String> list) {
			names = new Vector<String>();
			ids = new Vector<Integer>();
			for (Integer valueMapID : list.keySet()) {
				ids.add(valueMapID);
				names.add(list.get(valueMapID));
			}
		}
		
		public int getSize() { return names.size(); }
		
		public Object getElementAt(int index) {
			if (index >= 0 && index < names.size())
				return " " + names.get(index);
			else
				return null;
		}
		
		public void addMap(int mapID, String name) {
			ids.add(mapID);
			names.add(name);
			
			fireIntervalAdded(this, ids.size(), ids.size());
		}
		
		public int getIDAt(int index) {
			return ids.get(index);
		}
	};
	
	public class MapRangesDataModel extends AbstractTableModel {
		private int currentMapID = -1;
		
		private Hashtable<Integer, Vector<int[]>> hash = new Hashtable<Integer, Vector<int[]>>();
		private String[] columnNames = { "Value", "Min", "Max" };
		private Vector<int[]> values = null;
		
		public MapRangesDataModel(Vector<int[]> ranges) {
			reset();
			
			for (int[] range : ranges) {
				Integer valueMapID = range[0];
				
				if (!hash.containsKey(valueMapID))
					hash.put(valueMapID, new Vector<int[]>());
					
				hash.get(valueMapID).add(new int[] { range[1], range[2], range[3] });
			}
		}
		
		public void setCurrentMapID(int mapID) {
			currentMapID = mapID;
			
			values = hash.get(currentMapID);
		}
		
		public int saveSet(String name) {
			int mapID = db.saveMap(name, values);
			hash.put(mapID, getValuesCopy());
			
			currentMapID = mapID;
			
			return mapID;
		}
		
		public void addSplitPoint(int value) {
			values = getValuesCopy();
			
			for (int i = 0; i < getRowCount(); i++) {
				int[] row = values.get(i);
				
				if (row[1] < value && row[2] > value) {
					values.remove(i);
					values.insertElementAt(new int[] { row[0], row[1], value }, i);
					values.insertElementAt(new int[] { row[0], value, row[2] }, ++i);

					currentMapID = -1;
					mapList.clearSelection();
					
					for (; i < values.size(); i++)
						values.get(i)[0] = values.get(i)[0] + 1;
					
					return;
				}
			}
			
			System.err.println("Value is already a split point: " + value);
		}
		
		public void deleteSplitPoint(int row, int column) {
			values = getValuesCopy();
			
			if (column == 1) 
				values.get(row - 1)[2] = values.get(row)[2];
			else
				values.get(row + 1)[1] = values.get(row)[1];
				
			values.remove(row);

			for (int i = row; i < values.size(); i++)
				values.get(i)[0] = values.get(i)[0] - 1;
			
			mapList.clearSelection();
		}
		
		public void reset() {
			currentMapID = -1;
			
			values = new Vector<int[]>();
			values.add(new int[] { 0, Integer.MIN_VALUE, Integer.MAX_VALUE });
			
			mapList.clearSelection();
		}

		public String getColumnName(int col) {
			return columnNames[col];
		}
		
		public int getColumnCount() { return 3; }
		public int getRowCount() { return values == null ? 0 : values.size(); }
		
		public boolean isCellEditable(int row, int column) { return false; }
		
		public Object getValueAt(int row, int column) {
			int curVal = values.get(row)[column];
			if      (curVal == Integer.MIN_VALUE) return "-Inf.";
			else if (curVal == Integer.MAX_VALUE) return "Inf.";
			else return curVal;
		}
		
		public void setValueAt(Object value, int row, int column) {
			assert(column == 0);
			
			try {
				values.get(row)[column] = Integer.parseInt((String) value);
			} catch (NumberFormatException e) {
				System.err.println("Invalid value: " + value);
			}
		}
		
		public Vector<int[]> getCurrentMap() {
			return values;
		}
		
		private Vector<int[]> getValuesCopy() {
			Vector<int[]> valuesCopy = new Vector<int[]>();
			for (int i = 0; i < values.size(); i++) {
				int[] oldVals = values.get(i);
				int[] newVals = new int[3];
				
				for (int j = 0; j < 3; j++)
					newVals[j] = oldVals[j];
				
				valuesCopy.add(newVals);
			}
			
			return valuesCopy;
		}
	};
}
