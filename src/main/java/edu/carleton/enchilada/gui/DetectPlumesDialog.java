package edu.carleton.enchilada.gui;


import java.awt.FileDialog;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import edu.carleton.enchilada.collection.Collection;

import edu.carleton.enchilada.database.Database;


public class DetectPlumesDialog  extends JDialog implements ActionListener, ItemListener{
	private JFrame parent;
	private JButton okButton;
	private JButton cancelButton;
	private JButton restoreDefault;
	
	private JTextField magnitudeField;
	private JTextField durationField;
	private JComboBox metricDropdown;
	
	private CollectionTree cTree;
	private Database db;
	public DetectPlumesDialog(MainFrame frame, CollectionTree cTree, Database db){
		super(frame,"Plume Detection Options", true);
		parent = frame;
		this.cTree = cTree;
		this.db = db;
		setSize(500,300);
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		
		JLabel header = new JLabel("Plumes Detection Parameters:");
		
		JPanel panel = new JPanel();
		JLabel magnitudeLabel = new JLabel("Plume Magnitude Requirements:");
		String[] metricNames = {">= Top N% of the peaks", ">= N * median peak-size", ">= N"};
		
		metricDropdown = new JComboBox(metricNames);
		metricDropdown.addItemListener(this);
		
		String initialMagnitude = Float.toString(0.75f);
		magnitudeField = new JTextField(initialMagnitude, 5);
		magnitudeField.setEditable(true);
		JLabel durationLabel = new JLabel("Minimum Plume Duration(in seconds):");
		String initialDuration = Integer.toString(5);
		
		durationField = new JTextField(initialDuration, 5);
		durationField.setEditable(true);
		
		panel.setLayout(new BoxLayout(panel,BoxLayout.Y_AXIS));
		
		JPanel magnitudePanel = new JPanel();
		magnitudePanel.add(magnitudeLabel);
		magnitudePanel.add(magnitudeField);
		magnitudePanel.add(metricDropdown);
		
		JPanel durationPanel = new JPanel();
		durationPanel.add(durationLabel);
		durationPanel.add(durationField);
		
		panel.add(magnitudePanel);
		panel.add(durationPanel);
		
		JPanel buttons = new JPanel();
		okButton = new JButton("OK");
		okButton.addActionListener(this);
		cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(this);
		restoreDefault = new JButton("Restore Default");
		restoreDefault.addActionListener(this);
		buttons.add(okButton);
		buttons.add(cancelButton);
		buttons.add(restoreDefault);
		
		add(header);
		add(panel);
		add(buttons);
		setLayout(new FlowLayout());
		
		getRootPane().setDefaultButton(okButton);
		setVisible(true);
	}
	public void actionPerformed(ActionEvent e) {
		Object source = e.getSource();
		if (source == okButton) {
			double threshold = Double.parseDouble(magnitudeField.getText());
			int duration = Integer.parseInt(durationField.getText());
			
			FileDialog fileChooser = new FileDialog(this, 
                    "Choose a place to write the plumes:",
                     FileDialog.LOAD);
			//fileChooser.setFile(fileFilter);
			fileChooser.setVisible(true);
			String filename = fileChooser.getDirectory()+fileChooser.getFile();
			if(fileChooser.getFile()==null) return;
			Collection curCollection = cTree.getSelectedCollection();
			metricDropdown.getSelectedIndex();
			
			ArrayList<TreeMap<Date, Double>> plumes = null;
			switch(metricDropdown.getSelectedIndex()){
			case 0:
				plumes = db.createAndDetectPlumesFromPercent(
						curCollection, threshold, duration);
				break;
			case 1:
				plumes = db.createAndDetectPlumesFromMedian(
						curCollection, threshold, duration);
				break;
			case 2:
				plumes = db.createAndDetectPlumesFromValue(
						curCollection, threshold, duration);
				break;
			default: 
				break;
			}
			

			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			FileWriter output;
			try {
				output = new FileWriter(filename);
				for (int i = 0; i < plumes.size(); i++) {
					output.write("Plume # "+i+"\n");
					for(Date date : plumes.get(i).keySet()){
						output.write(formatter.format(date)+", ");
					}
					output.write("\n");
					for(Double value : plumes.get(i).values()){
						output.write(value+", ");
					}
					output.write("\n");
					output.write("\n");
				}
				output.flush();
				output.close();
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			JOptionPane.showMessageDialog(parent, 
					plumes.size()+" plumes were detected and were written to the file: "+filename,
					"Plumes Detected",JOptionPane.INFORMATION_MESSAGE);
			dispose();
		}else if (source == cancelButton) {
				dispose();
		}
	}
	public void itemStateChanged(ItemEvent arg0) {
		// TODO Auto-generated method stub
		
	}
}
