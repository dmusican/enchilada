package edu.carleton.enchilada.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.ArrayList;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.TableColumn;

import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.analysis.clustering.Cluster;
import edu.carleton.enchilada.analysis.clustering.ClusterQuery;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.errorframework.*;


/**
 * @author turetske
 *
 * ClusterQueryDialog opens a dialogue window that allows the user to 
 * cluster a selected collection using cluster centers that the user imputs.
 * There is an expanding list that the user can input a file into as well as
 * an input box for a distance parameter.
 *
 */

public class ClusterQueryDialog extends JDialog implements ActionListener{
	
	private final JButton okButton;
	private final JButton cancelButton;
	private final JTextField distance;
    private final JTextField commentField;
	private ClusterTableModel clusterTableModel;
	private final Database db;
	private final CollectionTree cTree;

	/**
	 * Extends JDialog to form a modal dialogue box for setting
	 * cluster centers.  
	 * @param owner The parent frame of this dialog box, should be the 
	 * main frame.    
	 * @throws java.awt.HeadlessException From the constructor of 
	 * JDialog.  
	 */
	public ClusterQueryDialog(JFrame owner, CollectionTree cTree, Database db) throws HeadlessException {
		super(owner, "Cluster with Chosen Centers", true);
		this.db = db;
		this.cTree = cTree;
		setSize(500,400);
			
		setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		JTable clusterTable = getClusterTable();
		JScrollPane scrollPane = new JScrollPane(clusterTable);
		
		okButton = new JButton("OK");
		okButton.setMnemonic(KeyEvent.VK_O);
		okButton.addActionListener(this);
		
		cancelButton = new JButton("Cancel");
		cancelButton.setMnemonic(KeyEvent.VK_C);
		cancelButton.addActionListener(this);
		
		scrollPane.setPreferredSize(new Dimension(400,300));
		
		
		JLabel label = new JLabel("Choose Cluster Centers");
		label.setLabelFor(clusterTable);
		label.setHorizontalAlignment(JLabel.CENTER);
		label.setHorizontalTextPosition(JLabel.CENTER);
		
		JPanel listPane = new JPanel();
		listPane.setLayout(new BoxLayout(listPane, BoxLayout.Y_AXIS));
		
		listPane.add(Box.createRigidArea(new Dimension(0,5)));
		listPane.add(scrollPane);
		listPane.setBorder(BorderFactory.createEmptyBorder(10,10,10,10));
		
		JPanel comment = new JPanel();
		JLabel commentLabel = new JLabel("Comment: ");
		commentField = new JTextField(30);
		comment.add(commentLabel);
		comment.add(commentField);
		
		JPanel panel = new JPanel(new BorderLayout());
		label.setHorizontalAlignment(JLabel.CENTER);
		label.setHorizontalTextPosition(JLabel.CENTER);
		panel.add(label, BorderLayout.NORTH);
		panel.add(listPane, BorderLayout.CENTER);
		panel.add(comment, BorderLayout.SOUTH);
		panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
		
		
		
		JPanel buttonPane = new JPanel();
		buttonPane.setLayout(new BoxLayout(buttonPane, BoxLayout.X_AXIS));
		buttonPane.setBorder(BorderFactory.createEmptyBorder(0,10,10,10));
		
		JLabel label2 = new JLabel("Maximum distance:");
		distance = new JTextField("0.5", 5);
		distance.setHorizontalAlignment(JTextField.RIGHT);
		buttonPane.add(label2);
		buttonPane.add(distance);
		buttonPane.add(Box.createRigidArea(new Dimension(210,0)));
		buttonPane.add(Box.createHorizontalGlue());
		buttonPane.add(okButton);
		buttonPane.add(Box.createRigidArea(new Dimension(10,0)));
		buttonPane.add(cancelButton);
		
		add(panel, BorderLayout.CENTER);
		add(buttonPane, BorderLayout.SOUTH);
		
		setVisible(true);	
	}
	
	private JTable getClusterTable()
	{
		clusterTableModel = new ClusterTableModel();
		JTable pTable = new JTable(clusterTableModel);		
		TableColumn[] tableColumns = new TableColumn[1];
		for (int i = 0; i < 1; i++)
			tableColumns[i] = pTable.getColumnModel().getColumn(i+1);
		tableColumns[0].setCellEditor(
				new FileDialogPickerEditor("csv","Center",this));
		tableColumns[0].setPreferredWidth(395);
		
		TableColumn numColumn = pTable.getColumnModel().getColumn(0);
		numColumn.setPreferredWidth(5);
		return pTable;
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == okButton) {
			System.out.println("okay button");
			float d = (float) 0.0;
			try{
				d = Float.parseFloat(distance.getText());
				System.out.println(d);
				if(d>2.0||d<0.0){
					System.out.println("out of range");
					JOptionPane.showMessageDialog(this, "Please enter a number between 0.0 and 2.0 for the distance");
					
				}
				else{
					ArrayList<String> filenames = new ArrayList<String>();
					for(int i = 0; i< clusterTableModel.getRowCount(); i++){
						if(!clusterTableModel.getValueAt(i,1).equals("")){
							filenames.add((String) clusterTableModel.getValueAt(i,1));
						}
					}
					if(!filenames.isEmpty()){
						try{
						ClusterQuery qc = new ClusterQuery(
								cTree.getSelectedCollection().
								getCollectionID(),db, 
								"Cluster Query", commentField.getText(), false, filenames,d);
						qc.setDistanceMetric(DistanceMetric.EUCLIDEAN_SQUARED);
							System.out.println("setting cursor type");
							qc.setCursorType(Cluster.DISK_BASED);
						try{
							qc.divide();
						}catch (NoSubCollectionException sce){
							JOptionPane.showMessageDialog(this, "No clusters found for these particles");
						}
						
						dispose();
					} catch (AssertionError ae){
						JOptionPane.showMessageDialog(this, "One or more of the particles is empty.");
						dispose();
					}
					}
					else
						JOptionPane.showMessageDialog(this, "Please select a file to cluster on");
				}
			}
			catch(Exception e1){
				System.out.println("not a number");
				e1.printStackTrace();
				JOptionPane.showMessageDialog(this, "Please enter a number between 0.0 and 2.0 for the distance");
			}
			
		}
		else if (source == cancelButton) {
			dispose();
		}
			
	}


}
