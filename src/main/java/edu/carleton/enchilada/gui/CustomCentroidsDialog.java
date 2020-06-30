package edu.carleton.enchilada.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.TableColumn;

// This is pretty much a verbatim copy of ClusterQueryDialog, except it passes the centroids onto a clustering algorithm
// Michael Murphy 2014

public class CustomCentroidsDialog extends JDialog implements ActionListener{
	
	private JButton okButton;
	private JButton cancelButton;
	private JTextField distance, commentField;
	private ClusterTableModel clusterTableModel;
	private int dataSetCount;
	private AbstractClusterDialog parent = null;
	private boolean kCluster = false;
	
	/**
	 * Extends JDialog to form a modal dialogue box for setting
	 * cluster centers.  
	 * @param owner The parent frame of this dialog box, should be the 
	 * main frame.    
	 * @throws java.awt.HeadlessException From the constructor of 
	 * JDialog.  
	 */
	public CustomCentroidsDialog(AbstractClusterDialog owner) throws HeadlessException {
		super(owner, "Cluster with Chosen Centers", true);
		this.parent = owner;
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
	
		buttonPane.add(Box.createRigidArea(new Dimension(210,0)));
		buttonPane.add(Box.createHorizontalGlue());
		buttonPane.add(okButton);
		buttonPane.add(Box.createRigidArea(new Dimension(10,0)));
		buttonPane.add(cancelButton);
		
		add(panel, BorderLayout.CENTER);
		add(buttonPane, BorderLayout.SOUTH);
		
		setVisible(true);	
	}
	
	// multiple select would be great here
	private JTable getClusterTable()
	{
		clusterTableModel = new ClusterTableModel();
		JTable pTable = new JTable(clusterTableModel);		
		TableColumn[] tableColumns = new TableColumn[1];
		for (int i = 0; i < 1; i++)
			tableColumns[i] = pTable.getColumnModel().getColumn(i+1);
		tableColumns[0].setCellEditor(
				new MultiFileDialogPickerEditor("csv","Center",this));
		tableColumns[0].setPreferredWidth(395);
		
		TableColumn numColumn = pTable.getColumnModel().getColumn(0);
		numColumn.setPreferredWidth(5);
		return pTable;
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object source = e.getSource();
		if (source == okButton) {
			ArrayList<String> filenames = new ArrayList<String>();
			for(int i = 0; i< clusterTableModel.getRowCount(); i++){
				if(!clusterTableModel.getValueAt(i,1).equals("")){
					// hack to accept multiple filenames in a cell
					filenames.addAll(Arrays.asList(((String)clusterTableModel.getValueAt(i,1)).split(",")));
				}
			}
			if(filenames.size()>0){
				try {
					parent.setCentroidFilenames(filenames);
					dispose();
				} catch (AssertionError ae){
					JOptionPane.showMessageDialog(this, "One or more of the particles is empty.");
					dispose();
				}
			}
			else
				JOptionPane.showMessageDialog(this, "Please select a file to cluster on");
		}
		else if (source == cancelButton) {
			dispose();
		}
	}
}
