package edu.carleton.enchilada.gui;

import java.awt.*;

import javax.swing.JOptionPane;

import edu.carleton.enchilada.dataImporters.*;
import edu.carleton.enchilada.dataImporters.TSImport.UnsupportedFormatException;
import edu.carleton.enchilada.database.Database;

import edu.carleton.enchilada.errorframework.*;

/**
 * GUI for importing CSV files, using a "task" file.  An example of such a file
 * is in the Importation Files directory of the source tree.
 * 
 * @author smitht
 */

public class FlatImportGUI {
	private TSImport importer;
	private Frame parent;
	public static final String dateMessage = "The date is stored in an improper format." +
	"  The time-stamp must be stored as 'yyyy-mm-dd hh:mm:ss' with the hour being 0-24." +
	"  You can correct your date format by opening the file in a spreadsheet " +
	"application and creating a custom format.";	


	public FlatImportGUI(Frame parent, Database db) {
		this.parent = parent;

		/*FileDialog fileChooser = new FileDialog(this, 
                "Choose a place to write the plumes:",
                 FileDialog.LOAD);
		//fileChooser.setFile(fileFilter);
		fileChooser.setVisible(true);
		String filename = fileChooser.getDirectory()+fileChooser.getFile();
		*/ 
		
		Object[] options = {"bulk .task file",
		".csv file",
		"Cancel"};
		int n = JOptionPane.showOptionDialog(parent,
		"What do you want to import?","Select Your Import Format",
		JOptionPane.YES_NO_CANCEL_OPTION,
		JOptionPane.QUESTION_MESSAGE,
		null,
		options,
		options[0]);
		
		if(n==2)return;
		if(n==0){


			FileDialog fileChooser = new FileDialog(parent, 
					"Locate a .task File:",
					FileDialog.LOAD);
			fileChooser.setFile("*.task");
			fileChooser.setVisible(true);
			String filename = fileChooser.getDirectory()+fileChooser.getFile();
			if (fileChooser.getFile() == null) {
				return;
				// the user selected cancel, cancel the operation
			}
			importer = new TSImport(db, parent, true);

			try {
				importer.readTaskFile(filename);
			} catch(UnsupportedFormatException u){
				//If the date format was wrong, let the user know, because they can fix it
				JOptionPane.showMessageDialog(parent, dateMessage);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Exception importing (generally)");
				ErrorLogger.writeExceptionToLogAndPrompt("FlatImport",e.toString());
			}
		}else if(n==1){
			FileDialog fileChooser = new FileDialog(parent, 
					"Locate a .csv File:",
					FileDialog.LOAD);
			fileChooser.setFile("*.csv");
			fileChooser.setVisible(true);
			if (fileChooser.getFile() == null) {
				return;
			}
			String filename = fileChooser.getDirectory()+fileChooser.getFile();

			importer = new TSImport(db, parent, true);

			try {
				importer.readCSVFile(filename);

			} catch(UnsupportedFormatException u){
				//If the date format was wrong, let the user know, because they can fix it
				JOptionPane.showMessageDialog(parent, dateMessage);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Exception importing (generally)");
				ErrorLogger.writeExceptionToLogAndPrompt("FlatImport",e.toString());
				JOptionPane.showMessageDialog(parent, "There was an error importing data.  Your data may not be" +
						" in the correct format.  The first column must be a date.  All other columns must be" +
						" data values.");
			}
			System.out.println("imported data");
		}
	}
}
