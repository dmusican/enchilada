package edu.carleton.enchilada.chartlib.hist;

import java.awt.*;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.swing.*;

import edu.carleton.enchilada.chartlib.*;

import edu.carleton.enchilada.database.Database;
import edu.carleton.enchilada.externalswing.ProgressTask;
import edu.carleton.enchilada.externalswing.SwingWorker;
import edu.carleton.enchilada.gui.MainFrame;

/**
 * A chart with special stuff for spectrum histograms.
 * @author smitht
 *
 */

public class HistogramsPlot extends Chart {
	private HistogramsChartArea posHistArea, negHistArea;
	private ZeroHistogramChartArea posZerosArea, negZerosArea;
	
	private HistogramDataset[] baseSpectra, brushSpectra;
	
	public HistogramsPlot(final int collID) throws SQLException {
		ProgressTask task = new ProgressTask(null, "Analysing collection", true) {
			public void run() {
				pSetInd(true);
				setStatus("Creating histogram for collection.");
				try {
					baseSpectra = HistogramDataset.analyseCollection(collID, Color.BLACK);
					
					if (terminate)
						return;
					
					// EDT for gui work
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							addDatasets(baseSpectra);
						}
					});
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		};
		task.start();
		// task.start() does not return until the task is complete, since we
		// told the dialog to be modal.
		
		this.validate();
	}
	
	
	public static Database getDB() {
		if (MainFrame.db != null) {
			return MainFrame.db;
		} else {
			Database db = Database.getDatabase();
			db.openConnection();
			return db;
		}
	}

	private void addDatasets(HistogramDataset[] datasets) {
		if (posHistArea == null) {
			posHistArea = new HistogramsChartArea(datasets[0]);
			posHistArea.setTitle("Positive Spectrum");
			posHistArea.setAxisBounds(0, 300, 0, 1);
			posHistArea.setTitleY("Relative Area");
			this.add(posHistArea);
		} else {
			posHistArea.addDataset(datasets[0]);
		}
		if (posZerosArea == null) {
			posZerosArea = new ZeroHistogramChartArea(datasets[0]);
			posZerosArea.setXAxisBounds(0, 300);
			posZerosArea.setTitleY("Peaks Detected");
			this.add(posZerosArea);
		} else {
			//zerosArea.addDataset(dataset);  // not implemented yet.
			System.err.println(
			"HistogramsPlot: zerosArea doesn't know about multiple datasets yet.");
		}
		
		
		if (negHistArea == null) {
			negHistArea = new HistogramsChartArea(datasets[1]);
			negHistArea.setTitle("Negative Spectrum");
			negHistArea.setAxisBounds(0, 300, 0, 1);
			negHistArea.setTitleY("Relative Area");
			this.add(negHistArea);
		} else {
			negHistArea.addDataset(datasets[1]);
		}
		if (negZerosArea == null) {
			negZerosArea = new ZeroHistogramChartArea(datasets[1]);
			negZerosArea.setXAxisBounds(0, 300);
			negZerosArea.setTitleY("Peaks Detected");
			this.add(negZerosArea);
		} else {
			//zerosArea.addDataset(dataset);  // not implemented yet.
			System.err.println(
			"HistogramsPlot: zerosArea doesn't know about multiple datasets yet.");
		}
	}
	
	public void removeDatasets(HistogramDataset[] sets) {
		for (HistogramDataset set : sets) {
			posHistArea.removeDataset(set);
			negHistArea.removeDataset(set);
			posZerosArea.removeDataset(set);
			negZerosArea.removeDataset(set);
		}
	}


	@Override
	protected JPanel createChartPanel() {
		JPanel chartPanel = super.createChartPanel();
		chartPanel.setLayout(new BoxLayout(chartPanel, BoxLayout.Y_AXIS));
		chartPanel.validate();
		return chartPanel;
	}
	
	/**
	 * Decides whether the point is above the negative or positive graph.
	 * 
	 * @param p a point relative to the HistogramsPlot coord system.
	 * @return 0 if positive, 1 if negative, -1 if neither.
	 */
	public int whichGraph(Point p) {
		Component c = findComponentAt(p);
		if (c == posHistArea || c == posZerosArea) {
			return 0;
		} else if (c == negHistArea || c == negZerosArea) {
			return 1;
		} else return -1;
	}


	public void setBrushSelection(final ArrayList<BrushSelection> selected) {
		if (brushSpectra != null) removeDatasets(brushSpectra);
		repaint();
		
		final HistogramsPlot hplot = this;
		SwingWorker sw = new SwingWorker() {
			@Override
			public Object construct() {
				final String oldTitle = hplot.getTitle();
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						hplot.setTitle("Finding matching particles...");
					}
				});
				
				hplot.brushSpectra 
					= HistogramDataset.getSelection(baseSpectra, selected);
				hplot.brushSpectra[0].color = Color.RED;
				hplot.brushSpectra[1].color = Color.RED;
				
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						hplot.setTitle(oldTitle);
					}
				});
				return null;
			}
			
			@Override
			public void finished() {
				hplot.addDatasets(brushSpectra);
				hplot.repaint();
			}
			
		};
		
		sw.start();
	}

}
