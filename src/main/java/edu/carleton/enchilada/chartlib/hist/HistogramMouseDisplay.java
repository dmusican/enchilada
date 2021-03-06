package edu.carleton.enchilada.chartlib.hist;

import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Point2D;

import javax.swing.*;
import javax.swing.border.BevelBorder;

import java.awt.*;

/**
 * Tells the user what m/z value and relative area and hit count they're looking at.
 * 
 * @author smitht
 *
 */

public class HistogramMouseDisplay extends JPanel
	implements MouseMotionListener 
{
	private HistogramsPlot chart;
	private JLabel xLabel, xValue, yLabel, yValue, zLabel, zValue;

	public HistogramMouseDisplay(HistogramsPlot chart) {
		this(chart, true);
	}

	public HistogramMouseDisplay(HistogramsPlot chart, boolean verticalAlign) {
		super();
		this.chart = chart;
		chart.addMouseMotionListener(this);
		
		if (verticalAlign) {
			this.setLayout(new GridLayout(3,1));
		}
		else {
			this.setLayout(new GridLayout(1,3));
		}
		xLabel = new JLabel("m/z:");
		xValue = new JLabel("        ");
		
		yLabel = new JLabel("Rel. Area:");
		yValue = new JLabel("        ");
		
		zLabel = new JLabel("Count:");
		zValue = new JLabel("        ");
		
		JPanel temp;
		temp = new JPanel();
		temp.add(xLabel);
		temp.add(xValue);
		this.add(temp);
		
		temp = new JPanel();
		temp.add(yLabel);
		temp.add(yValue);
		this.add(temp);
		
		temp = new JPanel();
		temp.add(zLabel);
		temp.add(zValue);
		this.add(temp);
		
		this.setBorder(new BevelBorder(BevelBorder.RAISED));
		
		this.validate();
		this.setMinimumSize(this.getSize());
	}
	
	public void mouseDragged(MouseEvent e) {
		mouseMoved(e);
	}

	public void mouseMoved(MouseEvent e) {
		assert(e.getSource().equals(chart));  
		// actually, might be the zoomable chart.
		// do clicks go through the looking glass?
		
		Point p = e.getPoint();
		
		Component source = chart.findComponentAt(p);
		if (! (source instanceof HistogramsChartArea)) 
			return;
		HistogramsChartArea ca = (HistogramsChartArea) source;
		
		/*
		 *  We are a mouselistener on the HistogramPlot, which has a different
		 *  coordinate system than the charts.  So for the calculation of the
		 *  data value to be correct, the point must get translated to the
		 *  right coordinates.
		 */
		Point destCoords = source.getLocationOnScreen();
		Point sourceCoords = chart.getLocationOnScreen();
		p.translate(sourceCoords.x - destCoords.x, sourceCoords.y - destCoords.y);
		
		if (ca.isInDataArea(p)) {
			Point2D.Double dataPoint = ca.getDataValueForPoint(p);
			
			xValue.setText(ca.xLabel(dataPoint.getX()));
			yValue.setText(ca.yLabel(dataPoint.getY()));
			zValue.setText("" + ca.getCountAt((int) dataPoint.getX(), (float) dataPoint.getY()));
		}
	}
}
