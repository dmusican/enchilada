package edu.carleton.enchilada.gui;

import java.awt.Component;
import java.text.DecimalFormat;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

public class FloatRenderer extends DefaultTableCellRenderer{
	DecimalFormat floatFormat;
	
    public FloatRenderer() {
    	// we only use 7 digits after the decimal place because the internal
    	// representation of some numbers is not as clean, e.g. .005 is 
    	// 0.004999999888241291 internally.  No need to show the crummy-looking
    	// decimal numbers.
    	floatFormat = new DecimalFormat("#.#######");
    }

    public Component getTableCellRendererComponent(
                            JTable table, Object value,
                            boolean isSelected, boolean hasFocus,
                            int row, int column) {
    	String formatted = floatFormat.format(((Float) value).floatValue());
    	Component result=super.getTableCellRendererComponent(table, formatted, isSelected, hasFocus, row, column);
    	return result;
    }	
}
