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

package edu.carleton.enchilada.gui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

     /**
	 * This is the updated TimePanel class. The old class used pulldowns for date and time selection, 
	 * which was not very easy, along with being incorporated into AggregateWindow.
	 * Instead, it now uses a JTextField that updates on every change within it. The parsing style 
	 * is configurable through radio buttons, and the TimePanel adopts whatever style is currently selected
	 * upon creation.
	 * This class now represents a refactoring out of AggregateWindow such that both Aggregate and Query can
	 * share the same user interface elements. Be sure any changes here work in both menus.
	 * 
	 * @author rzeszotj
	 *
	 */
	public class TimePanel extends JPanel implements DocumentListener {
		private boolean isInterval;
		
		private SimpleDateFormat NADate_1 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		private SimpleDateFormat NADate_2 = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
		private SimpleDateFormat EDate_1 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		private SimpleDateFormat EDate_2 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		private SimpleDateFormat Hours_1 = new SimpleDateFormat("HH:mm:ss");
		private JRadioButton eurDateRadio, naDateRadio;
		
		private Date currentTime;
		private JTextField timeField;
		private ParsePosition p = new ParsePosition(0);
		
		private final Color ERROR = new Color(255,128,128);
		private final Color GOOD = Color.WHITE;
		
		public TimePanel(String name, Calendar init, boolean interval, JRadioButton NA, JRadioButton EU) {
			setLayout(new FlowLayout(FlowLayout.LEFT, 2, 0));
			isInterval = interval;
			eurDateRadio = EU;
			naDateRadio = NA;
			
			JLabel label = new JLabel(name);
			if (isInterval)
			{
				timeField = new JTextField(getTimeString(init),9);
			}
			else
			{
				timeField = new JTextField(getTimeString(init),19);
			}
			
			timeField.getDocument().addDocumentListener(this);
	        
			label.setPreferredSize(new Dimension(70, 20));
			
			add(label);
			add(timeField);
			updateDate();
		}
		
		/**
		 * Formats a calendar into a string using whatever style is selected
		 * @param c The calendar to format
		 */
	    private String getTimeString(Calendar c)
	    {
	    	String s = "";
	    	Date cur = c.getTime();
	    	if(isInterval)
	    		s = Hours_1.format(cur);
			else if(naDateRadio.isSelected())
			{
				s = NADate_1.format(cur);
			}
			else if(eurDateRadio.isSelected())
			{
				s = EDate_1.format(cur);
			}
	    	return s;
	    }
	    
	    /**
		 * Formats the current entry into a string using whatever style is selected
		 */
	    public String getTimeString() {
	    	updateDate();
			return NADate_1.format(currentTime);
	    }
	    
	    /**
		 * Updates currentTime with text in timeField, changing background
		 */
		private void updateDate()
		{
			//Get text from the text box, if blank, show error and stop
			String cur = null;
			try
			{
				cur = timeField.getText();
			}
			catch(NullPointerException e)
			{
				timeField.setBackground(ERROR);
				currentTime = null;
				return;
			}
			//Make sure we have a string with something in it
			p.setIndex(0);
			if (cur == null)
			{
				timeField.setBackground(ERROR);
				return;
			}
			
			//Parse the string depending on what is selected
			if(isInterval)
			{
				currentTime = Hours_1.parse(cur,p);
			}
			else if(naDateRadio.isSelected())
			{
				currentTime = NADate_1.parse(cur,p);
				if (currentTime == null)
				{
					p.setIndex(0);
					currentTime = NADate_2.parse(cur,p);
				}
			}
			else if(eurDateRadio.isSelected())
			{
				currentTime = EDate_1.parse(cur,p);
				if (currentTime == null)
				{
					p.setIndex(0);
					currentTime = EDate_2.parse(cur,p);
				}
			}
			else
				timeField.setBackground(ERROR);
			
			//If we parsed correctly, color it GOOD, otherwise color an ERROR
			if(currentTime == null)
			{
				timeField.setBackground(ERROR);
			}
			else
			{
				timeField.setBackground(GOOD);
			}
		}
		
		/**
		 * Returns true if an unparsable entry is present
		 */
		public boolean isBad()
		{
			if (currentTime == null)
				return true;
			else
				return false;
		}
		
		/**
		 * Returns a calendar object based on the string in textField
		 */
		public GregorianCalendar getDate() {
			GregorianCalendar c = new GregorianCalendar();
			c.setTime(currentTime);
			return c;
		}

		public Date getRawDate() {
			return currentTime;
		}

		public void insertUpdate(DocumentEvent ev) {
	        updateDate();
	    }
	    
	    public void removeUpdate(DocumentEvent ev) {
	        updateDate();
	    }
	    
	    public void changedUpdate(DocumentEvent ev) {
	    }
	}