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
 * The Original Code is EDAM Enchilada's AtomTableModel class.
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


/*
 * Created on Dec 3, 2004
 */
package edu.carleton.enchilada.gui;

import javax.swing.table.AbstractTableModel;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;


import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database;

/**
 * This table model goes straight to the database to lookup 
 * particles.  It is also never used?
 * 
 * TODO: Implement cacheing (and figure out how to spell it as a 
 * verb)
 * @author andersbe
 */
public class AtomTableModel extends AbstractTableModel {
	Database db;
	int collectionID;
	int lastCheckedRow = -1;
	CollectionCursor curs;
	/**
	 * 
	 */
	public AtomTableModel(Database db, int collectionID) {
		super();
		this.db = db;
		this.collectionID = collectionID;
		curs = db.getAtomInfoOnlyCursor(db.getCollection(collectionID));
	}

	/* (non-Javadoc)
	 * @see javax.swing.table.TableModel#getRowCount()
	 */
	public int getRowCount() {
		return db.getCollectionSize(collectionID);
	}

	/* (non-Javadoc)
	 * @see javax.swing.table.TableModel#getColumnCount()
	 */
	public int getColumnCount() {
		return 4;
	}

	/* (non-Javadoc)
	 * @see javax.swing.table.TableModel#getValueAt(int, int)
	 */
	public Object getValueAt(int rowIndex, int columnIndex) {
		ParticleInfo pInfo = null;
		if (rowIndex == lastCheckedRow + 1)
		{
			curs.next();
			pInfo = curs.getCurrent();
		}
		else
		{
			curs.reset();
			for (int i = 0; i <= rowIndex; i++)
				curs.next();
			pInfo = curs.getCurrent();
		}
		lastCheckedRow = rowIndex;
		
		if (columnIndex == 0)
		{
			return new Integer(
					pInfo.getATOFMSParticleInfo().getAtomID());
		}
		else if (columnIndex == 1)
		{
			return pInfo.getATOFMSParticleInfo().getFilename();
		}
		else if (columnIndex == 2)
		{
			return new Float(pInfo.getATOFMSParticleInfo().getSize());
		}
		else if (columnIndex == 3)
		{
			return pInfo.getATOFMSParticleInfo().getDateString();
		}
		else
			return null;
	}
	
	public void close() {
		curs.close();
	}

}
