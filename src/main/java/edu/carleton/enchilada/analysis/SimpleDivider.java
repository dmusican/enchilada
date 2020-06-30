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
 * The Original Code is EDAM Enchilada's SimpleDivider class.
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
 * Created on Aug 20, 2004
 *
 * By Ben Anderson
 */
package edu.carleton.enchilada.analysis;

import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.database.Database;

/**
 * @author andersbe
 *
 * SimpleDivider is a test class.  It divides a collection in half.  
 *
 */
public class SimpleDivider extends CollectionDivider {

	/**
	 * @param cID
	 * @param database
	 * @param name
	 * @param comment
	 */
	public SimpleDivider(int cID, Database database) {
		super(cID, database, "halfandhalf", "testdivder");
	}

	/* (non-Javadoc)
	 * @see analysis.CollectionDivider#cursorType(int)
	 */
	public boolean setCursorType(int type) {
		switch (type) {
		case CollectionDivider.DISK_BASED :
			curs = db.getBinnedCursor(collection);
			return true;
		case CollectionDivider.STORE_ON_FIRST_PASS : 
			//TODO REMOVE MEMORY BINNED CURSOR IF UNNECCESARY
			curs = db.getMemoryBinnedCursor(collection);
			return true;
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see analysis.CollectionDivider#divide()
	 */
	public int divide() {
		if (curs == null)
			curs = db.getBinnedCursor(collection);
		createSubCollection();
		createSubCollection();
		ParticleInfo temp;
		boolean which = false;
		while (curs.next())
		{
			temp = curs.getCurrent();
			if (which)
			{
				putInSubCollection(temp.getATOFMSParticleInfo().
						getAtomID(),1);
				which = false;
			}
			else
			{
				putInSubCollection(temp.getATOFMSParticleInfo().
						getAtomID(),2);
				which = true;
			}
		}
		return 0;
	}
}
