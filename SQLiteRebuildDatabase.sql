--/* ***** BEGIN LICENSE BLOCK *****
-- * Version: MPL 1.1/GPL 2.0/LGPL 2.1
-- *
-- * The contents of this file are subject to the Mozilla Public License Version
-- * 1.1 (the "License"); you may not use this file except in compliance with
-- * the License. You may obtain a copy of the License at
-- * http://www.mozilla.org/MPL/
-- *
-- * Software distributed under the License is distributed on an "AS IS" basis,
-- * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
-- * for the specific language governing rights and limitations under the
-- * License.
-- *
-- * The Original Code is EDAM Enchilada's database creation code.
-- *
-- * The Initial Developer of the Original Code is
-- * The EDAM Project at Carleton College.
-- * Portions created by the Initial Developer are Copyright (C) 2005
-- * the Initial Developer. All Rights Reserved.
-- *
-- Contributor(s):
-- * Ben J Anderson andersbe@gmail.com
-- * David R Musicant dmusican@carleton.edu
-- * Anna Ritz ritza@carleton.edu
-- *
-- * Alternatively, the contents of this file may be used under the terms of
-- * either the GNU General Public License Version 2 or later (the "GPL"), or
-- * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
-- * in which case the provisions of the GPL or the LGPL are applicable instead
-- * of those above. If you wish to allow use of your version of this file only
-- * under the terms of either the GPL or the LGPL, and not to allow others to
-- * use your version of this file under the terms of the MPL, indicate your
-- * decision by deleting the provisions above and replace them with the notice
-- * and other provisions required by the GPL or the LGPL. If you do not delete
-- * the provisions above, a recipient may use your version of this file under
-- * the terms of any one of the MPL, the GPL or the LGPL.
-- *
-- * ***** END LICENSE BLOCK ***** */
CREATE TABLE DBInfo (Name VARCHAR(50) PRIMARY KEY, Value VARCHAR(7500))
-- %version-next% Don't change the spacing of the version number, it gets parsed by the program as well as by SQL.  Also, don't put any lines between this one and the insertion of the version.  Versions are compared by STRING equality.
INSERT INTO DBInfo VALUES ('Version','Sept2006.1')
CREATE TABLE Collections (CollectionID INT PRIMARY KEY, Name VARCHAR(8000), Comment VARCHAR(8000), Description TEXT, Datatype VARCHAR(8000))
INSERT INTO Collections VALUES (0, 'ROOT', 'root for unsynchronized data','root', 'root')
INSERT INTO Collections VALUES (1, 'ROOT-SYNCHRONIZED', 'root for synchronized data','root', 'root')
CREATE TABLE AtomMembership (CollectionID INT, AtomID INT, PRIMARY KEY (CollectionID, AtomID))
CREATE TABLE CollectionRelationships(ParentID INT, ChildID INT PRIMARY KEY)
CREATE TABLE CenterAtoms (AtomID INT PRIMARY KEY, CollectionID INT UNIQUE)
CREATE TABLE DataSetMembers (OrigDataSetID INT, AtomID INT PRIMARY KEY)
CREATE TABLE MetaData (Datatype VARCHAR(8000), ColumnName VARCHAR(8000), ColumnType VARCHAR(8000), PrimaryKey BIT, TableID INT, ColumnOrder INT, PRIMARY KEY (Datatype, ColumnName, TableID))
INSERT INTO MetaData VALUES ('ATOFMS','DataSetID', 'INT', 1, 0, 1)
INSERT INTO MetaData VALUES ('ATOFMS','DataSet', 'VARCHAR(8000)', 0, 0, 2)
INSERT INTO MetaData VALUES ('ATOFMS','MassCalFile','VARCHAR(8000)', 0, 0, 3)
INSERT INTO MetaData VALUES ('ATOFMS','SizeCalFile','VARCHAR(8000)', 0, 0, 4)
INSERT INTO MetaData VALUES ('ATOFMS','MinHeight','INT', 0,0,5)
INSERT INTO MetaData VALUES ('ATOFMS','MinArea','INT', 0,0,6)
INSERT INTO MetaData VALUES ('ATOFMS','MinRelArea','REAL',0,0,7)
INSERT INTO MetaData VALUES ('ATOFMS','Autocal','BIT', 0,0,8)
INSERT INTO MetaData VALUES ('ATOFMS','AtomID', 'INT', 1, 1,1)
INSERT INTO MetaData VALUES ('ATOFMS','Time','DATETIME', 0,1,2)
INSERT INTO MetaData VALUES ('ATOFMS','LaserPower','REAL', 0,1,3)
INSERT INTO MetaData VALUES ('ATOFMS','Size','REAL', 0,1,4)
INSERT INTO MetaData VALUES ('ATOFMS','ScatDelay','INT', 0,1,5)
INSERT INTO MetaData VALUES ('ATOFMS','OrigFilename','VARCHAR(8000)', 0,1,6)
INSERT INTO MetaData VALUES ('ATOFMS','AtomID','INT', 1, 2, 1)
INSERT INTO MetaData VALUES ('ATOFMS','PeakLocation','REAL', 1,2,2)
INSERT INTO MetaData VALUES ('ATOFMS','PeakArea','INT', 0,2,3)
INSERT INTO MetaData VALUES ('ATOFMS','RelPeakArea','REAL', 0,2,4)
INSERT INTO MetaData VALUES ('ATOFMS','PeakHeight','INT', 0,2,5)
INSERT INTO MetaData VALUES ('TimeSeries','DataSetID','INT', 1,0,1)
INSERT INTO MetaData VALUES ('TimeSeries','DataSet','VARCHAR(8000)', 0,0,2)
INSERT INTO MetaData VALUES ('TimeSeries','OrigCollectionID','INT', 0,0,3)
INSERT INTO MetaData VALUES ('TimeSeries','IsSynchronized','BIT', 0,0,4)
INSERT INTO MetaData VALUES ('TimeSeries','AtomID','INT', 1,1,1)
INSERT INTO MetaData VALUES ('TimeSeries','Time','DATETIME', 0,1,2)
INSERT INTO MetaData VALUES ('TimeSeries','Value','REAL', 0,1,3)
INSERT INTO MetaData VALUES ('AMS','DataSetID','INT', 1,0,1)
INSERT INTO MetaData VALUES ('AMS','DataSet','VARCHAR(8000)', 0,0,2)
INSERT INTO MetaData VALUES ('AMS','OrigFilename','VARCHAR(8000)', 0,0,3)
INSERT INTO MetaData VALUES ('AMS','TimeSeriesFile','VARCHAR(8000)', 0,0,4)
INSERT INTO MetaData VALUES ('AMS','MassToChargeFile','VARCHAR(8000)', 0,0,5)
INSERT INTO MetaData VALUES ('AMS','AtomID','INT', 1,1,1)
INSERT INTO MetaData VALUES ('AMS','Time','DATETIME', 0,1,2)
INSERT INTO MetaData VALUES ('AMS','AtomID','INT', 1,2,1)
INSERT INTO MetaData VALUES ('AMS','PeakLocation','REAL', 1,2,2)
INSERT INTO MetaData VALUES ('AMS','PeakHeight','REAL', 0,2,3)
CREATE TABLE ATOFMSDataSetInfo (DataSetID INT, "DataSet" VARCHAR(8000), "MassCalFile" VARCHAR(8000), "SizeCalFile" VARCHAR(8000), "MinHeight" INT, "MinArea" INT, "MinRelArea" REAL, "Autocal" BIT,  PRIMARY KEY ("DataSetID"))
CREATE TABLE ATOFMSAtomInfoDense ("AtomID" INT, "Time" TIMESTAMP, "LaserPower" REAL, "Size" REAL, "ScatDelay" INT, "OrigFilename" VARCHAR(8000),  PRIMARY KEY ("AtomID"))
CREATE TABLE ATOFMSAtomInfoSparse ("AtomID" INT, "PeakLocation" REAL, "PeakArea" INT, "RelPeakArea" REAL, "PeakHeight" INT, PRIMARY KEY ("AtomID", "PeakLocation"))
CREATE TABLE TimeSeriesDataSetInfo("DataSetID" INT, "DataSet" VARCHAR(8000), "OrigCollectionID" INT NULL, "IsSynchronized" bit, PRIMARY KEY ("DataSetID"))
CREATE TABLE TimeSeriesAtomInfoDense("AtomID" INT, "Time" TIMESTAMP, "Value" REAL, PRIMARY KEY ("AtomID"))
CREATE TABLE AMSDataSetInfo ("DataSetID" INT, "DataSet" VARCHAR(8000), "OrigFilename" VARCHAR(8000), "TimeSeriesFile" VARCHAR(8000), "MassToChargeFile" VARCHAR(8000), PRIMARY KEY ("DataSetID"))
CREATE TABLE AMSAtomInfoDense ("AtomID" INT, "Time" TIMESTAMP, PRIMARY KEY ("AtomID"))
CREATE TABLE AMSAtomInfoSparse ("AtomID" INT, "PeakLocation" REAL, "PeakHeight" REAL, PRIMARY KEY ("AtomID","PeakLocation"))
CREATE TABLE ValueMaps(ValueMapID INTEGER PRIMARY KEY AUTOINCREMENT, Name VARCHAR(100))
CREATE TABLE ValueMapRanges(ValueMapID INT, Value INT, Low INT, High INT, FOREIGN KEY (ValueMapID) REFERENCES ValueMaps(ValueMapID))
CREATE TABLE IonSignature(IonID INTEGER PRIMARY KEY, IsPositive BIT(1), Name VARCHAR(7000))
CREATE TABLE AtomIonSignaturesRemoved(AtomID INT, IonID INT, PRIMARY KEY (AtomID, IonID), FOREIGN KEY (IonID) REFERENCES IonSignature(IonID))
CREATE TABLE InternalAtomOrder(AtomID INT, CollectionID INT, PRIMARY KEY (CollectionID, AtomID))
