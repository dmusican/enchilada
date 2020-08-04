# SpASMSdb histogram generation script - size and m/z binning
# Michael Murphy 2013
# Server name needs to be changed for individual PC
# Dependencies: pyodbc, numpy
import os
import sqlite3
import numpy
import sys
import datetime
import time

print '\n*** Histogram generator for EDAM Enchilada ***'

cnxn = sqlite3.connect(os.path.expanduser('~/enchilada/sqlitedata/SpASMSdb'))

qtype = raw_input('\nEnter query type (H-height sum, R-rel. area sum, A-area sum, C-peak count, S-size count): ').upper().strip()
if qtype not in ('H', 'R', 'A', 'C', 'S'):
	sys.exit('Invalid query type')
options = {'H':'[PeakHeight]','R':'[RelPeakArea]','A':'[PeakArea]'}
qt = options.get(qtype)

numcolls = int(raw_input('\nEnter number of collections: '))
collnames = [raw_input('Enter name of collection ' + str(x+1) + ': ') for x in range(0,numcolls)]

collids = []
for cn in collnames:
	collcsr = cnxn.cursor()
	collcsr.execute('SELECT DISTINCT [CollectionID] FROM Collections WHERE [Name] = \'' + cn + '\' AND [Datatype] = \'ATOFMS\'')
	try:
		collids.append(collcsr.fetchall()[0][0])
	except:
		sys.exit('Invalid collection name entered')

ltime = raw_input('\nEnter start time (YYYY-MM-DD hh:mm:ss, blank for all): ')
if ltime == '':
	ltime = '1753-01-01 00:00:00' # minimum SQL date
utime = raw_input('Enter end time (YYYY-MM-DD hh:mm:ss, blank for all): ')
if utime == '':
	utime = '9999-12-31 23:59:59' # maximum SQL date
timeres = int(raw_input('Enter time resolution (seconds): ')) # only works up to hour scale

if timeres >= 3600:
	hf = '/' + str(timeres / 3600)
	hl = timeres / 3600
	mf = '*0'
	ml = 1
	sf = '*0'
	sl = 1
elif 3600 > timeres >= 60:
	hf = ''
	hl = 1
	mf = '/' + str(timeres / 60)
	ml = timeres / 60
	sf = '*0'
	sl = 1
else:
	hf = ''
	hl = 1
	mf = ''
	ml = 1
	sf = '/' + str(timeres)
	sl = timeres

if qtype == 'S':
        choice = raw_input('\nEnter 16, 32, 64, or 128 for standard bins per decade or press enter to create your own bins: ')
        if choice == '16':
                numbins = 49
                bins = [0, 0.010, 0.012, 0.013, 0.015, 0.018, 0.021, 0.024, 0.027, 0.032, 0.037, 0.042, 0.049, 0.056, 0.065, 0.075, 0.087, 0.100, 0.115,
                        0.133, 0.154, 0.178, 0.205, 0.237, 0.274, 0.316, 0.365, 0.422, 0.487, 0.562, 0.649, 0.750, 0.866, 1.000, 1.155, 1.334, 1.540,
                        1.778, 2.054, 2.371, 2.738, 3.162, 3.652, 4.217, 4.870, 5.623, 6.494, 7.499, 8.660, 10.000]
                cols = 'd.[AtomID], d.[Time], d.[Size] '
                join = ', '
        elif choice == '32':
                numbins = 97
                bins = [0, 0.010, 0.011, 0.012, 0.012, 0.013, 0.014, 0.015, 0.017, 0.018, 0.019, 0.021, 0.022, 0.024, 0.025, 0.027, 0.029, 0.032,
                        0.034, 0.037, 0.039, 0.042, 0.045, 0.049, 0.052, 0.056, 0.060, 0.065 ,0.070, 0.075, 0.081, 0.087, 0.093, 0.100, 0.107, 0.115,
                        0.124, 0.133, 0.143, 0.154, 0.165, 0.178, 0.191, 0.205, 0.221, 0.237, 0.255, 0.274, 0.294, 0.316, 0.340, 0.365, 0.392, 0.422,
                        0.453, 0.487, 0.523, 0.562, 0.604, 0.649, 0.698, 0.750, 0.806, 0.866, 0.931, 1.000, 1.075, 1.155, 1.241, 1.334, 1.433, 1.540,
                        1.655, 1.778, 1.911, 2.054, 2.207, 2.371, 2.548, 2.738, 2.943, 3.162, 3.398, 3.652, 3.924, 4.217, 4.532, 4.870, 5.233, 5.623,
                        6.043, 6.494, 6.978, 7.499, 8.058, 8.660, 9.306, 10.000]
                cols = 'd.[AtomID], d.[Time], d.[Size] '
                join = ', '
        elif choice == '64':
                numbins = 193
                bins = [0, 0.010000, 0.010366, 0.010746, 0.011140, 0.011548, 0.011971, 0.012409, 0.012864, 0.013335, 0.013824, 0.014330, 0.014855,
                        0.015399, 0.015963, 0.016548, 0.017154, 0.017783, 0.018434, 0.019110, 0.019810, 0.020535, 0.021288, 0.022067, 0.022876,
                        0.023714, 0.024582, 0.025483, 0.026416, 0.027384, 0.028387, 0.029427, 0.030505, 0.031623, 0.032781, 0.033982, 0.035227,
                        0.036517, 0.037855, 0.039242, 0.040679, 0.042170, 0.043714, 0.045316, 0.046976, 0.048697, 0.050481, 0.052330, 0.054247,
                        0.056234, 0.058294, 0.060430, 0.062643, 0.064938, 0.067317, 0.069783, 0.072339, 0.074989, 0.077737, 0.080584, 0.083536,
                        0.086596, 0.089769, 0.093057, 0.096466, 0.100000, 0.103663, 0.107461, 0.111397, 0.115478, 0.119709, 0.124094, 0.128640,
                        0.133352, 0.138237, 0.143301, 0.148551, 0.153993, 0.159634, 0.165482, 0.171544, 0.177828, 0.184342, 0.191095, 0.198096,
                        0.205353, 0.212875, 0.220673, 0.228757, 0.237137, 0.245824, 0.254830, 0.264165, 0.273842, 0.283874, 0.294273, 0.305053,
                        0.316228, 0.327812, 0.339821, 0.352269, 0.365174, 0.378552, 0.392419, 0.406794, 0.421697, 0.437144, 0.453158, 0.469759,
                        0.486968, 0.504807, 0.523299, 0.542469, 0.562341, 0.582942, 0.604296, 0.626434, 0.649382, 0.673170, 0.697831, 0.723394,
                        0.749894, 0.777365, 0.805842, 0.835363, 0.865964, 0.897687, 0.930572, 0.964662, 1.000000, 1.036633, 1.074608, 1.113974,
                        1.154782, 1.197085, 1.240938, 1.286397, 1.333521, 1.382372, 1.433013, 1.485508, 1.539927, 1.596339, 1.654817, 1.715438,
                        1.778279, 1.843423, 1.910953, 1.980957, 2.053525, 2.128752, 2.206734, 2.287573, 2.371374, 2.458244, 2.548297, 2.641648,
                        2.738420, 2.838736, 2.942727, 3.050528, 3.162278, 3.278121, 3.398208, 3.522695, 3.651741, 3.785515, 3.924190, 4.067944,
                        4.216965, 4.371445, 4.531584, 4.697589, 4.869675, 5.048066, 5.232991, 5.424691, 5.623413, 5.829415, 6.042964, 6.264335,
                        6.493816, 6.731704, 6.978306, 7.233942, 7.498942, 7.773650, 8.058422, 8.353625, 8.659643, 8.976871, 9.305720, 9.646616,
                        10.000000]
                cols = 'd.[AtomID], d.[Time], d.[Size] '
                join = ', '
        elif choice == '128':
                numbins = 387
                bins = [0, 0.010000, 0.010182, 0.010366, 0.010554, 0.010746, 0.010941, 0.011140, 0.011342, 0.011548, 0.011757, 0.011971, 0.012188,
                        0.012409, 0.012635, 0.012864, 0.013097, 0.013335, 0.013577, 0.013824, 0.014075, 0.014330, 0.014590, 0.014855, 0.015125, 0.015399,
                        0.015679, 0.015963, 0.016253, 0.016548, 0.016849, 0.017154, 0.017466, 0.017783, 0.018106, 0.018434, 0.018769, 0.019110, 0.019456,
                        0.019810, 0.020169, 0.020535, 0.020908, 0.021288, 0.021674, 0.022067, 0.022468, 0.022876, 0.023291, 0.023714, 0.024144, 0.024582,
                        0.025029, 0.025483, 0.025946, 0.026416, 0.026896, 0.027384, 0.027881, 0.028387, 0.028903, 0.029427, 0.029961, 0.030505, 0.031059,
                        0.031623, 0.032197, 0.032781, 0.033376, 0.033982, 0.034599, 0.035227, 0.035866, 0.036517, 0.037180, 0.037855, 0.038542, 0.039242,
                        0.039954, 0.040679, 0.041418, 0.042170, 0.042935, 0.043714, 0.044508, 0.045316, 0.046138, 0.046976, 0.047829, 0.048697, 0.049581,
                        0.050481, 0.051397, 0.052330, 0.053280, 0.054247, 0.055232, 0.056234, 0.057255, 0.058294, 0.059352, 0.060430, 0.061527, 0.062643,
                        0.063780, 0.064938, 0.066117, 0.067317, 0.068539, 0.069783, 0.071050, 0.072339, 0.073653, 0.074989, 0.076351, 0.077737, 0.079148,
                        0.080584, 0.082047, 0.083536, 0.085053, 0.086596, 0.088168, 0.089769, 0.091398, 0.093057, 0.094746, 0.096466, 0.098217, 0.100000,
                        0.101815, 0.103663, 0.105545, 0.107461, 0.109411, 0.111397, 0.113419, 0.115478, 0.117574, 0.119709, 0.121881, 0.124094, 0.126346,
                        0.128640, 0.130975, 0.133352, 0.135773, 0.138237, 0.140746, 0.143301, 0.145902, 0.148551, 0.151247, 0.153993, 0.156788, 0.159634,
                        0.162531, 0.165482, 0.168485, 0.171544, 0.174658, 0.177828, 0.181056, 0.184342, 0.187688, 0.191095, 0.194564, 0.198096, 0.201691,
                        0.205353, 0.209080, 0.212875, 0.216739, 0.220673, 0.224679, 0.228757, 0.232910, 0.237137, 0.241442, 0.245824, 0.250287, 0.254830,
                        0.259455, 0.264165, 0.268960, 0.273842, 0.278813, 0.283874, 0.289026, 0.294273, 0.299614, 0.305053, 0.310590, 0.316228, 0.321968,
                        0.327812, 0.333762, 0.339821, 0.345989, 0.352269, 0.358664, 0.365174, 0.371803, 0.378552, 0.385423, 0.392419, 0.399542, 0.406794,
                        0.414178, 0.421697, 0.429351, 0.437144, 0.445079, 0.453158, 0.461384, 0.469759, 0.478286, 0.486968, 0.495807, 0.504807, 0.513970,
                        0.523299, 0.532798, 0.542469, 0.552316, 0.562341, 0.572549, 0.582942, 0.593523, 0.604296, 0.615265, 0.626434, 0.637804, 0.649382,
                        0.661169, 0.673170, 0.685390, 0.697831, 0.710497, 0.723394, 0.736525, 0.749894, 0.763506, 0.777365, 0.791476, 0.805842, 0.820470,
                        0.835363, 0.850526, 0.865964, 0.881683, 0.897687, 0.913982, 0.930572, 0.947464, 0.964662, 0.982172, 1.000000, 1.018152, 1.036633,
                        1.055450, 1.074608, 1.094114, 1.113974, 1.134194, 1.154782, 1.175743, 1.197085, 1.218814, 1.240938, 1.263463, 1.286397, 1.309747,
                        1.333521, 1.357727, 1.382372, 1.407465, 1.433013, 1.459024, 1.485508, 1.512473, 1.539927, 1.567879, 1.596339, 1.625315, 1.654817,
                        1.684855, 1.715438, 1.746576, 1.778279, 1.810558, 1.843423, 1.876884, 1.910953, 1.945640, 1.980957, 2.016915, 2.053525, 2.090800,
                        2.128752, 2.167392, 2.206734, 2.246790, 2.287573, 2.329097, 2.371374, 2.414418, 2.458244, 2.502865, 2.548297, 2.594553, 2.641648,
                        2.689599, 2.738420, 2.788127, 2.838736, 2.890264, 2.942727, 2.996143, 3.050528, 3.105900, 3.162278, 3.219678, 3.278121, 3.337625,
                        3.398208, 3.459892, 3.522695, 3.586638, 3.651741, 3.718027, 3.785515, 3.854229, 3.924190, 3.995421, 4.067944, 4.141785, 4.216965,
                        4.293510, 4.371445, 4.450794, 4.531584, 4.613840, 4.697589, 4.782858, 4.869675, 4.958068, 5.048066, 5.139697, 5.232991, 5.327979,
                        5.424691, 5.523158, 5.623413, 5.725488, 5.829415, 5.935229, 6.042964, 6.152654, 6.264335, 6.378044, 6.493816, 6.611690, 6.731704,
                        6.853896, 6.978306, 7.104974, 7.233942, 7.365250, 7.498942, 7.635061, 7.773650, 7.914755, 8.058422, 8.204696, 8.353625, 8.505258,
                        8.659643, 8.816831, 8.976871, 9.139817, 9.305720, 9.474635, 9.646616, 9.821719, 10.000000]
                cols = 'd.[AtomID], d.[Time], d.[Size] '
                join = ', '
        else:
                numbins = int(raw_input('\nEnter total number of size bins: '))
                bins = [0] + [float(raw_input('Enter upper bound for size bin ' + str(x+1) + ': ')) for x in range(0,numbins)]
                cols = 'd.[AtomID], d.[Time], d.[Size] '
                join = ', '
else:
	lpeak = int(raw_input('\nEnter lower bound for peak range: '))
	upeak = int(raw_input('Enter upper bound for peak range: '))
	bins = range(lpeak, upeak+1)
	cols = 'd.[AtomID], d.[Time], d.[Size], s.[PeakHeight], s.[PeakLocation], s.[PeakArea], s.[RelPeakArea] '
	join = ''' JOIN (
				SELECT *
				FROM ATOFMSAtomInfoSparse)
				AS s ON d.[AtomID] = s.[AtomID], '''

for i in range(len(collids)):
	cid = collids[i]
	cn = collnames[i]
	
	select = ''
	labels = ['Date','StartTime']
		
	if qtype == 'S':
		for j in range(len(bins)-1):
			select += 'SUM(CAST((CASE WHEN [Size] BETWEEN ' + str(bins[j]) + ' AND ' + str(bins[j+1]) +\
						' THEN 1 ELSE 0 END) AS FLOAT)) AS [bin' + str(j+1) + '], '
			labels.append(str(float(bins[j])) + '-' + str(float(bins[j+1])))
	
	elif qtype == 'C':
		for j in bins:
			select += 'SUM(CAST((CASE WHEN [PeakLocation] = ' + str(j) +\
						' THEN 1 ELSE 0 END) AS FLOAT)) AS [bin' + str(j) + '], '
			labels.append(str(j))
			
	else:
		for j in bins:
			select += 'SUM(CAST(' + qt + ' * (CASE WHEN [PeakLocation] = ' + str(j) +\
						' THEN 1 ELSE 0 END) AS FLOAT)) AS [bin' + str(j) + '], '
			labels.append(str(j))
			
	select = select.rstrip(', ')

	datacsr = cnxn.cursor()
	
	timer = time.clock()

	print('\nProcessing collection ' + str(i+1) + '... '),

	query = '''		
		SELECT
			CAST(strftime("%Y", Time) AS integer) as y,
			CAST(strftime("%m", Time) AS integer) as m,
			CAST(strftime("%d", Time) AS integer) as d,
			CAST(strftime("%H", Time) AS INTEGER)''' + hf + ''' AS h,
			CAST(strftime("%M", Time) AS INTEGER)''' + mf + ''' AS mi,
			CAST(strftime("%S", Time) AS INTEGER)''' + sf + ''' AS s, 
			''' + select + ''' 
			FROM (
				SELECT ''' + cols + '''
				FROM ATOFMSAtomInfoDense AS d''' + join + '''
					InternalAtomOrder AS i
				WHERE i.[CollectionID] = ''' + str(cid) + ''' AND d.[AtomID] = i.[AtomID]
				AND d.[Time] BETWEEN \'''' + ltime + '\' AND \'' + utime + '''\'
			) AS data
		
		GROUP BY y, m, d, h, mi, s  
		ORDER BY y, m, d, h, mi, s
		'''

	datacsr.execute(query)

	data = numpy.array(datacsr.fetchall())

	datetimes = []
	for k in range(len(data[:,0])):
		yyyy = int(data[k,0])
		mm = int(data[k,1])
		dd = int(data[k,2])
		hh = int(data[k,3]*hl)
		mi = int(data[k,4]*ml)
		ss = int(data[k,5]*sl)
		datetimes.append(datetime.datetime(yyyy,mm,dd,hh,mi,ss))
	
	print '\n\nSQL querying took: ' + str(time.clock()-timer) + ' seconds'
	
	timer = time.clock()
	
	l = 0
	
	zrow = [0 for x in data[0,:]]
	
	while (datetimes[l] < datetimes[-1]):
		delta = datetimes[l+1] - datetimes[l]
		if (delta.total_seconds() != timeres):
			thistime = datetimes[l] + datetime.timedelta(seconds=timeres)
			datetimes.insert(l+1, thistime)
			data = numpy.insert(data, l+1, zrow, axis=0)
		l += 1
	
	datelabels = []
	timelabels = []
	for x in datetimes:
		datelabels.append(x.isoformat().split('T')[0])
		timelabels.append(x.isoformat().split('T')[1])
	
	table = numpy.empty(shape=(len(timelabels)+1,len(labels)), dtype='a24')
	table[0,:] = labels
	table[1:,0] = datelabels
	table[1:,1] = timelabels
	table[1:,2:] = data[:,6:]

	numpy.savetxt('histogram_' + cn + '_' + qtype + '.csv', table, fmt='%s', delimiter=',')

	print 'Post-processing took: ' + str(time.clock()-timer) + ' seconds'
	print '\nCSV saved as: ' + 'histogram_' + cn + '_' + qtype + '.csv'

cnxn.close()
