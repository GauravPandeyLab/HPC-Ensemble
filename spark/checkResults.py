'''
	Check if job finished for specific data and core on some day.
	@author: Linhua Wang
'''

from subprocess import call
from sys import argv

dn = str(raw_input('Enter the data name: '))
core = str(raw_input('Enter #cores requested: '))
date = str(raw_input('Enter date as MMDD on which job was run: '))
print "Checking Results...... "
for run in range(1,11):
	run = '%02d' %(run,)
	folder = dn+'_'+core+'_'+date+'-10'+run
	b0 = call(['hadoop','fs','-test','-d','parquetDatabase/%s/finalResult0.parquet' %folder])==0
	b1 = call(['hadoop','fs','-test','-d','parquetDatabase/%s/finalResult1.parquet' %folder])==0
	b2 = call(['hadoop','fs','-test','-d','parquetDatabase/%s/finalResult2.parquet' %folder])==0
	b3 = call(['hadoop','fs','-test','-d','parquetDatabase/%s/finalResult3.parquet' %folder])==0
	b4 = call(['hadoop','fs','-test','-d','parquetDatabase/%s/finalResult4.parquet' %folder])==0

	flag =  (b0 and b1 and b2 and b3 and b4)
	print run, flag



