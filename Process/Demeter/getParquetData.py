from os import system
from sys import argv
#from os.path import exists
from os import mkdir

data = argv[1] 
mkdir(data)
f = open('succeeded_queues.txt')
for line in f:
	if line.split('_')[0] == data:
		queues = line.split('\n')[0]
		system('hadoop fs -get parquetDatabase/%s %s' %(queues,data))
		



