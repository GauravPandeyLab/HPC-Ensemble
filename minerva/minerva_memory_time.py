'''
Get disk usage, memory usage, cpu time and duration etc. from stdoutput of completed jobs.
Crashed jobs recorded into data-error.txt, and resubmit via resubmit.py.
Successful jobs info recorded into data-usage.txt.
@author: Linhua Wang
'''
from os.path import abspath,getsize,exists
from sys import argv
import pandas as pd
from os import remove

def getduration(start,end):
    s = start.split()
    e = end.split()
    duration = 0
    if e[4] == s[3]:
        stime = s[5].split(":")
        etime = e[6].split(":")
        duration = int(etime[2]) - int(stime[2]) + (int(etime[1]) - int(stime[1])) * 60 + (int(etime[0]) - int(stime[0])) * 3600 + (int(e[5]) - int(s[4])) * 24 * 3600
    else:
        print "Cross month, do it manually use bhist instead!"
    return duration
'''
### Helper function to check if job finished successfully.
def checkfiles(data,fold,node):
    pjdir = abspath('/sc/orga/work/wangl35/projects/datasink/data/%s/r%d/n%d/' %(data,fold,node))
    flag = True
    for i in range(0,5):
        flag = flag and exists('%s/predictions-%d.csv.gz' %(pjdir,i)) and exists('%s/validation-%d.csv.gz' %(pjdir,i))
        for classifier in ['bayes.NaiveBayes','trees.RandomForest','functions.Logistic','functions.SMO','trees.J48']:
            subdir = abspath('%s/weka.classifiers.%s/' %(pjdir,classifier))
            for outer in range(0,5):
                for bagging in range(0,10):
                    flag = flag and exists('%s/predictions-%d-%02d.csv.gz' %(subdir,outer,bagging))
                    for inner in range(0,5):
                        flag = flag and exists('%s/validation-%d-%02d-%02d.csv.gz' %(subdir,outer,inner,bagging))
    return flag
'''
def resourceUsage(data,outpath,fold,node):
    file = open(abspath("%s/r%s/n%s.stdout" %(outpath,fold,node)),"r")
    cpu = ""
    maxmem = ""
    avemem = ""
    flag = 0
    start = ""
    end = ""
    for line in file:
        if len(line.split()) > 0 and line.split()[0] == "Successfully":
            flag = 1
        elif len(line.split()) > 0 and line.split()[0] == "Started":
            start = line
        elif len(line.split()) > 0 and line.split()[0] == "Results":
            end = line
        else:
            if len(line.split(":")) > 0 and len(line.split(":")[0].split()) > 0:
                fw = line.split(":")[0].split()[0]
                if fw == "CPU":
                    cpu = line.split(":")[1].split()[0]
                elif fw == "Max" and line.split(":")[0].split()[1] == "Memory":
                    maxmem = " ".join([line.split(":")[1].split()[0],line.split(":")[1].split()[1]])
                elif fw == "Average" and line.split(":")[0].split()[1] == "Memory":
                    avemem = " ".join([line.split(":")[1].split()[0],line.split(":")[1].split()[1]])
    duration = getduration(start,end)
    return flag,duration,cpu,maxmem,avemem

def noncpu(cpu,node,duration):
    s = [float(t) for t in [cpu,node,duration]]
    return (s[1] + 1)* s[2] - s[0]

print 'care, this is only for node 24'
dfs = []
efs = []

stdout_path = argv[1] # path to the standard output files
data_path = argv[2] # path to the .arff file
data = argv[3] # data name

stdout_path = abspath(stdout_path)
### Raw data has the same size, thus just calculate it for one.
size = getsize(data_path)
### Get the usage from individual folder.
for fold in range(1,11):
	try:
	        flag,duration,cpu,maxmem,avemem = resourceUsage(data,stdout_path,fold,node)
        	if flag == 0:
           		efs.append(pd.DataFrame({"data":data,"fold": fold, "cores": node},index = [0]))
        	else:
		    	df = pd.DataFrame({"data":data,"size":size,"fold":fold,"cores":node,"cpu":cpu,"duration":duration,"maxmem":maxmem,"avemem":avemem,"noncpu":noncpu(cpu,node,duration)},index = [0])
        	    	dfs.append(df)
	except:
		print 'Run %s not completed!' %fold
dfs = pd.concat(dfs,ignore_index = True)

if exists("%s-error.txt" %data):
    remove("%s-error.txt" %data)

if len(efs) > 0:
    efs = pd.concat(efs,ignore_index = True)
    efs.to_csv("%s-error.txt" %data, sep = "\t", index = False)
    print "There are errors!"

print "error count: %d" %len(efs)
dfs = dfs[['data','size','fold','cores','duration','cpu','noncpu','avemem','maxmem']]
dfs.to_csv("%s-usage-24.txt" %data,index=False)
