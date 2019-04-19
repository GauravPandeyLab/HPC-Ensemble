'''
	This script is used to calculate the file size of individual intermediate files.
	Outputs are used to evaluate the IO usage of the traditional HPC on Minerva platform.
	@author: Linhua Wang
'''

from sys import argv
from os.path import dirname, exists, abspath, getsize
from glob import glob
from pandas import DataFrame as DF
import pandas as pd
method = "generate"
path = argv[1] # Path to the folder that have the .arff file.
data = argv[2] # Name of the data.
iofile = "%s-interm-files.txt" %data
files = glob("%s/*.gz" %path)
dfs = []

### Intermediate files in data folder.
for f in files:
	fn = f.split("/")[-1]
	method = fn.split("-")[0]
	outerfold = fn.split(".")[0].split("-")[1]
	inner = "NA"
        print f, getsize(f)
	df = DF({"filename":fn,"classifier":"integrate","method":method,"outerfold":outerfold,"innerfold":inner,"bagcount":"NA","filesize": getsize(f)},index = [0])
	dfs.append(df)


### Intermediate files in child folders.
dirs = glob("%s/weka.classifiers*" %path)
for direct in dirs:
	subpath = abspath(direct)
	fs = glob("%s/*.gz" %subpath)
	for f in fs:
		fn = f.split("/")[-1]
		classifier = f.split("/")[-2].split("weka.classifiers.")[1].split(".")[1]
		if classifier == "SGD":
			classifier = "logistic regression"
		elif classifier == "SMO":
			classifier = "svm"
		elif classifier == "J48":
			classifier = "decision tree"

		method = fn.split("-")[0]
		outerfold = fn.split(".")[0].split("-")[1]
		innerfold = fn.split(".")[0].split("-")[2]
		if method == "validation":
			bagcount = fn.split(".")[0].split("-")[3]
		else:
			bagcount = "NA"
		df = DF({"filename":fn,"classifier":classifier,"method":method,"outerfold":outerfold,"innerfold":innerfold,"bagcount":bagcount,"filesize": getsize(f)},index = [0])
		dfs.append(df)
### Normalize and integrate.
heads = ["filename","classifier","method","outerfold","innerfold","bagcount","filesize"]
dfs = pd.concat(dfs)
dfs = dfs[heads]
dfs.to_csv(iofile,index = False,sep = "\t")
