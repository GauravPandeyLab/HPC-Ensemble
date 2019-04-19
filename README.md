# Spark

	cd spark/

## Setup
HDFS configurations are saved in new-gc.conf. Jars include the Machine Learning libraries and also the pipeline itself (by prem).  
### Download JAVA8 to current folder.
Run:
	sh set_java_path.sh

### Run experiments
Place data to hdfs:

	hdfs fs -put [DATA.csv] [HDFS_PATH]
	
To run spark on HDFS for data with specified number of cores:

	mprof run --include-children python sparkSubmitYarn.py [core] [data_name] 

This will generate the mprof file which contains the memory usage per second. Elapsed time and CPU time can be obtained from mprof files.
Elapsed time will also be generated via python time package and written into TimeCal folder. 

### Analysis
To get disk usage:
	
	sh get_disk_usage.sh

To check if job finished for data, core, date:

	python checkResults.py

and answer the questions as poped up in the terminal.


# Minerva

	cd minerva/

## Setup
Install LargeGOPred (https://github.com/linhuawang/LargeGOPred).

### Run experiments
Run LargeGOPred for all data, core, round of experiments.

### Analysis
To get disk usage:

	python minerva_disk.py [data_path] [data_name]

To get memory usage and computational time:
	
	python minerva_memory_time.py [stdout_path] [arff_path] [data_name]


