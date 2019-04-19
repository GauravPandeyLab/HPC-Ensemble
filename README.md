# Spark

	cd spark/

## Setup
HDFS configurations are saved in new-gc.conf. Jars include the Machine Learning libraries and also the pipeline itself (by Prem).  
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

### Process results
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

### Process results
To get disk usage:

	python minerva_disk.py [data_path] [data_name]

To get memory usage and computational time:
	
	python minerva_memory_time.py [stdout_path] [arff_path] [data_name]

# Analysis
1. Computational time 
	i. Use notebook visualize_time.ipynb to analyze computaitonal time.
	ii. Minerva time usage is saved in [GDrive]/final_results/minerva-all-usage.csv.
	iii. PDF for all time usage is saved in [GDrive]/final_results/computational_time.pdf
	iv. PNGs for individual data time usage are saved in [GDrive]/final_results/individual_data_time/ folder.

2. Disk usage 
	i. Minerva disk usage is manually calculated using linux du -hs command.
	ii. Spark disk usage is calculated using 'hdfs dfs -ls -R' command.
	iii. Jupyter notebook disk_usage.ipynb is used to generate the barplot.
	iv. Plot saved as [GDrive]/final_results/disk_usage_barplot.png. 

3. Memory usage 
	i. average memory plot.ipynb plot the average memory of both platforms.
	ii. For Minerva, raw data is saved in final_results/minerva-all-usage.csv, unit is MB. Plot is saved as [GDrive]/final_results/minerva_avemem.pdf.
	iii. For Spark, raw data is saved in Spark/demeter_spark_comprehensive_stats_all_data.csv, unit is MB. Plot is saved as [GDrive]/final_results/spark_avemem.png.

4. Classifiers
	i. For Minerva, classifiers and parameters are saved in classifiers.txt.
	ii. Spark classifiers are corresponding classifiers from the mllib.
