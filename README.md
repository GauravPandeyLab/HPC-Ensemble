# Run ensemble models on Demeter Hadoop

	cd Process/Demeter

## Setup
HDFS configurations are saved in new-gc.conf. Jars include the Machine Learning libraries and also the pipeline itself (by Prem). Implementation scripts of Datasink (heterogeneous ensemble models) are in Process/Demeter/Datasink_Hadoop folder. Example data is provided at Process/ExampleData/. Other data will be provided upon request.

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


# Run ensembles on Minerva

	cd Process/Minerva/

## Setup
Install LargeGOPred (https://github.com/linhuawang/LargeGOPred).

### Run experiments
Run LargeGOPred for all data, core, round of experiments.

### Process results
To get disk usage:

	python minerva_disk.py [data_path] [data_name]

To get memory usage and computational time:
	
	python minerva_memory_time.py [stdout_path] [arff_path] [data_name]

# Example data

The example data for both platforms are saved in Process/ExampleData folder.	

1. File pf1.csv is an example input for our heterogeneous ensemble models on Demeter. 	

2. File pf1.arff is an example input for LargeGOPred, which is Minerva platform based. 	

# Analysis

	cd Analysis/

1. Computational time 

	i. Use notebook "Computational time.ipynb" to analyze computaitonal time.  
	ii. Minerva time usage is saved in: Minerva_results/minerva-all-usage.csv.  
	iii. PNGs for all time usage is saved in Minerva_results/individual_data_time.  
	iv. PDFs for figure in the paper is saved at paper_figures/Figure_2a/b.pdf.  

2. Disk usage 

	i. Minerva disk usage is manually calculated using linux du -hs command.  
	ii. Demeter disk usage is calculated using 'hdfs dfs -ls -R' command.  
	iii. Jupyter notebook "Disk usage.ipynb" is used to generate the barplot.  
	iv. Plot saved in paper_figures/Figure3_disk_usage.pdf.   

3. Memory usage 

	i. For Minerva, raw data is saved in Minerva_results/minerva-all-usage.csv, unit is MB. Plot is saved as paper_results/Figure_4a_Minerva_memory.pdf.  
	ii. For Demeter, raw data is saved in Demeter_results/demeter_spark_comprehensive_stats_all_data.  csv, unit is MB. Plot is saved as paper_results/Figure_4b_Demeter_memory.pdf.  

4. Classifiers

	i. For Minerva, classifiers and parameters are saved in classifiers.txt.  
	ii. Spark classifiers are corresponding classifiers from the mllib.  
