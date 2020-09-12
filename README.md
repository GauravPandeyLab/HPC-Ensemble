Below, we provide code and instructions for running our heterogeneous ensemble frameworks on Hadoop and traditional high-performance computing platforms, as well as evaluating their computational performance. 

# Run ensemble models on Hadoop

This code was developed for our local Demeter Hadoop cluster. Users are recommended to adapt the code for their own systems.

Start with:

	cd Process/Demeter

### Setup
HDFS configurations are saved in new-gc.conf. Jars include the Machine Learning libraries and also the Hadoop ensemble pipeline itself. Scripts implementing the  Datasink framework for developing heterogeneous ensemble models are available in the Process/Demeter/Datasink_Hadoop folder. 

### Download JAVA8 to current folder
Run:

	sh set_java_path.sh

### To run the experiments in our study
An example dataset for testing the code is available as Process/ExampleData/pf1.csv. Other datasets used in our study are available upon request.

Place data on hdfs:

	hdfs fs -put [DATA.csv] [HDFS_PATH]
	
To run the Hadoop DataSink version, implemented in Spark, with the specified number of cores:

	mprof run --include-children python sparkSubmitYarn.py [#cores] [data_name] 

### To generate computational performance statistics
The above command will generate the mprof file, which contains the memory usage per second. Elapsed time and CPU time can be obtained from this file.
Elapsed time will also be generated via python time package and written into the TimeCal folder. 

To get disk usage:
	
	sh get_disk_usage.sh

# Run ensembles on a traditional high-performance computing (HPC) system

Again, this code was developed for our local Minerva HPC system (https://labs.icahn.mssm.edu/minervalab/). Users are recommended to adapt the code for their own systems.

Start with:

	cd Process/Minerva/

### Setup
Install LargeGOPred (https://github.com/linhuawang/LargeGOPred), which is an implementation of the DataSink framework designed to run on large-scale traditional HPC systems like Minerva.

### To run experiments in our study
Follow the instructions of LargeGOPred to run it for the dataset under consideration. An example dataset for testing the code is available as Process/ExampleData/pf1.csv. Other datasets used in our study are available upon request.

### To generate computational performance statistics

Run the following commands to get the basic computational performance statistics.

To get disk usage:

	python minerva_disk.py [data_path] [data_name]

To get memory usage and computational time:
	
	python minerva_memory_time.py [stdout_path] [arff_path] [data_name]


# Computational performance analysis and result visualization code

The following code can be used to process the basic computational performance statistics calculated for Hadoop and traditional HPC systems, and generate the results and figures included in our paper.

Start with:

	cd Analysis/

1. Computational time 

	i. Use notebook "Computational time.ipynb" to analyze computational time.  
	ii. Minerva time usage is saved in: Minerva_results/minerva-all-usage.csv.  
	iii. PDFs for all time usage is saved in Minerva_results/individual_data_time.  
	iv. PDFs for figure in the paper is saved at paper_figures/Figure_2a/b.pdf.  

2. Disk usage 

	i. Minerva disk usage is manually calculated using linux du -hs command.  
	ii. Demeter disk usage is calculated using 'hdfs dfs -ls -R' command.  
	iii. Jupyter notebook "Disk usage.ipynb" is used to generate the barplot.  
	iv. Plot saved in paper_figures/Figure3_disk_usage.pdf.   

3. Memory usage 

	i. For Minerva, raw data is saved in Minerva_results/minerva-all-usage.csv, unit is MB. Plot is saved as paper_results/Figure_4a_Minerva_memory.pdf.  
	ii. For Demeter, raw data is saved in Demeter_results/demeter_spark_comprehensive_stats_all_data.  csv, unit is MB. Plot is saved as paper_results/Figure_4b_Demeter_memory.pdf.  
	
Sample result files for the example dataset, as well as the corresponding result figures included in our paper, are available in the Analysis directory.

# Contact

Please submit any issues with the code to Prem Timsina (prem.timsina@mssm.edu), Linhua Wang (linhuaw15213@gmail.com) and Gaurav Pandey (gaurav.pandey@mssm.edu).

# Reference

Forthcoming.
