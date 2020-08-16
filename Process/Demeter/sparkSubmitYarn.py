from multiprocessing import Pool
from subprocess import call
import datetime
import sys
import time
#from datetime import datetime
core = int(sys.argv[1])
data_name =sys.argv[2]
name = str(data_name)+"_"+str(core)+"_"+str(sys.argv[3])
exe=1
parr=core/(1*exe)
def phaseIFeatureEngineering(arg):
    print(datetime.datetime.now())
    submitCommand = "/hpc/users/wangl35/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIFeatureEngineering --queue %s --properties-file /hpc/users/wangl35/new-gc.conf --master yarn --deploy-mode cluster --num-executors %d --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar phasei_featurevector_creator_2.11-1.0.jar %s %s" %(name,exe,arg,name)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIBaseModeling(arg):
    model, inner, outer = arg
    print(datetime.datetime.now())
    submitCommand= "/hpc/users/wangl35/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIModelingBase --queue %s --properties-file /hpc/users/wangl35/new-gc.conf --master yarn --deploy-mode cluster --num-executors %d --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar phasei_featurevector_creator_2.11-1.0.jar %s %d %d %s" %(name,exe,model,inner,outer,name)
    submitCommandArray=submitCommand.split(" ")
    #call(submitCommandArray)
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIOuterModeling(arg):
    model, inner, outer = arg
    print(datetime.datetime.now())
    submitCommand= "/hpc/users/wangl35/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIModelingOuter --queue %s --properties-file /hpc/users/wangl35/new-gc.conf --master yarn --deploy-mode cluster --num-executors %d --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar phasei_featurevector_creator_2.11-1.0.jar %s %d %s %s" %(name,exe,model ,inner, outer,name)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIICombination(arg):
    iteration = arg
    print(datetime.datetime.now())
    submitCommand= "/hpc/users/wangl35/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIICombination --queue %s --properties-file /hpc/users/wangl35/new-gc.conf --master yarn --deploy-mode cluster --num-executors %d --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar phasei_featurevector_creator_2.11-1.0.jar %d %s %s" %(name,exe,iteration, name,data_name)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())
'''
def phaseIVEvaluation(arg):
    iteration = arg
    print(datetime.datetime.now())
    submitCommand= "/hpc/users/wangl35/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIVEvaluation --queue %s --properties-file /demeter/users/willir31/new-gc.conf --master yarn --deploy-mode cluster --num-executors %d --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar phasei_featurevector_creator_2.11-1.0.jar"%(name,exe)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())
'''

if __name__ == '__main__':
     
    
    # Phase I Data Preparation, Feature Engineering
    fh = open('TimeCalc/'+name,"a")
    fh.write("started  running " + name + ","+str(time.time()) + ",Phase I," +str(time.ctime())+ ",")
    fh.close()
    p1 = Pool(parr)
    p1.map(phaseIFeatureEngineering, [data_name])
    

    #Checking 1
    itrCount =0
    while(itrCount < 4):
        p1t = Pool(parr)
        import os
        path= 'parquetDatabase/'+name
        #print path
        if (call(["hadoop", "fs", "-test", "-d", path])==1):
		p1t.map(phaseIFeatureEngineering, [data_name])
	else:
		break
	itrCount=itrCount+1

    
    # Phase II Base level Prediction
    #model 1
    innerIteration = [0,1,2,3,4]
    outerIteration = [0,1,2,3,4]
    p21 = Pool(parr)
    
    model21 = ["randomForest", "decisiontree", "svm", "logistic", "naivebayes" ]
    parameter21 = [ [i,j,k]  for i in model21 for j in innerIteration for k in outerIteration]
    p21.map(phaseIIBaseModeling, parameter21)
    #p21.map(phaseIIBaseModeling, [["svm",0,0]])
    p21.close()

    fh = open('TimeCalc/'+name,"a")
    fh.write(str(time.time()) + ","+str(time.ctime())+ ",")
    fh.close()
    
   #Checking 1
    
    itrCount =0
    paramList = parameter21
    while(itrCount <6):
    	l=list()
    	p21t = Pool(parr)
    	import os
    	for item in paramList:
        	path= 'parquetDatabase/'+name+'/Result/'+str(item[2])+str(item[0])+str(item[1])+'.parquet'
		#print path
        	if (call(["hadoop", "fs", "-test", "-d", path])==1):
            		l.append(item)
    	if (len(l) ==0):
		break
	paramList=l
	p21t.map(phaseIIBaseModeling, l)
    	print l
	
    	p21t.close()
        itrCount = itrCount+1
    
    
    # Phase II Outer level Prediction

    fh = open('TimeCalc/'+name,"a")
    fh.write(str(time.time()) + ","+str(time.ctime())+ ",")
    fh.close()


    p2X = Pool(parr)
    outerIterationX = ["outer"]
    modelX = ["randomForest", "decisiontree", "svm", "logistic", "naivebayes"]
    innerIterationX = range(0, 5)
    parameterX = [ [i,j,k]  for i in modelX for j in innerIterationX for k in outerIterationX]
    p2X.map(phaseIIOuterModeling, parameterX)
    
    fh = open('TimeCalc/'+name,"a")
    fh.write(str(time.time()) + ","+str(time.ctime())+ ",")
    fh.close()
    
    
    #Checking 1
    itrCount =0
    paramList1 = parameterX
    while(itrCount <6):
    	n=list()
    	p21t3 = Pool(parr)
    	import os
    	for item in paramList1:
        	path= 'parquetDatabase/'+name+'/'+str(item[0])+str(item[2])+str(item[1])+'.parquet'
        	if (call(["hadoop", "fs", "-test", "-d", path])==1):
            		n.append(item)
    	if(len(n) ==0):
		break
	paramList1=n
	p21t3.map(phaseIIOuterModeling, n)
    	p21t3.close()
	itrCount = itrCount+1

    #Checking 2
  
    
    #Phase III Combination
    fh = open('TimeCalc/'+name,"a")
    fh.write(str(time.time()) + ","+str(time.ctime())+ ",")
    fh.close()

    p3 = Pool(parr)
    parameter3 = [0,1,2,3,4]
    p3.map(phaseIIICombination, parameter3)
    fh = open('TimeCalc/'+name,"a")
    fh.write(str(time.time()) + ","+str(time.ctime()))
    fh.close()
    
  

    """
    #Phase IV Evaluation
    p4 = Pool(5)
    p4.map(phaseIVEvaluation, [0])
    """ 
