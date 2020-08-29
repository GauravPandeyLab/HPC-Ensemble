from multiprocessing import Pool
from subprocess import call
import datetime
import sys


def phaseIFeatureEngineering(arg):
    print(datetime.datetime.now())
    submitCommand = "spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIFeatureEngineering --properties-file new-gc.conf --master yarn --deploy-mode cluster --num-executors 2 --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar DataSink_OpenSource_2.11-1.0.jar %s" % arg
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIBaseModeling(arg):
    model, inner, outer = arg
    print(datetime.datetime.now())
    submitCommand= "spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIModelingBase --properties-file new-gc.conf --master yarn --deploy-mode cluster --num-executors 2 --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar DataSink_OpenSource_2.11-1.0.jar %s %d %d" %(model ,inner, outer)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIOuterModeling(arg):
    model, inner, outer = arg
    print(datetime.datetime.now())
    submitCommand= "spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIModelingOuter --properties-file new-gc.conf --master yarn --deploy-mode cluster --num-executors 2 --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar DataSink_OpenSource_2.11-1.0.jar %s %d %s" %(model ,inner, outer)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIIICombination(arg):
    iteration = arg
    print(datetime.datetime.now())
    submitCommand= "spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class PhaseIIICombination --properties-file new-gc.conf --master yarn --deploy-mode cluster --num-executors 2 --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar DataSink_OpenSource_2.11-1.0.jar %d" %(iteration)
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

def phaseIVEvaluation(arg):
    iteration = arg
    print(datetime.datetime.now())
    submitCommand= "spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class phaseIVEvaluation --properties-file new-gc.conf --master yarn --deploy-mode cluster --num-executors 2 --packages com.databricks:spark-csv_2.11:1.5.0 --jars spark/spark-2.2.0-bin-hadoop2.7/lib/config-1.3.1.jar,spark-mllib_2.10-2.2.0.jar DataSink_OpenSource_2.11-1.0.jar"
    submitCommandArray=submitCommand.split(" ")
    print(call(submitCommandArray))
    print(datetime.datetime.now())

if __name__ == '__main__':
    # Phase I Data Preparation, Feature Engineering
    #takes csv file name as argument
    p1 = Pool(5)
    p1.map(phaseIFeatureEngineering, ["pima-indians-diabetes"])

    # # Phase II Base level Prediction
    # takes (inner iteration, outer iteration and model) name as parameter
    p2 = Pool(40)
    innerIteration = range(0,10)
    outerIteration = range(0,10)
    model = ["randomForest", "decisiontree", "svm", "logistic", "naivebayes"]
    parameter = [ [i,j,k]  for i in model for j in innerIteration for k in outerIteration]
    p2.map(phaseIIBaseModeling, parameter)


    # Phase II Outer level Prediction
    p2X = Pool(5)
    outerIterationX = ["Outer"]
    modelX = ["randomForest", "decisiontree", "svm", "logistic", "naivebayes"]
    innerIterationX = range(0, 10)
    parameterX = [ [i,j,k]  for i in modelX for j in innerIterationX for k in outerIterationX]
    p2X.map(phaseIIOuterModeling, parameterX)

    # Phase III Combination
    p3 = Pool(5)
    parameter3 = range(0,10)
    p3.map(phaseIIICombination, parameter3)

    # Phase IV Evaluation
    p4 = Pool(5)
    p4.map(phaseIVEvaluation, 0)


