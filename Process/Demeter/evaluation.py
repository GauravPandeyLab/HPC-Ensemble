from pyspark.sql import SparkSession
from sklearn import metrics
import numpy as np
import pandas as pd
from sys import argv

def getFinalResults(dp):
    spark = SparkSession.builder.appName("appName").master("local[2]").getOrCreate()
    finalResults = []
    for i in range(5):
        logData = spark.read.parquet("%s/finalResult%d.parquet" %(dp,i)).select("SN","label", "rawPrediction", "probability", "prediction")
#         logData.show(20,False)
        fri = logData.toPandas()
        finalResults.append(fri)
    finalResults = pd.concat(finalResults)
    return finalResults

def fmax_score(labels, pos_probs, beta = 1.0, pos_label = 1):
    """
        Radivojac, P. et al. (2013). A Large-Scale Evaluation of Computational Protein Function Prediction. Nature Methods, 10(3), 221-227.
        Manning, C. D. et al. (2008). Evaluation in Information Retrieval. In Introduction to Information Retrieval. Cambridge University Press.
    """
    precision, recall, _ = metrics.precision_recall_curve(labels, pos_probs, pos_label)
    f1 = (1 + beta**2) * (precision * recall) / ((beta**2 * precision) + recall)
    return np.nanmax(f1)


def auc_score(labels,pos_probs):
    fpr, tpr, thresholds = metrics.roc_curve(labels, pos_probs, pos_label=1)
    auc = metrics.auc(fpr, tpr)
    return auc


# fp = '/Users/wangl35/Desktop/spark-minerva/spark/parquetDatabase/pf1/pf1_1_0208-1001'
fp = argv[1]
prediction_df = getFinalResults(fp)

pos_probs = np.array(prediction_df['probability'].tolist())[:,1]
labels = np.array(prediction_df['label'].tolist())

fmax = fmax_score(labels,pos_probs)
auc = auc_score(labels,pos_probs)
print len(pos_probs)
print 'fmax: %.3f; auc: %.3f' %(fmax,auc)
