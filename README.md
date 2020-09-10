# EVA-denovo

# How to run the jar

```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 3 eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 --conf spark.yarn.appMasterEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.yarn.appMasterEnv.SPARK_HOME=/mydata/spark --conf spark.executorEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.executorEnv.SPARK_HOME=/mydata/spark eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```
